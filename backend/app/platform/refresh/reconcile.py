"""Reconcile active keyed refresh runs from durable facts.

One pass answers, for every active run carrying an ``execution_key`` with no
live Procrastinate delivery, what its durable rows say happened:

1. A ``dataset_versions`` row keyed to its job (the publish fact) forces the
   run to ``succeeded`` — the commit landed even if its acknowledgement or
   the worker did not.
2. A ``complete`` job (the no-change postgis/stac doors) likewise succeeds it.
3. A ``cancelled`` job is a cancellation backstop.
4. A ``running`` job is left alone; only the heartbeat stale pass may judge a
   live worker dead.
5. A ``failed`` job with deliveries left re-drives the SAME run and job under
   a fresh ``execution_key``/``attempt_id``; an exhausted run fails.
6. A ``pending`` job is re-sent with its current key when its deferral never
   landed (or was lost past the grace), re-driven after its claim deadline,
   failed once deliveries are exhausted, or left to wait.

The module never commits and never defers: callers own the transaction and
hand the returned :class:`Redelivery` entries to the overlay lifecycle, which
actually enqueues ``scheduled-refresh-v1``.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.platform.jobs import ledger
from app.platform.jobs.ledger import StaleIngestAttempt
from app.platform.refresh.service import (
    ABANDONED_ERROR_CODE,
    ABANDONED_ERROR_MESSAGE,
    ADMITTED_CLAIM_EXPIRED_ERROR_CODE,
    ADMITTED_CLAIM_EXPIRED_ERROR_MESSAGE,
    ADMITTED_EXPIRY_BATCH_SIZE,
    REDRIVE_DEFER_GRACE_SECONDS,
    SCHEDULED_CLAIM_DEADLINE_SECONDS,
    SCHEDULED_MAX_DELIVERIES,
)

logger = structlog.get_logger(__name__)


class ReconcileAction(StrEnum):
    PUBLISH_SUCCEEDED = "publish_succeeded"
    COMPLETED = "completed"
    REDRIVE = "redrive"
    RESEND = "resend"
    TERMINAL_FAIL = "terminal_fail"
    TERMINAL_CANCEL = "terminal_cancel"
    LEAVE = "leave"


@dataclass(frozen=True, slots=True)
class Redelivery:
    """One (re)delivery the overlay must enqueue for a reconciled run."""

    run_id: uuid.UUID
    job_id: uuid.UUID
    execution_key: str
    tenant_id: uuid.UUID | None
    attempts: int


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    redeliveries: tuple[Redelivery, ...] = ()
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0
    touched_run_ids: tuple[uuid.UUID, ...] = field(default_factory=tuple)

    @property
    def changed(self) -> bool:
        return bool(
            self.redeliveries or self.succeeded or self.failed or self.cancelled
        )


_CANDIDATE_SQL = text(
    """
    SELECT r.id AS run_id, r.ingest_job_id AS job_id, r.execution_key,
           r.attempts, r.phase, r.claim_deadline, r.last_dispatched_at,
           r.tenant_id, r.dataset_id,
           j.status AS job_status, j.error_code AS job_error_code,
           j.error_message AS job_error_message, j.attempt_id AS job_attempt_id
    FROM catalog.dataset_refresh_runs r
    JOIN catalog.ingest_jobs j ON j.id = r.ingest_job_id
    WHERE r.execution_key IS NOT NULL
      AND r.status IN ('pending', 'running')
      AND EXISTS (SELECT 1 FROM catalog.datasets d WHERE d.id = r.dataset_id)
      AND NOT EXISTS (
          SELECT 1 FROM catalog.procrastinate_jobs pj
          WHERE pj.args->>'job_id' = j.id::text
            AND pj.status IN ('todo', 'doing')
      )
      AND (CAST(:job_ids AS uuid[]) IS NULL
           OR j.id = ANY(CAST(:job_ids AS uuid[])))
    ORDER BY r.claim_deadline NULLS LAST, r.created_at
    LIMIT :limit
    FOR UPDATE OF r, j SKIP LOCKED
    """
)

_TERMINAL_SUCCESS_SQL = text(
    """
    UPDATE catalog.dataset_refresh_runs
    SET status = 'succeeded',
        phase = 'published',
        finished_at = :now,
        dataset_version_id = COALESCE(dataset_version_id, :version_id)
    WHERE id = :run_id
      AND status IN ('pending', 'running')
      AND execution_key = :execution_key
    RETURNING id
    """
)

_TERMINAL_CANCEL_SQL = text(
    """
    UPDATE catalog.dataset_refresh_runs
    SET status = 'cancelled',
        phase = NULL,
        finished_at = :now,
        error_code = COALESCE(:error_code, error_code),
        error_message = COALESCE(:error_message, error_message)
    WHERE id = :run_id
      AND status IN ('pending', 'running')
      AND execution_key = :execution_key
    RETURNING id
    """
)

_TERMINAL_FAIL_SQL = text(
    """
    UPDATE catalog.dataset_refresh_runs
    SET status = 'failed',
        phase = NULL,
        finished_at = :now,
        error_code = :error_code,
        error_message = :error_message
    WHERE id = :run_id
      AND status IN ('pending', 'running')
      AND execution_key = :execution_key
    RETURNING id
    """
)

_RESET_RUN_SQL = text(
    """
    UPDATE catalog.dataset_refresh_runs
    SET status = 'pending',
        phase = CASE WHEN CAST(:deliver AS boolean) THEN 'dispatched' ELSE 'admitted' END,
        execution_key = :new_key,
        claim_deadline = CAST(:new_deadline AS timestamptz),
        claimed_at = NULL,
        attempts = attempts + 1,
        last_dispatched_at = CASE
            WHEN CAST(:deliver AS boolean)
            THEN CAST(:now AS timestamptz)
            ELSE NULL
        END,
        last_attempt_error_code = :error_code,
        last_attempt_error_message = :error_message,
        last_attempt_at = CAST(:now AS timestamptz),
        error_code = NULL,
        error_message = NULL,
        finished_at = NULL
    WHERE id = :run_id
      AND status IN ('pending', 'running')
      AND execution_key = :old_key
      AND attempts < :max_attempts
    RETURNING id, attempts
    """
)

_RESEND_RUN_SQL = text(
    """
    UPDATE catalog.dataset_refresh_runs
    SET phase = 'dispatched',
        last_dispatched_at = :now
    WHERE id = :run_id
      AND status = 'pending'
      AND phase IN ('admitted', 'dispatched')
      AND execution_key = :execution_key
    RETURNING id, attempts
    """
)

_PUBLISHED_VERSION_SQL = text(
    """
    SELECT v.id
    FROM catalog.dataset_versions v
    WHERE v.ingest_job_id = :job_id AND v.dataset_id = :dataset_id
    ORDER BY v.version_number DESC
    LIMIT 1
    """
)


async def reconcile_keyed_refresh_runs(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    job_ids: Sequence[uuid.UUID] | None = None,
    limit: int = ADMITTED_EXPIRY_BATCH_SIZE,
    deliver: bool = True,
) -> ReconcileResult:
    """Settle one bounded batch of active keyed runs from durable facts.

    ``deliver`` marks re-driven runs ``dispatched`` and returns their
    redeliveries; the startup stale pass passes False so runs wait
    ``admitted`` for the worker interval to send them. Does not commit.
    """
    resolved_now = now or datetime.now(timezone.utc)
    candidates = (
        await session.execute(
            _CANDIDATE_SQL,
            {
                "job_ids": None if job_ids is None else [str(j) for j in job_ids],
                "limit": limit,
            },
        )
    ).all()
    redeliveries: list[Redelivery] = []
    succeeded = failed = cancelled = 0
    touched: list[uuid.UUID] = []
    for candidate in candidates:
        action, redelivery = await _settle_candidate(
            session, candidate, resolved_now, deliver=deliver
        )
        if action is ReconcileAction.LEAVE:
            continue
        if action in (ReconcileAction.REDRIVE, ReconcileAction.RESEND):
            logger.info(
                "refresh_run_redelivery",
                run_id=str(candidate.run_id),
                job_id=str(candidate.job_id),
                action=action.value,
            )
        touched.append(candidate.run_id)
        if action in (ReconcileAction.PUBLISH_SUCCEEDED, ReconcileAction.COMPLETED):
            succeeded += 1
        elif action == ReconcileAction.TERMINAL_FAIL:
            failed += 1
        elif action == ReconcileAction.TERMINAL_CANCEL:
            cancelled += 1
        if redelivery is not None:
            redeliveries.append(redelivery)
    return ReconcileResult(
        redeliveries=tuple(redeliveries),
        succeeded=succeeded,
        failed=failed,
        cancelled=cancelled,
        touched_run_ids=tuple(touched),
    )


async def _settle_candidate(
    session: AsyncSession, candidate, now: datetime, *, deliver: bool
) -> tuple[ReconcileAction, Redelivery | None]:
    if candidate.job_status == "running":
        return ReconcileAction.LEAVE, None
    version_id = await session.scalar(
        _PUBLISHED_VERSION_SQL,
        {"job_id": candidate.job_id, "dataset_id": candidate.dataset_id},
    )
    if version_id is not None:
        if await _terminal_success(session, candidate, now, version_id):
            return ReconcileAction.PUBLISH_SUCCEEDED, None
    if candidate.job_status == "complete":
        if await _terminal_success(session, candidate, now, None):
            return ReconcileAction.COMPLETED, None
    if candidate.job_status == "cancelled":
        if await _terminal_cancel(session, candidate, now):
            return ReconcileAction.TERMINAL_CANCEL, None
        return ReconcileAction.LEAVE, None
    if candidate.job_status == "failed":
        return await _settle_failed_job(session, candidate, now, deliver=deliver)
    return await _settle_pending_job(session, candidate, now, deliver=deliver)


async def _settle_failed_job(
    session: AsyncSession, candidate, now: datetime, *, deliver: bool
) -> tuple[ReconcileAction, Redelivery | None]:
    if candidate.attempts >= SCHEDULED_MAX_DELIVERIES:
        code = candidate.job_error_code or ABANDONED_ERROR_CODE
        message = candidate.job_error_message or ABANDONED_ERROR_MESSAGE
        landed = await _terminal_fail(session, candidate, now, code, message)
        if landed:
            return ReconcileAction.TERMINAL_FAIL, None
        return ReconcileAction.LEAVE, None
    redelivery = await _redrive(
        session,
        candidate,
        now,
        deliver=deliver,
        code=candidate.job_error_code,
        message=candidate.job_error_message,
    )
    if redelivery is None:
        return ReconcileAction.LEAVE, None
    return ReconcileAction.REDRIVE, redelivery


async def _settle_pending_job(
    session: AsyncSession, candidate, now: datetime, *, deliver: bool
) -> tuple[ReconcileAction, Redelivery | None]:
    deadline_passed = (
        candidate.claim_deadline is not None and candidate.claim_deadline <= now
    )
    if candidate.last_dispatched_at is None and not deadline_passed:
        # Admitted but never deferred and still inside its claim window.
        redelivery = await _resend(session, candidate, now)
        if redelivery is not None:
            return ReconcileAction.RESEND, redelivery
        return ReconcileAction.LEAVE, None
    if deadline_passed:
        if candidate.attempts >= SCHEDULED_MAX_DELIVERIES:
            # Abort the pending job first so its hook ends the run in the
            # same transaction; the run write is the CAS on the delivery.
            from app.platform.refresh.service import (
                reject_pending_admitted_refresh,
            )

            run_id = await reject_pending_admitted_refresh(
                session,
                ingest_job_id=candidate.job_id,
                execution_key=candidate.execution_key,
                error_code=ADMITTED_CLAIM_EXPIRED_ERROR_CODE,
                error_message=ADMITTED_CLAIM_EXPIRED_ERROR_MESSAGE,
            )
            return (
                (ReconcileAction.TERMINAL_FAIL, None)
                if run_id is not None
                else (ReconcileAction.LEAVE, None)
            )
        redelivery = await _redrive(
            session,
            candidate,
            now,
            deliver=deliver,
            code=ADMITTED_CLAIM_EXPIRED_ERROR_CODE,
            message=ADMITTED_CLAIM_EXPIRED_ERROR_MESSAGE,
        )
        if redelivery is None:
            return ReconcileAction.LEAVE, None
        return ReconcileAction.REDRIVE, redelivery
    grace = now - timedelta(seconds=REDRIVE_DEFER_GRACE_SECONDS)
    if (
        candidate.phase == "dispatched"
        and candidate.last_dispatched_at is not None
        and candidate.last_dispatched_at < grace
    ):
        redelivery = await _resend(session, candidate, now)
        if redelivery is not None:
            return ReconcileAction.RESEND, redelivery
    return ReconcileAction.LEAVE, None


async def _terminal_success(
    session: AsyncSession, candidate, now: datetime, version_id
) -> bool:
    from app.platform.refresh.service import _emit_refresh_succeeded

    landed = (
        await session.execute(
            _TERMINAL_SUCCESS_SQL,
            {
                "now": now,
                "version_id": version_id,
                "run_id": candidate.run_id,
                "execution_key": candidate.execution_key,
            },
        )
    ).scalar_one_or_none()
    if landed is None:
        return False
    await _release_consumed(session, [candidate.run_id])
    await _emit_refresh_succeeded(session, candidate.run_id)
    return True


async def _terminal_cancel(session: AsyncSession, candidate, now: datetime) -> bool:
    from app.platform.refresh.service import (
        USER_CANCELLED_ERROR_CODE,
        _emit_refresh_cancelled,
        _release_consumed,
        redact_run_error,
    )

    code = USER_CANCELLED_ERROR_CODE
    message = (
        redact_run_error(candidate.job_error_message)
        if candidate.job_error_message
        else "Cancelled by user."
    )
    landed = (
        await session.execute(
            _TERMINAL_CANCEL_SQL,
            {
                "now": now,
                "error_code": code,
                "error_message": message,
                "run_id": candidate.run_id,
                "execution_key": candidate.execution_key,
            },
        )
    ).scalar_one_or_none()
    if landed is None:
        return False
    await _release_consumed(session, [candidate.run_id])
    await _emit_refresh_cancelled(session, candidate.run_id)
    return True


async def _terminal_fail(
    session: AsyncSession,
    candidate,
    now: datetime,
    code: str | None,
    message: str | BaseException,
) -> bool:
    from app.platform.refresh.service import (
        _emit_refresh_failed,
        redact_run_error,
    )

    landed = (
        await session.execute(
            _TERMINAL_FAIL_SQL,
            {
                "now": now,
                "error_code": (code or ABANDONED_ERROR_CODE)[:64],
                "error_message": redact_run_error(message),
                "run_id": candidate.run_id,
                "execution_key": candidate.execution_key,
            },
        )
    ).scalar_one_or_none()
    if landed is None:
        return False
    await _release_consumed(session, [candidate.run_id])
    await _emit_refresh_failed(session, candidate.run_id)
    return True


async def _redrive(
    session: AsyncSession,
    candidate,
    now: datetime,
    *,
    deliver: bool,
    code: str | None,
    message: str | BaseException | None,
) -> Redelivery | None:
    """Re-drive the same run/job under fresh attempt and execution keys."""
    job = await ledger.hold(
        session,
        candidate.job_id,
        expect=candidate.job_status,
        attempt_id=candidate.job_attempt_id,
    )
    if job is None:
        return None
    return await _readmit_with_reset(
        session,
        job=job,
        run_id=candidate.run_id,
        old_key=candidate.execution_key,
        attempts=candidate.attempts,
        tenant_id=candidate.tenant_id,
        now=now,
        deliver=deliver,
        code=code,
        message=message,
        expect=(candidate.job_status,),
    )


_RUN_FOR_REDRIVE_SQL = text(
    """
    SELECT id, execution_key, attempts, tenant_id
    FROM catalog.dataset_refresh_runs
    WHERE ingest_job_id = :job_id
      AND status IN ('pending', 'running')
      AND execution_key IS NOT NULL
    FOR UPDATE
    """
)


async def readmit_running_attempt(
    session: AsyncSession,
    *,
    job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    code: str,
    message: str | BaseException,
    now: datetime | None = None,
    expect: tuple[str, ...] = ("running",),
) -> Redelivery | None:
    """Re-drive a run whose current delivery failed transiently.

    The caller still owns the job in one of ``expect`` (running for a
    worker failure, pending for a pre-task credential-store outage). The job
    returns to pending under a new attempt and the run to ``admitted``; the
    worker interval's reconciler sends the new key. None when the attempt no
    longer owns the job, the run is gone/terminal, or deliveries are
    exhausted.
    """
    job = await ledger.hold(session, job_id, expect=expect[0], attempt_id=attempt_id)
    if job is None and len(expect) > 1:
        job = await ledger.hold(
            session,
            job_id,
            expect=expect[1],
            attempt_id=attempt_id,
        )
    if job is None:
        return None
    row = (
        await session.execute(_RUN_FOR_REDRIVE_SQL, {"job_id": job_id})
    ).one_or_none()
    if row is None or row.attempts >= SCHEDULED_MAX_DELIVERIES:
        return None
    return await _readmit_with_reset(
        session,
        job=job,
        run_id=row.id,
        old_key=row.execution_key,
        attempts=row.attempts,
        tenant_id=row.tenant_id,
        now=now or datetime.now(timezone.utc),
        deliver=False,
        code=code,
        message=message,
        expect=(job.status,),
    )


async def _readmit_with_reset(
    session: AsyncSession,
    *,
    job,
    run_id: uuid.UUID,
    old_key: uuid.UUID,
    attempts: int,
    tenant_id: uuid.UUID | None,
    now: datetime,
    deliver: bool,
    code: str | None,
    message: str | BaseException | None,
    expect: tuple[str, ...],
) -> Redelivery | None:
    """Rotate the job attempt and reset the run row in one SAVEPOINT."""
    from app.platform.refresh.service import redact_run_error

    new_key = uuid.uuid4()
    reason_code = code or ADMITTED_CLAIM_EXPIRED_ERROR_CODE
    reason_message = redact_run_error(message or ADMITTED_CLAIM_EXPIRED_ERROR_MESSAGE)

    async def reset_run(inner: AsyncSession) -> None:
        reset = (
            await inner.execute(
                _RESET_RUN_SQL,
                {
                    "now": now,
                    "deliver": deliver,
                    "new_key": new_key,
                    "new_deadline": now
                    + timedelta(seconds=SCHEDULED_CLAIM_DEADLINE_SECONDS),
                    "error_code": reason_code[:64],
                    "error_message": reason_message,
                    "run_id": run_id,
                    "old_key": old_key,
                    "max_attempts": SCHEDULED_MAX_DELIVERIES,
                },
            )
        ).one_or_none()
        if reset is None:
            raise StaleIngestAttempt(f"Refresh run {run_id} is no longer re-drivable")

    if not await ledger.readmit(session, job, linked=reset_run, expect=expect):
        return None
    return Redelivery(
        run_id=run_id,
        job_id=job.id,
        execution_key=str(new_key),
        tenant_id=tenant_id,
        attempts=attempts + 1,
    )


async def _resend(session: AsyncSession, candidate, now: datetime) -> Redelivery | None:
    """Re-send the current key without consuming another delivery attempt."""
    resent = (
        await session.execute(
            _RESEND_RUN_SQL,
            {
                "now": now,
                "run_id": candidate.run_id,
                "execution_key": candidate.execution_key,
            },
        )
    ).one_or_none()
    if resent is None:
        return None
    return Redelivery(
        run_id=candidate.run_id,
        job_id=candidate.job_id,
        execution_key=str(candidate.execution_key),
        tenant_id=candidate.tenant_id,
        attempts=resent.attempts,
    )


async def _release_consumed(
    session: AsyncSession, run_ids: Sequence[uuid.UUID]
) -> None:
    from app.platform.refresh.service import _release_consumed_acceptances

    await _release_consumed_acceptances(session, list(run_ids))
