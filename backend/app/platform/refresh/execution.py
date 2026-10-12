"""Core-neutral scheduled refresh execution identities and request DTOs.

The actual refresh executor remains in the ingest domain. This module owns
only the durable values which callers may persist or place in queue payloads.
"""

from __future__ import annotations

import inspect
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.failure_reason import FixedReason, coded_failure_reason
from app.platform.jobs import ledger
from app.platform.jobs.models import IngestJob
from app.platform.refresh.credentials import CredentialStoreUnavailable
from app.platform.refresh.models import DatasetRefreshRun
from app.platform.refresh.service import (
    claim_admitted_run_for_job,
    create_pending_run,
    expire_unclaimed_admitted_runs,
    fail_claimed_admitted_refresh,
    reject_pending_admitted_refresh,
    record_refresh_failure,
)

SCHEDULED_REFRESH_TASK_NAME = "scheduled-refresh-v1"
ARC_GIS_ID_SET_VERIFICATION_POLICY = "arcgis_id_set_v1"

RefreshTrigger = Literal["manual", "scheduled"]


@dataclass(frozen=True, slots=True)
class RefreshAdmissionRequest:
    """Values captured before an admitted refresh becomes asynchronous work."""

    source_binding_fingerprint: str
    local_edit_baseline: datetime | None
    origin_kind: str
    source_filename: str | None = None
    source_url: str | None = None
    source_layer: str | None = None
    job_metadata: Mapping[str, object] = field(default_factory=dict)
    feature_count_before: int | None = None
    verification_policy: str = ARC_GIS_ID_SET_VERIFICATION_POLICY
    credential_reference: str | None = None
    credential_version: str | None = None
    # Opaque revision of the schedule configuration admitting this run.
    schedule_version: str | None = None


@dataclass(frozen=True, slots=True)
class RefreshAdmission:
    """Identity and fences created atomically with a refresh job and run."""

    dataset_id: UUID
    job_id: UUID
    run_id: UUID
    tenant_id: UUID | None
    trigger: RefreshTrigger
    source_binding_fingerprint: str
    local_edit_baseline: datetime | None
    verification_policy: str
    execution_key: str
    claim_deadline: datetime | None
    scheduled_for: datetime | None = None
    occurrence_key: str | None = None
    schedule_version: str | None = None


@dataclass(frozen=True, slots=True)
class RefreshExecutionResult:
    """Result returned by a claim-fenced worker execution attempt."""

    job_id: UUID
    run_id: UUID
    status: Literal["completed", "rejected", "already_settled", "redelivered"]


class ScheduledRefreshHeld(Exception):
    """A hold on the dataset refuses scheduled refreshes; ``reason`` says why."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"Scheduled refresh is held: {reason}")
        self.reason = reason


async def release_scheduled_refresh_hold(
    session: AsyncSession, dataset_id: UUID
) -> None:
    """Let scheduled refreshes of the dataset be admitted again; never commits."""
    await session.execute(
        text(
            "UPDATE catalog.datasets SET scheduled_refresh_hold = NULL "
            "WHERE id = :dataset_id AND scheduled_refresh_hold IS NOT NULL"
        ),
        {"dataset_id": dataset_id},
    )


_HOLD = text("SELECT scheduled_refresh_hold FROM catalog.datasets WHERE id = :id")
_HOLD_FOR_SHARE = text(
    "SELECT scheduled_refresh_hold FROM catalog.datasets WHERE id = :id FOR SHARE"
)


async def _refuse_scheduled_hold(
    session: AsyncSession, dataset_id: UUID, *, lock: bool = False
) -> None:
    """Raise ``ScheduledRefreshHeld`` when the dataset row carries a hold."""
    # Read from the row, not the caller's instance, which may predate a restore.
    hold = await session.scalar(_HOLD_FOR_SHARE if lock else _HOLD, {"id": dataset_id})
    if hold is not None:
        raise ScheduledRefreshHeld(hold)


CredentialResolver = Callable[[str, str | None], Awaitable[str]]
ScheduledRefreshTaskExecutor = Callable[[str, str, str | None], Awaitable[None]]
_REJECTION_CODE_CHARACTERS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")


def register_scheduled_refresh_task(
    task_app: object,
    *,
    execute: ScheduledRefreshTaskExecutor,
) -> object:
    """Register the versioned scheduled task around an overlay-owned callback.

    The core owns the task identity, queue, and retry behavior. The callback
    owns tenant validation, outbox state, and in-memory credential resolution,
    which keeps Community independent of paid configuration.
    """
    task_factory = getattr(task_app, "task", None)
    if not callable(task_factory):
        raise TypeError("task_app must provide a callable task decorator")

    @task_factory(queue="ingest", retry=0, name=SCHEDULED_REFRESH_TASK_NAME)
    async def scheduled_refresh_task(
        job_id: str, execution_key: str, tenant_id: str | None = None
    ) -> None:
        await execute(job_id, execution_key, tenant_id)

    return scheduled_refresh_task


async def prepare_admitted_refresh(
    session: AsyncSession,
    *,
    dataset: object,
    actor: object,
    request: RefreshAdmissionRequest,
    trigger: RefreshTrigger,
    scheduled_for: datetime | None = None,
    occurrence_key: str | None = None,
) -> RefreshAdmission:
    """Create the job and refresh run in the caller's transaction.

    Authorization belongs to the caller's domain guard. This facade requires
    a user-bound actor and refuses malformed identities before writing. It never
    commits, defers queue work, or accepts a credential value. A scheduled
    admission of a dataset under a hold raises ``ScheduledRefreshHeld``.
    """
    dataset_id = getattr(dataset, "id", None)
    actor_id = getattr(actor, "id", None)
    if not isinstance(dataset_id, UUID):
        raise ValueError("dataset must expose a UUID id")
    if not isinstance(actor_id, UUID):
        raise ValueError("actor must expose a UUID id")
    if not request.source_binding_fingerprint:
        raise ValueError("source_binding_fingerprint is required")
    if trigger == "scheduled" and (scheduled_for is None or not occurrence_key):
        raise ValueError(
            "scheduled admission requires scheduled_for and occurrence_key"
        )

    if trigger == "scheduled":
        await _refuse_scheduled_hold(session, dataset_id)

    execution_key = uuid.uuid4()
    # A savepoint, so a hold found once the run is reserved leaves no job or run.
    async with session.begin_nested():
        job = ledger.create(
            session,
            created_by=actor_id,
            dataset_id=dataset_id,
            source_filename=request.source_filename,
            source_url=request.source_url,
            source_layer=request.source_layer,
            user_metadata={
                **dict(request.job_metadata),
                "refresh": True,
                "dataset_id": str(dataset_id),
                "origin_kind": request.origin_kind,
                "verification_policy": request.verification_policy,
            },
        )
        await session.flush()
        run = await create_pending_run(
            session,
            dataset_id=dataset_id,
            origin_kind=request.origin_kind,
            trigger=trigger,
            triggered_by=actor_id,
            ingest_job_id=job.id,
            feature_count_before=request.feature_count_before,
            scheduled_for=scheduled_for,
            occurrence_key=occurrence_key,
            execution_key=execution_key,
            source_binding_fingerprint=request.source_binding_fingerprint,
            local_edit_baseline=request.local_edit_baseline,
            verification_policy=request.verification_policy,
            credential_reference=request.credential_reference,
            credential_version=request.credential_version,
            schedule_version=request.schedule_version,
        )
        if trigger == "scheduled":
            # A restore that committed while the reservation waited on its run
            # set the hold after the read above. FOR SHARE keeps this answer
            # until the admission commits: a later restore must first update
            # the row.
            await _refuse_scheduled_hold(session, dataset_id, lock=True)
    return RefreshAdmission(
        dataset_id=dataset_id,
        job_id=job.id,
        run_id=run.id,
        tenant_id=run.tenant_id,
        trigger=trigger,
        source_binding_fingerprint=request.source_binding_fingerprint,
        local_edit_baseline=request.local_edit_baseline,
        verification_policy=request.verification_policy,
        execution_key=str(execution_key),
        claim_deadline=run.claim_deadline,
        scheduled_for=scheduled_for,
        occurrence_key=occurrence_key,
        schedule_version=request.schedule_version,
    )


async def reject_admitted_refresh(
    session: AsyncSession,
    job_id: UUID,
    execution_key: str,
    reason: str,
) -> UUID | None:
    """Terminalize an unclaimed admitted job/run without touching claimed work.

    Callers pass a stable, non-secret reason code after an authorization or
    source fence rejects queued work. The caller owns the surrounding
    transaction; ``None`` means a worker already claimed or settled it.
    """
    try:
        claim_key = UUID(execution_key)
    except ValueError as exc:
        raise ValueError("execution_key must be a UUID") from exc
    if (
        not reason
        or len(reason) > 64
        or not set(reason).issubset(_REJECTION_CODE_CHARACTERS)
    ):
        raise ValueError("reason must be a lowercase underscore-delimited code")
    return await reject_pending_admitted_refresh(
        session,
        ingest_job_id=job_id,
        execution_key=claim_key,
        error_code=reason,
        error_message=f"Refresh was rejected before execution: {reason}.",
    )


async def cancel_admitted_refresh(
    session: AsyncSession,
    job_id: UUID,
    execution_key: str,
    *,
    reason: str,
) -> UUID | None:
    """Cancel a keyed run and its job, scoped to one delivery's execution key.

    An overlay scheduler uses this when a schedule is disabled or its source
    is edited after the run was claimed. The key fences the cancel so a stale
    delivery cannot cancel a run reconciliation re-drove under a newer key.
    The caller owns the surrounding transaction; ``None`` means the run was
    already settled or belongs to another delivery.
    """
    try:
        claim_key = UUID(execution_key)
    except ValueError as exc:
        raise ValueError("execution_key must be a UUID") from exc
    if (
        not reason
        or len(reason) > 64
        or not set(reason).issubset(_REJECTION_CODE_CHARACTERS)
    ):
        raise ValueError("reason must be a lowercase underscore-delimited code")
    job = await ledger.hold(session, job_id, expect="pending") or await ledger.hold(
        session, job_id, expect="running"
    )
    if job is None:
        return None
    # The job end is not key-scoped, so verify the delivery first: a stale
    # key must cancel neither the job nor the run it no longer names.
    active_run = await session.scalar(
        select(DatasetRefreshRun.id).where(
            DatasetRefreshRun.ingest_job_id == job_id,
            DatasetRefreshRun.status.in_(("pending", "running")),
            DatasetRefreshRun.execution_key == claim_key,
        )
    )
    if active_run is None:
        return None
    ended = await ledger.cancel(session, job, actor=None, run_execution_key=claim_key)
    return ended.linked.get("run")


async def _readmit_credential_outage(session: AsyncSession, job_id: UUID) -> bool:
    """Re-drive a claimed run whose credential vault is temporarily unavailable."""
    from app.platform.refresh.reconcile import readmit_running_attempt

    job = await session.get(IngestJob, job_id)
    if job is None or job.attempt_id is None:
        return False
    redelivery = await readmit_running_attempt(
        session,
        job_id=job_id,
        attempt_id=job.attempt_id,
        code="credential_store_unavailable",
        message="The credential store was temporarily unavailable.",
        expect=("pending",),
    )
    return redelivery is not None


async def _resolve_credential(
    job_id: UUID,
    run_id: UUID,
    claim_key: uuid.UUID,
    *,
    reference: str,
    version: str | None,
    resolver: CredentialResolver | None,
) -> tuple[str | None, RefreshExecutionResult | None]:
    """Resolve the run's credential, or settle/re-drive the run on failure."""
    from app.core.db import async_session

    if resolver is None:
        resolver_error: BaseException = RuntimeError(
            "No credential resolver is available for this scheduled refresh."
        )
    else:
        try:
            token = await resolver(reference, version)
        except (
            Exception
        ) as exc:  # broad: vault implementations surface provider-specific failures
            resolver_error = exc
        else:
            if token:
                return token, None
            resolver_error = RuntimeError(
                "Credential resolver returned no credential for this refresh."
            )

    async with async_session() as session:
        redelivered = False
        if isinstance(resolver_error, CredentialStoreUnavailable):
            # The credential vault is temporarily down: re-drive the same
            # run instead of failing the scheduled occurrence.
            redelivered = await _readmit_credential_outage(session, job_id)
        if not redelivered:
            await fail_claimed_admitted_refresh(
                session,
                ingest_job_id=job_id,
                execution_key=claim_key,
                error_code="scheduled_credential_unavailable",
                error_message=coded_failure_reason(
                    "Scheduled refresh credential resolution failed",
                    resolver_error,
                ),
            )
        await session.commit()
    outcome_status: Literal["redelivered", "rejected"] = (
        "redelivered" if redelivered else "rejected"
    )
    return None, RefreshExecutionResult(
        job_id=job_id, run_id=run_id, status=outcome_status
    )


async def execute_admitted_refresh(
    job_id: UUID,
    execution_key: str,
    *,
    credential_resolver: CredentialResolver | None = None,
) -> RefreshExecutionResult:
    """Claim and invoke the registered verified-refresh task for one admission.

    This is intentionally the only cross-domain execution entry: it invokes
    the public CatalogPort task, never a router or a private ingest helper.
    """
    from app.core.db import async_session
    from app.platform.extensions import get_catalog_port

    try:
        claim_key = UUID(execution_key)
    except ValueError:
        return RefreshExecutionResult(
            job_id=job_id, run_id=UUID(int=0), status="rejected"
        )

    async with async_session() as session:
        run = await session.scalar(
            select(DatasetRefreshRun).where(
                DatasetRefreshRun.ingest_job_id == job_id,
                DatasetRefreshRun.execution_key == claim_key,
            )
        )
        if run is None:
            return RefreshExecutionResult(
                job_id=job_id, run_id=UUID(int=0), status="already_settled"
            )
        claimed_run_id = await claim_admitted_run_for_job(
            session, job_id, execution_key=claim_key
        )
        if claimed_run_id is None:
            await expire_unclaimed_admitted_runs(session)
            await session.commit()
            return RefreshExecutionResult(
                job_id=job_id, run_id=run.id, status="already_settled"
            )
        job = await session.get(IngestJob, job_id)
        if job is None:
            await record_refresh_failure(
                session,
                ingest_job_id=job_id,
                error_code="scheduled_job_missing",
                error_message=FixedReason(
                    "The admitted refresh job no longer exists.",
                    code="scheduled_job_missing",
                ),
            )
            await session.commit()
            return RefreshExecutionResult(
                job_id=job_id, run_id=claimed_run_id, status="rejected"
            )
        task_args = {
            "job_id": str(job.id),
            "dataset_id": str(job.dataset_id),
            "source_url": job.source_url or "",
            "source_layer": job.source_layer or "",
            "user_id": str(job.created_by),
            "attempt_id": str(job.attempt_id),
            "tenant_id": str(run.tenant_id) if run.tenant_id is not None else None,
        }
        await session.commit()
        credential_reference = run.credential_reference
        credential_version = run.credential_version
        scheduled = run.trigger == "scheduled"
        dataset_id = run.dataset_id

    if scheduled:
        # Admission refuses a held dataset; this covers a claim of a run
        # admitted by a release that did not check.
        async with async_session() as session:
            try:
                await _refuse_scheduled_hold(session, dataset_id)
            except ScheduledRefreshHeld:
                await fail_claimed_admitted_refresh(
                    session,
                    ingest_job_id=job_id,
                    execution_key=claim_key,
                    error_code="scheduled_refresh_held",
                    error_message="Scheduled refreshes of this dataset are on hold.",
                )
                await session.commit()
                return RefreshExecutionResult(
                    job_id=job_id, run_id=claimed_run_id, status="rejected"
                )

    if credential_reference is not None:
        token, credential_outcome = await _resolve_credential(
            job_id,
            claimed_run_id,
            claim_key,
            reference=credential_reference,
            version=credential_version,
            resolver=credential_resolver,
        )
        if credential_outcome is not None:
            return credential_outcome
    else:
        token = None

    task = get_catalog_port().verified_refresh_service_task()
    try:
        result = task.func(
            **task_args,
            token=token,
            credential_ref=None,
            verification_policy=run.verification_policy,
            credential_version=credential_version,
            scheduled_execution_key=execution_key,
        )
        if inspect.isawaitable(result):
            await result
    except (
        Exception
    ) as exc:  # broad: task implementations own provider-specific failures
        async with async_session() as session:
            await record_refresh_failure(
                session,
                ingest_job_id=job_id,
                error_code="scheduled_executor_failed",
                error_message=exc,
            )
            await session.commit()
        return RefreshExecutionResult(
            job_id=job_id, run_id=claimed_run_id, status="rejected"
        )

    async with async_session() as session:
        settled = await session.get(DatasetRefreshRun, claimed_run_id)
        status = (
            "completed"
            if settled is not None and settled.status == "succeeded"
            else "rejected"
        )
    return RefreshExecutionResult(job_id=job_id, run_id=claimed_run_id, status=status)
