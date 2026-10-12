"""Per-item retirement state machine for one dataset retirement batch.

Every item moves through its stages in separate transactions, so a crash
between stages leaves durable evidence of what landed:

    fence -> quiesce -> catalog_delete -> reaping -> completed

* catalog delete and its ``storage_prefixes`` evidence commit in ONE
  transaction (the hard delete is all-or-nothing);
* object storage is reaped AFTER that commit, so a crash there resumes from
  the persisted prefixes instead of guessing what survived;
* a retryable outcome keeps the fence (new work stays blocked); conflict and
  rejected outcomes release it, so the dataset stays usable.

All item/batch writes here are raw SQL with scalar locals, not ORM
attributes: a worker session expires instances at every commit, so reading an
expired attribute off-greenlet would raise between the item's transactions.

External callers reach this through the ``...domain.service`` façade.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db.sqlstate import is_lock_conflict
from app.modules.audit.service import AuditEvent, audit_emit
from app.modules.catalog.datasets.domain.models import Dataset, Record
from app.modules.catalog.datasets.domain.retirement_models import (
    CODE_ACTIVE_JOBS,
    CODE_ATTEMPTS_EXHAUSTED,
    CODE_DATASET_RETIRING,
    CODE_LOCK_CONFLICT,
    CODE_STORAGE_ERROR,
    CODE_TITLE_MISMATCH,
    CODE_VRT_DEPENDENTS,
    ITEM_COMPLETED,
    ITEM_CONFLICT,
    ITEM_REJECTED,
    ITEM_RETRYABLE,
    ITEM_RUNNING,
    MAX_ITEM_ATTEMPTS,
    STAGE_QUIESCE,
    STAGE_REAPING,
)
from app.modules.catalog.datasets.domain.service_lifecycle import (
    DatasetTitleMismatchError,
    DependentVrtError,
    delete_dataset,
    reap_managed_storage,
)
from app.platform.cache.provider import notify_table_invalidated
from app.platform.cache.tiles import invalidate_catalog_cache
from app.platform.catalog_locks import (
    lock_catalog_rows,
    lock_ingest_jobs,
    worker_lock_budget,
)
from app.platform.extensions import get_catalog_port
from app.platform.jobs import ledger
from app.platform.jobs.models import IngestJob
from app.platform.refresh.service import cancel_active_runs_for_dataset
from app.platform.retirement import fence_dataset, unfence_dataset

logger = structlog.get_logger(__name__)

# Bounded wait for running workers to observe their cancel. Pending jobs are
# cancelled at once; running workers wind down at their next heartbeat/fence.
QUIESCE_WAIT_SECONDS = 60.0
QUIESCE_POLL_SECONDS = 2.0

# Item-level fence-phase outcomes.
_OWNED = "owned"
_GONE = "gone"
_BLOCKED = "blocked"
_FAILED = "failed"


@dataclass
class _ItemWork:
    """Scalar snapshot of the claimed item, safe across commits."""

    id: uuid.UUID
    dataset_id: uuid.UUID
    confirm_title: str
    attempts: int
    stage: str | None
    storage_prefixes: list[str]
    table_name: str | None
    status: str = ITEM_RUNNING


_CLAIM_SQL = text(
    "SELECT id, dataset_id, confirm_title, attempts, stage, "
    "storage_prefixes, table_name FROM catalog.dataset_retirement_items "
    "WHERE batch_id = :batch_id AND status IN ('pending', 'running') "
    "ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1"
)
_MARK_CLAIMED_SQL = text(
    "UPDATE catalog.dataset_retirement_items SET status = 'running', "
    "attempts = attempts + 1, "
    "stage = CASE WHEN stage = 'reaping' THEN 'reaping' ELSE 'fence' END, "
    "updated_at = now() WHERE id = :item_id"
)
_BATCH_SQL = text(
    "SELECT requested_by, tenant_id FROM catalog.dataset_retirement_batches "
    "WHERE id = :batch_id"
)
_HEARTBEAT_SQL = text(
    "UPDATE catalog.dataset_retirement_batches SET heartbeat_at = now() "
    "WHERE id = :batch_id"
)
_ACTIVE_JOB_SQL = text(
    "SELECT id FROM catalog.ingest_jobs "
    "WHERE dataset_id = :dataset_id AND status IN ('pending', 'running') "
    "ORDER BY id FOR UPDATE"
)
_ACTIVE_COUNT_SQL = text(
    "SELECT ("
    "SELECT count(*) FROM catalog.ingest_jobs "
    "WHERE dataset_id = :dataset_id AND status IN ('pending', 'running')"
    ") + ("
    "SELECT count(*) FROM catalog.dataset_refresh_runs "
    "WHERE dataset_id = :dataset_id AND status IN ('pending', 'running'))"
)
_RECORD_ID_SQL = text("SELECT record_id FROM catalog.datasets WHERE id = :dataset_id")
_DATASET_EXISTS_SQL = text("SELECT 1 FROM catalog.datasets WHERE id = :id")
_SET_REAPING_SQL = text(
    "UPDATE catalog.dataset_retirement_items SET table_name = :table_name, "
    "storage_prefixes = CAST(:prefixes AS jsonb), stage = 'reaping', "
    "updated_at = now() WHERE id = :item_id"
)


async def process_retirement_item(
    session: AsyncSession, batch_id: uuid.UUID
) -> dict[str, Any] | None:
    """Drive one claimable item to its next terminal outcome.

    Returns a summary dict, or None when no item is claimable. Each call owns
    one item end to end; the worker calls this until None.
    """
    work = await _claim_item(session, batch_id)
    if work is None:
        return None
    batch_row = (await session.execute(_BATCH_SQL, {"batch_id": batch_id})).one()
    requested_by: uuid.UUID = batch_row[0]
    tenant_id = batch_row[1]
    await session.execute(_HEARTBEAT_SQL, {"batch_id": batch_id})
    await session.commit()

    if work.attempts > MAX_ITEM_ATTEMPTS:
        await _terminal(
            session,
            batch_id,
            work,
            ITEM_REJECTED,
            code=CODE_ATTEMPTS_EXHAUSTED,
            detail="The item exhausted its retry budget.",
            unfence=True,
        )
        return _summary(work)

    # Crash window: the catalog delete committed but the reap did not.
    if work.stage == STAGE_REAPING and work.storage_prefixes:
        await _reap_then_complete(session, batch_id, work, tenant_id)
        return _summary(work)

    fenced = await _fence_phase(session, batch_id, work)
    if fenced == _GONE:
        if work.storage_prefixes:
            await _reap_then_complete(session, batch_id, work, tenant_id)
        else:
            await _terminal(session, batch_id, work, ITEM_COMPLETED)
        return _summary(work)
    if fenced in (_BLOCKED, _FAILED):
        return _summary(work)

    if not await _quiesce_phase(session, batch_id, work, requested_by):
        return _summary(work)

    await _catalog_delete_phase(session, batch_id, work, requested_by, tenant_id)
    return _summary(work)


async def _claim_item(session: AsyncSession, batch_id: uuid.UUID) -> _ItemWork | None:
    """Atomically take one pending/stale-running item of the batch."""
    row = (await session.execute(_CLAIM_SQL, {"batch_id": batch_id})).first()
    if row is None:
        return None
    await session.execute(_MARK_CLAIMED_SQL, {"item_id": row[0]})
    await session.commit()
    return _ItemWork(
        id=row[0],
        dataset_id=row[1],
        confirm_title=row[2],
        attempts=row[3] + 1,
        stage=row[4],
        storage_prefixes=list(row[5] or []),
        table_name=row[6],
    )


async def _fence_phase(
    session: AsyncSession, batch_id: uuid.UUID, work: _ItemWork
) -> str:
    """Lock the dataset and open this batch's fence."""
    try:
        async with worker_lock_budget(session):
            record_id = await session.scalar(
                _RECORD_ID_SQL, {"dataset_id": work.dataset_id}
            )
            if record_id is None:
                await session.rollback()
                return _GONE
            await lock_catalog_rows(
                session,
                dataset_cls=Dataset,
                record_cls=Record,
                dataset_id=work.dataset_id,
                record_id=record_id,
                lock_timeout=None,
            )
            generation = await fence_dataset(session, work.dataset_id, batch_id)
            if generation is None:
                await session.rollback()
                await _terminal(
                    session,
                    batch_id,
                    work,
                    ITEM_CONFLICT,
                    code=CODE_DATASET_RETIRING,
                    detail="The dataset is fenced by another retirement batch.",
                )
                return _BLOCKED
            await session.execute(
                text(
                    "UPDATE catalog.dataset_retirement_items "
                    "SET generation = :generation, stage = :stage "
                    "WHERE id = :item_id"
                ),
                {
                    "generation": generation,
                    "stage": STAGE_QUIESCE,
                    "item_id": work.id,
                },
            )
            await session.commit()
            return _OWNED
    except Exception as exc:  # broad: lock waits and surprises stay item-local
        await session.rollback()
        if is_lock_conflict(exc):
            await _retryable(
                session,
                batch_id,
                work,
                CODE_LOCK_CONFLICT,
                "Another operation is updating this dataset's catalog entry.",
            )
        else:
            logger.exception(
                "retirement_fence_failed",
                dataset_id=str(work.dataset_id),
                batch_id=str(batch_id),
            )
            await _retryable(
                session,
                batch_id,
                work,
                "fence_failed",
                "The retirement fence could not be acquired.",
            )
        return _FAILED


async def _quiesce_phase(
    session: AsyncSession,
    batch_id: uuid.UUID,
    work: _ItemWork,
    requested_by: uuid.UUID,
) -> bool:
    """Cancel active jobs/runs, then wait for workers to observe it."""
    try:
        async with worker_lock_budget(session):
            await lock_ingest_jobs(
                session,
                job_cls=IngestJob,
                dataset_id=work.dataset_id,
                lock_timeout=None,
            )
            active_ids = list(
                (
                    await session.execute(
                        _ACTIVE_JOB_SQL, {"dataset_id": work.dataset_id}
                    )
                ).scalars()
            )
            for job_id in active_ids:
                job = await session.get(IngestJob, job_id)
                if job is not None and job.status in ("pending", "running"):
                    await ledger.cancel(session, job, actor=requested_by)
            # Runs without a live bound job (scheduled/manual admission);
            # bound ones were just ended by the cancel's linked-run hook, and
            # the transition CAS leaves those untouched here.
            await cancel_active_runs_for_dataset(
                session, work.dataset_id, cancelled_by=requested_by
            )
            await session.commit()
    except Exception as exc:  # broad: a cancel failure is the item's, not batch's
        await session.rollback()
        code = CODE_LOCK_CONFLICT if is_lock_conflict(exc) else "quiesce_failed"
        detail = (
            "Another operation is updating this dataset's catalog entry."
            if is_lock_conflict(exc)
            else "Cancelling active jobs failed."
        )
        await _retryable(session, batch_id, work, code, detail)
        return False

    # Best-effort queue abort after the DB cancel is durable; never decisive —
    # the status fences are the guarantee, the poll below is the gate.
    try:
        await get_catalog_port().abort_live_jobs_for_dataset(session, work.dataset_id)
    except Exception:  # broad: acceleration only
        logger.warning("retirement_queue_abort_failed", dataset_id=str(work.dataset_id))

    deadline = asyncio.get_event_loop().time() + QUIESCE_WAIT_SECONDS
    while True:
        active = await session.scalar(
            _ACTIVE_COUNT_SQL, {"dataset_id": work.dataset_id}
        )
        await session.rollback()
        if not active:
            return True
        if asyncio.get_event_loop().time() >= deadline:
            await _terminal(
                session,
                batch_id,
                work,
                ITEM_CONFLICT,
                code=CODE_ACTIVE_JOBS,
                detail=(
                    "Active imports or analyses did not stop in time; the "
                    "dataset remains usable. Retry the batch to try again."
                ),
                unfence=True,
            )
            return False
        await asyncio.sleep(QUIESCE_POLL_SECONDS)
        await session.execute(_HEARTBEAT_SQL, {"batch_id": batch_id})
        await session.commit()


async def _catalog_delete_phase(
    session: AsyncSession,
    batch_id: uuid.UUID,
    work: _ItemWork,
    requested_by: uuid.UUID,
    tenant_id: uuid.UUID | None,
) -> None:
    """Ordered teardown in ONE transaction, then post-commit invalidations."""
    table_name: str | None = None
    try:
        async with worker_lock_budget(session):
            await lock_ingest_jobs(
                session,
                job_cls=IngestJob,
                dataset_id=work.dataset_id,
                lock_timeout=None,
            )
            record_id = await session.scalar(
                _RECORD_ID_SQL, {"dataset_id": work.dataset_id}
            )
            if record_id is not None:
                await lock_catalog_rows(
                    session,
                    dataset_cls=Dataset,
                    record_cls=Record,
                    dataset_id=work.dataset_id,
                    record_id=record_id,
                    lock_timeout=None,
                )
                # Dependency order: map bindings, grants, versions — then the
                # table drop/detach and record cascade delete_dataset() does.
                for statement in (
                    "DELETE FROM catalog.map_layers WHERE dataset_id = :id",
                    "DELETE FROM catalog.dataset_grants WHERE dataset_id = :id",
                    "DELETE FROM catalog.dataset_versions WHERE dataset_id = :id",
                ):
                    await session.execute(text(statement), {"id": work.dataset_id})
                deletion = await delete_dataset(
                    session, work.dataset_id, work.confirm_title
                )
                table_name = deletion.table_name
                work.table_name = table_name
                work.storage_prefixes = list(deletion.storage_prefixes)
                await session.execute(
                    _SET_REAPING_SQL,
                    {
                        "table_name": table_name,
                        "prefixes": json.dumps(list(deletion.storage_prefixes)),
                        "item_id": work.id,
                    },
                )
                await audit_emit(
                    session,
                    AuditEvent(
                        user_id=requested_by,
                        action="dataset.delete",
                        resource_type="dataset",
                        resource_id=work.dataset_id,
                        details={
                            "title": work.confirm_title,
                            "table_name": deletion.table_name,
                            "retirement_batch_id": str(batch_id),
                        },
                    ),
                )
        await session.commit()
    except DependentVrtError as exc:
        await session.rollback()
        await _terminal(
            session,
            batch_id,
            work,
            ITEM_CONFLICT,
            code=CODE_VRT_DEPENDENTS,
            detail=str(exc),
            dependents=exc.dependents,
            unfence=True,
        )
        return
    except DatasetTitleMismatchError as exc:
        await session.rollback()
        await _terminal(
            session,
            batch_id,
            work,
            ITEM_REJECTED,
            code=CODE_TITLE_MISMATCH,
            detail=str(exc),
            unfence=True,
        )
        return
    except Exception as exc:  # broad: classify the rest, per-item isolation
        await session.rollback()
        if await session.scalar(_DATASET_EXISTS_SQL, {"id": work.dataset_id}) is None:
            await _terminal(session, batch_id, work, ITEM_COMPLETED)
            return
        if is_lock_conflict(exc):
            await _retryable(
                session,
                batch_id,
                work,
                CODE_LOCK_CONFLICT,
                "Another operation is updating this dataset's catalog entry.",
            )
            return
        logger.exception(
            "retirement_catalog_delete_failed",
            dataset_id=str(work.dataset_id),
            batch_id=str(batch_id),
        )
        await _retryable(
            session,
            batch_id,
            work,
            "catalog_delete_failed",
            "Dataset deletion failed unexpectedly.",
        )
        return

    # Only now is the delete visible, so only now can the tile map be
    # evicted without a request re-caching the deleted row.
    if table_name:
        await invalidate_catalog_cache()
        notify_table_invalidated(table_name)
    await _reap_then_complete(session, batch_id, work, tenant_id)


async def _reap_then_complete(
    session: AsyncSession,
    batch_id: uuid.UUID,
    work: _ItemWork,
    tenant_id: uuid.UUID | None,
) -> None:
    """Reap persisted storage prefixes (idempotent), then complete the item."""
    try:
        await reap_managed_storage(work.storage_prefixes, tenant_id=tenant_id)
    except Exception:  # broad: rows are gone; the reap alone must be retryable
        logger.exception(
            "retirement_storage_reap_failed",
            dataset_id=str(work.dataset_id),
            prefixes=work.storage_prefixes,
        )
        await _retryable(
            session,
            batch_id,
            work,
            CODE_STORAGE_ERROR,
            "Some stored objects could not be deleted.",
        )
        return
    await _terminal(session, batch_id, work, ITEM_COMPLETED)


async def _retryable(
    session: AsyncSession,
    batch_id: uuid.UUID,
    work: _ItemWork,
    code: str,
    detail: str,
) -> None:
    """Record a retryable outcome; the fence and stage stay in place."""
    await session.execute(
        text(
            "UPDATE catalog.dataset_retirement_items "
            "SET status = 'retryable', error_code = :code, detail = :detail, "
            "updated_at = now() WHERE id = :item_id"
        ),
        {"code": code, "detail": detail, "item_id": work.id},
    )
    await session.execute(_HEARTBEAT_SQL, {"batch_id": batch_id})
    await session.commit()
    work.status = ITEM_RETRYABLE


async def _terminal(
    session: AsyncSession,
    batch_id: uuid.UUID,
    work: _ItemWork,
    status: str,
    *,
    code: str | None = None,
    detail: str | None = None,
    dependents: list[dict[str, Any]] | None = None,
    unfence: bool = False,
) -> None:
    """Persist a terminal item outcome and commit, releasing its fence."""
    now = datetime.now(timezone.utc)
    if unfence:
        await unfence_dataset(session, work.dataset_id, batch_id)
    dependents_json = json.dumps(dependents) if dependents is not None else None
    await session.execute(
        text(
            "UPDATE catalog.dataset_retirement_items SET "
            "status = :status, error_code = :code, detail = :detail, "
            "dependents = CAST(:dependents AS jsonb), stage = NULL, "
            "finished_at = :now, updated_at = :now WHERE id = :item_id"
        ),
        {
            "status": status,
            "code": code,
            "detail": detail,
            "dependents": dependents_json,
            "now": now,
            "item_id": work.id,
        },
    )
    await session.execute(_HEARTBEAT_SQL, {"batch_id": batch_id})
    await session.commit()
    work.stage = None
    work.status = status


def _summary(work: _ItemWork) -> dict[str, Any]:
    return {
        "item_id": str(work.id),
        "dataset_id": str(work.dataset_id),
        "status": work.status,
    }
