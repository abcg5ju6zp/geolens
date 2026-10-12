"""Batch-level orchestration for dataset retirement.

Owns batch creation (fixing requester and target set), idempotent resubmission,
the worker's batch claim/finish CAS, preview, retry and read access. The
per-item pipeline lives in ``service_retirement_items``; this module only
moves the batch row and its counters.

External callers reach this through the ``...domain.service`` façade.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

import structlog
from fastapi import HTTPException, status
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db.tenant_session import current_tenant_var
from app.core.identity import Identity
from app.modules.catalog.authorization import (
    check_dataset_write_access,
    get_user_roles,
)
from app.modules.catalog.datasets.domain.retirement_models import (
    BATCH_COMPLETED,
    BATCH_PENDING,
    BATCH_RUNNING,
    CODE_FORBIDDEN,
    CODE_NOT_FOUND,
    ITEM_COMPLETED,
    ITEM_CONFLICT,
    ITEM_PENDING,
    ITEM_REJECTED,
    ITEM_RETRYABLE,
    ITEM_RUNNING,
    DatasetRetirementBatch,
    DatasetRetirementItem,
)
from app.modules.catalog.datasets.domain.service_query import get_dataset
from app.platform.extensions import get_catalog_port

logger = structlog.get_logger(__name__)

# A running batch whose heartbeat is older than this is treated as crashed:
# its worker died (or the response was lost) and GET/retry redrives it.
BATCH_STALE_SECONDS = 300


class RetirementTarget(NamedTuple):
    dataset_id: uuid.UUID
    confirm_title: str


class RetirementIdempotencyMismatch(Exception):
    """The same idempotency key was reused with a different target set."""


_ACTIVE_JOB_COUNT_SQL = text(
    "SELECT count(*) FROM catalog.ingest_jobs "
    "WHERE dataset_id = :dataset_id AND status IN ('pending', 'running')"
)
_ACTIVE_RUN_COUNT_SQL = text(
    "SELECT count(*) FROM catalog.dataset_refresh_runs "
    "WHERE dataset_id = :dataset_id AND status IN ('pending', 'running')"
)
_MAP_LAYER_COUNT_SQL = text(
    "SELECT count(*) FROM catalog.map_layers WHERE dataset_id = :dataset_id"
)
_VRT_DEPENDENTS_SQL = text(
    """
    SELECT d.id FROM catalog.vrt_source_links vsl
    JOIN catalog.datasets d ON d.id = vsl.vrt_dataset_id
    WHERE vsl.source_dataset_id = :dataset_id
    UNION
    SELECT d.id FROM catalog.vrt_generations g
    JOIN catalog.datasets d ON d.id = g.vrt_dataset_id
    WHERE g.status IN ('pending', 'running')
      AND g.staged_source_ids @> to_jsonb(CAST(:dataset_id_text AS text))
    """
)


async def preview_retirement(
    session: AsyncSession, user: Identity, targets: list[RetirementTarget]
) -> list[dict[str, Any]]:
    """Read-only per-target impact report. Never mutates."""
    results: list[dict[str, Any]] = []
    for target in targets:
        dataset = await get_dataset(session, target.dataset_id)
        if dataset is None:
            results.append(
                {
                    "dataset_id": target.dataset_id,
                    "found": False,
                    "writable": False,
                    "title": None,
                    "title_matches": False,
                    "active_jobs": 0,
                    "active_refresh_runs": 0,
                    "map_layers": 0,
                    "vrt_dependents": [],
                }
            )
            continue
        try:
            await check_dataset_write_access(session, dataset, target.dataset_id, user)
            writable = True
        except HTTPException:
            writable = False
        vrt_rows = (
            await session.execute(
                _VRT_DEPENDENTS_SQL,
                {
                    "dataset_id": target.dataset_id,
                    "dataset_id_text": str(target.dataset_id),
                },
            )
        ).scalars()
        results.append(
            {
                "dataset_id": target.dataset_id,
                "found": True,
                "writable": writable,
                "title": dataset.record.title,
                "title_matches": dataset.record.title == target.confirm_title,
                "active_jobs": int(
                    await session.scalar(
                        _ACTIVE_JOB_COUNT_SQL,
                        {"dataset_id": target.dataset_id},
                    )
                    or 0
                ),
                "active_refresh_runs": int(
                    await session.scalar(
                        _ACTIVE_RUN_COUNT_SQL,
                        {"dataset_id": target.dataset_id},
                    )
                    or 0
                ),
                "map_layers": int(
                    await session.scalar(
                        _MAP_LAYER_COUNT_SQL,
                        {"dataset_id": target.dataset_id},
                    )
                    or 0
                ),
                "vrt_dependents": [str(row) for row in vrt_rows],
            }
        )
    return results


async def create_retirement_batch(
    session: AsyncSession,
    *,
    user: Identity,
    targets: list[RetirementTarget],
    idempotency_key: str | None,
) -> tuple[DatasetRetirementBatch, bool]:
    """Fix requester and targets, persist the batch and its items.

    Permission is evaluated HERE against this user and snapshotted as item
    outcomes: an invisible/unowned target is a rejected item, not a request
    failure. Returns ``(batch, created)``; an idempotent replay returns the
    existing batch with False.
    """
    ids = [target.dataset_id for target in targets]
    if len(ids) != len(set(ids)):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Duplicate dataset_id in retirement targets.",
        )

    if idempotency_key is not None:
        existing = await session.scalar(
            select(DatasetRetirementBatch).where(
                DatasetRetirementBatch.requested_by == user.id,
                DatasetRetirementBatch.idempotency_key == idempotency_key,
            )
        )
        if existing is not None:
            await _assert_same_targets(session, existing, ids)
            return existing, False

    batch = DatasetRetirementBatch(
        tenant_id=current_tenant_var.get(),
        requested_by=user.id,
        idempotency_key=idempotency_key,
        status=BATCH_PENDING,
        total_count=len(targets),
    )
    session.add(batch)
    await session.flush()

    for target in targets:
        item_kwargs = await _admission_outcome(session, user, target)
        session.add(
            DatasetRetirementItem(
                batch_id=batch.id,
                dataset_id=target.dataset_id,
                confirm_title=target.confirm_title,
                **item_kwargs,
            )
        )
    await session.commit()
    return batch, True


async def _admission_outcome(
    session: AsyncSession, user: Identity, target: RetirementTarget
) -> dict[str, Any]:
    """Evaluate one target at creation time; returns the item's initial fields.

    An invisible/unowned target is recorded rejected up front so the worker
    never re-evaluates the fixed permission snapshot.
    """
    dataset = await get_dataset(session, target.dataset_id)
    if dataset is None:
        return _rejected_seed(CODE_NOT_FOUND, "Dataset not found")
    try:
        await check_dataset_write_access(session, dataset, target.dataset_id, user)
    except HTTPException as exc:
        code = (
            CODE_NOT_FOUND
            if exc.status_code == status.HTTP_404_NOT_FOUND
            else CODE_FORBIDDEN
        )
        detail = (
            "Dataset not found"
            if code == CODE_NOT_FOUND
            else "Only the dataset owner or an admin may retire this dataset."
        )
        return _rejected_seed(code, detail)
    return {"status": ITEM_PENDING}


def _rejected_seed(code: str, detail: str) -> dict[str, Any]:
    return {
        "status": ITEM_REJECTED,
        "error_code": code,
        "detail": detail,
        "finished_at": datetime.now(timezone.utc),
    }


async def _assert_same_targets(
    session: AsyncSession,
    batch: DatasetRetirementBatch,
    dataset_ids: list[uuid.UUID],
) -> None:
    existing_ids = set(
        (
            await session.execute(
                select(DatasetRetirementItem.dataset_id).where(
                    DatasetRetirementItem.batch_id == batch.id
                )
            )
        ).scalars()
    )
    if existing_ids != set(dataset_ids):
        raise RetirementIdempotencyMismatch()


async def claim_retirement_batch(session: AsyncSession, batch_id: uuid.UUID) -> bool:
    """CAS a pending or heartbeat-stale batch to this driver.

    Returns False when a fresh driver owns it. A batch whose worker died keeps
    status 'running' with an old heartbeat, so this same CAS recovers it.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=BATCH_STALE_SECONDS)
    won = await session.scalar(
        text(
            "UPDATE catalog.dataset_retirement_batches "
            "SET status = :running, "
            "started_at = COALESCE(started_at, now()), heartbeat_at = now() "
            "WHERE id = :batch_id AND ("
            "status = :pending OR "
            "(status = :running AND "
            "(heartbeat_at IS NULL OR heartbeat_at < :cutoff))) "
            "RETURNING id"
        ),
        {
            "batch_id": batch_id,
            "running": BATCH_RUNNING,
            "pending": BATCH_PENDING,
            "cutoff": cutoff,
        },
    )
    await session.commit()
    return won is not None


async def complete_retirement_batch_if_done(
    session: AsyncSession, batch_id: uuid.UUID
) -> None:
    """Recompute counters from items; finish the batch when none are active."""
    grouped = (
        await session.execute(
            select(DatasetRetirementItem.status, func.count())
            .where(DatasetRetirementItem.batch_id == batch_id)
            .group_by(DatasetRetirementItem.status)
        )
    ).all()
    counts = {row[0]: int(row[1]) for row in grouped}
    active = counts.get(ITEM_PENDING, 0) + counts.get(ITEM_RUNNING, 0)
    batch = await session.get(DatasetRetirementBatch, batch_id)
    if batch is None:
        return
    batch.completed_count = counts.get(ITEM_COMPLETED, 0)
    batch.conflict_count = counts.get(ITEM_CONFLICT, 0)
    batch.retryable_count = counts.get(ITEM_RETRYABLE, 0)
    batch.rejected_count = counts.get(ITEM_REJECTED, 0)
    batch.heartbeat_at = datetime.now(timezone.utc)
    if active == 0 and batch.status != BATCH_COMPLETED:
        batch.status = BATCH_COMPLETED
        batch.finished_at = datetime.now(timezone.utc)
    await session.commit()


async def retry_retirement_batch(session: AsyncSession, batch_id: uuid.UUID) -> int:
    """Reopen retryable/conflict items; completed and rejected stay fixed."""
    result = await session.execute(
        text(
            "UPDATE catalog.dataset_retirement_items "
            "SET status = :pending, error_code = NULL, detail = NULL, "
            "dependents = NULL, finished_at = NULL, updated_at = now() "
            "WHERE batch_id = :batch_id AND status IN ('retryable', 'conflict')"
        ),
        {"batch_id": batch_id, "pending": ITEM_PENDING},
    )
    reopened = int(result.rowcount or 0)
    if reopened:
        # Back to 'pending', not 'running': the claim CAS only takes a running
        # batch whose heartbeat is stale (a crashed driver); a fresh 'running'
        # row would look driven when no driver exists yet.
        await session.execute(
            text(
                "UPDATE catalog.dataset_retirement_batches "
                "SET status = :pending, finished_at = NULL, "
                "heartbeat_at = NULL, last_error = NULL WHERE id = :batch_id"
            ),
            {"batch_id": batch_id, "pending": BATCH_PENDING},
        )
    await session.commit()
    return reopened


async def enqueue_retirement_batch(batch_id: uuid.UUID) -> None:
    """Defer the worker driver. A failed enqueue leaves the batch redrivable."""
    from app.core.db.tenant_session import defer_async_with_tenant

    await defer_async_with_tenant(
        get_catalog_port().retirement_batch_task(),
        batch_id=str(batch_id),
    )


def batch_needs_drive(batch: DatasetRetirementBatch) -> bool:
    """Whether a GET should lazily redrive this batch (pending/stale running)."""
    if batch.status == BATCH_PENDING:
        return True
    if batch.status != BATCH_RUNNING:
        return False
    reference = batch.heartbeat_at or batch.started_at or batch.created_at
    return datetime.now(timezone.utc) - reference > timedelta(
        seconds=BATCH_STALE_SECONDS
    )


async def get_visible_batch(
    session: AsyncSession, batch_id: uuid.UUID, user: Identity
) -> DatasetRetirementBatch | None:
    """The batch when it belongs to this tenant and to the user or an admin."""
    stmt = select(DatasetRetirementBatch).where(DatasetRetirementBatch.id == batch_id)
    tenant_id = current_tenant_var.get()
    if tenant_id is not None:
        stmt = stmt.where(DatasetRetirementBatch.tenant_id == tenant_id)
    batch = (await session.execute(stmt)).scalar_one_or_none()
    if batch is None:
        return None
    if batch.requested_by == user.id:
        return batch
    roles = await get_user_roles(session, user)
    return batch if "admin" in roles else None


async def list_visible_batches(
    session: AsyncSession, user: Identity, *, skip: int, limit: int
) -> tuple[list[DatasetRetirementBatch], int]:
    roles = await get_user_roles(session, user)
    filters = []
    tenant_id = current_tenant_var.get()
    if tenant_id is not None:
        filters.append(DatasetRetirementBatch.tenant_id == tenant_id)
    if "admin" not in roles:
        filters.append(DatasetRetirementBatch.requested_by == user.id)
    total = await session.scalar(
        select(func.count()).select_from(DatasetRetirementBatch).where(*filters)
    )
    rows = await session.execute(
        select(DatasetRetirementBatch)
        .where(*filters)
        .order_by(DatasetRetirementBatch.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    return list(rows.scalars()), int(total or 0)
