"""Durable batch dataset retirement endpoints.

A batch fixes the requesting user and its target set at creation; the worker
then retires each dataset independently, recording completed, conflict,
retryable and rejected outcomes per item. Responses are resumable: the same
idempotency key returns the same batch, a stale batch is lazily re-driven by
GET, and retry never touches a completed item.
"""

from __future__ import annotations

import uuid

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_db
from app.core.identity import Identity
from app.modules.auth.dependencies import require_permission
from app.modules.catalog.datasets.domain.retirement_models import (
    DatasetRetirementBatch,
    DatasetRetirementItem,
)
from app.modules.catalog.datasets.domain.schemas_retirement import (
    RetirementBatchCreateRequest,
    RetirementBatchListResponse,
    RetirementBatchResponse,
    RetirementBatchSummary,
    RetirementItemOutcome,
    RetirementPreviewRequest,
    RetirementPreviewResponse,
    RetirementPreviewItem,
    RetirementRetryResponse,
)
from app.modules.catalog.datasets.domain.service import (
    RetirementIdempotencyMismatch,
    RetirementTarget,
    batch_needs_drive,
    create_retirement_batch,
    enqueue_retirement_batch,
    get_visible_batch,
    list_visible_batches,
    preview_retirement,
    retry_retirement_batch,
)
from app.standards.ogc.errors import ERROR_RESPONSES_WRITE

router = APIRouter(
    prefix="/datasets/retirement-batches",
    tags=["Datasets - Retirement"],
    responses=ERROR_RESPONSES_WRITE,
)
logger = structlog.get_logger(__name__)


def _item_outcome(item: DatasetRetirementItem) -> RetirementItemOutcome:
    return RetirementItemOutcome(
        id=item.id,
        dataset_id=item.dataset_id,
        status=item.status,
        stage=item.stage,
        attempts=item.attempts,
        code=item.error_code,
        detail=item.detail,
        dependents=item.dependents,
        table_name=item.table_name,
        created_at=item.created_at,
        updated_at=item.updated_at,
        finished_at=item.finished_at,
    )


def _batch_response(batch: DatasetRetirementBatch) -> RetirementBatchResponse:
    return RetirementBatchResponse(
        id=batch.id,
        status=batch.status,
        total_count=batch.total_count,
        completed_count=batch.completed_count,
        conflict_count=batch.conflict_count,
        retryable_count=batch.retryable_count,
        rejected_count=batch.rejected_count,
        created_at=batch.created_at,
        started_at=batch.started_at,
        heartbeat_at=batch.heartbeat_at,
        finished_at=batch.finished_at,
        items=[_item_outcome(item) for item in batch.items],
    )


def _targets(body: RetirementPreviewRequest) -> list[RetirementTarget]:
    # Both request bodies share the same targets list shape.
    return [
        RetirementTarget(dataset_id=item.dataset_id, confirm_title=item.confirm_title)
        for item in body.datasets
    ]


async def _redrive_if_needed(db: AsyncSession, batch: DatasetRetirementBatch) -> None:
    """Recover a never-deferred or crashed batch (lost response/worker)."""
    if not batch_needs_drive(batch):
        return
    try:
        await enqueue_retirement_batch(batch.id)
    except (
        Exception
    ):  # broad: a failed redrive leaves the batch retryable via GET/retry
        logger.warning("retirement_batch_redrive_failed", batch_id=str(batch.id))


@router.post(
    "/preview", response_model=RetirementPreviewResponse, include_in_schema=False
)
@router.post("/preview/", response_model=RetirementPreviewResponse)
async def preview_retirement_batch(
    body: RetirementPreviewRequest,
    user: Identity = Depends(require_permission("edit_metadata")),
    db: AsyncSession = Depends(get_db),
) -> RetirementPreviewResponse:
    """Read-only per-dataset retirement impact report."""
    rows = await preview_retirement(db, user, _targets(body))
    return RetirementPreviewResponse(
        items=[RetirementPreviewItem(**row) for row in rows]
    )


@router.post(
    "",
    response_model=RetirementBatchResponse,
    status_code=status.HTTP_201_CREATED,
    include_in_schema=False,
)
@router.post(
    "/", response_model=RetirementBatchResponse, status_code=status.HTTP_201_CREATED
)
async def create_retirement_batch_endpoint(
    body: RetirementBatchCreateRequest,
    idempotency_key: str | None = Header(default=None, max_length=200),
    user: Identity = Depends(require_permission("edit_metadata")),
    db: AsyncSession = Depends(get_db),
) -> RetirementBatchResponse | JSONResponse:
    """Create a retirement batch for a fixed target set and enqueue its worker."""
    try:
        batch, created = await create_retirement_batch(
            db,
            user=user,
            targets=_targets(body),
            idempotency_key=idempotency_key or None,
        )
    except RetirementIdempotencyMismatch as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "idempotency_key_target_mismatch",
                "message": (
                    "This Idempotency-Key was already used with a different "
                    "set of datasets."
                ),
            },
        ) from exc

    if created:
        # A failed enqueue does not fail the request: the batch is durable and
        # GET/retry redrive it if the queue write was lost.
        try:
            await enqueue_retirement_batch(batch.id)
        except Exception:  # broad: durable batch, lazy redrive
            logger.warning("retirement_batch_enqueue_failed", batch_id=str(batch.id))

    fresh = await get_visible_batch(db, batch.id, user)
    response = _batch_response(fresh if fresh is not None else batch)
    if not created:
        await _redrive_if_needed(db, fresh if fresh is not None else batch)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response.model_dump(mode="json"),
        )
    return response


@router.get("", response_model=RetirementBatchListResponse, include_in_schema=False)
@router.get("/", response_model=RetirementBatchListResponse)
async def list_retirement_batches_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    user: Identity = Depends(require_permission("edit_metadata")),
    db: AsyncSession = Depends(get_db),
) -> RetirementBatchListResponse:
    batches, total = await list_visible_batches(db, user, skip=skip, limit=limit)
    return RetirementBatchListResponse(
        items=[
            RetirementBatchSummary(
                id=batch.id,
                status=batch.status,
                total_count=batch.total_count,
                completed_count=batch.completed_count,
                conflict_count=batch.conflict_count,
                retryable_count=batch.retryable_count,
                rejected_count=batch.rejected_count,
                created_at=batch.created_at,
                started_at=batch.started_at,
                finished_at=batch.finished_at,
            )
            for batch in batches
        ],
        total=total,
    )


@router.get(
    "/{batch_id}", response_model=RetirementBatchResponse, include_in_schema=False
)
@router.get("/{batch_id}/", response_model=RetirementBatchResponse)
async def get_retirement_batch_endpoint(
    batch_id: uuid.UUID,
    user: Identity = Depends(require_permission("edit_metadata")),
    db: AsyncSession = Depends(get_db),
) -> RetirementBatchResponse:
    """Get one batch with every item's outcome; lazily redrives a stale one."""
    batch = await get_visible_batch(db, batch_id, user)
    if batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Retirement batch not found",
        )
    await _redrive_if_needed(db, batch)
    return _batch_response(batch)


@router.post(
    "/{batch_id}/retry", response_model=RetirementRetryResponse, include_in_schema=False
)
@router.post("/{batch_id}/retry/", response_model=RetirementRetryResponse)
async def retry_retirement_batch_endpoint(
    batch_id: uuid.UUID,
    user: Identity = Depends(require_permission("edit_metadata")),
    db: AsyncSession = Depends(get_db),
) -> RetirementRetryResponse:
    """Reopen conflicted/retryable items; completed and rejected stay fixed."""
    batch = await get_visible_batch(db, batch_id, user)
    if batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Retirement batch not found",
        )
    reopened = await retry_retirement_batch(db, batch_id)
    if reopened:
        try:
            await enqueue_retirement_batch(batch_id)
        except Exception:  # broad: durable reset, GET redrives the batch
            logger.warning(
                "retirement_batch_retry_enqueue_failed", batch_id=str(batch_id)
            )
    fresh = await get_visible_batch(db, batch_id, user)
    return RetirementRetryResponse(
        reopened=reopened, batch=_batch_response(fresh if fresh is not None else batch)
    )
