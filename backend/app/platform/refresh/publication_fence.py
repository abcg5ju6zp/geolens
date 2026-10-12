"""Re-verify a keyed refresh run at publication, inside the held transaction.

The admission fingerprint pins the source configuration a run was admitted
against; this is the last check before its swap commits, so a run whose
source rebind raced it, whose local record was edited, or which another actor
already settled cannot publish onto the current configuration. Non-keyed
runs (manual doors, initial imports) carry no fingerprint and pass through.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.platform.refresh.models import DatasetRefreshRun
from app.platform.refresh.verification import (
    canonical_service_source_binding_fingerprint,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class PublicationFenceError(RuntimeError):
    """A keyed run may not publish; ``code`` is its refresh failure code."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


# Compatibility alias for the name the ingest strategies used before the
# check moved to this shared module.
RefreshPublicationFenceError = PublicationFenceError


async def enforce_publication_fence(
    session: "AsyncSession",
    *,
    job_id: uuid.UUID,
    dataset: object,
) -> None:
    """Refuse a late swap of a keyed run whose admission facts no longer hold.

    Runs without a stored binding fingerprint are manual/legacy doors and
    pass. The service binding is canonicalized through the same function the
    admission used, so a changed URL, layer or credential shape mismatches.
    """
    from app.platform.extensions import get_processing_port

    run = await session.scalar(
        select(DatasetRefreshRun).where(DatasetRefreshRun.ingest_job_id == job_id)
    )
    if run is None or run.source_binding_fingerprint is None:
        return
    if run.status not in ("pending", "running"):
        raise PublicationFenceError(
            "run_settled", "The refresh run was already settled."
        )

    if run.origin_kind != "service":
        # Only the service door's binding has a canonical fingerprint. The
        # other doors re-verify their own subject under the held rows (tile
        # version / STAC binding), and upload/restore have no remote binding.
        return

    record_cls = get_processing_port().get_record_orm_class()
    current_origin, current_record_modified_at = (
        await session.execute(
            select(dataset.__class__.origin_ref, record_cls.updated_at)
            .join(record_cls, record_cls.id == dataset.record_id)
            .where(dataset.__class__.id == dataset.id)
        )
    ).one()
    if not isinstance(current_origin, dict):
        raise PublicationFenceError(
            "source_changed", "Refresh source changed before publication."
        )
    try:
        current_fingerprint = canonical_service_source_binding_fingerprint(
            current_origin
        )
    except ValueError as exc:
        raise PublicationFenceError(
            "source_changed", "Refresh source changed before publication."
        ) from exc
    if current_fingerprint != run.source_binding_fingerprint:
        raise PublicationFenceError(
            "source_changed", "Refresh source changed before publication."
        )
    if (
        run.local_edit_baseline is not None
        and current_record_modified_at is not None
        and current_record_modified_at > run.local_edit_baseline
    ):
        raise PublicationFenceError(
            "local_edits_changed",
            "Dataset changed locally before refresh publication.",
        )
