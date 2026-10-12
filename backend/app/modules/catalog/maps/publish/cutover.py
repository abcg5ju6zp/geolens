"""Claim, build and atomically activate a publish generation.

The worker entry :func:`run_map_publish_build` is idempotent and fence-safe:
a conditional claim admits at most one builder per generation, asset
confirmation is CAS (only missing assets are rebuilt), and the activation
transaction flips ``maps.published_generation_id`` only when no larger
generation is still alive. A losing generation is marked superseded and its
staged objects are left for GC — it never touches live keys.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.modules.catalog.maps.models import Map, MapPublishAsset, MapPublishGeneration
from app.modules.catalog.maps.publish.assets import materialize_required_assets
from app.modules.catalog.maps.publish.state import (
    ACTIVATABLE_STATUSES,
    ASSET_OG_IMAGE,
    ASSET_THUMBNAIL,
    IMAGE_ASSETS,
    STATUS_ACTIVE,
    STATUS_BUILDING,
    STATUS_CANCELLED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_SUPERSEDED,
)

logger = structlog.stdlib.get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def claim_generation(
    session: AsyncSession,
    generation_id: uuid.UUID,
    worker_id: str,
) -> MapPublishGeneration | None:
    """Conditionally take a pending/stale-building generation.

    Returns the claimed row, or None when it is already active/superseded/
    cancelled or another worker's lease is still fresh.
    """
    lease_cutoff = _utcnow() - timedelta(seconds=settings.map_publish_lease_seconds)
    result = await session.execute(
        update(MapPublishGeneration)
        .where(
            MapPublishGeneration.id == generation_id,
            MapPublishGeneration.status.in_([STATUS_PENDING, STATUS_BUILDING]),
            (MapPublishGeneration.leased_at.is_(None))
            | (MapPublishGeneration.leased_at < lease_cutoff),
        )
        .values(
            status=STATUS_BUILDING,
            claimed_by=worker_id,
            leased_at=_utcnow(),
            attempts=MapPublishGeneration.attempts + 1,
            next_attempt_at=None,
            error_code=None,
        )
        .returning(MapPublishGeneration)
    )
    generation = result.scalar_one_or_none()
    if generation is not None:
        await session.commit()
    return generation


async def _schedule_next_attempt(
    session_factory: Any, generation_id: uuid.UUID, *, fatal: str | None
) -> None:
    async with session_factory() as session:
        generation = await session.get(MapPublishGeneration, generation_id)
        if generation is None or generation.status not in (
            STATUS_PENDING,
            STATUS_BUILDING,
        ):
            return
        if (
            fatal is not None
            or generation.attempts >= settings.map_publish_max_attempts
        ):
            generation.status = STATUS_FAILED
            generation.failed_at = _utcnow()
            generation.error_code = fatal or "attempts_exhausted"
            generation.next_attempt_at = None
        else:
            backoff = min(
                settings.map_publish_backoff_base_seconds
                * 2 ** max(0, generation.attempts - 1),
                settings.map_publish_backoff_max_seconds,
            )
            generation.status = STATUS_PENDING
            generation.leased_at = None
            generation.next_attempt_at = _utcnow() + timedelta(seconds=backoff)
        await session.commit()


async def _activate(session: AsyncSession, generation: MapPublishGeneration) -> str:
    """Single cutover transaction. Returns active/superseded/cancelled/incomplete."""
    map_obj = (
        await session.execute(
            select(Map).where(Map.id == generation.map_id).with_for_update()
        )
    ).scalar_one_or_none()
    if map_obj is None:
        generation.status = STATUS_CANCELLED
        generation.superseded_at = _utcnow()
        await session.commit()
        return STATUS_CANCELLED

    generation = (
        await session.execute(
            select(MapPublishGeneration)
            .where(MapPublishGeneration.id == generation.id)
            .with_for_update()
        )
    ).scalar_one()
    if generation.status in (STATUS_SUPERSEDED, STATUS_CANCELLED):
        await session.commit()
        return generation.status

    # The map went private (or is not public for any other reason): never serve.
    if map_obj.visibility != "public":
        generation.status = STATUS_CANCELLED
        generation.superseded_at = _utcnow()
        await session.commit()
        return STATUS_CANCELLED

    largest = await session.scalar(
        select(
            func.coalesce(func.max(MapPublishGeneration.generation_number), 0)
        ).where(
            MapPublishGeneration.map_id == generation.map_id,
            MapPublishGeneration.status.in_(ACTIVATABLE_STATUSES),
        )
    )
    if largest > generation.generation_number:
        generation.status = STATUS_SUPERSEDED
        generation.superseded_at = _utcnow()
        await session.commit()
        return STATUS_SUPERSEDED

    missing = (
        await session.scalars(
            select(MapPublishAsset.name).where(
                MapPublishAsset.generation_id == generation.id,
                MapPublishAsset.required.is_(True),
                MapPublishAsset.confirmed_at.is_(None),
            )
        )
    ).all()
    if missing:
        # Assets were not all confirmed (a concurrent confirm should have
        # committed); hand back to the retry scheduler without changing state.
        await session.commit()
        return "incomplete"

    now = _utcnow()
    await session.execute(
        update(MapPublishGeneration)
        .where(
            MapPublishGeneration.map_id == generation.map_id,
            MapPublishGeneration.status == STATUS_ACTIVE,
        )
        .values(status=STATUS_SUPERSEDED, superseded_at=now)
    )
    generation.status = STATUS_ACTIVE
    generation.ready_at = now
    generation.activated_at = now
    generation.error_code = None
    map_obj.published_generation_id = generation.id

    # Promote confirmed optional images onto the map columns, so every legacy
    # image reader serves the new capture. Unconfirmed images leave the columns
    # untouched (old image, or none). Freeze updated_at: an image capture is
    # not an edit (mirrors _record_image_capture).
    previous_updated_at = map_obj.updated_at
    for name in IMAGE_ASSETS:
        asset = await session.scalar(
            select(MapPublishAsset).where(
                MapPublishAsset.generation_id == generation.id,
                MapPublishAsset.name == name,
                MapPublishAsset.confirmed_at.is_not(None),
            )
        )
        if asset is None:
            continue
        if name == ASSET_THUMBNAIL:
            map_obj.thumbnail_uri = asset.storage_key
            map_obj.thumbnail_updated_at = now
        elif name == ASSET_OG_IMAGE:
            map_obj.og_image_uri = asset.storage_key
    map_obj.updated_at = previous_updated_at

    await session.commit()
    return STATUS_ACTIVE


async def activate_generation(
    session_factory: Any, generation: MapPublishGeneration
) -> str:
    async with session_factory() as session:
        # Expire detached instance; reload inside the cutover transaction.
        return await _activate(session, generation)


async def run_map_publish_build(
    generation_id: str | uuid.UUID,
    *,
    worker_id: str = "worker",
    session_factory: Any = None,
) -> str:
    """Worker entry: claim, build missing assets, cut over. Never raises."""
    from app.core.db import async_session

    factory = session_factory or async_session
    generation_id = uuid.UUID(str(generation_id))

    async with factory() as session:
        generation = await claim_generation(session, generation_id, worker_id)
    if generation is None:
        return "noop"

    try:
        remaining = await materialize_required_assets(factory, generation)
    except Exception:  # broad: turn an unexpected build crash into a retry
        logger.warning(
            "map_publish_build_error",
            generation_id=str(generation_id),
            exc_info=True,
        )
        await _schedule_next_attempt(factory, generation_id, fatal=None)
        return STATUS_PENDING

    if remaining:
        await _schedule_next_attempt(factory, generation_id, fatal=None)
        return STATUS_PENDING

    outcome = await activate_generation(factory, generation)
    if outcome == "incomplete":
        await _schedule_next_attempt(factory, generation_id, fatal=None)
        return STATUS_PENDING
    return outcome
