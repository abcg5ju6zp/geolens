"""Recovery, bootstrap and garbage collection for publish generations.

- recover: re-enqueue pending builds (defer lost or backoff elapsed) and
  building builds whose worker lease died.
- bootstrap: give existing public maps their first generation, in batches,
  so the frozen read path applies without a data migration.
- gc: after the nonce grace window, delete a terminal generation's own staged
  objects and row, unless a live map column still points at one of its keys.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.db.tenant_session import defer_async_with_tenant
from app.modules.catalog.maps.models import Map, MapPublishAsset, MapPublishGeneration
from app.modules.catalog.maps.publish.service_publish import (
    dispatch_map_publication,
    prepare_map_publication,
)
from app.modules.catalog.maps.publish.state import (
    ORIGIN_BOOTSTRAP,
    STATUS_BUILDING,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_SUPERSEDED,
    map_generations_prefix,
)
from app.modules.catalog.maps.service_shared import _fetch_layer_rows_ordered
from app.platform.extensions import get_catalog_port

logger = structlog.stdlib.get_logger(__name__)

_RECOVER_BATCH = 100
_TERMINAL_GC_STATUSES = (STATUS_SUPERSEDED, STATUS_FAILED, "cancelled")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def recover_map_publish_generations(session: AsyncSession) -> int:
    """Re-enqueue due generations. Returns how many were dispatched.

    Called at worker startup and from the periodic sweep; the conditional
    claim in the build task makes duplicate enqueues harmless.
    """
    lease_cutoff = _utcnow() - timedelta(
        seconds=settings.map_publish_lease_seconds
    )
    due = await session.scalars(
        select(MapPublishGeneration)
        .where(
            or_(
                and_(
                    MapPublishGeneration.status == STATUS_PENDING,
                    or_(
                        MapPublishGeneration.next_attempt_at.is_(None),
                        MapPublishGeneration.next_attempt_at <= _utcnow(),
                    ),
                ),
                and_(
                    MapPublishGeneration.status == STATUS_BUILDING,
                    MapPublishGeneration.leased_at < lease_cutoff,
                ),
            )
        )
        .order_by(MapPublishGeneration.generation_number)
        .limit(_RECOVER_BATCH)
        .with_for_update(skip_locked=True)
    )
    generations = list(due)
    if not generations:
        return 0
    # Locks release at commit; the rows keep their status and the build claim
    # remains the only admission gate.
    await session.commit()

    enqueued = 0
    task = get_catalog_port().map_publish_build_task()
    for generation in generations:
        try:
            await defer_async_with_tenant(
                task,
                generation_id=str(generation.id),
                tenant_id=(
                    str(generation.tenant_id) if generation.tenant_id else None
                ),
            )
            enqueued += 1
        except Exception:  # broad: next sweep retries; enqueue is not the gate
            logger.warning(
                "map_publish_recover_defer_failed",
                generation_id=str(generation.id),
                exc_info=True,
            )
    return enqueued


async def bootstrap_public_maps(
    session_factory: Any, *, limit: int | None = None
) -> int:
    """Create the first generation for public maps that have none."""
    batch = limit or settings.map_publish_bootstrap_batch
    async with session_factory() as session:
        map_ids = list(
            (
                await session.scalars(
                    select(Map.id)
                    .where(
                        Map.visibility == "public",
                        ~exists().where(
                            MapPublishGeneration.map_id == Map.id
                        ),
                    )
                    .order_by(Map.created_at)
                    .limit(batch)
                )
            ).all()
        )

    created = 0
    for map_id in map_ids:
        async with session_factory() as session:
            map_obj = await session.get(Map, map_id)
            if map_obj is None or map_obj.visibility != "public":
                continue
            rows = await _fetch_layer_rows_ordered(session, map_id)
            generation = await prepare_map_publication(
                session, map_obj, rows, origin=ORIGIN_BOOTSTRAP
            )
            await session.commit()
        await dispatch_map_publication(generation)
        if generation is not None:
            created += 1
    return created


async def _key_is_live(session: AsyncSession, keys: list[str]) -> bool:
    if not keys:
        return False
    return (
        await session.scalar(
            select(Map.id)
            .where(
                or_(
                    Map.thumbnail_uri.in_(keys),
                    Map.og_image_uri.in_(keys),
                )
            )
            .limit(1)
        )
    ) is not None


async def gc_map_publish_generations(session: AsyncSession) -> int:
    """Delete terminal generations past the grace window and their objects."""
    from app.platform.storage import get_storage
    from app.platform.storage.titiler_url import resolve_current_storage_key

    cutoff = _utcnow() - timedelta(
        seconds=settings.map_publish_grace_seconds
    )
    terminal_at = func.coalesce(
        MapPublishGeneration.superseded_at,
        MapPublishGeneration.failed_at,
        MapPublishGeneration.updated_at,
    )
    generations = list(
        (
            await session.scalars(
                select(MapPublishGeneration)
                .where(
                    MapPublishGeneration.status.in_(_TERMINAL_GC_STATUSES),
                    terminal_at < cutoff,
                )
                .order_by(MapPublishGeneration.generation_number)
                .limit(_RECOVER_BATCH)
            )
        ).all()
    )

    storage = get_storage()
    reclaimed = 0
    for generation in generations:
        assets = list(
            (
                await session.scalars(
                    select(MapPublishAsset).where(
                        MapPublishAsset.generation_id == generation.id
                    )
                )
            ).all()
        )
        keys = [asset.storage_key for asset in assets if asset.storage_key]
        if await _key_is_live(session, keys):
            # A promoted image on a superseded generation is still served from
            # the map column when the newer generation shipped no replacement.
            continue
        for key in keys:
            try:
                await storage.delete(resolve_current_storage_key(key))
            except Exception:  # broad: orphan costs less than a live delete
                logger.warning(
                    "map_publish_gc_delete_failed",
                    storage_key=key,
                    exc_info=True,
                )
        await _purge_prefix(storage, map_generations_prefix(generation.map_id), generation.id)
        await session.delete(generation)
        await session.commit()
        reclaimed += 1
    return reclaimed


async def _purge_prefix(storage: Any, prefix: str, generation_id: uuid.UUID) -> None:
    """Delete leftovers under one generation's prefix not recorded as assets."""
    needle = f"/{generation_id}/"
    try:
        keys = await storage.list(prefix)
    except Exception:  # broad: list support varies across backends
        logger.warning("map_publish_gc_list_failed", prefix=prefix, exc_info=True)
        return
    for key in keys:
        if needle not in key:
            continue
        try:
            await storage.delete(key)
        except Exception:  # broad: best-effort sweep
            logger.warning(
                "map_publish_gc_prefix_delete_failed",
                storage_key=key,
                exc_info=True,
            )


async def purge_map_publish_namespace(map_id: uuid.UUID) -> None:
    """Best-effort full-prefix removal when a map is deleted."""
    from app.platform.storage import get_storage

    storage = get_storage()
    prefix = map_generations_prefix(map_id)
    try:
        keys = await storage.list(prefix)
    except Exception:  # broad: rows already cascaded; objects are orphans at worst
        logger.warning(
            "map_publish_purge_list_failed",
            map_id=str(map_id),
            exc_info=True,
        )
        return
    for key in keys:
        try:
            await storage.delete(key)
        except Exception:  # broad: never fail a committed map delete
            logger.warning(
                "map_publish_purge_delete_failed",
                storage_key=key,
                exc_info=True,
            )
