"""Create, supersede and enqueue map publish generations (save-side logic).

Every function here runs inside the caller's transaction unless its name says
otherwise. The save endpoints call :func:`prepare_map_publication` on the
flushed save state and :func:`dispatch_map_publication` after commit; the
worker-side build/cutover lives in ``assets``/``cutover``.
"""

from __future__ import annotations

import secrets
import uuid

import structlog
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db.tenant_session import defer_async_with_tenant
from app.modules.catalog.maps.models import Map, MapPublishAsset, MapPublishGeneration
from app.modules.catalog.maps.publish.manifest import (
    build_manifest,
    compute_fingerprint,
)
from app.modules.catalog.maps.publish.state import (
    ASSET_SPECS,
    ORIGIN_SAVE,
    REQUIRED_ASSETS,
    STATUS_ACTIVE,
    STATUS_BUILDING,
    STATUS_CANCELLED,
    STATUS_PENDING,
    STATUS_READY,
    STATUS_SUPERSEDED,
    generation_asset_key,
)
from app.modules.catalog.maps.service_shared import LayerRow
from app.platform.extensions import get_catalog_port

logger = structlog.stdlib.get_logger(__name__)

# Statuses a new save replaces. A generation already `active` stays live until
# the replacement wins cutover; the cutover fence (not this update) demotes it.
_REPLACEABLE_STATUSES = (STATUS_PENDING, STATUS_BUILDING, STATUS_READY)


async def _lock_map_for_publish(session: AsyncSession, map_id: uuid.UUID) -> None:
    """Serialize generation allocation for one map.

    Save paths normally hold only a KEY SHARE lock, which is compatible across
    concurrent saves; number allocation needs a real exclusive so two saves
    cannot both insert the one pending/building row the partial unique index
    allows. Upgrading the same transaction's KEY SHARE to FOR UPDATE is safe.
    """
    await session.execute(
        select(Map.id).where(Map.id == map_id).with_for_update()
    )


async def prepare_map_publication(
    session: AsyncSession,
    map_obj: Map,
    layer_rows: list[LayerRow],
    *,
    created_by: uuid.UUID | None = None,
    origin: str = ORIGIN_SAVE,
) -> MapPublishGeneration | None:
    """Create the next generation for a public map's current content.

    Returns the new pending generation, or None when the map is not public or
    the active generation already serves identical content. Does not commit.
    """
    if map_obj.visibility != "public":
        return None
    manifest = build_manifest(map_obj, layer_rows)
    fingerprint = compute_fingerprint(manifest)

    await _lock_map_for_publish(session, map_obj.id)

    active = await session.scalar(
        select(MapPublishGeneration).where(
            MapPublishGeneration.map_id == map_obj.id,
            MapPublishGeneration.status == STATUS_ACTIVE,
        )
    )
    if active is not None and active.content_fingerprint == fingerprint:
        return None

    await session.execute(
        update(MapPublishGeneration)
        .where(
            MapPublishGeneration.map_id == map_obj.id,
            MapPublishGeneration.status.in_(_REPLACEABLE_STATUSES),
        )
        .values(
            status=STATUS_SUPERSEDED,
            superseded_at=func.now(),
            next_attempt_at=None,
        )
    )
    # coalesce against the whole history so numbers stay monotonic.
    next_number = (
        await session.scalar(
            select(
                func.coalesce(func.max(MapPublishGeneration.generation_number), 0)
            ).where(MapPublishGeneration.map_id == map_obj.id)
        )
    ) + 1

    generation = MapPublishGeneration(
        map_id=map_obj.id,
        generation_number=next_number,
        status=STATUS_PENDING,
        manifest=manifest,
        content_fingerprint=fingerprint,
        read_nonce=secrets.token_hex(16),
        origin=origin,
        created_by=created_by if created_by is not None else map_obj.created_by,
        tenant_id=map_obj.tenant_id,
    )
    session.add(generation)
    await session.flush()

    for spec in ASSET_SPECS:
        session.add(
            MapPublishAsset(
                generation_id=generation.id,
                name=spec.name,
                required=spec.required,
                storage_key=generation_asset_key(map_obj.id, generation.id, spec.name),
                media_type=spec.media_type,
            )
        )
    await session.flush()
    return generation


async def cancel_in_flight_publications(
    session: AsyncSession, map_id: uuid.UUID
) -> int:
    """Cancel generations still able to activate (map went private/deleted).

    Active generations are left for history but stop serving immediately: the
    nonce routes re-check ``maps.visibility`` fail-closed.
    """
    result = await session.execute(
        update(MapPublishGeneration)
        .where(
            MapPublishGeneration.map_id == map_id,
            MapPublishGeneration.status.in_(_REPLACEABLE_STATUSES),
        )
        .values(
            status=STATUS_CANCELLED,
            superseded_at=func.now(),
            next_attempt_at=None,
        )
    )
    return result.rowcount


async def dispatch_map_publication(
    generation: MapPublishGeneration | None,
) -> None:
    """Enqueue the build AFTER the creating transaction committed.

    A failure here must not fail the (already committed) save: the row is
    pending and the worker recovery sweep re-enqueues it.
    """
    if generation is None:
        return
    try:
        await defer_async_with_tenant(
            get_catalog_port().map_publish_build_task(),
            generation_id=str(generation.id),
            tenant_id=str(generation.tenant_id) if generation.tenant_id else None,
        )
    except Exception:  # broad: deferral failures are recovered by the sweep
        logger.warning(
            "map_publish_dispatch_failed",
            generation_id=str(generation.id),
            map_id=str(generation.map_id),
            exc_info=True,
        )


async def latest_generation(
    session: AsyncSession, map_id: uuid.UUID
) -> MapPublishGeneration | None:
    return await session.scalar(
        select(MapPublishGeneration)
        .where(MapPublishGeneration.map_id == map_id)
        .order_by(MapPublishGeneration.generation_number.desc())
        .limit(1)
    )


async def active_generation(
    session: AsyncSession, map_id: uuid.UUID
) -> MapPublishGeneration | None:
    return await session.scalar(
        select(MapPublishGeneration).where(
            MapPublishGeneration.map_id == map_id,
            MapPublishGeneration.status == STATUS_ACTIVE,
        )
    )


async def generation_by_nonce(
    session: AsyncSession, map_id: uuid.UUID, nonce: str
) -> MapPublishGeneration | None:
    return await session.scalar(
        select(MapPublishGeneration).where(
            MapPublishGeneration.map_id == map_id,
            MapPublishGeneration.read_nonce == nonce,
        )
    )


async def assets_by_name(
    session: AsyncSession, generation_id: uuid.UUID
) -> dict[str, MapPublishAsset]:
    rows = await session.scalars(
        select(MapPublishAsset).where(
            MapPublishAsset.generation_id == generation_id
        )
    )
    return {row.name: row for row in rows}


async def missing_required_assets(
    session: AsyncSession, generation_id: uuid.UUID
) -> list[str]:
    rows = await session.scalars(
        select(MapPublishAsset.name)
        .where(
            MapPublishAsset.generation_id == generation_id,
            MapPublishAsset.name.in_(REQUIRED_ASSETS),
            MapPublishAsset.confirmed_at.is_(None),
        )
        .order_by(MapPublishAsset.name)
    )
    return list(rows)


__all__ = [
    "active_generation",
    "assets_by_name",
    "cancel_in_flight_publications",
    "dispatch_map_publication",
    "generation_by_nonce",
    "latest_generation",
    "missing_required_assets",
    "prepare_map_publication",
]
