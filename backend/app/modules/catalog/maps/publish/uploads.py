"""Stage thumbnail/OG captures into a publish generation.

An upload either confirms an in-flight generation's optional image (the
cutover transaction promotes it), promotes straight onto the map columns when
the target generation is already active and still current, or lands on a
superseded generation without ever moving the map pointer — that last arm is
what keeps a late capture from publishing a stale image over a newer save.
"""

from __future__ import annotations

import uuid
from typing import NamedTuple

import structlog
from fastapi import HTTPException, status
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.catalog.maps.models import Map, MapPublishAsset, MapPublishGeneration
from app.modules.catalog.maps.publish.assets import sha256_hex
from app.modules.catalog.maps.publish.state import (
    ACTIVATABLE_STATUSES,
    ASSET_THUMBNAIL,
    STATUS_ACTIVE,
)
from app.modules.catalog.maps.service_crud import (
    lock_map_for_asset_write,
    map_asset_publication,
)
from app.platform.storage.titiler_url import resolve_current_storage_key

logger = structlog.stdlib.get_logger(__name__)


class StagedImage(NamedTuple):
    outcome: str  # "staged" | "promoted" | "superseded"
    previous_key: str | None


async def resolve_image_generation(
    session: AsyncSession,
    map_obj: Map,
    asset_name: str,
    generation_id: uuid.UUID | None,
) -> MapPublishGeneration | None:
    """Pick the generation an image upload targets.

    Explicit id wins (validated against the map); without one, public maps
    target their latest generation and private maps keep the legacy path.
    """
    if generation_id is not None:
        generation = await session.get(MapPublishGeneration, generation_id)
        if generation is None or generation.map_id != map_obj.id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Publish generation not found",
            )
        return generation
    if map_obj.visibility != "public":
        return None
    return await session.scalar(
        select(MapPublishGeneration)
        .where(MapPublishGeneration.map_id == map_obj.id)
        .order_by(MapPublishGeneration.generation_number.desc())
        .limit(1)
    )


async def _larger_live_number(
    session: AsyncSession, generation: MapPublishGeneration
) -> int | None:
    return await session.scalar(
        select(func.max(MapPublishGeneration.generation_number)).where(
            MapPublishGeneration.map_id == generation.map_id,
            MapPublishGeneration.id != generation.id,
            MapPublishGeneration.status.in_(ACTIVATABLE_STATUSES),
        )
    )


async def stage_generation_image(
    session: AsyncSession,
    *,
    map_obj: Map,
    asset_name: str,
    image_bytes: bytes,
    media_type: str,
    storage,
    generation_id: uuid.UUID | None,
) -> StagedImage | None:
    """Confirm an image into a generation, under the map asset write lock.

    Owns its object/row publication and commit. Returns None when no
    generation applies (private map, or explicit legacy write), so the caller
    falls back to the direct map-column path.
    """
    generation = await resolve_image_generation(
        session, map_obj, asset_name, generation_id
    )
    if generation is None:
        return None

    async with map_asset_publication() as publication:
        locked = await lock_map_for_asset_write(session, map_obj.id)
        # Re-read under the lock so a concurrent cutover cannot be missed.
        generation = await session.get(MapPublishGeneration, generation.id)
        asset = await session.scalar(
            select(MapPublishAsset).where(
                MapPublishAsset.generation_id == generation.id,
                MapPublishAsset.name == asset_name,
            )
        )
        if asset is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Publish generation not found",
            )

        physical_key = resolve_current_storage_key(asset.storage_key)
        publication.record(physical_key)
        try:
            await storage.put(physical_key, image_bytes)
        except Exception as exc:  # broad: storage backends raise varied SDK errors
            logger.exception(
                "map_publish_image_upload_failed",
                map_id=str(map_obj.id),
                asset=asset_name,
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Map image storage unavailable",
            ) from exc

        asset.media_type = media_type
        asset.checksum = sha256_hex(image_bytes)
        asset.size_bytes = len(image_bytes)
        asset.confirmed_at = func.now()
        asset.last_error_code = None

        previous_key: str | None = None
        outcome = "superseded"
        promote_columns = False
        if generation.status == STATUS_ACTIVE:
            larger = await _larger_live_number(session, generation)
            if larger is None or larger < generation.generation_number:
                promote_columns = True
                outcome = "promoted"
        elif generation.status in ("pending", "building", "ready"):
            outcome = "staged"
            # Before the first cutover there is no active generation, so legacy
            # readers (and deployments/tests without a worker drain) must keep
            # seeing the capture now. Stage it AND move the map column; the
            # eventual cutover promotes the same key idempotently.
            if map_obj.published_generation_id is None:
                promote_columns = True
        if promote_columns:
            values: dict[str, object] = {"updated_at": Map.updated_at}
            if asset_name == ASSET_THUMBNAIL:
                values["thumbnail_uri"] = asset.storage_key
                values["thumbnail_updated_at"] = func.now()
                previous_key = locked.thumbnail_uri
            else:
                values["og_image_uri"] = asset.storage_key
                previous_key = locked.og_image_uri
            await session.execute(
                update(Map)
                .where(Map.id == map_obj.id)
                .values(**values)
                .execution_options(synchronize_session=False)
            )

        publication.committing()
        await session.commit()
        publication.settled()

    return StagedImage(outcome, previous_key)
