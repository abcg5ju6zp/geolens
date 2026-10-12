"""Anonymous, nonce-namespaced reads of a published generation's assets.

The nonce is per-generation, so these URLs are immutable once served:
cutover only moves the map's active pointer, and a superseded generation's
nonce keeps resolving inside the grace window for clients holding the old
document. Going private fails closed regardless of the window.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.dependencies import get_db
from app.modules.catalog.maps.models import Map, MapPublishAsset, MapPublishGeneration
from app.modules.catalog.maps.publish.state import (
    ASSET_OG_IMAGE,
    ASSET_SPRITE_JSON,
    ASSET_SPRITE_PNG,
    ASSET_STYLE,
    ASSET_THUMBNAIL,
    STATUS_ACTIVE,
    STATUS_SUPERSEDED,
)
from app.platform.binary_response import binary_response
from app.platform.storage.titiler_url import resolve_current_storage_key
from app.standards.ogc.errors import ERROR_RESPONSES_PUBLIC

router = APIRouter(prefix="/maps/published", tags=["Maps"], responses=ERROR_RESPONSES_PUBLIC)

_IMMUTABLE = "public, max-age=31536000, immutable"


def _grace_cutoff() -> datetime:
    return datetime.now(timezone.utc) - timedelta(
        seconds=settings.map_publish_grace_seconds
    )


async def _served_generation(
    db: AsyncSession, map_id: uuid.UUID, nonce: str
) -> MapPublishGeneration:
    from sqlalchemy import select

    generation = (
        await db.scalars(
            select(MapPublishGeneration)
            .where(
                MapPublishGeneration.map_id == map_id,
                MapPublishGeneration.read_nonce == nonce,
            )
            .limit(1)
        )
    ).first()
    if generation is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    if generation.status == STATUS_SUPERSEDED:
        if generation.superseded_at is None or (
            generation.superseded_at < _grace_cutoff()
        ):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Not found"
            )
    elif generation.status != STATUS_ACTIVE:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")

    # Fail closed: a map taken private stops serving generations immediately,
    # independent of the nonce grace window.
    map_visibility = await db.scalar(
        select(Map.visibility).where(Map.id == map_id)
    )
    if map_visibility != "public":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return generation


async def _asset_response(db: AsyncSession, generation: MapPublishGeneration, name: str):
    from sqlalchemy import select

    from app.platform.storage.provider import get_storage

    asset = (
        await db.scalars(
            select(MapPublishAsset)
            .where(
                MapPublishAsset.generation_id == generation.id,
                MapPublishAsset.name == name,
                MapPublishAsset.confirmed_at.is_not(None),
            )
            .limit(1)
        )
    ).first()
    if asset is None or not asset.storage_key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    try:
        payload = await get_storage().get(
            resolve_current_storage_key(asset.storage_key)
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Not found"
        ) from exc
    return Response(
        content=payload,
        media_type=asset.media_type or "application/octet-stream",
        headers={"Cache-Control": _IMMUTABLE},
    )


@router.get(
    "/{map_id}/{nonce}/style.json",
    response_class=Response,
    responses={200: binary_response("The frozen MapLibre style document", "application/json")},
)
async def published_style(
    map_id: uuid.UUID,
    nonce: str,
    db: AsyncSession = Depends(get_db),
) -> Response:
    generation = await _served_generation(db, map_id, nonce)
    return await _asset_response(db, generation, ASSET_STYLE)


@router.get("/{map_id}/{nonce}/sprite.json")
async def published_sprite_json(
    map_id: uuid.UUID,
    nonce: str,
    db: AsyncSession = Depends(get_db),
) -> Response:
    generation = await _served_generation(db, map_id, nonce)
    return await _asset_response(db, generation, ASSET_SPRITE_JSON)


@router.get(
    "/{map_id}/{nonce}/sprite.png",
    response_class=Response,
    responses={200: binary_response("The frozen sprite sheet", "image/png")},
)
async def published_sprite_png(
    map_id: uuid.UUID,
    nonce: str,
    db: AsyncSession = Depends(get_db),
) -> Response:
    generation = await _served_generation(db, map_id, nonce)
    return await _asset_response(db, generation, ASSET_SPRITE_PNG)


@router.get("/{map_id}/{nonce}/sprite@2x.json", include_in_schema=False)
async def published_sprite_json_2x(
    map_id: uuid.UUID,
    nonce: str,
    db: AsyncSession = Depends(get_db),
) -> Response:
    generation = await _served_generation(db, map_id, nonce)
    return await _asset_response(db, generation, ASSET_SPRITE_JSON)


@router.get("/{map_id}/{nonce}/sprite@2x.png", include_in_schema=False)
async def published_sprite_png_2x(
    map_id: uuid.UUID,
    nonce: str,
    db: AsyncSession = Depends(get_db),
) -> Response:
    generation = await _served_generation(db, map_id, nonce)
    return await _asset_response(db, generation, ASSET_SPRITE_PNG)


@router.get(
    "/{map_id}/{nonce}/thumbnail",
    response_class=Response,
    responses={200: binary_response("The generation thumbnail", "image/jpeg")},
)
async def published_thumbnail(
    map_id: uuid.UUID,
    nonce: str,
    db: AsyncSession = Depends(get_db),
) -> Response:
    generation = await _served_generation(db, map_id, nonce)
    return await _asset_response(db, generation, ASSET_THUMBNAIL)


@router.get(
    "/{map_id}/{nonce}/og-image",
    response_class=Response,
    responses={200: binary_response("The generation social card", "image/jpeg")},
)
async def published_og_image(
    map_id: uuid.UUID,
    nonce: str,
    db: AsyncSession = Depends(get_db),
) -> Response:
    generation = await _served_generation(db, map_id, nonce)
    return await _asset_response(db, generation, ASSET_OG_IMAGE)
