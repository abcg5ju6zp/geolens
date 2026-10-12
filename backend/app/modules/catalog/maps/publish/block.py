"""Assemble the optional ``publish`` block of map API responses."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.catalog.maps.models import Map, MapPublishGeneration
from app.modules.catalog.maps.publish.service_publish import (
    assets_by_name,
    latest_generation,
)
from app.modules.catalog.maps.publish.state import (
    ASSET_OG_IMAGE,
    ASSET_SPRITE_JSON,
    ASSET_SPRITE_PNG,
    ASSET_STYLE,
    ASSET_THUMBNAIL,
    REQUIRED_ASSETS,
    STATUS_ACTIVE,
    published_asset_url,
    sprite_base_url,
)
from app.modules.catalog.maps.schemas import (
    MapPublishAssetUrls,
    MapPublishBlock,
)


async def publish_block_for_generation(
    session: AsyncSession,
    generation: MapPublishGeneration,
) -> MapPublishBlock:
    """Build the block for an active generation from its confirmed assets."""
    assets = await assets_by_name(session, generation.id)
    urls = MapPublishAssetUrls()

    def confirmed(name: str) -> bool:
        return name in assets and assets[name].confirmed_at is not None

    if confirmed("style"):
        urls.style_url = published_asset_url(
            generation.map_id, generation.read_nonce, "style"
        )
    if confirmed("sprite_json") and confirmed("sprite_png"):
        urls.sprite_url = sprite_base_url(generation.map_id, generation.read_nonce)
        urls.sprite_json_url = published_asset_url(
            generation.map_id, generation.read_nonce, "sprite_json"
        )
    if confirmed("thumbnail"):
        urls.thumbnail_url = published_asset_url(
            generation.map_id, generation.read_nonce, "thumbnail"
        )
    if confirmed("og_image"):
        urls.og_image_url = published_asset_url(
            generation.map_id, generation.read_nonce, "og_image"
        )
    missing = [
        name
        for name in REQUIRED_ASSETS
        if name not in assets or assets[name].confirmed_at is None
    ]
    return MapPublishBlock(
        generation_id=generation.id,
        generation_number=generation.generation_number,
        status=generation.status,
        missing_required=missing,
        assets=urls,
    )


async def build_publish_block(
    session: AsyncSession, map_obj: Map
) -> MapPublishBlock | None:
    """The publish block for an owner/builder response, or None.

    URLs only exist for an active generation (only its nonce resolves). The
    latest generation's id is returned regardless so the client can stage the
    next thumbnail capture at it.
    """
    if map_obj.visibility != "public":
        return None
    generation = await latest_generation(session, map_obj.id)
    if generation is None:
        return None
    assets = await assets_by_name(session, generation.id)
    missing = [
        name
        for name in REQUIRED_ASSETS
        if name not in assets or assets[name].confirmed_at is None
    ]
    urls = MapPublishAssetUrls()
    if generation.status == STATUS_ACTIVE:
        confirmed = {
            name: row for name, row in assets.items() if row.confirmed_at is not None
        }
        if ASSET_STYLE in confirmed:
            urls.style_url = published_asset_url(
                map_obj.id, generation.read_nonce, ASSET_STYLE
            )
        if ASSET_SPRITE_JSON in confirmed and ASSET_SPRITE_PNG in confirmed:
            urls.sprite_url = sprite_base_url(map_obj.id, generation.read_nonce)
            urls.sprite_json_url = published_asset_url(
                map_obj.id, generation.read_nonce, ASSET_SPRITE_JSON
            )
        if ASSET_THUMBNAIL in confirmed:
            urls.thumbnail_url = published_asset_url(
                map_obj.id, generation.read_nonce, ASSET_THUMBNAIL
            )
        if ASSET_OG_IMAGE in confirmed:
            urls.og_image_url = published_asset_url(
                map_obj.id, generation.read_nonce, ASSET_OG_IMAGE
            )
    return MapPublishBlock(
        generation_id=generation.id,
        generation_number=generation.generation_number,
        status=generation.status,
        missing_required=missing,
        assets=urls,
    )
