"""Render, validate and confirm one generation's staged assets.

Style and sprite are pure rebuilds from the frozen manifest; they write only
to the generation's isolated ``maps/publish/{map}/{gen}/`` namespace. Each
asset is confirmed independently (a CAS on ``confirmed_at IS NULL``), so a
retry after a single-asset failure never rebuilds or rewrites a confirmed one.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from io import BytesIO
from typing import Any

import structlog
from PIL import Image
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.catalog.maps.models import (
    Map,
    MapIconAsset,
    MapPublishAsset,
    MapPublishGeneration,
)
from app.modules.catalog.maps.publish.manifest import manifest_to_style_inputs
from app.modules.catalog.maps.publish.state import (
    ASSET_SPRITE_JSON,
    ASSET_SPRITE_PNG,
    ASSET_STYLE,
    STATUS_CANCELLED,
    STATUS_SUPERSEDED,
    sprite_base_url,
)

logger = structlog.stdlib.get_logger(__name__)

# Sprites are single-ratio 24px cells; @2x requests serve the same bytes.
TERMINAL_BUILD_STATUSES = (STATUS_SUPERSEDED, STATUS_CANCELLED)


class AssetBuildError(Exception):
    """An asset rendered, validated or round-tripped unsafely. Machine code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _validate_json_asset(name: str, payload: bytes) -> dict[str, Any]:
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssetBuildError(f"{name}_invalid_json") from exc
    if not isinstance(document, dict):
        raise AssetBuildError(f"{name}_not_object")
    return document


def _validate_style(payload: bytes, *, nonce: str, map_id: uuid.UUID) -> None:
    document = _validate_json_asset(ASSET_STYLE, payload)
    if document.get("version") != 8:
        raise AssetBuildError("style_bad_version")
    if not isinstance(document.get("sources"), dict) or not isinstance(
        document.get("layers"), list
    ):
        raise AssetBuildError("style_bad_shape")
    sprite = document.get("sprite")
    expected = sprite_base_url(map_id, nonce)
    if not any(
        isinstance(item, dict) and item.get("url") == expected
        for item in (sprite if isinstance(sprite, list) else [sprite])
    ):
        raise AssetBuildError("style_sprite_not_pinned")
    for source in document["sources"].values():
        for tile_url in source.get("tiles", []):
            # Frozen documents must never embed an expiring HMAC tile URL.
            if "sig=" in tile_url or "exp=" in tile_url:
                raise AssetBuildError("style_tile_url_signed")


def _validate_png(payload: bytes) -> None:
    try:
        image = Image.open(BytesIO(payload))
        image.verify()
        # verify() leaves the file unusable; reopen to force decode of size.
        image = Image.open(BytesIO(payload))
        image.load()
    except Exception as exc:  # broad: Pillow raises varied decode errors
        raise AssetBuildError("sprite_png_invalid") from exc
    if image.width < 1 or image.height != 24:
        raise AssetBuildError("sprite_png_bad_dimensions")


def icon_slugs_from_style(style: dict[str, Any]) -> set[str]:
    """Slugs referenced by ``icon-image`` layouts in a built style document.

    Walks match expressions recursively; ``geolens:`` sprite-prefixed names
    are reduced to bare slugs. Non-string expressions (data-driven fallbacks)
    can't name a catalog icon and are skipped.
    """
    slugs: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            icon = node.get("icon-image")
            if isinstance(icon, str) and icon.startswith("geolens:"):
                slugs.add(icon.split(":", 1)[1])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(style.get("layers", []))
    return slugs


async def _build_sprite_payloads(
    session: AsyncSession, slugs: set[str]
) -> tuple[bytes, bytes]:
    # Lazy import: sprites imports the maps service facade, and importing it at
    # module load would circle back through this package's facade exports.
    from app.modules.catalog.maps import sprites

    builtin_icons = [
        sprites._builtin_response(icon)
        for icon in sprites.DEFAULT_ICONS
        if icon.slug in slugs
    ]
    uploaded_rows = list(
        (
            await session.scalars(
                select(MapIconAsset).where(MapIconAsset.slug.in_(slugs))
            )
        ).all()
    )
    uploaded_rows.sort(key=lambda asset: asset.slug)
    icons = [*builtin_icons, *[sprites._asset_response(row) for row in uploaded_rows]]
    if not icons:
        return (
            json.dumps(sprites._build_sprite_index([]), separators=(",", ":")).encode(),
            sprites._blank_png(sprites.SPRITE_CELL_SIZE),
        )
    index = sprites._build_sprite_index(icons)
    png = await sprites._render_sprite_png(icons, uploaded_rows)
    return json.dumps(index, separators=(",", ":")).encode(), png


async def _render_required_payloads(
    session: AsyncSession, generation: MapPublishGeneration
) -> dict[str, bytes]:
    """Render style then sprite (the sprite set derives from the style)."""
    map_obj = await session.get(Map, generation.map_id)
    if map_obj is None:
        raise AssetBuildError("map_gone")
    map_view, layer_responses = manifest_to_style_inputs(
        generation.manifest, name=map_obj.name, description=map_obj.description
    )
    from app.modules.catalog.maps.style_json import build_maplibre_style

    style = build_maplibre_style(
        map_view,
        layer_responses,
        sprite_url=sprite_base_url(generation.map_id, generation.read_nonce),
        tile_template_mode="public_unsigned",
    )
    style_payload = json.dumps(style, separators=(",", ":")).encode("utf-8")
    _validate_style(
        style_payload, nonce=generation.read_nonce, map_id=generation.map_id
    )

    slugs = icon_slugs_from_style(style) | set(
        generation.manifest.get("icon_slugs", [])
    )
    sprite_json, sprite_png = await _build_sprite_payloads(session, slugs)
    _validate_json_asset(ASSET_SPRITE_JSON, sprite_json)
    _validate_png(sprite_png)
    return {
        ASSET_STYLE: style_payload,
        ASSET_SPRITE_JSON: sprite_json,
        ASSET_SPRITE_PNG: sprite_png,
    }


async def _unconfirmed_required(
    session: AsyncSession, generation_id: uuid.UUID
) -> list[MapPublishAsset]:
    rows = await session.scalars(
        select(MapPublishAsset)
        .where(
            MapPublishAsset.generation_id == generation_id,
            MapPublishAsset.required.is_(True),
            MapPublishAsset.confirmed_at.is_(None),
        )
        .order_by(MapPublishAsset.name)
    )
    return list(rows)


async def confirm_asset(
    session: AsyncSession,
    generation_id: uuid.UUID,
    name: str,
    *,
    checksum: str,
    size_bytes: int,
) -> bool:
    """CAS the confirmed bit; False when another attempt confirmed first."""
    result = await session.execute(
        update(MapPublishAsset)
        .where(
            MapPublishAsset.generation_id == generation_id,
            MapPublishAsset.name == name,
            MapPublishAsset.confirmed_at.is_(None),
        )
        .values(
            confirmed_at=func.now(),
            checksum=checksum,
            size_bytes=size_bytes,
        )
    )
    return result.rowcount == 1


async def materialize_required_assets(
    session_factory,
    generation: MapPublishGeneration,
) -> list[str]:
    """Build+stage+confirm every still-unconfirmed required asset.

    Own session per asset so each confirmation commits independently: a crash
    mid-asset leaves the others confirmed and skips them on retry. Returns the
    names still unconfirmed after this pass. ``session_factory`` opens the
    worker's own async sessions (it must not share a request session).
    """
    from app.platform.storage import get_storage
    from app.platform.storage.titiler_url import resolve_current_storage_key

    storage = get_storage()
    remaining: list[str] = []
    payloads: dict[str, bytes] | None = None

    async with session_factory() as session:
        unconfirmed = await _unconfirmed_required(session, generation.id)
        names = [row.name for row in unconfirmed]
        del unconfirmed

    for name in names:
        async with session_factory() as session:
            fresh = await session.get(MapPublishGeneration, generation.id)
            if fresh is None or fresh.status in TERMINAL_BUILD_STATUSES:
                return names  # superseded/cancelled: stop touching anything
            asset = await session.scalar(
                select(MapPublishAsset).where(
                    MapPublishAsset.generation_id == generation.id,
                    MapPublishAsset.name == name,
                )
            )
            if asset is None or asset.confirmed_at is not None:
                continue
            asset_id = asset.id
            asset.attempts += 1
            try:
                if payloads is None:
                    payloads = await _render_required_payloads(session, fresh)
                payload = payloads[name]
                physical_key = resolve_current_storage_key(asset.storage_key)
                await storage.put(physical_key, payload)
                roundtrip = await storage.get(physical_key)
                if roundtrip != payload:
                    raise AssetBuildError(f"{name}_readback_mismatch")
                if not await confirm_asset(
                    session,
                    generation.id,
                    name,
                    checksum=sha256_hex(payload),
                    size_bytes=len(payload),
                ):
                    continue
                await session.commit()
            except Exception as exc:  # broad: one asset must not fail its siblings
                # Rollback expires every ORM instance, so keep only scalars and
                # write the error code through a fresh session/core statement.
                await session.rollback()
                code = exc.code if isinstance(exc, AssetBuildError) else "build_failed"
                logger.warning(
                    "map_publish_asset_failed",
                    generation_id=str(generation.id),
                    asset=name,
                    error=code,
                    exc_info=not isinstance(exc, AssetBuildError),
                )
                async with session_factory() as retry_session:
                    await retry_session.execute(
                        update(MapPublishAsset)
                        .where(MapPublishAsset.id == asset_id)
                        .values(
                            last_error_code=code,
                            attempts=MapPublishAsset.attempts + 1,
                        )
                    )
                    await retry_session.commit()
                remaining.append(name)
    return remaining
