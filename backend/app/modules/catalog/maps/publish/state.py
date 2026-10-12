"""Generation statuses, asset specs and generation-scoped key/URL builders."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

STATUS_PENDING = "pending"
STATUS_BUILDING = "building"
STATUS_READY = "ready"
STATUS_ACTIVE = "active"
STATUS_SUPERSEDED = "superseded"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

# A save replaces generations still on these tracks.
LIVE_STATUSES = (
    STATUS_PENDING,
    STATUS_BUILDING,
    STATUS_READY,
    STATUS_ACTIVE,
)
# Fence pool: a generation yields if a larger number is still able to activate.
ACTIVATABLE_STATUSES = (STATUS_PENDING, STATUS_BUILDING, STATUS_READY, STATUS_ACTIVE)
TERMINAL_STATUSES = (STATUS_SUPERSEDED, STATUS_FAILED, STATUS_CANCELLED)

ORIGIN_SAVE = "save"
ORIGIN_BOOTSTRAP = "bootstrap"

ASSET_STYLE = "style"
ASSET_SPRITE_JSON = "sprite_json"
ASSET_SPRITE_PNG = "sprite_png"
ASSET_THUMBNAIL = "thumbnail"
ASSET_OG_IMAGE = "og_image"
REQUIRED_ASSETS = (ASSET_STYLE, ASSET_SPRITE_JSON, ASSET_SPRITE_PNG)
IMAGE_ASSETS = (ASSET_THUMBNAIL, ASSET_OG_IMAGE)


@dataclass(frozen=True)
class AssetSpec:
    name: str
    filename: str
    media_type: str
    required: bool


_ASSET_SPECS: tuple[AssetSpec, ...] = (
    AssetSpec(ASSET_STYLE, "style.json", "application/json", True),
    AssetSpec(ASSET_SPRITE_JSON, "sprite.json", "application/json", True),
    AssetSpec(ASSET_SPRITE_PNG, "sprite.png", "image/png", True),
    AssetSpec(ASSET_THUMBNAIL, "thumbnail.jpg", "image/jpeg", False),
    AssetSpec(ASSET_OG_IMAGE, "og-image.jpg", "image/jpeg", False),
)
ASSET_SPECS_BY_NAME = {spec.name: spec for spec in _ASSET_SPECS}
ASSET_SPECS = _ASSET_SPECS


def generation_prefix(map_id: uuid.UUID, generation_id: uuid.UUID) -> str:
    """The isolated storage namespace for one generation's objects."""
    return f"maps/publish/{map_id}/{generation_id}"


def generation_asset_key(
    map_id: uuid.UUID, generation_id: uuid.UUID, name: str
) -> str:
    spec = ASSET_SPECS_BY_NAME[name]
    return f"{generation_prefix(map_id, generation_id)}/{spec.filename}"


def map_generations_prefix(map_id: uuid.UUID) -> str:
    return f"maps/publish/{map_id}/"


def published_asset_path(map_id: uuid.UUID, nonce: str, name: str) -> str:
    """Public URL path segment after /maps/published/... for one nonce asset."""
    filename = ASSET_SPECS_BY_NAME[name].filename
    if name in (ASSET_SPRITE_JSON, ASSET_SPRITE_PNG):
        leaf = "sprite.json" if filename == "sprite.json" else "sprite.png"
        return f"{map_id}/{nonce}/{leaf}"
    if name == ASSET_STYLE:
        return f"{map_id}/{nonce}/style.json"
    leaf = "thumbnail" if name == ASSET_THUMBNAIL else "og-image"
    return f"{map_id}/{nonce}/{leaf}"


def published_asset_url(map_id: uuid.UUID, nonce: str, name: str) -> str:
    return f"/maps/published/{published_asset_path(map_id, nonce, name)}"


def sprite_base_url(map_id: uuid.UUID, nonce: str) -> str:
    # MapLibre appends .json/.png (and @2x variants) to this base.
    return f"/maps/published/{map_id}/{nonce}/sprite"
