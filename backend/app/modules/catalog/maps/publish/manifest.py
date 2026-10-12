"""Immutable content snapshots for publish generations.

The manifest freezes the content of one save: ordered layers with the dataset
publication/tile-cache versions they were saved against, and the view
parameters. Name, description and visibility deliberately stay live — they
are metadata, and metadata-only saves never create a generation.

Two rebuilds are possible without touching live rows:

- ``manifest_to_style_inputs`` feeds :func:`build_maplibre_style`, so a worker
  can render the staged style document after the editor has saved again.
- ``shared_layers_from_manifest`` reproduces the layer list
  ``get_shared_map`` returns, so anonymous readers see the pinned layers.
"""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from typing import Any, Iterable

from app.core.geo import extent_to_bbox
from app.core.record_types import RASTER_FAMILY_RECORD_TYPES
from app.core.tile_scope import tile_template_query
from app.modules.catalog.maps.models import Map
from app.modules.catalog.maps.schemas import MapLayerResponse
from app.modules.catalog.maps.service_shared import LayerRow
from app.modules.catalog.maps.style_json import DEFAULT_ARROW_ICON
from app.modules.catalog.maps.style_import import GEOLENS_SPRITE_ID

MANIFEST_SCHEMA = 1

_SPRITE_PREFIX = f"{GEOLENS_SPRITE_ID}:"

_VIEW_FIELDS = (
    "center_lng",
    "center_lat",
    "zoom",
    "bearing",
    "pitch",
    "basemap_style",
    "show_basemap_labels",
    "basemap_config",
    "terrain_config",
    "plugins",
    "legend_title",
)

# Layer style/config blobs frozen into the manifest.
_LAYER_CONTENT_FIELDS = (
    "paint",
    "layout",
    "filter",
    "label_config",
    "popup_config",
    "style_config",
)


def _view_from_map(map_obj: Map) -> dict[str, Any]:
    return {field: getattr(map_obj, field) for field in _VIEW_FIELDS}


def _freeze_layer(row: LayerRow) -> dict[str, Any]:
    layer = row.layer
    return {
        "id": str(layer.id),
        "dataset_id": str(layer.dataset_id),
        "sort_order": layer.sort_order,
        "visible": layer.visible,
        "opacity": layer.opacity,
        "layer_type": layer.layer_type,
        "display_name": layer.display_name,
        "show_in_legend": layer.show_in_legend,
        **{field: getattr(layer, field) for field in _LAYER_CONTENT_FIELDS},
        "dataset": {
            "name": row.title,
            "geometry_type": row.geometry_type,
            "table_name": row.table_name,
            "column_info": row.column_info,
            "record_type": row.record_type,
            "feature_count": row.feature_count,
            "is_3d": row.is_3d,
            "is_dem": row.is_dem,
            "dem_vertical_units": row.dem_vertical_units,
            "band_count": row.band_count,
            "tile_cache_version": row.tile_version,
            "publication_version": row.publication_version,
            "visibility": row.visibility,
            "attribution": row.attribution,
            "extent_bbox": extent_to_bbox(row.spatial_extent),
        },
    }


def _bare_slug(icon: Any) -> str | None:
    if not isinstance(icon, str):
        return None
    return icon[len(_SPRITE_PREFIX) :] if icon.startswith(_SPRITE_PREFIX) else icon


def extract_icon_slugs(layer_rows: Iterable[LayerRow]) -> list[str]:
    """Icon slugs a save references from its stored layer style_config.

    Mirrors the symbol rules in style_json: the builder symbol section is
    overlaid by the plain one, ``iconImage`` (default ``marker`` is added by
    the emitter only when a symbol layer exists, so it is not collected
    implicitly here), category icons, and the line-arrow companion. The sprite
    build intersects this with the catalog, so unknown or deleted slugs cost
    nothing.
    """
    slugs: list[str] = []

    def add(icon: Any) -> None:
        slug = _bare_slug(icon)
        if slug and slug not in slugs:
            slugs.append(slug)

    for row in layer_rows:
        config = row.layer.style_config or {}
        if not isinstance(config, dict):
            continue
        symbol = {
            **((config.get("builder") or {}).get("symbol") or {}),
            **(config.get("symbol") or {}),
        }
        add(symbol.get("iconImage") or symbol.get("icon_image"))
        categories = symbol.get("categories")
        if isinstance(categories, list):
            for entry in categories:
                if isinstance(entry, dict):
                    add(entry.get("icon"))
        if config.get("render_mode") == "arrow":
            add(DEFAULT_ARROW_ICON)
    return slugs


def build_manifest(map_obj: Map, layer_rows: Iterable[LayerRow]) -> dict[str, Any]:
    """Freeze the map view and ordered layers into a manifest dict."""
    rows = list(layer_rows)
    return {
        "schema": MANIFEST_SCHEMA,
        "map_id": str(map_obj.id),
        "view": _view_from_map(map_obj),
        "layers": [_freeze_layer(row) for row in rows],
        "icon_slugs": extract_icon_slugs(rows),
        "mvt_source_layer_prefix": "data",
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )


def _fingerprint_parts(manifest: dict[str, Any]) -> dict[str, Any]:
    """Content identity, independent of map-layer row ids (replaced each save)."""
    return {
        "view": manifest["view"],
        "layers": [
            {
                "dataset_id": entry["dataset_id"],
                "sort_order": entry["sort_order"],
                "visible": entry["visible"],
                "opacity": entry["opacity"],
                "layer_type": entry["layer_type"],
                "display_name": entry["display_name"],
                "show_in_legend": entry["show_in_legend"],
                **{field: entry[field] for field in _LAYER_CONTENT_FIELDS},
                "dataset": {
                    "table_name": entry["dataset"]["table_name"],
                    "tile_cache_version": entry["dataset"]["tile_cache_version"],
                    "publication_version": entry["dataset"]["publication_version"],
                },
            }
            for entry in manifest["layers"]
        ],
    }


def compute_fingerprint(manifest: dict[str, Any]) -> str:
    """Stable sha256 over the pinned content; identical saves share a value."""
    return hashlib.sha256(
        _canonical_json(_fingerprint_parts(manifest)).encode("utf-8")
    ).hexdigest()


def manifest_to_layer_responses(manifest: dict[str, Any]) -> list[MapLayerResponse]:
    """Reconstruct MapLayerResponse values without reading live rows."""
    import uuid

    responses: list[MapLayerResponse] = []
    for entry in manifest["layers"]:
        dataset = entry["dataset"]
        responses.append(
            MapLayerResponse(
                id=uuid.UUID(entry["id"]),
                dataset_id=uuid.UUID(entry["dataset_id"]),
                dataset_name=dataset["name"] or "",
                dataset_geometry_type=dataset["geometry_type"],
                dataset_table_name=dataset["table_name"] or "",
                dataset_extent_bbox=dataset["extent_bbox"],
                dataset_column_info=dataset["column_info"],
                dataset_feature_count=dataset["feature_count"],
                display_name=entry["display_name"],
                sort_order=entry["sort_order"],
                visible=entry["visible"],
                opacity=entry["opacity"],
                paint=entry["paint"] or {},
                layout=entry["layout"] or {},
                layer_type=entry["layer_type"],
                dataset_record_type=dataset["record_type"],
                filter=entry["filter"],
                label_config=entry["label_config"],
                popup_config=entry["popup_config"],
                style_config=entry["style_config"],
                show_in_legend=entry["show_in_legend"],
                is_3d=dataset["is_3d"],
                is_dem=dataset["is_dem"],
                dem_vertical_units=dataset["dem_vertical_units"],
                band_count=dataset["band_count"],
                tile_version=dataset["tile_cache_version"],
                publication_version=dataset["publication_version"],
                dataset_visibility=dataset["visibility"],
                dataset_attribution=dataset["attribution"],
            )
        )
    return responses


def manifest_to_style_inputs(
    manifest: dict[str, Any], *, name: str, description: str | None
) -> tuple[SimpleNamespace, list[MapLayerResponse]]:
    """Build the ``(map_obj, layers)`` pair ``build_maplibre_style`` consumes."""
    view = dict(manifest["view"])
    map_view = SimpleNamespace(
        id=manifest["map_id"],
        name=name,
        description=description,
        terrain_config=view.get("terrain_config"),
        basemap_config=view.get("basemap_config"),
        **{
            field: value
            for field, value in view.items()
            if field not in ("terrain_config", "basemap_config")
        },
    )
    return map_view, manifest_to_layer_responses(manifest)


def _shared_tile_url(entry: dict[str, Any]) -> str:
    dataset = entry["dataset"]
    if (
        entry["layer_type"] == "raster_geolens"
        or dataset["record_type"] in RASTER_FAMILY_RECORD_TYPES
    ):
        return f"/raster-tiles/{entry['dataset_id']}/tiles/{{z}}/{{x}}/{{y}}.png" + (
            tile_template_query(
                dataset["tile_cache_version"], dataset["publication_version"]
            )
        )
    return f"/tiles/data.{dataset['table_name']}/{{z}}/{{x}}/{{y}}.pbf" + (
        tile_template_query(None, dataset["publication_version"])
    )


def shared_layer_dict(entry: dict[str, Any]) -> dict[str, Any]:
    """One frozen layer in the shape ``get_shared_map`` returns today."""
    dataset = entry["dataset"]
    return {
        "id": entry["id"],
        "dataset_id": entry["dataset_id"],
        "dataset_name": dataset["name"],
        "display_name": entry["display_name"],
        "table_name": dataset["table_name"],
        "geometry_type": dataset["geometry_type"],
        "column_info": dataset["column_info"],
        "sort_order": entry["sort_order"],
        "visible": entry["visible"],
        "opacity": entry["opacity"],
        "paint": entry["paint"],
        "layout": entry["layout"],
        "layer_type": entry["layer_type"] or "vector_geolens",
        "dataset_record_type": dataset["record_type"],
        "filter": entry["filter"],
        "label_config": entry["label_config"],
        "popup_config": entry["popup_config"],
        "style_config": entry["style_config"],
        "show_in_legend": entry["show_in_legend"],
        "tile_url": _shared_tile_url(entry),
        "is_dem": dataset["is_dem"],
        "dem_vertical_units": dataset["dem_vertical_units"],
        "is_3d": dataset["is_3d"],
        "feature_count": dataset["feature_count"],
        "tile_version": dataset["tile_cache_version"],
        "dataset_attribution": dataset["attribution"],
        "dataset_extent_bbox": dataset["extent_bbox"],
    }


def shared_layers_from_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [shared_layer_dict(entry) for entry in manifest["layers"]]
