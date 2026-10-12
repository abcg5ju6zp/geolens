"""Assemble anonymous/shared map responses from an active generation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.modules.catalog.maps.models import Map, MapPublishGeneration
from app.modules.catalog.maps.publish.block import publish_block_for_generation
from app.modules.catalog.maps.publish.manifest import shared_layers_from_manifest
from app.modules.catalog.maps.style_json import project_terrain_config
from app.platform.extensions import get_catalog_port

if TYPE_CHECKING:
    from fastapi import Request

    from app.core.identity import Identity
    from sqlalchemy.ext.asyncio import AsyncSession


async def build_shared_snapshot(
    session: "AsyncSession",
    map_obj: Map,
    generation: MapPublishGeneration,
    *,
    embed_scope: set,
    request: "Request | None",
    user: "Identity | None",
    user_roles: set[str] | None,
    allowed_origins: list[str] | None,
) -> tuple[dict, list[dict], list[str] | None, bool]:
    """Same 4-tuple shape as ``get_shared_map``, but pinned to a generation.

    Embed-scoped layers stay live (they are a capability widening, not part of
    the anonymous snapshot) and are unioned in exactly as the live path does;
    everything else comes from the frozen manifest.
    """
    from app.modules.catalog.datasets.domain.models import Dataset, Record
    from app.modules.catalog.maps.models import MapLayer
    from app.modules.catalog.maps.service_public import _build_shared_layer_dict

    manifest = generation.manifest
    layers = shared_layers_from_manifest(manifest)
    snapshot_ids = {layer["id"] for layer in layers}
    visible_dataset_ids = {layer["dataset_id"] for layer in layers}
    has_non_public = False

    if embed_scope:
        from app.modules.catalog.maps.service_shared import _extract_dem_vertical_units

        RasterAsset = get_catalog_port().raster_asset_orm_class()
        stmt = (
            _snapshot_join(MapLayer, Map, Dataset, Record, RasterAsset)
            .where(
                MapLayer.map_id == map_obj.id,
                Map.visibility == "public",
                Dataset.id.in_(embed_scope),
            )
            .order_by(MapLayer.sort_order, MapLayer.id)
        )
        for row in (await session.execute(stmt)).all():
            layer = row[1]
            if str(layer.id) in snapshot_ids:
                continue
            layer_dict, is_non_public = _build_shared_layer_dict(
                layer,
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                _extract_dem_vertical_units(row[11]),
                row[12],
                row[13],
                row[14],
                row[15],
            )
            if is_non_public:
                has_non_public = True
            visible_dataset_ids.add(str(layer.dataset_id))
            layers.append(layer_dict)
        layers.sort(key=lambda item: (item["sort_order"], item["id"]))

    view = manifest.get("view", {})
    terrain_config = project_terrain_config(
        view.get("terrain_config"), visible_dataset_ids
    )
    map_data = {
        "name": map_obj.name,
        "description": map_obj.description,
        "center_lng": view.get("center_lng") or 0.0,
        "center_lat": view.get("center_lat") or 0.0,
        "zoom": view.get("zoom") or 2.0,
        "bearing": view.get("bearing") or 0,
        "pitch": view.get("pitch") or 0,
        "basemap_style": view.get(
            "basemap_style", "openfreemap-positron"
        ),
        "show_basemap_labels": view.get("show_basemap_labels", True),
        "basemap_config": view.get("basemap_config"),
        "terrain_config": terrain_config,
        "has_non_public_layers": has_non_public,
        "legend_title": view.get("legend_title"),
        "publish": await publish_block_for_generation(session, generation),
    }
    return map_data, layers, allowed_origins, bool(embed_scope)


def _snapshot_join(MapLayer, Map, Dataset, Record, RasterAsset):
    """Mirror get_shared_map's base SELECT for embed-scope extra layers."""
    from sqlalchemy import select

    return (
        select(
            Map,
            MapLayer,
            Record.title,
            Dataset.geometry_type,
            Dataset.table_name,
            Dataset.column_info,
            Record.visibility,
            Record.record_type,
            Dataset.is_3d,
            Dataset.feature_count,
            RasterAsset.is_dem,
            RasterAsset.band_info,
            Dataset.tile_cache_version,
            Dataset.publication_version,
            Record.attribution,
            Record.spatial_extent,
        )
        .join(Map, Map.id == MapLayer.map_id)
        .join(Dataset, MapLayer.dataset_id == Dataset.id)
        .join(Record, Dataset.record_id == Record.id)
        .outerjoin(RasterAsset, RasterAsset.dataset_id == Dataset.id)
    )
