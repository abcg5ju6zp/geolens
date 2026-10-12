"""Publish generation lifecycle: snapshots, per-asset confirmation, fencing.

These tests drive the service layer directly (plus one API smoke for the
shared/nonce read paths) against the real test database and local storage.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import select

from app.modules.catalog.maps.models import Map, MapLayer, MapPublishGeneration
from app.modules.catalog.maps.publish import state as pub_state
from app.modules.catalog.maps.publish.cutover import run_map_publish_build
from app.modules.catalog.maps.publish.manifest import (
    build_manifest,
    compute_fingerprint,
    manifest_to_style_inputs,
    shared_layers_from_manifest,
)
from app.modules.catalog.maps.publish.service_publish import (
    assets_by_name,
    prepare_map_publication,
)
from app.modules.catalog.maps.service_shared import _fetch_layer_rows_ordered

from tests.factories import create_dataset, get_user_id


async def _public_map(session, user_id, *, datasets=1):
    dataset = await create_dataset(session, created_by=user_id, geometry_type="Point")
    map_obj = Map(
        name="Pub",
        description=None,
        visibility="public",
        center_lng=10.0,
        center_lat=20.0,
        zoom=5.0,
        bearing=0,
        pitch=0,
        basemap_style="openfreemap-positron",
        show_basemap_labels=True,
        created_by=user_id,
    )
    session.add(map_obj)
    await session.flush()
    layer = MapLayer(
        map_id=map_obj.id,
        dataset_id=dataset.id,
        sort_order=0,
        visible=True,
        opacity=1.0,
        paint={},
        layout={},
        layer_type="vector_geolens",
    )
    session.add(layer)
    await session.commit()
    await session.refresh(map_obj)
    return map_obj, dataset, layer


# ---------------------------------------------------------------------------
# Manifest / fingerprint
# ---------------------------------------------------------------------------


async def test_manifest_pins_view_layers_and_dataset_versions(test_db_session):
    user_id = await get_user_id(test_db_session, "admin")
    map_obj, dataset, _layer = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)

    manifest = build_manifest(map_obj, rows)
    assert manifest["view"]["zoom"] == 5.0
    assert manifest["layers"][0]["dataset"]["table_name"] == dataset.table_name
    assert manifest["layers"][0]["dataset"]["publication_version"] == (
        dataset.publication_version
    )

    rebuilt = shared_layers_from_manifest(manifest)
    assert rebuilt[0]["tile_url"].startswith(f"/tiles/data.{dataset.table_name}/")
    assert "sig=" not in rebuilt[0]["tile_url"]


async def test_fingerprint_ignores_layer_row_ids_but_tracks_view(test_db_session):
    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    fp1 = compute_fingerprint(build_manifest(map_obj, rows))

    # A view-port change changes content identity.
    map_obj.zoom = 9.0
    fp2 = compute_fingerprint(build_manifest(map_obj, rows))
    assert fp1 != fp2


async def test_manifest_rebuilds_unsigned_style_with_nonce_sprite(test_db_session):
    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    manifest = build_manifest(map_obj, rows)
    from app.modules.catalog.maps.publish.state import sprite_base_url
    from app.modules.catalog.maps.style_json import build_maplibre_style

    map_view, layer_responses = manifest_to_style_inputs(
        manifest, name=map_obj.name, description=None
    )
    style = build_maplibre_style(
        map_view,
        layer_responses,
        sprite_url=sprite_base_url(map_obj.id, "nonce123"),
        tile_template_mode="public_unsigned",
    )
    assert style["sprite"][0]["url"].endswith("/nonce123/sprite")
    for source in style["sources"].values():
        for tile_url in source.get("tiles", []):
            assert "sig=" not in tile_url
            assert "exp=" not in tile_url


# ---------------------------------------------------------------------------
# Prepare / supersede
# ---------------------------------------------------------------------------


async def test_prepare_is_noop_for_private_map(test_db_session):
    user_id = await get_user_id(test_db_session, "admin")
    map_obj = Map(
        name="Priv",
        visibility="private",
        bearing=0,
        pitch=0,
        basemap_style="x",
        show_basemap_labels=True,
        created_by=user_id,
    )
    test_db_session.add(map_obj)
    await test_db_session.commit()
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    assert await prepare_map_publication(test_db_session, map_obj, rows) is None


async def test_prepare_creates_generation_assets_and_supersedes_previous(
    test_db_session,
):
    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)

    first = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    assert first is not None and first.generation_number == 1
    assets = await assets_by_name(test_db_session, first.id)
    assert set(assets) == {
        "style",
        "sprite_json",
        "sprite_png",
        "thumbnail",
        "og_image",
    }
    assert assets["style"].storage_key.endswith("/style.json")
    assert assets["style"].required and not assets["thumbnail"].required
    assert len(first.read_nonce) == 32

    map_obj.zoom = 7.0
    second = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    assert second.generation_number == 2

    await test_db_session.refresh(first)
    assert first.status == pub_state.STATUS_SUPERSEDED
    # Only the replacement is in-flight.
    in_flight = list(
        (
            await test_db_session.scalars(
                select(MapPublishGeneration).where(
                    MapPublishGeneration.map_id == map_obj.id,
                    MapPublishGeneration.status.in_(
                        [pub_state.STATUS_PENDING, pub_state.STATUS_BUILDING]
                    ),
                )
            )
        )
    )
    assert [g.id for g in in_flight] == [second.id]


# ---------------------------------------------------------------------------
# Build + cutover + fencing
# ---------------------------------------------------------------------------


def _session_factory():
    import app.core.db as db_module

    return db_module.async_session()


async def test_build_confirms_assets_and_activates_generation(test_db_session):
    from app.platform.storage.provider import get_storage

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()

    outcome = await run_map_publish_build(
        generation.id, session_factory=_session_factory
    )
    assert outcome == pub_state.STATUS_ACTIVE

    await test_db_session.refresh(map_obj)
    assert map_obj.published_generation_id == generation.id

    storage = get_storage()
    assets = await assets_by_name(test_db_session, generation.id)
    for name in pub_state.REQUIRED_ASSETS:
        row = assets[name]
        assert row.confirmed_at is not None and row.checksum and row.size_bytes
        assert await storage.get(row.storage_key)

    # Idempotent re-run is a noop (already active, not claimable).
    again = await run_map_publish_build(generation.id, session_factory=_session_factory)
    assert again == "noop"


async def test_older_generation_loses_cutover_fence(test_db_session):
    from app.modules.catalog.maps.publish.cutover import activate_generation

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)

    first = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    # A newer save lands while the first worker holds its lease: prepare
    # supersedes the first and inserts the larger-numbered pending gen.
    map_obj.zoom = 3.0
    second = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()

    # The old worker cannot re-claim a superseded generation.
    outcome = await run_map_publish_build(first.id, session_factory=_session_factory)
    assert outcome == "noop"

    # Direct fence: a ready older generation yields to the larger live one.
    first.status = pub_state.STATUS_READY
    first.superseded_at = None
    first.leased_at = None
    now = datetime.now(timezone.utc)
    for asset in (await assets_by_name(test_db_session, first.id)).values():
        if asset.confirmed_at is None:
            asset.confirmed_at = now
    await test_db_session.commit()
    outcome = await activate_generation(
        _session_factory,
        SimpleNamespace(
            id=first.id,
            map_id=map_obj.id,
            generation_number=first.generation_number,
        ),
    )
    assert outcome == pub_state.STATUS_SUPERSEDED
    await test_db_session.refresh(map_obj)
    assert map_obj.published_generation_id is None

    # The newer generation builds and activates, demoting nothing yet.
    outcome = await run_map_publish_build(second.id, session_factory=_session_factory)
    assert outcome == pub_state.STATUS_ACTIVE
    await test_db_session.refresh(map_obj)
    assert map_obj.published_generation_id == second.id


async def test_failed_asset_retries_only_the_missing_one(test_db_session, monkeypatch):
    from app.platform.storage.local import LocalStorageProvider

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()

    real_put = LocalStorageProvider.put
    failed = {"done": False}

    async def flaky_put(self, key, data):
        if not failed["done"] and key.endswith("sprite.json"):
            failed["done"] = True
            raise OSError("storage down")
        return await real_put(self, key, data)

    monkeypatch.setattr(LocalStorageProvider, "put", flaky_put)

    outcome = await run_map_publish_build(
        generation.id, session_factory=_session_factory
    )
    # First pass failed one asset (alphabetical order makes that sprite_json):
    # scheduled for retry; its siblings still confirmed independently.
    assert outcome == pub_state.STATUS_PENDING
    assets = await assets_by_name(test_db_session, generation.id)
    assert assets["sprite_json"].confirmed_at is None
    assert assets["style"].confirmed_at is not None
    assert assets["sprite_png"].confirmed_at is not None

    monkeypatch.undo()
    outcome = await run_map_publish_build(
        generation.id, session_factory=_session_factory
    )
    test_db_session.expire_all()
    await test_db_session.refresh(generation)
    assets = await assets_by_name(test_db_session, generation.id)
    assert outcome == pub_state.STATUS_ACTIVE, (
        outcome,
        generation.status,
        generation.attempts,
        generation.error_code,
        {n: (a.confirmed_at is not None, a.last_error_code) for n, a in assets.items()},
    )
    assert all(
        assets[name].confirmed_at is not None for name in pub_state.REQUIRED_ASSETS
    )


# ---------------------------------------------------------------------------
# Image staging
# ---------------------------------------------------------------------------

_PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


async def test_thumbnail_staged_then_promoted_by_cutover(test_db_session):
    from app.modules.catalog.maps.publish.uploads import stage_generation_image
    from app.platform.storage.provider import get_storage

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()

    storage = get_storage()
    staged = await stage_generation_image(
        test_db_session,
        map_obj=map_obj,
        asset_name="thumbnail",
        image_bytes=_PNG_1X1,
        media_type="image/png",
        storage=storage,
        generation_id=generation.id,
    )
    # First-ever publish: stage AND move the column so legacy readers serve it
    # before the cutover runs.
    assert staged.outcome == "staged" and staged.previous_key is None
    assets = await assets_by_name(test_db_session, generation.id)
    assert assets["thumbnail"].confirmed_at is not None
    await test_db_session.refresh(map_obj)
    assert map_obj.thumbnail_uri == assets["thumbnail"].storage_key

    outcome = await run_map_publish_build(
        generation.id, session_factory=_session_factory
    )
    assert outcome == pub_state.STATUS_ACTIVE
    await test_db_session.refresh(map_obj)
    assert map_obj.thumbnail_uri == assets["thumbnail"].storage_key


async def test_upload_to_superseded_generation_never_promotes(test_db_session):
    from app.modules.catalog.maps.publish.uploads import stage_generation_image
    from app.platform.storage.provider import get_storage

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    first = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    map_obj.zoom = 4.0
    second = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    assert first.status == pub_state.STATUS_SUPERSEDED

    staged = await stage_generation_image(
        test_db_session,
        map_obj=map_obj,
        asset_name="thumbnail",
        image_bytes=_PNG_1X1,
        media_type="image/png",
        storage=get_storage(),
        generation_id=first.id,
    )
    assert staged.outcome == "superseded" and staged.previous_key is None
    await test_db_session.refresh(map_obj)
    assert map_obj.thumbnail_uri is None
    # The old generation may keep the bytes (GC reclaims), but no map pointer.
    assets = await assets_by_name(test_db_session, first.id)
    assert assets["thumbnail"].confirmed_at is not None
    assert second.id != first.id


# ---------------------------------------------------------------------------
# GC / bootstrap
# ---------------------------------------------------------------------------


async def test_gc_reclaims_only_expired_terminal_generations(test_db_session):
    from app.modules.catalog.maps.publish.gc import gc_map_publish_generations
    from app.platform.storage.provider import get_storage

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    # Simulate a fully built, then superseded, generation with an object.
    storage = get_storage()
    assets = await assets_by_name(test_db_session, generation.id)
    await storage.put(assets["style"].storage_key, b"{}")
    generation.status = pub_state.STATUS_SUPERSEDED
    generation.superseded_at = generation.created_at
    await test_db_session.commit()

    # Recent terminal generations are kept for the nonce grace window.
    assert await gc_map_publish_generations(test_db_session) == 0

    generation.superseded_at = generation.created_at - timedelta(days=30)
    await test_db_session.commit()
    assert await gc_map_publish_generations(test_db_session) == 1
    assert await test_db_session.get(MapPublishGeneration, generation.id) is None


async def test_gc_skips_generation_whose_image_the_map_still_serves(test_db_session):
    from app.modules.catalog.maps.publish.gc import gc_map_publish_generations

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    assets = await assets_by_name(test_db_session, generation.id)
    # Newer generation shipped no replacement image, so the map column still
    # points into the old (superseded) generation's namespace.
    map_obj.thumbnail_uri = assets["thumbnail"].storage_key
    generation.status = pub_state.STATUS_SUPERSEDED
    generation.superseded_at = generation.created_at - timedelta(days=30)
    await test_db_session.commit()

    assert await gc_map_publish_generations(test_db_session) == 0
    assert await test_db_session.get(MapPublishGeneration, generation.id)


async def test_shared_and_nonce_reads_serve_active_generation(
    client, admin_auth_header, test_db_session
):
    """End-to-end read path: frozen snapshot on /shared, assets on the nonce."""
    import base64

    admin_id = await get_user_id(test_db_session, "admin")
    dataset = await create_dataset(test_db_session, created_by=admin_id)
    create_resp = await client.post(
        "/maps/", json={"name": "Nonce Map"}, headers=admin_auth_header
    )
    assert create_resp.status_code == 201
    map_id = create_resp.json()["id"]
    add = await client.post(
        f"/maps/{map_id}/layers",
        json={"dataset_id": str(dataset.id)},
        headers=admin_auth_header,
    )
    assert add.status_code == 201, add.text
    put = await client.put(
        f"/maps/{map_id}",
        json={"visibility": "public", "zoom": 4},
        headers=admin_auth_header,
    )
    assert put.status_code == 200, put.text
    generation_id = put.json()["publish"]["generation_id"]

    # No worker in the test harness: run the idempotent build entry directly.
    outcome = await run_map_publish_build(
        generation_id, session_factory=_session_factory
    )
    assert outcome == pub_state.STATUS_ACTIVE

    share = await client.post(f"/maps/{map_id}/share/", headers=admin_auth_header)
    token = share.json()["token"]
    shared = await client.get(f"/maps/shared/{token}")
    assert shared.status_code == 200
    body = shared.json()
    assert body["publish"]["status"] == "active"
    assert body["publish"]["assets"]["style_url"]
    nonce = body["publish"]["assets"]["style_url"].split("/")[4]

    style = await client.get(f"/maps/published/{map_id}/{nonce}/style.json")
    assert style.status_code == 200
    assert style.headers["cache-control"] == "public, max-age=31536000, immutable"
    assert b"sig=" not in style.content
    assert (
        await client.get(f"/maps/published/{map_id}/{nonce}/sprite.json")
    ).status_code == 200
    assert (
        await client.get(f"/maps/published/{map_id}/{nonce}/sprite.png")
    ).status_code == 200
    assert (
        await client.get(f"/maps/published/{map_id}/{nonce}/sprite@2x.png")
    ).status_code == 200
    # Optional image not captured yet.
    assert (
        await client.get(f"/maps/published/{map_id}/{nonce}/thumbnail")
    ).status_code == 404

    # Late capture against the active generation promotes straight away.
    data_uri = "data:image/png;base64," + base64.b64encode(_PNG_1X1).decode()
    upload = await client.put(
        f"/maps/{map_id}/thumbnail/",
        json={"data_uri": data_uri},
        headers=admin_auth_header,
    )
    assert upload.status_code == 204
    assert upload.headers["x-geolens-publish"] == "promoted"
    assert (
        await client.get(f"/maps/published/{map_id}/{nonce}/thumbnail")
    ).status_code == 200

    # Going private fails closed for the nonce, regardless of grace.
    await client.put(
        f"/maps/{map_id}", json={"visibility": "private"}, headers=admin_auth_header
    )
    assert (
        await client.get(f"/maps/published/{map_id}/{nonce}/style.json")
    ).status_code == 404


async def test_shared_snapshot_assembles_response_without_embed(test_db_session):
    from app.modules.catalog.maps.publish.shared_read import build_shared_snapshot

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()
    outcome = await run_map_publish_build(
        generation.id, session_factory=_session_factory
    )
    assert outcome == pub_state.STATUS_ACTIVE

    map_id = map_obj.id
    generation_id = generation.id
    test_db_session.expire_all()
    map_data, layers, origins, embed = await build_shared_snapshot(
        test_db_session,
        await test_db_session.get(Map, map_id),
        await test_db_session.get(MapPublishGeneration, generation_id),
        embed_scope=set(),
        request=None,
        user=None,
        user_roles=set(),
        allowed_origins=[],
    )
    assert embed is False
    assert len(layers) == 1
    assert map_data["publish"].status == pub_state.STATUS_ACTIVE
    assert map_data["publish"].assets.sprite_url
    assert origins == []


async def test_cutover_cancels_when_map_is_private_or_gone(test_db_session):
    from types import SimpleNamespace

    from app.modules.catalog.maps.publish.cutover import activate_generation

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()

    map_obj.visibility = "private"
    await test_db_session.commit()
    outcome = await activate_generation(
        _session_factory,
        SimpleNamespace(id=generation.id, map_id=map_obj.id, generation_number=1),
    )
    assert outcome == pub_state.STATUS_CANCELLED


async def test_exhausted_retries_fail_the_generation(test_db_session, monkeypatch):
    from app.platform.storage.local import LocalStorageProvider

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    generation = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()

    async def always_fail(self, key, data):
        raise OSError("storage down")

    monkeypatch.setattr(LocalStorageProvider, "put", always_fail)
    monkeypatch.setattr(
        "app.core.config.settings.map_publish_max_attempts", 1, raising=False
    )
    gen_id = generation.id
    outcome = await run_map_publish_build(
        gen_id, session_factory=_session_factory
    )
    assert outcome == pub_state.STATUS_PENDING
    test_db_session.expire_all()
    failed = await test_db_session.get(MapPublishGeneration, gen_id)
    assert failed.status == pub_state.STATUS_FAILED
    assert failed.error_code


async def test_recover_enqueues_due_generations_only(
    test_db_session, monkeypatch
):
    from app.modules.catalog.maps.publish import gc as gc_module
    from app.modules.catalog.maps.publish.gc import recover_map_publish_generations

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)
    rows = await _fetch_layer_rows_ordered(test_db_session, map_obj.id)
    due = await prepare_map_publication(test_db_session, map_obj, rows)
    await test_db_session.commit()

    # Second map with a generation parked behind a backoff window.
    map_obj2, _, _ = await _public_map(test_db_session, user_id)
    rows2 = await _fetch_layer_rows_ordered(test_db_session, map_obj2.id)
    waiting = await prepare_map_publication(test_db_session, map_obj2, rows2)
    waiting.status = pub_state.STATUS_PENDING
    waiting.leased_at = None
    waiting.next_attempt_at = datetime.now(timezone.utc) + timedelta(hours=1)
    await test_db_session.commit()

    enqueued: list[str] = []

    async def fake_defer(task, **kwargs):
        enqueued.append(kwargs["generation_id"])

    # A fresh builder lease is not stolen.
    map_obj3, _, _ = await _public_map(test_db_session, user_id)
    rows3 = await _fetch_layer_rows_ordered(test_db_session, map_obj3.id)
    leased = await prepare_map_publication(test_db_session, map_obj3, rows3)
    leased.status = pub_state.STATUS_BUILDING
    leased.leased_at = datetime.now(timezone.utc)
    await test_db_session.commit()

    monkeypatch.setattr(gc_module, "defer_async_with_tenant", fake_defer)
    await recover_map_publish_generations(test_db_session)
    # Other tests' due generations may share the worker DB; assert membership.
    assert str(due.id) in enqueued
    assert str(waiting.id) not in enqueued
    assert str(leased.id) not in enqueued


async def test_bootstrap_creates_first_generation_for_public_maps(test_db_session):
    import app.core.db as db_module

    from app.modules.catalog.maps.publish.gc import bootstrap_public_maps

    user_id = await get_user_id(test_db_session, "admin")
    map_obj, _dataset, _ = await _public_map(test_db_session, user_id)

    created = await bootstrap_public_maps(db_module.async_session, limit=5)
    assert created >= 1
    generation = (
        await test_db_session.scalars(
            select(MapPublishGeneration).where(
                MapPublishGeneration.map_id == map_obj.id
            )
        )
    ).one()
    assert generation.origin == pub_state.ORIGIN_BOOTSTRAP
