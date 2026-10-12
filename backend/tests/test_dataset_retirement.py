"""Durable batch dataset retirement: batching, fencing, recovery, isolation.

Covers the batch lifecycle end to end with direct worker invocation (no live
Procrastinate worker): per-item outcomes, quiesce, crash-window recovery,
idempotent resubmission and the admission/publication fences.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from app.core.config import settings
from app.modules.catalog.datasets.domain.models import (
    Dataset,
    DatasetGrant,
)
from app.modules.catalog.datasets.domain.retirement_models import (
    ITEM_COMPLETED,
    ITEM_CONFLICT,
    ITEM_REJECTED,
    ITEM_RETRYABLE,
)
from app.modules.catalog.datasets.domain.service_lifecycle import (
    DependentVrtError,
)
from app.modules.catalog.collections.models import DatasetVersion
from app.modules.catalog.maps.models import Map, MapLayer
from app.platform.jobs.models import IngestJob
from app.platform.retirement import (
    DatasetRetiringError,
    assert_dataset_not_retiring,
)

from tests.factories import create_dataset

pytestmark = pytest.mark.anyio

BASE = "/datasets/retirement-batches"


async def _admin_id(test_db_session) -> uuid.UUID:
    row = await test_db_session.execute(
        text("SELECT id FROM catalog.users WHERE username = :name"),
        {"name": settings.geolens_admin_username},
    )
    return row.scalar_one()


async def _make_dataset(test_db_session, admin_id, *, name=None):
    return await create_dataset(
        test_db_session,
        created_by=admin_id,
        name=name or f"Retire {uuid.uuid4().hex[:8]}",
        record_type="vector_dataset",
        feature_count=1,
    )


async def _target(session, ds, *, title=None):
    # Read the title with scalar SQL: the record relationship is lazy and
    # would emit IO off-greenlet when touched after the factory commit.
    if title is None:
        title = await session.scalar(
            text("SELECT title FROM catalog.records WHERE id = :r"),
            {"r": ds.record_id},
        )
    return {"dataset_id": str(ds.id), "confirm_title": title}


async def _create_batch(client, admin_auth_header, targets, *, key=None):
    headers = dict(admin_auth_header)
    if key is not None:
        headers["Idempotency-Key"] = key
    return await client.post(f"{BASE}/", json={"datasets": targets}, headers=headers)


async def _run_batch(batch_id: str) -> None:
    # Direct invocation, mirroring the worker delivery (single-tenant tests,
    # so the tenant_task wrapper is a no-op).
    from app.processing.retirement.tasks import run_dataset_retirement_batch

    await run_dataset_retirement_batch(batch_id=batch_id)


async def _items_by_dataset(test_db_session, batch_id):
    rows = await test_db_session.execute(
        text(
            "SELECT dataset_id, status, error_code, stage, dependents, "
            "storage_prefixes FROM catalog.dataset_retirement_items "
            "WHERE batch_id = :b"
        ),
        {"b": batch_id},
    )
    return {
        str(row[0]): {
            "status": row[1],
            "error_code": row[2],
            "stage": row[3],
            "dependents": row[4],
            "storage_prefixes": row[5],
        }
        for row in rows
    }


async def _fence_dataset(test_db_session, dataset_id, batch_id):
    await test_db_session.execute(
        text(
            "UPDATE catalog.datasets SET retiring_batch_id = :b, "
            "retirement_generation = retirement_generation + 1 WHERE id = :d"
        ),
        {"b": batch_id, "d": dataset_id},
    )
    await test_db_session.commit()


class TestRetirementPreview:
    async def test_preview_is_read_only_and_reports_state(
        self, client, admin_auth_header, test_db_session
    ):
        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()

        title = await test_db_session.scalar(
            text("SELECT title FROM catalog.records WHERE id = :r"),
            {"r": ds.record_id},
        )
        resp = await client.post(
            f"{BASE}/preview/",
            json={
                "datasets": [
                    {"dataset_id": str(ds.id), "confirm_title": title},
                    {"dataset_id": str(ds.id), "confirm_title": "Nope"},
                ]
            },
            headers=admin_auth_header,
        )
        assert resp.status_code == 200
        rows = resp.json()["items"]
        assert rows[0]["found"] is True and rows[0]["writable"] is True
        assert rows[0]["title_matches"] is True
        assert rows[0]["title"] == title
        assert rows[1]["title_matches"] is False

        unknown = await client.post(
            f"{BASE}/preview/",
            json={
                "datasets": [{"dataset_id": str(uuid.uuid4()), "confirm_title": "x"}]
            },
            headers=admin_auth_header,
        )
        item = unknown.json()["items"][0]
        assert item["found"] is False and item["writable"] is False

        count = await test_db_session.scalar(
            text("SELECT count(*) FROM catalog.dataset_retirement_batches")
        )
        assert count == 0


class TestBatchLifecycle:
    async def test_batch_completes_and_deletes_dataset(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()

        resp = await _create_batch(
            client, admin_auth_header, [await _target(test_db_session, ds)]
        )
        assert resp.status_code == 201, resp.text
        batch = resp.json()
        assert batch["status"] == "pending" and batch["total_count"] == 1

        await _run_batch(batch["id"])

        got = await client.get(f"{BASE}/{batch['id']}/", headers=admin_auth_header)
        body = got.json()
        assert body["status"] == "completed"
        assert body["completed_count"] == 1
        item = body["items"][0]
        assert item["status"] == ITEM_COMPLETED

        remaining = await test_db_session.scalar(
            text("SELECT count(*) FROM catalog.datasets WHERE id = :id"),
            {"id": ds.id},
        )
        assert remaining == 0
        retired = await test_db_session.scalar(
            text(
                "SELECT count(*) FROM catalog.retired_table_names "
                "WHERE dataset_id = :id"
            ),
            {"id": ds.id},
        )
        assert retired == 1

    async def test_per_item_isolation_title_mismatch(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        admin_id = await _admin_id(test_db_session)
        good = await _make_dataset(test_db_session, admin_id, name="Good DS")
        bad = await _make_dataset(test_db_session, admin_id, name="Bad DS")
        await test_db_session.commit()

        resp = await _create_batch(
            client,
            admin_auth_header,
            [
                await _target(test_db_session, good),
                await _target(test_db_session, bad, title="Wrong Title"),
            ],
        )
        assert resp.status_code == 201
        await _run_batch(resp.json()["id"])

        items = await _items_by_dataset(test_db_session, resp.json()["id"])
        assert items[str(good.id)]["status"] == ITEM_COMPLETED
        assert items[str(bad.id)]["status"] == ITEM_REJECTED
        assert items[str(bad.id)]["error_code"] == "title_mismatch"
        # The rejected dataset survives and is unfenced.
        bad_row = await test_db_session.get(Dataset, bad.id)
        assert bad_row is not None and bad_row.retiring_batch_id is None
        assert (
            await test_db_session.scalar(
                text("SELECT count(*) FROM catalog.datasets WHERE id = :id"),
                {"id": good.id},
            )
            == 0
        )

    async def test_pending_job_is_cancelled_then_dataset_completes(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.flush()
        job = IngestJob(
            dataset_id=ds.id,
            status="pending",
            created_by=admin_id,
            source_filename="x.geojson",
        )
        test_db_session.add(job)
        await test_db_session.commit()

        resp = await _create_batch(
            client, admin_auth_header, [await _target(test_db_session, ds)]
        )
        await _run_batch(resp.json()["id"])

        items = await _items_by_dataset(test_db_session, resp.json()["id"])
        assert items[str(ds.id)]["status"] == ITEM_COMPLETED
        await test_db_session.refresh(job)
        assert job.status == "cancelled"

    async def test_unstoppable_active_job_conflicts_and_unfences(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        from unittest.mock import AsyncMock

        import app.modules.catalog.datasets.domain.service_retirement_items as si

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        # The cancel write "fails" to land and the wait budget is exhausted.
        monkeypatch.setattr(si.ledger, "cancel", AsyncMock(return_value=None))
        monkeypatch.setattr(si, "QUIESCE_WAIT_SECONDS", 0.02)
        monkeypatch.setattr(si, "QUIESCE_POLL_SECONDS", 0.01)

        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.flush()
        test_db_session.add(
            IngestJob(
                dataset_id=ds.id,
                status="running",
                created_by=admin_id,
                source_filename="x.geojson",
            )
        )
        await test_db_session.commit()

        resp = await _create_batch(
            client, admin_auth_header, [await _target(test_db_session, ds)]
        )
        batch_id = resp.json()["id"]
        await _run_batch(batch_id)

        items = await _items_by_dataset(test_db_session, batch_id)
        assert items[str(ds.id)]["status"] == ITEM_CONFLICT
        assert items[str(ds.id)]["error_code"] == "active_jobs"
        ds_row = await test_db_session.get(Dataset, ds.id)
        assert ds_row.retiring_batch_id is None
        assert resp.json()  # batch still readable

        # Once the job is gone, retry re-drives to completion.
        await test_db_session.execute(
            text(
                "UPDATE catalog.ingest_jobs SET status = 'cancelled' "
                "WHERE dataset_id = :d"
            ),
            {"d": ds.id},
        )
        await test_db_session.commit()
        retry = await client.post(
            f"{BASE}/{batch_id}/retry/", headers=admin_auth_header
        )
        assert retry.status_code == 200
        assert retry.json()["reopened"] == 1
        await _run_batch(batch_id)
        items = await _items_by_dataset(test_db_session, batch_id)
        assert items[str(ds.id)]["status"] == ITEM_COMPLETED

    async def test_storage_failure_is_retryable_and_resumes_by_prefix(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        import app.modules.catalog.datasets.domain.service_retirement_items as si

        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()

        async def _boom(*args, **kwargs):
            raise RuntimeError("object store down")

        monkeypatch.setattr(si, "reap_managed_storage", _boom)
        batch_id = await self._created_batch_id(
            client, admin_auth_header, [await _target(test_db_session, ds)], monkeypatch
        )
        await _run_batch(batch_id)

        items = await _items_by_dataset(test_db_session, batch_id)
        assert items[str(ds.id)]["status"] == ITEM_RETRYABLE
        assert items[str(ds.id)]["error_code"] == "storage_error"
        assert items[str(ds.id)]["storage_prefixes"]
        # Rows are already gone; the retry must only re-reap.
        assert (
            await test_db_session.scalar(
                text("SELECT count(*) FROM catalog.datasets WHERE id = :id"),
                {"id": ds.id},
            )
            == 0
        )

        monkeypatch.undo()
        reopened = await self._retry_open(test_db_session, batch_id)
        assert reopened == 1
        await _run_batch(batch_id)
        items = await _items_by_dataset(test_db_session, batch_id)
        assert items[str(ds.id)]["status"] == ITEM_COMPLETED

    async def test_vrt_dependents_are_conflict_with_payload(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        import app.modules.catalog.datasets.domain.service_retirement_items as si

        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()

        async def _raise(*args, **kwargs):
            raise DependentVrtError(
                [{"vrt_dataset_id": str(uuid.uuid4()), "vrt_dataset_title": "VRT"}]
            )

        monkeypatch.setattr(si, "delete_dataset", _raise)
        batch_id = await self._created_batch_id(
            client, admin_auth_header, [await _target(test_db_session, ds)], monkeypatch
        )
        await _run_batch(batch_id)
        items = await _items_by_dataset(test_db_session, batch_id)
        assert items[str(ds.id)]["status"] == ITEM_CONFLICT
        assert items[str(ds.id)]["error_code"] == "vrt_dependents"
        assert items[str(ds.id)]["dependents"][0]["vrt_dataset_title"] == "VRT"

    async def test_claim_cas_ignores_unknown_batch(self, test_db_session):
        # A duplicate/late delivery whose batch no longer exists simply exits.
        from app.modules.catalog.datasets.domain.service import (
            claim_retirement_batch,
        )

        assert await claim_retirement_batch(test_db_session, uuid.uuid4()) is False

    async def test_dataset_already_gone_completes_idempotently(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()
        batch_id = await self._created_batch_id(
            client, admin_auth_header, [await _target(test_db_session, ds)], monkeypatch
        )

        # A single delete wins the race before the worker starts.
        from app.core.db import async_session
        from app.modules.catalog.datasets.domain.service import (
            delete_dataset,
            get_dataset,
        )

        async with async_session() as session:
            live = await get_dataset(session, ds.id)
            await delete_dataset(session, ds.id, live.record.title)
            await session.commit()

        await _run_batch(batch_id)
        items = await _items_by_dataset(test_db_session, batch_id)
        assert items[str(ds.id)]["status"] == ITEM_COMPLETED

    async def _created_batch_id(self, client, headers, targets, monkeypatch) -> str:
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        resp = await _create_batch(client, headers, targets)
        assert resp.status_code == 201, resp.text
        return resp.json()["id"]

    async def _retry_open(self, test_db_session, batch_id) -> int:
        from app.modules.catalog.datasets.domain.service import (
            retry_retirement_batch,
        )

        return await retry_retirement_batch(test_db_session, batch_id)


class TestDependenciesRemoved:
    async def test_map_layers_grants_and_versions_are_deleted(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.flush()
        map_row = Map(name=f"Map {uuid.uuid4().hex[:6]}", created_by=admin_id)
        test_db_session.add(map_row)
        await test_db_session.flush()
        test_db_session.add(
            MapLayer(
                map_id=map_row.id,
                dataset_id=ds.id,
                layer_type="vector_geolens",
            )
        )
        role_id = await test_db_session.scalar(
            text("SELECT id FROM catalog.roles LIMIT 1")
        )
        test_db_session.add(DatasetGrant(dataset_id=ds.id, role_id=role_id))
        test_db_session.add(DatasetVersion(dataset_id=ds.id, version_number=1))
        await test_db_session.commit()

        resp = await _create_batch(
            client, admin_auth_header, [await _target(test_db_session, ds)]
        )
        await _run_batch(resp.json()["id"])

        for table, column in (
            ("map_layers", "dataset_id"),
            ("dataset_grants", "dataset_id"),
            ("dataset_versions", "dataset_id"),
        ):
            count = await test_db_session.scalar(
                text(f"SELECT count(*) FROM catalog.{table} WHERE {column} = :d"),
                {"d": ds.id},
            )
            assert count == 0


class TestIdempotencyAndAuth:
    async def test_replay_same_key_returns_same_batch_mismatch_conflicts(
        self, client, admin_auth_header, test_db_session, monkeypatch
    ):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        admin_id = await _admin_id(test_db_session)
        ds1 = await _make_dataset(test_db_session, admin_id, name="Idem 1")
        ds2 = await _make_dataset(test_db_session, admin_id, name="Idem 2")
        await test_db_session.commit()

        first = await _create_batch(
            client,
            admin_auth_header,
            [await _target(test_db_session, ds1)],
            key="key-1",
        )
        assert first.status_code == 201
        replay = await _create_batch(
            client,
            admin_auth_header,
            [await _target(test_db_session, ds1)],
            key="key-1",
        )
        assert replay.status_code == 200
        assert replay.json()["id"] == first.json()["id"]

        mismatch = await _create_batch(
            client,
            admin_auth_header,
            [await _target(test_db_session, ds2)],
            key="key-1",
        )
        assert mismatch.status_code == 409
        assert mismatch.json()["detail"]["code"] == "idempotency_key_target_mismatch"

    async def test_duplicate_dataset_ids_rejected(self, client, admin_auth_header):
        dup_id = str(uuid.uuid4())
        resp = await client.post(
            f"{BASE}/",
            json={
                "datasets": [
                    {"dataset_id": dup_id, "confirm_title": "x"},
                    {"dataset_id": dup_id, "confirm_title": "x"},
                ]
            },
            headers=admin_auth_header,
        )
        assert resp.status_code == 422

    async def test_foreign_user_cannot_read_batch(
        self,
        client,
        admin_auth_header,
        editor_auth_header,
        test_db_session,
        monkeypatch,
    ):
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "app.modules.catalog.datasets.api.router_retirement"
            ".enqueue_retirement_batch",
            AsyncMock(),
        )
        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()
        resp = await _create_batch(
            client, admin_auth_header, [await _target(test_db_session, ds)]
        )
        batch_id = resp.json()["id"]

        other = await client.get(f"{BASE}/{batch_id}/", headers=editor_auth_header)
        assert other.status_code == 404

    async def test_requires_auth(self, client):
        resp = await client.post(f"{BASE}/", json={"datasets": []})
        assert resp.status_code == 401


class TestFences:
    async def test_admission_fence_blocks_and_unblocks(self, test_db_session):
        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()

        await assert_dataset_not_retiring(test_db_session, ds.id)
        await _fence_dataset(test_db_session, ds.id, uuid.uuid4())
        with pytest.raises(DatasetRetiringError):
            await assert_dataset_not_retiring(test_db_session, ds.id)

    async def test_add_layer_refuses_fenced_dataset(self, test_db_session):
        from app.modules.catalog.maps.schemas import MapLayerInput
        from app.modules.catalog.maps.service_layers import add_layer

        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        map_row = Map(name=f"Map {uuid.uuid4().hex[:6]}", created_by=admin_id)
        test_db_session.add(map_row)
        await test_db_session.flush()
        await _fence_dataset(test_db_session, ds.id, uuid.uuid4())

        body = MapLayerInput(dataset_id=ds.id)
        with pytest.raises(DatasetRetiringError):
            await add_layer(test_db_session, map_row.id, body)

    async def test_publication_seam_refuses_fenced_dataset(self, test_db_session):
        from app.processing.ingest.publication import (
            DatasetRetiringError as SeamRetiring,
            _take_catalog_rows,
        )

        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()
        await _fence_dataset(test_db_session, ds.id, uuid.uuid4())

        strategy = SimpleNamespace(task="t", raster_row=False, catalog_event="test")
        with pytest.raises(SeamRetiring):
            await _take_catalog_rows(test_db_session, strategy, ds)

    async def test_analysis_entry_refuses_fenced_source(self, test_db_session):
        from app.processing.analysis.tasks import (
            _assert_source_not_retiring,
        )

        admin_id = await _admin_id(test_db_session)
        ds = await _make_dataset(test_db_session, admin_id)
        await test_db_session.commit()
        await _fence_dataset(test_db_session, ds.id, uuid.uuid4())

        with pytest.raises(DatasetRetiringError):
            await _assert_source_not_retiring(test_db_session, str(ds.id))
