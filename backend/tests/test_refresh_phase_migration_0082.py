"""Migration 0082 maps run status onto delivery phases and backfills publish facts."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests.alembic_helpers import (
    enterprise_migrations_present,
    fresh_query,
    run_alembic,
)

pytestmark = pytest.mark.skipif(
    enterprise_migrations_present(),
    reason="OSS migration round-trip runs in the no-overlay migration job",
)

_PREVIOUS = "0081_admin_entered_email_verified"
_TITLE_PREFIX = "migration-0082-"


async def _seed_dataset() -> tuple[str, str]:
    rows = await fresh_query(
        "INSERT INTO catalog.records (title, record_type, visibility, record_status) "
        "VALUES (:title, 'vector_dataset', 'private', 'draft') RETURNING id",
        {"title": f"{_TITLE_PREFIX}{uuid.uuid4().hex[:8]}"},
    )
    record_id = str(rows[0][0])
    rows = await fresh_query(
        "INSERT INTO catalog.datasets (record_id, table_name, source_format) "
        "VALUES (:record_id, :table_name, 'geojson') RETURNING id",
        {
            "record_id": record_id,
            "table_name": f"m0082_{uuid.uuid4().hex[:12]}",
        },
    )
    return str(rows[0][0]), record_id


async def _seed_job(dataset_id: str, *, status: str) -> str:
    job_id = str(uuid.uuid4())
    await fresh_query(
        "INSERT INTO catalog.ingest_jobs (id, dataset_id, status, user_metadata) "
        "VALUES (CAST(:id AS uuid), CAST(:dataset AS uuid), :status, "
        '\'{"reupload": "true"}\'::jsonb)',
        {"id": job_id, "dataset": dataset_id, "status": status},
    )
    return job_id


async def _seed_run(
    dataset_id: str,
    job_id: str,
    *,
    trigger: str,
    status: str,
    keyed: bool,
    version_id: str | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    await fresh_query(
        """
        INSERT INTO catalog.dataset_refresh_runs
            (id, dataset_id, ingest_job_id, origin_kind, trigger, status,
             started_at, created_at, scheduled_for, occurrence_key,
             claim_deadline, execution_key, source_binding_fingerprint,
             dataset_version_id)
        VALUES (CAST(:id AS uuid), CAST(:dataset AS uuid), CAST(:job AS uuid),
                'service', CAST(:trigger AS varchar), CAST(:status AS varchar),
                now(), now(),
                CAST(:scheduled_for AS timestamptz), :occurrence,
                CAST(:claim_deadline AS timestamptz), CAST(:key AS uuid),
                :fingerprint, CAST(:version AS uuid))
        """,
        {
            "id": run_id,
            "dataset": dataset_id,
            "job": job_id,
            "trigger": trigger,
            "status": status,
            "scheduled_for": now if trigger == "scheduled" else None,
            "claim_deadline": now + timedelta(minutes=30) if keyed else None,
            "key": str(uuid.uuid4()) if keyed else None,
            "fingerprint": "a" * 64 if keyed else None,
            "occurrence": f"m0082:{uuid.uuid4()}",
            "version": version_id,
        },
    )
    return run_id


async def _phase_of(run_id: str):
    rows = await fresh_query(
        "SELECT phase, attempts, schedule_version "
        "FROM catalog.dataset_refresh_runs WHERE id = CAST(:id AS uuid)",
        {"id": run_id},
    )
    return rows[0]


async def _cleanup(*, datasets: list[str], record_ids: list[str]) -> None:
    for dataset_id in datasets:
        await fresh_query(
            "DELETE FROM catalog.dataset_refresh_runs WHERE dataset_id = CAST(:d AS uuid)",
            {"d": dataset_id},
        )
        await fresh_query(
            "DELETE FROM catalog.dataset_versions WHERE dataset_id = CAST(:d AS uuid)",
            {"d": dataset_id},
        )
        await fresh_query(
            "DELETE FROM catalog.ingest_jobs WHERE dataset_id = CAST(:d AS uuid)",
            {"d": dataset_id},
        )
        await fresh_query(
            "DELETE FROM catalog.datasets WHERE id = CAST(:d AS uuid)",
            {"d": dataset_id},
        )
    for record_id in record_ids:
        await fresh_query(
            "DELETE FROM catalog.records WHERE id = CAST(:r AS uuid)",
            {"r": record_id},
        )
    restored = run_alembic("upgrade", "heads")
    assert restored.returncode == 0, restored.stderr


async def test_upgrade_maps_status_to_phase_and_backfills_publish_fact() -> None:
    # One dataset per run status: the one-active index forbids two active
    # rows for a dataset, even as migration test fixtures.
    dataset_ids: list[str] = []
    record_ids: list[str] = []
    try:
        down = run_alembic("downgrade", _PREVIOUS)
        assert down.returncode == 0, down.stderr

        succeeded_ds, rec = await _seed_dataset()
        dataset_ids.append(succeeded_ds)
        record_ids.append(rec)
        completed_job = await _seed_job(succeeded_ds, status="complete")
        version_rows = await fresh_query(
            "INSERT INTO catalog.dataset_versions (dataset_id, version_number) "
            "VALUES (CAST(:d AS uuid), 1) RETURNING id",
            {"d": succeeded_ds},
        )
        version_id = str(version_rows[0][0])
        succeeded_run = await _seed_run(
            succeeded_ds,
            completed_job,
            trigger="manual",
            status="succeeded",
            keyed=False,
            version_id=version_id,
        )

        running_ds, rec = await _seed_dataset()
        dataset_ids.append(running_ds)
        record_ids.append(rec)
        running_job = await _seed_job(running_ds, status="running")
        running_run = await _seed_run(
            running_ds,
            running_job,
            trigger="scheduled",
            status="running",
            keyed=True,
        )

        pending_ds, rec = await _seed_dataset()
        dataset_ids.append(pending_ds)
        record_ids.append(rec)
        pending_job = await _seed_job(pending_ds, status="pending")
        pending_run = await _seed_run(
            pending_ds, pending_job, trigger="manual", status="pending", keyed=False
        )

        failed_ds, rec = await _seed_dataset()
        dataset_ids.append(failed_ds)
        record_ids.append(rec)
        failed_job = await _seed_job(failed_ds, status="failed")
        failed_run = await _seed_run(
            failed_ds, failed_job, trigger="manual", status="failed", keyed=False
        )

        up = run_alembic("upgrade", "heads")
        assert up.returncode == 0, up.stderr

        assert await _phase_of(succeeded_run) == ("published", 1, None)
        running_phase, running_attempts, running_schedule = await _phase_of(running_run)
        assert (running_phase, running_attempts) == ("ingesting", 1)
        # Existing scheduled rows predate schedule_version and get the
        # placeholder the tightened identity CHECK requires.
        assert running_schedule == "legacy"
        assert await _phase_of(pending_run) == ("admitted", 1, None)
        failed_phase, _, failed_schedule = await _phase_of(failed_run)
        assert failed_phase is None
        assert failed_schedule is None

        fact = await fresh_query(
            "SELECT ingest_job_id FROM catalog.dataset_versions WHERE id = CAST(:v AS uuid)",
            {"v": version_id},
        )
        assert str(fact[0][0]) == completed_job
    finally:
        await _cleanup(datasets=dataset_ids, record_ids=record_ids)


async def test_downgrade_refuses_while_an_active_keyed_run_exists() -> None:
    dataset_id, record_id = await _seed_dataset()
    blocked_run = None
    try:
        blocked_job = await _seed_job(dataset_id, status="pending")
        now = datetime.now(timezone.utc)
        blocked_run = str(uuid.uuid4())
        # Head-shape row: the guard is tested against the schema that
        # introduces it, not the older one it downgrades to.
        await fresh_query(
            """
            INSERT INTO catalog.dataset_refresh_runs
                (id, dataset_id, ingest_job_id, origin_kind, trigger, status,
                 phase, attempts, started_at, created_at, scheduled_for,
                 occurrence_key, claim_deadline, execution_key,
                 source_binding_fingerprint, schedule_version)
            VALUES (CAST(:id AS uuid), CAST(:dataset AS uuid), CAST(:job AS uuid),
                    'service', 'scheduled', 'pending', 'admitted', 1, now(), now(),
                    CAST(:now AS timestamptz), :occurrence,
                    CAST(:deadline AS timestamptz), CAST(:key AS uuid),
                    repeat('a', 64), 'v1')
            """,
            {
                "id": blocked_run,
                "dataset": dataset_id,
                "job": blocked_job,
                "now": now,
                "occurrence": f"m0082:{uuid.uuid4()}",
                "deadline": now + timedelta(minutes=30),
                "key": str(uuid.uuid4()),
            },
        )

        refused = run_alembic("downgrade", _PREVIOUS, normalize=False)
        assert refused.returncode != 0
        assert "Stop admitted refreshes" in refused.stderr

        await fresh_query(
            "DELETE FROM catalog.dataset_refresh_runs WHERE id = CAST(:r AS uuid)",
            {"r": blocked_run},
        )
        down = run_alembic("downgrade", _PREVIOUS)
        assert down.returncode == 0, down.stderr
    finally:
        await _cleanup(datasets=[dataset_id], record_ids=[record_id])
