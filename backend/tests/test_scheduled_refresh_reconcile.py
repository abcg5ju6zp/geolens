"""Reconciliation of active keyed refresh runs from durable facts."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from app.platform.jobs.models import IngestJob
from app.platform.refresh.execution import cancel_admitted_refresh
from app.platform.refresh.reconcile import (
    Redelivery,
    readmit_running_attempt,
    reconcile_keyed_refresh_runs,
)
from app.platform.refresh.service import (
    SCHEDULED_MAX_DELIVERIES,
    RunOccurrenceIdentity,
    cancel_active_run_for_job,
    claim_admitted_run_for_job,
    create_pending_run,
    get_run_by_execution_key,
    get_scheduled_occurrence,
    mark_run_dispatched,
)
from tests.factories import create_dataset, get_user_id

pytestmark = pytest.mark.anyio


async def _scheduled_run(
    session,
    *,
    job_status: str = "pending",
    run_status: str = "pending",
    phase: str | None = None,
    attempts: int = 1,
    scheduled_for: datetime | None = None,
    deadline: datetime | None = None,
    last_dispatched_at: datetime | None = None,
):
    actor_id = await get_user_id(session, "admin")
    dataset = await create_dataset(session, created_by=actor_id)
    job = IngestJob(
        dataset_id=dataset.id,
        created_by=actor_id,
        status=job_status,
        user_metadata={"refresh": True, "dataset_id": str(dataset.id)},
    )
    session.add(job)
    await session.flush()
    now = datetime.now(timezone.utc)
    run = await create_pending_run(
        session,
        dataset_id=dataset.id,
        origin_kind="service",
        trigger="scheduled",
        triggered_by=actor_id,
        ingest_job_id=job.id,
        feature_count_before=dataset.feature_count,
        scheduled_for=scheduled_for or now,
        occurrence_key=f"occurrence:{uuid.uuid4()}",
        execution_key=uuid.uuid4(),
        source_binding_fingerprint="a" * 64,
        verification_policy="arcgis_id_set_v1",
        schedule_version="v1",
    )
    run.attempts = attempts
    run.status = run_status
    run.phase = (
        phase
        or {
            "pending": "admitted",
            "running": "ingesting",
            "succeeded": "published",
        }[run_status]
    )
    run.claim_deadline = (
        deadline if deadline is not None else now + timedelta(minutes=30)
    )
    run.last_dispatched_at = last_dispatched_at
    await session.commit()
    return dataset, job, run


async def _version(session, dataset, *, ingest_job_id: uuid.UUID) -> uuid.UUID:
    result = await session.execute(
        text(
            "INSERT INTO catalog.dataset_versions (dataset_id, version_number, ingest_job_id) "
            "VALUES (:dataset, 1, :job) RETURNING id"
        ),
        {"dataset": dataset.id, "job": ingest_job_id},
    )
    return result.scalar_one()


async def test_publish_fact_forces_success_over_a_failed_job(test_db_session) -> None:
    dataset, job, run = await _scheduled_run(
        session=test_db_session, job_status="failed"
    )
    version_id = await _version(test_db_session, dataset, ingest_job_id=job.id)
    await test_db_session.commit()

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()

    assert result.succeeded == 1
    assert result.redeliveries == ()
    await test_db_session.refresh(run)
    assert run.status == "succeeded"
    assert run.phase == "published"
    assert run.dataset_version_id == version_id


async def test_complete_job_succeeds_a_no_change_run(test_db_session) -> None:
    _dataset, job, run = await _scheduled_run(
        session=test_db_session,
        job_status="complete",
        run_status="running",
        phase="ingesting",
    )

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()

    assert result.succeeded == 1
    await test_db_session.refresh(run)
    assert run.status == "succeeded"
    assert run.dataset_version_id is None


async def test_a_running_job_is_left_alone(test_db_session) -> None:
    _dataset, _job, run = await _scheduled_run(
        session=test_db_session,
        job_status="running",
        run_status="running",
        phase="ingesting",
    )

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()

    assert result.changed is False
    await test_db_session.refresh(run)
    assert run.status == "running"
    assert run.attempts == 1


async def test_never_dispatched_run_is_re_sent_with_the_same_key(
    test_db_session,
) -> None:
    _dataset, _job, run = await _scheduled_run(session=test_db_session)
    key = run.execution_key

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()

    assert len(result.redeliveries) == 1
    redelivery = result.redeliveries[0]
    assert isinstance(redelivery, Redelivery)
    assert redelivery.execution_key == str(key)
    assert redelivery.attempts == 1
    await test_db_session.refresh(run)
    assert run.status == "pending"
    assert run.phase == "dispatched"
    assert run.attempts == 1
    assert run.last_dispatched_at is not None


async def test_a_lost_dispatch_past_the_grace_is_re_sent(test_db_session) -> None:
    old = datetime.now(timezone.utc) - timedelta(minutes=10)
    _dataset, _job, run = await _scheduled_run(
        session=test_db_session, phase="dispatched", last_dispatched_at=old
    )
    key = run.execution_key

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()

    assert result.redeliveries[0].execution_key == str(key)
    await test_db_session.refresh(run)
    assert run.attempts == 1
    assert run.last_dispatched_at > old


async def test_an_expired_run_is_re_driven_under_a_fresh_key(test_db_session) -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    _dataset, job, run = await _scheduled_run(
        session=test_db_session,
        phase="dispatched",
        deadline=past,
        last_dispatched_at=datetime.now(timezone.utc) - timedelta(minutes=31),
    )
    old_key, old_attempt = run.execution_key, job.attempt_id

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()

    assert len(result.redeliveries) == 1
    redelivery = result.redeliveries[0]
    assert redelivery.attempts == 2
    assert redelivery.execution_key != str(old_key)
    await test_db_session.refresh(run)
    assert run.status == "pending"
    assert run.phase == "dispatched"
    assert run.attempts == 2
    assert run.execution_key != old_key
    assert run.claim_deadline > past
    assert run.last_attempt_error_code == "scheduled_claim_expired"
    await test_db_session.refresh(job)
    assert job.status == "pending"
    assert job.attempt_id != old_attempt

    # The stale delivery can no longer claim; the new key can.
    assert (
        await claim_admitted_run_for_job(test_db_session, job.id, execution_key=old_key)
        is None
    )
    assert (
        await claim_admitted_run_for_job(
            test_db_session, job.id, execution_key=run.execution_key
        )
        == run.id
    )


async def test_an_exhausted_run_fails_after_its_last_deadline(test_db_session) -> None:
    _dataset, job, run = await _scheduled_run(
        session=test_db_session,
        phase="dispatched",
        attempts=SCHEDULED_MAX_DELIVERIES,
        deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
        last_dispatched_at=datetime.now(timezone.utc) - timedelta(minutes=31),
    )

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()

    assert result.failed == 1
    assert result.redeliveries == ()
    await test_db_session.refresh(run)
    assert run.status == "failed"
    assert run.phase is None
    assert run.error_code == "scheduled_claim_expired"
    await test_db_session.refresh(job)
    assert job.status == "failed"


async def test_a_failed_attempt_with_deliveries_left_re_drives(test_db_session) -> None:
    _dataset, job, run = await _scheduled_run(
        session=test_db_session,
        job_status="failed",
        run_status="running",
        phase="ingesting",
    )
    old_key = run.execution_key

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id], deliver=False
    )
    await test_db_session.commit()

    assert len(result.redeliveries) == 1
    await test_db_session.refresh(run)
    assert run.status == "pending"
    assert run.phase == "admitted"
    assert run.execution_key != old_key
    await test_db_session.refresh(job)
    assert job.status == "pending"


async def test_mark_run_dispatched_is_key_scoped(test_db_session) -> None:
    _dataset, job, run = await _scheduled_run(session=test_db_session)

    assert (
        await mark_run_dispatched(
            test_db_session,
            ingest_job_id=job.id,
            execution_key=uuid.uuid4(),
        )
        is False
    )
    assert await mark_run_dispatched(
        test_db_session, ingest_job_id=job.id, execution_key=run.execution_key
    )
    await test_db_session.commit()
    await test_db_session.refresh(run)
    assert run.phase == "dispatched"


async def test_identity_queries_find_the_occurrence(test_db_session) -> None:
    scheduled_for = datetime.now(timezone.utc)
    _dataset, _job, run = await _scheduled_run(
        session=test_db_session, scheduled_for=scheduled_for
    )

    by_key = await get_run_by_execution_key(
        test_db_session, execution_key=run.execution_key
    )
    assert isinstance(by_key, RunOccurrenceIdentity)
    assert by_key.run_id == run.id
    assert by_key.schedule_version == "v1"

    by_occurrence = await get_scheduled_occurrence(
        test_db_session,
        dataset_id=run.dataset_id,
        scheduled_for=scheduled_for,
        occurrence_key=run.occurrence_key,
    )
    assert by_occurrence is not None
    assert by_occurrence.run_id == run.id


async def test_a_cancel_carried_by_a_stale_delivery_misses(test_db_session) -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    _dataset, _job, run = await _scheduled_run(
        session=test_db_session,
        phase="dispatched",
        deadline=past,
        last_dispatched_at=datetime.now(timezone.utc) - timedelta(minutes=31),
    )
    stale_key = run.execution_key

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()
    assert len(result.redeliveries) == 1

    # A cancel carried by the old delivery cannot end the re-driven run.
    assert (
        await cancel_active_run_for_job(
            test_db_session, run.ingest_job_id, execution_key=stale_key
        )
        is None
    )
    await test_db_session.refresh(run)
    assert run.status == "pending"

    # One carrying the current key can.
    assert (
        await cancel_active_run_for_job(
            test_db_session, run.ingest_job_id, execution_key=run.execution_key
        )
        == run.id
    )
    await test_db_session.commit()
    await test_db_session.refresh(run)
    assert run.status == "cancelled"
    assert run.phase is None


async def test_a_transient_worker_failure_readmits_its_run(test_db_session) -> None:
    _dataset, job, run = await _scheduled_run(
        session=test_db_session,
        job_status="running",
        run_status="running",
        phase="ingesting",
    )
    old_key, old_attempt = run.execution_key, job.attempt_id

    redelivery = await readmit_running_attempt(
        test_db_session,
        job_id=job.id,
        attempt_id=job.attempt_id,
        code="source_unavailable",
        message="The origin timed out.",
    )
    await test_db_session.commit()

    assert redelivery is not None
    assert redelivery.execution_key != str(old_key)
    assert redelivery.attempts == 2
    await test_db_session.refresh(run)
    assert run.status == "pending"
    assert run.phase == "admitted"
    assert run.execution_key != old_key
    assert run.last_attempt_error_code == "source_unavailable"
    await test_db_session.refresh(job)
    assert job.status == "pending"
    assert job.attempt_id != old_attempt


async def test_cancel_admitted_refresh_is_key_scoped(test_db_session) -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    _dataset, job, run = await _scheduled_run(
        session=test_db_session,
        phase="dispatched",
        deadline=past,
        last_dispatched_at=datetime.now(timezone.utc) - timedelta(minutes=31),
    )
    stale_key = run.execution_key

    result = await reconcile_keyed_refresh_runs(
        test_db_session, job_ids=[run.ingest_job_id]
    )
    await test_db_session.commit()
    assert len(result.redeliveries) == 1
    new_key = uuid.UUID(result.redeliveries[0].execution_key)
    assert new_key != stale_key

    assert (
        await cancel_admitted_refresh(
            test_db_session,
            run.ingest_job_id,
            str(stale_key),
            reason="schedule_disabled",
        )
        is None
    )
    await test_db_session.refresh(run)
    assert run.status == "pending"

    cancelled = await cancel_admitted_refresh(
        test_db_session,
        run.ingest_job_id,
        str(new_key),
        reason="schedule_disabled",
    )
    assert cancelled == run.id
    await test_db_session.commit()
    await test_db_session.refresh(run)
    assert run.status == "cancelled"
    await test_db_session.refresh(job)
    assert job.status == "cancelled"
