"""Procrastinate driver for one dataset-retirement batch.

Deliberately thin: the catalog domain owns the per-item state machine and
all SQL, reached through ProcessingPort (the layering rule forbids a direct
processing -> catalog import). This module only:

* claims the batch with a compare-and-set (a duplicate/late delivery exits),
* opens a fresh session per item, so one item's rollback cannot expire the
  batch's other work,
* finishes the batch once no item remains claimable.

Every step is idempotent: a lost response, a killed worker or a duplicate
defer simply re-runs this, and terminal items (completed datasets above all)
are never touched again.
"""

from __future__ import annotations

import uuid

import structlog

from app.core.db.tenant_session import tenant_task
from app.platform.extensions import get_processing_port
from app.processing.ingest.tasks_common import task_app

logger = structlog.get_logger(__name__)


@task_app.task(queue="ingest", retry=0)
@tenant_task
async def run_dataset_retirement_batch(batch_id: str) -> None:
    """Drive every open item of one retirement batch to a terminal outcome."""
    # Imported at call time: tests patch ``app.core.db.async_session`` with
    # the test engine (module-load binding would keep the original one).
    from app.core.db import async_session

    batch_uuid = uuid.UUID(str(batch_id))
    port = get_processing_port()

    async with async_session() as session:
        claimed = await port.retirement_claim_batch(session, batch_uuid)
    if not claimed:
        # A fresher driver owns this batch (duplicate delivery), or it was
        # already finished. Nothing to do.
        logger.info("retirement_batch_claim_lost", batch_id=str(batch_id))
        return

    while True:
        async with async_session() as session:
            outcome = await port.retirement_process_item(session, batch_uuid)
        if outcome is None:
            break
        logger.info(
            "retirement_item_settled",
            batch_id=str(batch_id),
            dataset_id=outcome.get("dataset_id"),
            status=outcome.get("status"),
            error_code=outcome.get("error_code"),
        )

    async with async_session() as session:
        await port.retirement_finish_batch(session, batch_uuid)
    logger.info("retirement_batch_finished", batch_id=str(batch_id))
