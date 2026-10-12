"""Procrastinate task for building and activating map publish generations.

Thin wrapper: all business logic lives in the catalog maps facade so the
layering rule (catalog never imports processing) keeps holding. retry=0 —
idempotency is the claim lease plus per-asset CAS confirmation, and the
recovery sweep re-enqueues due rows.
"""

from __future__ import annotations

import structlog

from app.core.db.tenant_session import tenant_task
from app.platform.extensions import get_catalog_port
from app.processing.ingest.tasks import task_app

logger = structlog.stdlib.get_logger(__name__)


@task_app.task(
    queue="ingest",
    retry=0,
    aliases=["app.processing.maps_publish.tasks.build_map_generation"],
)
@tenant_task
async def build_map_generation(generation_id: str) -> None:
    """Build a generation's missing assets and atomically cut it over."""
    # Processing never imports catalog; the port default delegates to the
    # maps publish facade.
    outcome = await get_catalog_port().run_map_publish_build(generation_id)
    logger.info(
        "map_publish_build_finished",
        generation_id=generation_id,
        outcome=outcome,
    )
