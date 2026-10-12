"""ORM models for durable, resumable batch dataset retirement.

The batch fixes the requesting user and the target set; items survive the
datasets they hard-delete (so ``dataset_id`` is deliberately NOT an FK).
``catalog.datasets.retiring_batch_id`` / ``retirement_generation`` are the
per-dataset fence admission doors and worker publication seams read.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base

# Batch lifecycle. "completed" means every item is terminal; the per-outcome
# counters tell whether that meant success, conflicts, retries or refusals.
BATCH_PENDING = "pending"
BATCH_RUNNING = "running"
BATCH_COMPLETED = "completed"

ITEM_PENDING = "pending"
ITEM_RUNNING = "running"
ITEM_COMPLETED = "completed"
ITEM_CONFLICT = "conflict"
ITEM_RETRYABLE = "retryable"
ITEM_REJECTED = "rejected"
# Outcomes a retry may re-drive (completed/rejected never run again).
ITEM_REDRIVABLE = (ITEM_RETRYABLE, ITEM_CONFLICT)

# Item pipeline position; persisted so a resumed worker knows what remains.
STAGE_FENCE = "fence"
STAGE_QUIESCE = "quiesce"
STAGE_CATALOG_DELETE = "catalog_delete"
STAGE_REAPING = "reaping"

# Machine-readable outcome codes carried on non-completed items.
CODE_ACTIVE_JOBS = "active_jobs"
CODE_VRT_DEPENDENTS = "vrt_dependents"
CODE_LOCK_CONFLICT = "catalog_lock_conflict"
CODE_STORAGE_ERROR = "storage_error"
CODE_TITLE_MISMATCH = "title_mismatch"
CODE_NOT_FOUND = "not_found"
CODE_FORBIDDEN = "forbidden"
CODE_ATTEMPTS_EXHAUSTED = "attempts_exhausted"
CODE_DATASET_RETIRING = "dataset_retiring"

# Per-item attempt ceiling before a retryable failure turns permanent.
MAX_ITEM_ATTEMPTS = 5


class DatasetRetirementBatch(Base):
    __tablename__ = "dataset_retirement_batches"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'completed')",
            name="chk_dataset_retirement_batches_status",
        ),
        CheckConstraint(
            "total_count >= 0 AND completed_count >= 0 AND conflict_count >= 0 "
            "AND retryable_count >= 0 AND rejected_count >= 0",
            name="chk_dataset_retirement_batches_counts_nonneg",
        ),
        Index(
            "uq_dataset_retirement_batches_idem",
            "requested_by",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_dataset_retirement_batches_tenant", "tenant_id"),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    requested_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=BATCH_PENDING
    )
    total_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    completed_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    conflict_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    retryable_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    rejected_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    queue_job_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    items: Mapped[list["DatasetRetirementItem"]] = relationship(
        "DatasetRetirementItem",
        back_populates="batch",
        order_by="DatasetRetirementItem.created_at",
        lazy="selectin",
    )


class DatasetRetirementItem(Base):
    __tablename__ = "dataset_retirement_items"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'conflict', "
            "'retryable', 'rejected')",
            name="chk_dataset_retirement_items_status",
        ),
        CheckConstraint(
            "stage IS NULL OR stage IN ('fence', 'quiesce', 'catalog_delete', "
            "'reaping')",
            name="chk_dataset_retirement_items_stage",
        ),
        CheckConstraint("attempts >= 0", name="chk_dataset_retirement_items_attempts"),
        Index(
            "uq_dataset_retirement_items_batch_dataset",
            "batch_id",
            "dataset_id",
            unique=True,
        ),
        Index("ix_dataset_retirement_items_batch_status", "batch_id", "status"),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("catalog.dataset_retirement_batches.id", ondelete="CASCADE"),
        nullable=False,
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    confirm_title: Mapped[str] = mapped_column(String(500), nullable=False)
    generation: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default="0"
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=ITEM_PENDING
    )
    stage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    dependents: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSONB, nullable=True
    )
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    storage_prefixes: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    batch: Mapped["DatasetRetirementBatch"] = relationship(
        "DatasetRetirementBatch", back_populates="items"
    )
