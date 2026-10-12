"""Batch dataset retirement: durable batches/items and per-dataset fence.

A catalog administrator retires many datasets through one durable batch.
Each target gets a retirement generation (``catalog.datasets.retiring_*``)
that blocks new imports, analyses and map bindings while the batch runs.
Batch and item rows survive the hard-deleted datasets they name, so a lost
response, a worker restart or a duplicate submission resumes the same batch
and never re-runs a completed item.

Revision ID: 0082_dataset_retirement
Revises: 0081_admin_entered_email_verified
Create Date: 2026-10-12
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0082_dataset_retirement"
down_revision: Union[str, None] = "0081_admin_entered_email_verified"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")

    op.add_column(
        "datasets",
        sa.Column(
            "retirement_generation",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        schema="catalog",
    )
    # NULL: not retiring. Set under the dataset row lock when a retirement
    # attempt begins, cleared on a conflict/rejected terminal outcome.
    op.add_column(
        "datasets",
        sa.Column("retiring_batch_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema="catalog",
    )
    op.create_index(
        "ix_datasets_retiring_batch_id",
        "datasets",
        ["retiring_batch_id"],
        schema="catalog",
        postgresql_where=sa.text("retiring_batch_id IS NOT NULL"),
    )

    op.create_table(
        "dataset_retirement_batches",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # Dormant tenant column (TSEAM-01): no FK enforcement.
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=True),
        # Snapshot of the requesting user; never an FK (audit_logs convention).
        sa.Column("requested_by", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(200), nullable=True),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("total_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("conflict_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("retryable_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("queue_job_id", sa.String(80), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed')",
            name="chk_dataset_retirement_batches_status",
        ),
        sa.CheckConstraint(
            "total_count >= 0 AND completed_count >= 0 AND conflict_count >= 0 "
            "AND retryable_count >= 0 AND rejected_count >= 0",
            name="chk_dataset_retirement_batches_counts_nonneg",
        ),
        schema="catalog",
    )
    op.create_index(
        "uq_dataset_retirement_batches_idem",
        "dataset_retirement_batches",
        ["requested_by", "idempotency_key"],
        unique=True,
        schema="catalog",
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_index(
        "ix_dataset_retirement_batches_tenant",
        "dataset_retirement_batches",
        ["tenant_id"],
        schema="catalog",
    )

    op.create_table(
        "dataset_retirement_items",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # No FK to catalog.datasets: the named dataset is hard-deleted by the
        # item that completes.
        sa.Column(
            "batch_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("catalog.dataset_retirement_batches.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("confirm_title", sa.String(500), nullable=False),
        sa.Column(
            "generation",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("stage", sa.String(32), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("dependents", postgresql.JSONB(), nullable=True),
        sa.Column("table_name", sa.String(255), nullable=True),
        sa.Column("storage_prefixes", postgresql.JSONB(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'conflict', "
            "'retryable', 'rejected')",
            name="chk_dataset_retirement_items_status",
        ),
        sa.CheckConstraint(
            "stage IS NULL OR stage IN ('fence', 'quiesce', 'catalog_delete', "
            "'reaping')",
            name="chk_dataset_retirement_items_stage",
        ),
        sa.CheckConstraint(
            "attempts >= 0", name="chk_dataset_retirement_items_attempts"
        ),
        schema="catalog",
    )
    op.create_index(
        "uq_dataset_retirement_items_batch_dataset",
        "dataset_retirement_items",
        ["batch_id", "dataset_id"],
        unique=True,
        schema="catalog",
    )
    op.create_index(
        "ix_dataset_retirement_items_batch_status",
        "dataset_retirement_items",
        ["batch_id", "status"],
        schema="catalog",
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_table("dataset_retirement_items", schema="catalog")
    op.drop_table("dataset_retirement_batches", schema="catalog")
    op.drop_index(
        "ix_datasets_retiring_batch_id",
        table_name="datasets",
        schema="catalog",
    )
    op.drop_column("datasets", "retiring_batch_id", schema="catalog")
    op.drop_column("datasets", "retirement_generation", schema="catalog")
