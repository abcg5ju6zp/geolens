"""Publish generations for public maps.

A public map's content (ordered layers with the dataset publication versions
they were saved against, plus viewport/basemap/terrain view parameters) is
frozen into an immutable manifest each time an editor changes content. The
style document, a per-map sprite subset and the optional thumbnail/OG image
are built into a generation-scoped storage namespace and confirmed one asset
at a time; only after every required asset is confirmed does one transaction
move ``maps.published_generation_id`` to the new generation. Public readers
then see one coherent snapshot instead of live rows mixed with assets from
the previous save.

Two partial unique indexes carry the concurrency invariants: at most one
active generation per map, and at most one generation still being built. A
new save marks the previous in-flight generation superseded in the same
transaction that inserts its replacement, so the predicate never holds two
rows.

Existing public maps are not backfilled. The read path falls back to live
rows until a worker sweep bootstraps the first generation, so the migration
itself creates no objects and enqueues no work.

Revision ID: 0082_map_publish_generations
Revises: 0081_admin_entered_email_verified
Create Date: 2026-10-12
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0082_map_publish_generations"
down_revision: Union[str, None] = "0081_admin_entered_email_verified"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")

    op.create_table(
        "map_publish_generations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("map_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "generation_number", sa.BigInteger(), nullable=False
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("manifest", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("content_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("read_nonce", sa.String(length=32), nullable=False),
        sa.Column(
            "origin", sa.String(length=20), server_default="save", nullable=False
        ),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("claimed_by", sa.String(length=255), nullable=True),
        sa.Column("leased_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("created_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'building', 'ready', 'active', "
            "'superseded', 'failed', 'cancelled')",
            name="chk_map_publish_generations_status",
        ),
        sa.CheckConstraint(
            "origin IN ('save', 'bootstrap')",
            name="chk_map_publish_generations_origin",
        ),
        sa.ForeignKeyConstraint(
            ["map_id"], ["catalog.maps.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["created_by"], ["catalog.users.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "map_id",
            "generation_number",
            name="uq_map_publish_generations_map_number",
        ),
        sa.UniqueConstraint(
            "read_nonce", name="uq_map_publish_generations_nonce"
        ),
        schema="catalog",
    )

    op.create_table(
        "map_publish_assets",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "generation_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("name", sa.String(length=20), nullable=False),
        sa.Column(
            "required", sa.Boolean(), server_default=sa.text("true"), nullable=False
        ),
        sa.Column("storage_key", sa.Text(), nullable=True),
        sa.Column("media_type", sa.String(length=50), nullable=True),
        sa.Column("checksum", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "name IN ('style', 'sprite_json', 'sprite_png', "
            "'thumbnail', 'og_image')",
            name="chk_map_publish_assets_name",
        ),
        sa.ForeignKeyConstraint(
            ["generation_id"],
            ["catalog.map_publish_generations.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "generation_id", "name", name="uq_map_publish_assets_generation_name"
        ),
        schema="catalog",
    )

    op.add_column(
        "maps",
        sa.Column(
            "published_generation_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        schema="catalog",
    )
    op.create_index(
        "ix_map_publish_generations_status_next",
        "map_publish_generations",
        ["status", "next_attempt_at"],
        schema="catalog",
    )
    op.create_index(
        "ix_map_publish_generations_map_status",
        "map_publish_generations",
        ["map_id", "status"],
        schema="catalog",
    )
    op.create_index(
        "ix_map_publish_assets_generation_unconfirmed",
        "map_publish_assets",
        ["generation_id"],
        schema="catalog",
        postgresql_where=sa.text("confirmed_at IS NULL"),
    )
    op.create_index(
        "ix_maps_published_generation_id",
        "maps",
        ["published_generation_id"],
        schema="catalog",
    )
    op.create_index(
        "uq_map_publish_generations_one_active",
        "map_publish_generations",
        ["map_id"],
        schema="catalog",
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_index(
        "uq_map_publish_generations_one_building",
        "map_publish_generations",
        ["map_id"],
        schema="catalog",
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'building')"),
    )
    op.create_foreign_key(
        "fk_maps_published_generation",
        "maps",
        "map_publish_generations",
        ["published_generation_id"],
        ["id"],
        source_schema="catalog",
        referent_schema="catalog",
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_constraint(
        "fk_maps_published_generation", "maps", schema="catalog", type_="foreignkey"
    )
    op.drop_index(
        "uq_map_publish_generations_one_building",
        table_name="map_publish_generations",
        schema="catalog",
    )
    op.drop_index(
        "uq_map_publish_generations_one_active",
        table_name="map_publish_generations",
        schema="catalog",
    )
    op.drop_index(
        "ix_maps_published_generation_id", table_name="maps", schema="catalog"
    )
    op.drop_index(
        "ix_map_publish_assets_generation_unconfirmed",
        table_name="map_publish_assets",
        schema="catalog",
    )
    op.drop_index(
        "ix_map_publish_generations_map_status",
        table_name="map_publish_generations",
        schema="catalog",
    )
    op.drop_index(
        "ix_map_publish_generations_status_next",
        table_name="map_publish_generations",
        schema="catalog",
    )
    op.drop_column("maps", "published_generation_id", schema="catalog")
    op.drop_table("map_publish_assets", schema="catalog")
    op.drop_table("map_publish_generations", schema="catalog")
