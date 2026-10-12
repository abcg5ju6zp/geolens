import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base


class Map(Base):
    __tablename__ = "maps"
    __table_args__ = (
        CheckConstraint(
            "visibility IN ('private', 'public', 'internal')",
            name="chk_maps_visibility",
        ),
        # Trigram GIN indexes (migration 0010 H-07), declared here so
        # alembic-check sees them. `postgresql_ops` sits outside the
        # expression so alembic's index compare can match it.
        Index(
            "ix_maps_name_trgm",
            text("lower(name)"),
            postgresql_using="gin",
            postgresql_ops={"lower(name)": "gin_trgm_ops"},
        ),
        Index(
            "ix_maps_description_trgm",
            text("lower(coalesce(description, ''))"),
            postgresql_using="gin",
            postgresql_ops={"lower(coalesce(description, ''))": "gin_trgm_ops"},
        ),
        Index(
            "ix_maps_forked_from",
            "forked_from",
            postgresql_where=text("forked_from IS NOT NULL"),
        ),
        # No composite Map.visibility + created_by index; db-audit M-18
        # suggested one, but EXPLAIN showed no seq scan below ~10k rows; the
        # write + disk cost isn't worth it until row count crosses that.
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid()
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Viewport state
    center_lng: Mapped[float | None] = mapped_column(Float, nullable=True)
    center_lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    zoom: Mapped[float | None] = mapped_column(Float, nullable=True)
    bearing: Mapped[float] = mapped_column(Float, default=0, server_default="0")
    pitch: Mapped[float] = mapped_column(Float, default=0, server_default="0")

    # Basemap
    basemap_style: Mapped[str] = mapped_column(
        String(2000),
        default="openfreemap-positron",
        server_default="openfreemap-positron",
    )
    show_basemap_labels: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true"
    )
    basemap_config: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, default=None
    )

    # Active plugin IDs (null = use client defaults, [] = no plugins)
    plugins: Mapped[list | None] = mapped_column(JSONB, nullable=True, default=None)

    # Custom map-level legend title (null = no override). ENH-06. Can't reuse
    # `plugins` (holds enabled plugin IDs) or the basemap/terrain JSONB blobs
    # (extra="forbid"-validated), so this needs its own nullable column.
    legend_title: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Map-level terrain configuration (null = terrain disabled/unconfigured)
    terrain_config: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, default=None
    )

    # Visibility
    visibility: Mapped[str] = mapped_column(
        String(20), default="private", server_default="private"
    )

    # Preview thumbnail — storage key (e.g. "maps/thumbnails/{id}.jpg")
    thumbnail_uri: Mapped[str | None] = mapped_column(Text, nullable=True)

    # fix(#1005): thumbnail's own cache version, split from `updated_at`
    # — a lazy backfill (a read, not an edit) shouldn't bump the edit
    # timestamp. Nullable; frontend falls back to `updated_at` for old rows.
    thumbnail_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # OG/social-card image key. Separate from thumbnail_uri: the OG image is
    # 1200x630 and exceeds the 100KB thumbnail cap (SHARE-08).
    og_image_uri: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Lineage (fork tracking)
    forked_from: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("catalog.maps.id", ondelete="SET NULL"), nullable=True
    )

    # Ownership
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("catalog.users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    # TSEAM-01 (Phase 1207): dormant tenant_id — nullable, no FK enforcement.
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Active publish generation whose frozen manifest serves anonymous/shared
    # reads. Null = no published snapshot yet (private map, or a public map
    # waiting for its first generation to build); the read path falls back to
    # live rows. Only the cutover transaction writes this.
    published_generation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "catalog.map_publish_generations.id",
            ondelete="SET NULL",
            use_alter=True,
        ),
        nullable=True,
    )


class MapLayer(Base):
    __tablename__ = "map_layers"
    __table_args__ = (
        CheckConstraint(
            "layer_type IN ('vector_geolens', 'raster_geolens', 'geojson')",
            name="chk_map_layers_layer_type",
        ),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid()
    )
    map_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog.maps.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog.datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    sort_order: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    visible: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    opacity: Mapped[float] = mapped_column(Float, default=1.0, server_default="1.0")
    paint: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    layout: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    layer_type: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default="vector_geolens"
    )
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    filter: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    label_config: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    popup_config: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    style_config: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    show_in_legend: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MapEditHistoryEvent(Base):
    __tablename__ = "map_edit_history_events"
    __table_args__ = (
        CheckConstraint(
            "target_type IN ('map', 'layer')",
            name="chk_map_edit_history_events_target_type",
        ),
        Index(
            "ix_catalog_map_edit_history_events_map_created_at",
            "map_id",
            "created_at",
        ),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid()
    )
    map_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog.maps.id", ondelete="CASCADE"), nullable=False, index=True
    )
    actor_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("catalog.users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    actor_username: Mapped[str | None] = mapped_column(String(150), nullable=True)
    target_type: Mapped[str] = mapped_column(String(20), nullable=False)
    target_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)
    target_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    action: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MapShareToken(Base):
    __tablename__ = "map_share_tokens"
    __table_args__ = (
        UniqueConstraint("token_hash", name="uq_map_share_tokens_token_hash"),
        Index(
            "ix_map_share_tokens_created_by",
            "created_by",
            postgresql_where=text("created_by IS NOT NULL"),
        ),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid()
    )
    map_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog.maps.id", ondelete="CASCADE"), nullable=False, index=True
    )
    token_hash: Mapped[str] = mapped_column(Text, nullable=False)
    token_hint: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("catalog.users.id", ondelete="SET NULL"), nullable=True
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MapIconAsset(Base):
    __tablename__ = "map_icon_assets"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_map_icon_assets_slug"),
        CheckConstraint(
            "media_type IN ('image/svg+xml', 'image/png')",
            name="chk_map_icon_assets_media_type",
        ),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid()
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # db-audit #529 BLOAT-1: no index=True — uq_map_icon_assets_slug's
    # backing index already covers slug lookups; a second btree was pure
    # write amplification.
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    media_type: Mapped[str] = mapped_column(String(50), nullable=False)
    storage_key: Mapped[str] = mapped_column(Text, nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("catalog.users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MapPublishGeneration(Base):
    """One immutable content snapshot of a public map, awaiting or serving reads.

    Content (ordered layers with pinned dataset versions and view parameters)
    lives in ``manifest``; assets are confirmed individually in
    MapPublishAsset. Only a generation that wins the cutover fence moves
    ``Map.published_generation_id``; superseded generations keep their
    staged objects until GC reclaims the generation-scoped prefix.
    """

    __tablename__ = "map_publish_generations"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'building', 'ready', 'active', "
            "'superseded', 'failed', 'cancelled')",
            name="chk_map_publish_generations_status",
        ),
        CheckConstraint(
            "origin IN ('save', 'bootstrap')",
            name="chk_map_publish_generations_origin",
        ),
        UniqueConstraint(
            "map_id",
            "generation_number",
            name="uq_map_publish_generations_map_number",
        ),
        UniqueConstraint("read_nonce", name="uq_map_publish_generations_nonce"),
        # At most one served generation per map.
        Index(
            "uq_map_publish_generations_one_active",
            "map_id",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
        # A new save supersedes the previous in-flight generation in the same
        # transaction that inserts its replacement, so this never holds two.
        Index(
            "uq_map_publish_generations_one_building",
            "map_id",
            unique=True,
            postgresql_where=text("status IN ('pending', 'building')"),
        ),
        Index(
            "ix_map_publish_generations_status_next",
            "status",
            "next_attempt_at",
        ),
        Index("ix_map_publish_generations_map_status", "map_id", "status"),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid()
    )
    map_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog.maps.id", ondelete="CASCADE"), nullable=False
    )
    generation_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="pending"
    )
    manifest: Mapped[dict] = mapped_column(JSONB, nullable=False)
    content_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    read_nonce: Mapped[str] = mapped_column(String(32), nullable=False)
    origin: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="save"
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    claimed_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    leased_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("catalog.users.id", ondelete="SET NULL"), nullable=True
    )
    # Dormant tenant column, mirrors Map.tenant_id.
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    ready_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    activated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    superseded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class MapPublishAsset(Base):
    """One staged file of a generation. ``confirmed_at`` is the CAS gate."""

    __tablename__ = "map_publish_assets"
    __table_args__ = (
        CheckConstraint(
            "name IN ('style', 'sprite_json', 'sprite_png', "
            "'thumbnail', 'og_image')",
            name="chk_map_publish_assets_name",
        ),
        UniqueConstraint(
            "generation_id",
            "name",
            name="uq_map_publish_assets_generation_name",
        ),
        Index(
            "ix_map_publish_assets_generation_unconfirmed",
            "generation_id",
            postgresql_where=text("confirmed_at IS NULL"),
        ),
        {"schema": "catalog"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid()
    )
    generation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog.map_publish_generations.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(20), nullable=False)
    required: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    media_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    confirmed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
