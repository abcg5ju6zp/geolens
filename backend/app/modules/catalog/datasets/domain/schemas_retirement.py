"""Request/response schemas for durable batch dataset retirement."""

from __future__ import annotations

import datetime
import uuid

from pydantic import BaseModel, Field


class RetirementTarget(BaseModel):
    dataset_id: uuid.UUID
    confirm_title: str = Field(max_length=500)


class RetirementBatchCreateRequest(BaseModel):
    datasets: list[RetirementTarget] = Field(
        ..., min_length=1, max_length=100, description="1-100 datasets to retire"
    )


class RetirementPreviewRequest(BaseModel):
    datasets: list[RetirementTarget] = Field(..., min_length=1, max_length=100)


class RetirementPreviewItem(BaseModel):
    dataset_id: uuid.UUID
    found: bool
    writable: bool
    title: str | None = None
    title_matches: bool
    active_jobs: int
    active_refresh_runs: int
    map_layers: int
    vrt_dependents: list[str]


class RetirementPreviewResponse(BaseModel):
    items: list[RetirementPreviewItem]


class RetirementItemOutcome(BaseModel):
    id: uuid.UUID
    dataset_id: uuid.UUID
    status: str
    stage: str | None = None
    attempts: int
    # Outcome code (active_jobs/storage_error/...), deliberately NOT named
    # error_code: this is a batch-item state, not a job's provenance payload,
    # and batch reads are requester-or-admin gated.
    code: str | None = None
    detail: str | None = None
    dependents: list[dict] | None = None
    table_name: str | None = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    finished_at: datetime.datetime | None = None


class RetirementBatchResponse(BaseModel):
    id: uuid.UUID
    status: str
    total_count: int
    completed_count: int
    conflict_count: int
    retryable_count: int
    rejected_count: int
    created_at: datetime.datetime
    started_at: datetime.datetime | None = None
    heartbeat_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    items: list[RetirementItemOutcome]


class RetirementBatchSummary(BaseModel):
    id: uuid.UUID
    status: str
    total_count: int
    completed_count: int
    conflict_count: int
    retryable_count: int
    rejected_count: int
    created_at: datetime.datetime
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None


class RetirementBatchListResponse(BaseModel):
    items: list[RetirementBatchSummary]
    total: int


class RetirementRetryResponse(BaseModel):
    reopened: int
    batch: RetirementBatchResponse
