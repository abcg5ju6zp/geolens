"""Per-dataset retirement fence, as platform-level SQL.

Both the catalog admission doors and the processing publication seams read
this fence without importing ``app.modules.*`` (platform's layering rule).

The fence has two readers and one writer:

* admission doors (requests starting a new import/analysis/map binding) call
  :func:`assert_dataset_not_retiring`;
* worker publication seams, holding the dataset row lock, call
  :func:`read_retirement_fence` to decide whether a late job may publish;
* the retirement batch worker calls :func:`fence_dataset` /
  :func:`unfence_dataset` under the same row lock.

Callers that need a race-free answer MUST hold the dataset row lock taken by
``app.platform.catalog_locks.lock_catalog_rows``; a bare read without it is
only suitable for best-effort admission refusals.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# The machine-readable code every retirement refusal carries, HTTP or job.
DATASET_RETIRING_CODE = "dataset_retiring"
DATASET_RETIRING_MESSAGE = (
    "This dataset is being retired. New imports, analyses and map bindings "
    "are blocked until the retirement finishes."
)


class DatasetRetiringError(Exception):
    """A request or a worker publication reached a dataset under retirement.

    A domain error, like ``DatasetBusyError``: platform/ stays free of a
    FastAPI dependency and callers render it their own way (the API answers
    409 ``dataset_retiring``; a publication seam records a refused failure).
    """

    code = DATASET_RETIRING_CODE

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or DATASET_RETIRING_MESSAGE)


async def read_retirement_fence(
    session: AsyncSession, dataset_id: uuid.UUID
) -> tuple[int, uuid.UUID | None] | None:
    """Return ``(generation, retiring_batch_id)`` for a dataset, or None.

    None means the dataset row is absent (deleted). Call while holding the
    dataset row lock for an answer stable until the transaction ends.
    """
    row = (
        await session.execute(
            text(
                "SELECT retirement_generation, retiring_batch_id "
                "FROM catalog.datasets WHERE id = :dataset_id"
            ),
            {"dataset_id": dataset_id},
        )
    ).one_or_none()
    if row is None:
        return None
    return int(row[0]), row[1]


async def assert_dataset_not_retiring(
    session: AsyncSession, dataset_id: uuid.UUID
) -> None:
    """Refuse admission of new work against a dataset under retirement.

    Best-effort without a row lock: the authoritative block also sits in the
    worker publication seam, which reads the fence under its lock. A missing
    row is left to the caller's own not-found path.
    """
    fence = await read_retirement_fence(session, dataset_id)
    if fence is not None and fence[1] is not None:
        raise DatasetRetiringError()


async def fence_dataset(
    session: AsyncSession, dataset_id: uuid.UUID, batch_id: uuid.UUID
) -> int | None:
    """Open this batch's retirement fence on one dataset; new generation.

    Single atomic UPDATE under the caller's dataset row lock: the generation
    increments and the batch is stamped in one statement. Returns the new
    generation, or None when the row is absent or another batch already owns
    the fence. A retry by THIS batch still rolls the generation, so every
    attempt gets a distinct fencing token.
    """
    return await session.scalar(
        text(
            "UPDATE catalog.datasets "
            "SET retirement_generation = retirement_generation + 1, "
            "retiring_batch_id = :batch_id "
            "WHERE id = :dataset_id AND retiring_batch_id IS NULL "
            "RETURNING retirement_generation"
        ),
        {"dataset_id": dataset_id, "batch_id": batch_id},
    )


async def unfence_dataset(
    session: Any,
    dataset_id: uuid.UUID,
    batch_id: uuid.UUID,
) -> None:
    """Release this batch's fence, only while it still owns the fence.

    A fence a later state owns (or already cleared) is left untouched.
    Accepts a session or connection: the statement is the same on both.
    """
    await session.execute(
        text(
            "UPDATE catalog.datasets SET retiring_batch_id = NULL "
            "WHERE id = :dataset_id AND retiring_batch_id = :batch_id"
        ),
        {"dataset_id": dataset_id, "batch_id": batch_id},
    )
