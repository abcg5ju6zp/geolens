"""Refresh-run failure codes that name a TRANSIENT cause.

A run whose last attempt failed with one of these codes is re-driven with the
same run identity instead of waiting for the next scheduled occurrence. The
set is the contract an overlay scheduler reads to pick its backoff.
"""

from __future__ import annotations

# The origin could not be reached (transport/timeout) or its credential store
# was temporarily unavailable. Missing/gone sources, expired credentials,
# policy refusals and verification outcomes stay terminal.
TRANSIENT_RUN_ERROR_CODES = frozenset(
    {
        "source_unavailable",
        "source_inaccessible",
        "credential_store_unavailable",
    }
)


def is_transient_run_code(code: str | None) -> bool:
    """Whether a failure ``code`` permits another delivery of the same run."""
    return code in TRANSIENT_RUN_ERROR_CODES
