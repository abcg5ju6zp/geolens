"""Text helpers shared by the DCAT-family serializers."""

from __future__ import annotations


def has_text(value: str | None) -> bool:
    return bool(value and value.strip())


def required_description(title: str, summary: str | None) -> str:
    """Return a non-empty, truthful description for catalog harvesting."""
    if has_text(summary):
        return summary.strip()
    return title.strip() or "Untitled dataset"
