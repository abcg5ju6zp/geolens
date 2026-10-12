from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

from dateutil.parser import isoparse
from typing import cast
from uuid import UUID
import datetime


T = TypeVar("T", bound="RetirementBatchSummary")


@_attrs_define
class RetirementBatchSummary:
    """
    Attributes:
        id (UUID):
        status (str):
        total_count (int):
        completed_count (int):
        conflict_count (int):
        retryable_count (int):
        rejected_count (int):
        created_at (datetime.datetime):
        started_at (datetime.datetime | None | Unset):
        finished_at (datetime.datetime | None | Unset):
    """

    id: UUID
    status: str
    total_count: int
    completed_count: int
    conflict_count: int
    retryable_count: int
    rejected_count: int
    created_at: datetime.datetime
    started_at: datetime.datetime | None | Unset = UNSET
    finished_at: datetime.datetime | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        status = self.status

        total_count = self.total_count

        completed_count = self.completed_count

        conflict_count = self.conflict_count

        retryable_count = self.retryable_count

        rejected_count = self.rejected_count

        created_at = self.created_at.isoformat()

        started_at: None | str | Unset
        if isinstance(self.started_at, Unset):
            started_at = UNSET
        elif isinstance(self.started_at, datetime.datetime):
            started_at = self.started_at.isoformat()
        else:
            started_at = self.started_at

        finished_at: None | str | Unset
        if isinstance(self.finished_at, Unset):
            finished_at = UNSET
        elif isinstance(self.finished_at, datetime.datetime):
            finished_at = self.finished_at.isoformat()
        else:
            finished_at = self.finished_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "status": status,
                "total_count": total_count,
                "completed_count": completed_count,
                "conflict_count": conflict_count,
                "retryable_count": retryable_count,
                "rejected_count": rejected_count,
                "created_at": created_at,
            }
        )
        if started_at is not UNSET:
            field_dict["started_at"] = started_at
        if finished_at is not UNSET:
            field_dict["finished_at"] = finished_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = UUID(d.pop("id"))

        status = d.pop("status")

        total_count = d.pop("total_count")

        completed_count = d.pop("completed_count")

        conflict_count = d.pop("conflict_count")

        retryable_count = d.pop("retryable_count")

        rejected_count = d.pop("rejected_count")

        created_at = isoparse(d.pop("created_at"))

        def _parse_started_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                started_at_type_0 = isoparse(data)

                return started_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        started_at = _parse_started_at(d.pop("started_at", UNSET))

        def _parse_finished_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                finished_at_type_0 = isoparse(data)

                return finished_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        finished_at = _parse_finished_at(d.pop("finished_at", UNSET))

        retirement_batch_summary = cls(
            id=id,
            status=status,
            total_count=total_count,
            completed_count=completed_count,
            conflict_count=conflict_count,
            retryable_count=retryable_count,
            rejected_count=rejected_count,
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at,
        )

        retirement_batch_summary.additional_properties = d
        return retirement_batch_summary

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
