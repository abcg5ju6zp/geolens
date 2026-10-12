from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, TYPE_CHECKING

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

from dateutil.parser import isoparse
from typing import cast
from uuid import UUID
import datetime

if TYPE_CHECKING:
    from ..models.retirement_item_outcome import RetirementItemOutcome


T = TypeVar("T", bound="RetirementBatchResponse")


@_attrs_define
class RetirementBatchResponse:
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
        items (list[RetirementItemOutcome]):
        started_at (datetime.datetime | None | Unset):
        heartbeat_at (datetime.datetime | None | Unset):
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
    items: list[RetirementItemOutcome]
    started_at: datetime.datetime | None | Unset = UNSET
    heartbeat_at: datetime.datetime | None | Unset = UNSET
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

        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        started_at: None | str | Unset
        if isinstance(self.started_at, Unset):
            started_at = UNSET
        elif isinstance(self.started_at, datetime.datetime):
            started_at = self.started_at.isoformat()
        else:
            started_at = self.started_at

        heartbeat_at: None | str | Unset
        if isinstance(self.heartbeat_at, Unset):
            heartbeat_at = UNSET
        elif isinstance(self.heartbeat_at, datetime.datetime):
            heartbeat_at = self.heartbeat_at.isoformat()
        else:
            heartbeat_at = self.heartbeat_at

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
                "items": items,
            }
        )
        if started_at is not UNSET:
            field_dict["started_at"] = started_at
        if heartbeat_at is not UNSET:
            field_dict["heartbeat_at"] = heartbeat_at
        if finished_at is not UNSET:
            field_dict["finished_at"] = finished_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retirement_item_outcome import RetirementItemOutcome

        d = dict(src_dict)
        id = UUID(d.pop("id"))

        status = d.pop("status")

        total_count = d.pop("total_count")

        completed_count = d.pop("completed_count")

        conflict_count = d.pop("conflict_count")

        retryable_count = d.pop("retryable_count")

        rejected_count = d.pop("rejected_count")

        created_at = isoparse(d.pop("created_at"))

        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = RetirementItemOutcome.from_dict(items_item_data)

            items.append(items_item)

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

        def _parse_heartbeat_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                heartbeat_at_type_0 = isoparse(data)

                return heartbeat_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        heartbeat_at = _parse_heartbeat_at(d.pop("heartbeat_at", UNSET))

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

        retirement_batch_response = cls(
            id=id,
            status=status,
            total_count=total_count,
            completed_count=completed_count,
            conflict_count=conflict_count,
            retryable_count=retryable_count,
            rejected_count=rejected_count,
            created_at=created_at,
            items=items,
            started_at=started_at,
            heartbeat_at=heartbeat_at,
            finished_at=finished_at,
        )

        retirement_batch_response.additional_properties = d
        return retirement_batch_response

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
