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
    from ..models.retirement_item_outcome_dependents_type_0_item import (
        RetirementItemOutcomeDependentsType0Item,
    )


T = TypeVar("T", bound="RetirementItemOutcome")


@_attrs_define
class RetirementItemOutcome:
    """
    Attributes:
        id (UUID):
        dataset_id (UUID):
        status (str):
        attempts (int):
        created_at (datetime.datetime):
        updated_at (datetime.datetime):
        stage (None | str | Unset):
        code (None | str | Unset):
        detail (None | str | Unset):
        dependents (list[RetirementItemOutcomeDependentsType0Item] | None | Unset):
        table_name (None | str | Unset):
        finished_at (datetime.datetime | None | Unset):
    """

    id: UUID
    dataset_id: UUID
    status: str
    attempts: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    stage: None | str | Unset = UNSET
    code: None | str | Unset = UNSET
    detail: None | str | Unset = UNSET
    dependents: list[RetirementItemOutcomeDependentsType0Item] | None | Unset = UNSET
    table_name: None | str | Unset = UNSET
    finished_at: datetime.datetime | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        dataset_id = str(self.dataset_id)

        status = self.status

        attempts = self.attempts

        created_at = self.created_at.isoformat()

        updated_at = self.updated_at.isoformat()

        stage: None | str | Unset
        if isinstance(self.stage, Unset):
            stage = UNSET
        else:
            stage = self.stage

        code: None | str | Unset
        if isinstance(self.code, Unset):
            code = UNSET
        else:
            code = self.code

        detail: None | str | Unset
        if isinstance(self.detail, Unset):
            detail = UNSET
        else:
            detail = self.detail

        dependents: list[dict[str, Any]] | None | Unset
        if isinstance(self.dependents, Unset):
            dependents = UNSET
        elif isinstance(self.dependents, list):
            dependents = []
            for dependents_type_0_item_data in self.dependents:
                dependents_type_0_item = dependents_type_0_item_data.to_dict()
                dependents.append(dependents_type_0_item)

        else:
            dependents = self.dependents

        table_name: None | str | Unset
        if isinstance(self.table_name, Unset):
            table_name = UNSET
        else:
            table_name = self.table_name

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
                "dataset_id": dataset_id,
                "status": status,
                "attempts": attempts,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )
        if stage is not UNSET:
            field_dict["stage"] = stage
        if code is not UNSET:
            field_dict["code"] = code
        if detail is not UNSET:
            field_dict["detail"] = detail
        if dependents is not UNSET:
            field_dict["dependents"] = dependents
        if table_name is not UNSET:
            field_dict["table_name"] = table_name
        if finished_at is not UNSET:
            field_dict["finished_at"] = finished_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retirement_item_outcome_dependents_type_0_item import (
            RetirementItemOutcomeDependentsType0Item,
        )

        d = dict(src_dict)
        id = UUID(d.pop("id"))

        dataset_id = UUID(d.pop("dataset_id"))

        status = d.pop("status")

        attempts = d.pop("attempts")

        created_at = isoparse(d.pop("created_at"))

        updated_at = isoparse(d.pop("updated_at"))

        def _parse_stage(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        stage = _parse_stage(d.pop("stage", UNSET))

        def _parse_code(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        code = _parse_code(d.pop("code", UNSET))

        def _parse_detail(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        detail = _parse_detail(d.pop("detail", UNSET))

        def _parse_dependents(
            data: object,
        ) -> list[RetirementItemOutcomeDependentsType0Item] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                dependents_type_0 = []
                _dependents_type_0 = data
                for dependents_type_0_item_data in _dependents_type_0:
                    dependents_type_0_item = (
                        RetirementItemOutcomeDependentsType0Item.from_dict(
                            dependents_type_0_item_data
                        )
                    )

                    dependents_type_0.append(dependents_type_0_item)

                return dependents_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                list[RetirementItemOutcomeDependentsType0Item] | None | Unset, data
            )

        dependents = _parse_dependents(d.pop("dependents", UNSET))

        def _parse_table_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        table_name = _parse_table_name(d.pop("table_name", UNSET))

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

        retirement_item_outcome = cls(
            id=id,
            dataset_id=dataset_id,
            status=status,
            attempts=attempts,
            created_at=created_at,
            updated_at=updated_at,
            stage=stage,
            code=code,
            detail=detail,
            dependents=dependents,
            table_name=table_name,
            finished_at=finished_at,
        )

        retirement_item_outcome.additional_properties = d
        return retirement_item_outcome

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
