from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

from typing import cast
from uuid import UUID


T = TypeVar("T", bound="RetirementPreviewItem")


@_attrs_define
class RetirementPreviewItem:
    """
    Attributes:
        dataset_id (UUID):
        found (bool):
        writable (bool):
        title_matches (bool):
        active_jobs (int):
        active_refresh_runs (int):
        map_layers (int):
        vrt_dependents (list[str]):
        title (None | str | Unset):
    """

    dataset_id: UUID
    found: bool
    writable: bool
    title_matches: bool
    active_jobs: int
    active_refresh_runs: int
    map_layers: int
    vrt_dependents: list[str]
    title: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        dataset_id = str(self.dataset_id)

        found = self.found

        writable = self.writable

        title_matches = self.title_matches

        active_jobs = self.active_jobs

        active_refresh_runs = self.active_refresh_runs

        map_layers = self.map_layers

        vrt_dependents = self.vrt_dependents

        title: None | str | Unset
        if isinstance(self.title, Unset):
            title = UNSET
        else:
            title = self.title

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "dataset_id": dataset_id,
                "found": found,
                "writable": writable,
                "title_matches": title_matches,
                "active_jobs": active_jobs,
                "active_refresh_runs": active_refresh_runs,
                "map_layers": map_layers,
                "vrt_dependents": vrt_dependents,
            }
        )
        if title is not UNSET:
            field_dict["title"] = title

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        dataset_id = UUID(d.pop("dataset_id"))

        found = d.pop("found")

        writable = d.pop("writable")

        title_matches = d.pop("title_matches")

        active_jobs = d.pop("active_jobs")

        active_refresh_runs = d.pop("active_refresh_runs")

        map_layers = d.pop("map_layers")

        vrt_dependents = cast(list[str], d.pop("vrt_dependents"))

        def _parse_title(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        title = _parse_title(d.pop("title", UNSET))

        retirement_preview_item = cls(
            dataset_id=dataset_id,
            found=found,
            writable=writable,
            title_matches=title_matches,
            active_jobs=active_jobs,
            active_refresh_runs=active_refresh_runs,
            map_layers=map_layers,
            vrt_dependents=vrt_dependents,
            title=title,
        )

        retirement_preview_item.additional_properties = d
        return retirement_preview_item

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
