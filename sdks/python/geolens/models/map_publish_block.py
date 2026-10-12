from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, TYPE_CHECKING

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

from ..models.map_publish_block_status import check_map_publish_block_status
from ..models.map_publish_block_status import MapPublishBlockStatus
from typing import cast
from uuid import UUID

if TYPE_CHECKING:
    from ..models.map_publish_asset_urls import MapPublishAssetUrls


T = TypeVar("T", bound="MapPublishBlock")


@_attrs_define
class MapPublishBlock:
    """Publish-generation state attached to public map responses.

    Private maps return no block. ``generation_id`` targets the follow-up
    thumbnail/OG-image upload; ``missing_required`` names required assets not
    yet confirmed; URLs are populated once the generation is active.

        Attributes:
            generation_id (UUID):
            generation_number (int):
            status (MapPublishBlockStatus):
            missing_required (list[str] | Unset):
            assets (MapPublishAssetUrls | Unset): Nonce-namespaced URLs of a generation's confirmed public assets.
    """

    generation_id: UUID
    generation_number: int
    status: MapPublishBlockStatus
    missing_required: list[str] | Unset = UNSET
    assets: MapPublishAssetUrls | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        generation_id = str(self.generation_id)

        generation_number = self.generation_number

        status: str = self.status

        missing_required: list[str] | Unset = UNSET
        if not isinstance(self.missing_required, Unset):
            missing_required = self.missing_required

        assets: dict[str, Any] | Unset = UNSET
        if not isinstance(self.assets, Unset):
            assets = self.assets.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "generation_id": generation_id,
                "generation_number": generation_number,
                "status": status,
            }
        )
        if missing_required is not UNSET:
            field_dict["missing_required"] = missing_required
        if assets is not UNSET:
            field_dict["assets"] = assets

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.map_publish_asset_urls import MapPublishAssetUrls

        d = dict(src_dict)
        generation_id = UUID(d.pop("generation_id"))

        generation_number = d.pop("generation_number")

        status = check_map_publish_block_status(d.pop("status"))

        missing_required = cast(list[str], d.pop("missing_required", UNSET))

        _assets = d.pop("assets", UNSET)
        assets: MapPublishAssetUrls | Unset
        if isinstance(_assets, Unset):
            assets = UNSET
        else:
            assets = MapPublishAssetUrls.from_dict(_assets)

        map_publish_block = cls(
            generation_id=generation_id,
            generation_number=generation_number,
            status=status,
            missing_required=missing_required,
            assets=assets,
        )

        map_publish_block.additional_properties = d
        return map_publish_block

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
