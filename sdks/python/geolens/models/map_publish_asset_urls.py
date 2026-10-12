from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

from typing import cast


T = TypeVar("T", bound="MapPublishAssetUrls")


@_attrs_define
class MapPublishAssetUrls:
    """Nonce-namespaced URLs of a generation's confirmed public assets.

    Attributes:
        style_url (None | str | Unset):
        sprite_url (None | str | Unset):
        sprite_json_url (None | str | Unset):
        thumbnail_url (None | str | Unset):
        og_image_url (None | str | Unset):
    """

    style_url: None | str | Unset = UNSET
    sprite_url: None | str | Unset = UNSET
    sprite_json_url: None | str | Unset = UNSET
    thumbnail_url: None | str | Unset = UNSET
    og_image_url: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        style_url: None | str | Unset
        if isinstance(self.style_url, Unset):
            style_url = UNSET
        else:
            style_url = self.style_url

        sprite_url: None | str | Unset
        if isinstance(self.sprite_url, Unset):
            sprite_url = UNSET
        else:
            sprite_url = self.sprite_url

        sprite_json_url: None | str | Unset
        if isinstance(self.sprite_json_url, Unset):
            sprite_json_url = UNSET
        else:
            sprite_json_url = self.sprite_json_url

        thumbnail_url: None | str | Unset
        if isinstance(self.thumbnail_url, Unset):
            thumbnail_url = UNSET
        else:
            thumbnail_url = self.thumbnail_url

        og_image_url: None | str | Unset
        if isinstance(self.og_image_url, Unset):
            og_image_url = UNSET
        else:
            og_image_url = self.og_image_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if style_url is not UNSET:
            field_dict["style_url"] = style_url
        if sprite_url is not UNSET:
            field_dict["sprite_url"] = sprite_url
        if sprite_json_url is not UNSET:
            field_dict["sprite_json_url"] = sprite_json_url
        if thumbnail_url is not UNSET:
            field_dict["thumbnail_url"] = thumbnail_url
        if og_image_url is not UNSET:
            field_dict["og_image_url"] = og_image_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_style_url(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        style_url = _parse_style_url(d.pop("style_url", UNSET))

        def _parse_sprite_url(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        sprite_url = _parse_sprite_url(d.pop("sprite_url", UNSET))

        def _parse_sprite_json_url(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        sprite_json_url = _parse_sprite_json_url(d.pop("sprite_json_url", UNSET))

        def _parse_thumbnail_url(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        thumbnail_url = _parse_thumbnail_url(d.pop("thumbnail_url", UNSET))

        def _parse_og_image_url(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        og_image_url = _parse_og_image_url(d.pop("og_image_url", UNSET))

        map_publish_asset_urls = cls(
            style_url=style_url,
            sprite_url=sprite_url,
            sprite_json_url=sprite_json_url,
            thumbnail_url=thumbnail_url,
            og_image_url=og_image_url,
        )

        map_publish_asset_urls.additional_properties = d
        return map_publish_asset_urls

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
