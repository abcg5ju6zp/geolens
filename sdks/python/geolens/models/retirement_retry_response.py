from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, TYPE_CHECKING

from attrs import define as _attrs_define
from attrs import field as _attrs_field


if TYPE_CHECKING:
    from ..models.retirement_batch_response import RetirementBatchResponse


T = TypeVar("T", bound="RetirementRetryResponse")


@_attrs_define
class RetirementRetryResponse:
    """
    Attributes:
        reopened (int):
        batch (RetirementBatchResponse):
    """

    reopened: int
    batch: RetirementBatchResponse
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reopened = self.reopened

        batch = self.batch.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "reopened": reopened,
                "batch": batch,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retirement_batch_response import RetirementBatchResponse

        d = dict(src_dict)
        reopened = d.pop("reopened")

        batch = RetirementBatchResponse.from_dict(d.pop("batch"))

        retirement_retry_response = cls(
            reopened=reopened,
            batch=batch,
        )

        retirement_retry_response.additional_properties = d
        return retirement_retry_response

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
