from typing import Literal, cast

MapPublishBlockStatus = Literal[
    "active", "building", "cancelled", "failed", "pending", "ready", "superseded"
]

MAP_PUBLISH_BLOCK_STATUS_VALUES: set[MapPublishBlockStatus] = {
    "active",
    "building",
    "cancelled",
    "failed",
    "pending",
    "ready",
    "superseded",
}


def check_map_publish_block_status(value: str) -> MapPublishBlockStatus:
    if value in MAP_PUBLISH_BLOCK_STATUS_VALUES:
        return cast(MapPublishBlockStatus, value)
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MAP_PUBLISH_BLOCK_STATUS_VALUES!r}"
    )
