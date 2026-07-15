"""Shared heuristics for interpreting transaction clusters."""

from __future__ import annotations

from .categorizer import normalize_description

POSSIBLE_MULTI_PASSENGER_TRAVEL_FLAG = "possible_multi_passenger_travel"

_TRAVEL_TICKET_DESCRIPTION_MARKERS = (
    "air lines",
    "airlines",
    "airways",
    "air canada",
    "air france",
    "alaska airlines",
    "amtrak",
    "american airlines",
    "british airways",
    "delta air",
    "emirates",
    "flight",
    "frontier airlines",
    "jetblue",
    "klm",
    "lufthansa",
    "qatar",
    "southwest air",
    "southwest airlines",
    "spirit airlines",
    "turkish airlines",
    "united air",
    "united airlines",
    "virgin atlantic",
)


def is_possible_multi_passenger_travel_group(count: int, description_key: str) -> bool:
    """Return true when an even duplicate-looking travel group may be separate tickets."""

    normalized_description = normalize_description(description_key or "")
    return count > 1 and count % 2 == 0 and any(
        marker in normalized_description for marker in _TRAVEL_TICKET_DESCRIPTION_MARKERS
    )


def duplicate_charge_review_flags(count: int, description_key: str) -> tuple[str, ...]:
    if is_possible_multi_passenger_travel_group(count, description_key):
        return (POSSIBLE_MULTI_PASSENGER_TRAVEL_FLAG,)
    return ()
