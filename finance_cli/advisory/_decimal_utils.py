"""Decimal boundary coercion helpers for advisory math."""

from decimal import Decimal


def to_decimal(value: Decimal | float | int | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
