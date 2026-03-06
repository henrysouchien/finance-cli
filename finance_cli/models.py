"""Domain models and money conversion helpers."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_serializer

_CENTS = Decimal("100")


def dollars_to_cents(value: float | int | str | Decimal) -> int:
    quantized = (Decimal(str(value)) * _CENTS).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(quantized)


_MM_DD_YYYY_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
_MM_DD_YY_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$")


def normalize_date(value: str) -> str:
    """Convert MM/DD/YY or MM/DD/YYYY to YYYY-MM-DD; pass through others."""
    probe = value.strip()
    if _MM_DD_YYYY_RE.match(probe):
        return datetime.strptime(probe, "%m/%d/%Y").date().isoformat()
    if _MM_DD_YY_RE.match(probe):
        return datetime.strptime(probe, "%m/%d/%y").date().isoformat()
    return probe


def cents_to_dollars(value: int) -> float:
    return float((Decimal(value) / _CENTS).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


class TransactionModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    account_id: Optional[str] = None
    date: str
    description: str
    amount_cents: int
    category_id: Optional[str] = None
    category_source: Optional[str] = None
    category_confidence: Optional[float] = None
    use_type: Optional[str] = None
    is_payment: int = 0
    is_recurring: int = 0
    is_reviewed: int = 0
    is_active: int = 1

    @field_serializer("amount_cents")
    def serialize_amount_cents(self, value: int) -> int:
        return value

    @property
    def amount(self) -> float:
        return cents_to_dollars(self.amount_cents)
