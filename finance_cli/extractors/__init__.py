"""Statement extractor abstraction and shared helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol, runtime_checkable

from ..importers.pdf import ExtractResult
from ..models import dollars_to_cents

EXTRACTOR_BACKENDS: tuple[str, ...] = ("ai", "azure", "bsc")


@dataclass(frozen=True)
class ExtractOptions:
    allow_partial: bool = False
    require_reconciled: bool = False
    institution_hint: str | None = None
    card_ending_hint: str | None = None


@dataclass(frozen=True)
class ExtractorMeta:
    backend: str
    bank_parser_label: str
    provider: str
    model_version: str
    reconcile_status: str
    content_text: str | None = None
    raw_api_response: str | None = None
    validation_summary: dict | None = None
    ai_prompt_version: str | None = None
    ai_prompt_hash: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    elapsed_ms: int = 0


@dataclass(frozen=True)
class ExtractorOutput:
    result: ExtractResult
    meta: ExtractorMeta


@runtime_checkable
class StatementExtractor(Protocol):
    name: str

    def extract(self, pdf_path: Path, options: ExtractOptions) -> ExtractorOutput:
        ...


def _to_decimal(value: str | float | int) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"invalid amount value {value!r}")
    if isinstance(value, (int, float)):
        return Decimal(str(float(value)))

    probe = str(value).strip()
    if not probe:
        raise ValueError("amount is empty")
    probe = probe.replace("$", "").replace(",", "")
    negative = False
    if probe.startswith("(") and probe.endswith(")"):
        negative = True
        probe = probe[1:-1].strip()
    if not probe:
        raise ValueError("amount is empty")
    try:
        dec = Decimal(probe)
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount value {value!r}") from exc
    return -abs(dec) if negative else dec


def parse_amount_to_cents(
    value: str | float | int | None = None,
    *,
    debit: str | float | int | None = None,
    credit: str | float | int | None = None,
) -> int:
    """Parse amount strings/floats into integer cents.

    Parentheses indicate negative values. When debit/credit are provided,
    debit is treated as negative and credit as positive.
    """
    if debit is not None or credit is not None:
        debit_cents = 0
        credit_cents = 0

        if debit is not None and str(debit).strip() != "":
            debit_cents = abs(dollars_to_cents(_to_decimal(debit)))
        if credit is not None and str(credit).strip() != "":
            credit_cents = abs(dollars_to_cents(_to_decimal(credit)))

        if debit_cents and credit_cents:
            raise ValueError(f"both debit and credit provided (debit={debit!r}, credit={credit!r})")
        if debit_cents:
            return -debit_cents
        if credit_cents:
            return credit_cents
        raise ValueError("debit/credit amount is empty")

    if value is None:
        raise ValueError("amount is missing")
    return dollars_to_cents(_to_decimal(value))


_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def normalize_date(value: str, *, year_hint: int | None = None) -> str:
    """Normalize date strings to ISO format (YYYY-MM-DD).

    Supported formats: YYYY-MM-DD, MM/DD/YY[YY], MM/DD, "Mon DD", "Mon DD, YYYY".
    When year is missing, *year_hint* (default: current year) is used.
    """
    probe = str(value or "").strip()
    if not probe:
        raise ValueError("date is empty")

    # ISO passthrough with validation.
    if len(probe) == 10 and probe[4] == "-" and probe[7] == "-":
        try:
            return date.fromisoformat(probe).isoformat()
        except ValueError as exc:
            raise ValueError(f"invalid ISO date {value!r}") from exc

    # MM/DD/YY[YY] or MM/DD (no year).
    parts = probe.split("/")
    if len(parts) in (2, 3):
        try:
            month = int(parts[0])
            day = int(parts[1])
        except ValueError:
            raise ValueError(f"unrecognized date format {value!r}")

        if len(parts) == 3:
            try:
                year = int(parts[2])
            except ValueError:
                raise ValueError(f"unrecognized date format {value!r}")
            if year < 100:
                year = 2000 + year if year < 70 else 1900 + year
        else:
            year = year_hint or date.today().year

        try:
            return date(year, month, day).isoformat()
        except ValueError as exc:
            raise ValueError(f"invalid date {value!r}") from exc

    # "Mon DD" or "Mon DD, YYYY" (e.g. "Jan 21", "January 21, 2026").
    tokens = probe.replace(",", " ").split()
    if len(tokens) >= 2:
        abbr = tokens[0][:3].lower()
        month = _MONTH_ABBR.get(abbr)
        if month is not None:
            try:
                day = int(tokens[1])
            except ValueError:
                raise ValueError(f"unrecognized date format {value!r}")
            if len(tokens) >= 3:
                try:
                    year = int(tokens[2])
                except ValueError:
                    raise ValueError(f"unrecognized date format {value!r}")
            else:
                year = year_hint or date.today().year
            try:
                return date(year, month, day).isoformat()
            except ValueError as exc:
                raise ValueError(f"invalid date {value!r}") from exc

    raise ValueError(f"unrecognized date format {value!r}")


def get_extractor(backend: str, config: dict) -> StatementExtractor:
    normalized = str(backend or "").strip().lower()
    cfg = config if isinstance(config, dict) else {}

    if normalized == "ai":
        from .ai_extractor import AIExtractor

        return AIExtractor(cfg)
    if normalized == "azure":
        from .azure_extractor import AzureExtractor

        return AzureExtractor(cfg)
    if normalized == "bsc":
        from .bsc_extractor import BSCExtractor

        return BSCExtractor(cfg)

    supported = ", ".join(EXTRACTOR_BACKENDS)
    raise ValueError(f"Unknown extractor backend '{backend}'. Supported: {supported}")


__all__ = [
    "EXTRACTOR_BACKENDS",
    "ExtractOptions",
    "ExtractorMeta",
    "ExtractorOutput",
    "StatementExtractor",
    "get_extractor",
    "normalize_date",
    "parse_amount_to_cents",
]
