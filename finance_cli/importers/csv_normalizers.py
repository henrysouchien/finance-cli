"""Institution CSV adapters that emit canonical rows for CSV import."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ..institution_names import is_known
from ..models import normalize_date

logger = logging.getLogger(__name__)


@dataclass
class NormalizeResult:
    rows: list[dict[str, str]]
    source_name: str
    warnings: list[str]
    raw_row_count: int
    skipped_row_count: int


_ALLOWED_ACCOUNT_TYPES = {"checking", "savings", "credit_card", "investment", "loan"}
_REQUIRED_ROW_KEYS = {"Account Type", "Amount", "Date", "Description"}
_OPTIONAL_ROW_KEYS = {"Card Ending", "Category", "Is Payment", "Source", "Transaction ID", "Use Type"}
_ALLOWED_ROW_KEYS = _REQUIRED_ROW_KEYS | _OPTIONAL_ROW_KEYS


def _row_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row and row[key] is not None:
            return str(row[key]).strip()
    return ""


def _parse_amount(value: str) -> Decimal:
    cleaned = value.strip().replace("$", "").replace(",", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount '{value}'") from exc


def _format_amount(amount: Decimal) -> str:
    quantized = amount.quantize(Decimal("0.01"))
    if quantized == Decimal("-0.00"):
        quantized = Decimal("0.00")
    return f"{quantized:.2f}"


def _extract_card_ending(line: str) -> str:
    account_match = re.search(r"account\s*number\s*:?\s*(.+)", line, flags=re.IGNORECASE)
    if account_match:
        digits = re.sub(r"\D", "", account_match.group(1))
        if len(digits) >= 4:
            return digits[-4:]

    masked_match = re.search(r"[Xx*]{4,}\s*(\d{4})", line)
    if masked_match:
        return masked_match.group(1)

    return ""


def normalize_result_from_payload(payload: Mapping[str, Any]) -> NormalizeResult:
    if not isinstance(payload, Mapping):
        raise ValueError("NormalizeResult payload must be a mapping")

    raw_rows = payload.get("rows", [])
    if not isinstance(raw_rows, list):
        raise ValueError("NormalizeResult.rows must be a list")

    rows: list[dict[str, str]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, Mapping):
            raise ValueError("Each normalized row must be a mapping")
        row = {str(key): "" if value is None else str(value) for key, value in raw_row.items() if key is not None}
        rows.append(row)

    warnings = payload.get("warnings", [])
    if not isinstance(warnings, list):
        raise ValueError("NormalizeResult.warnings must be a list")

    return NormalizeResult(
        rows=rows,
        source_name=str(payload.get("source_name", "")).strip(),
        warnings=[str(item) for item in warnings],
        raw_row_count=int(payload.get("raw_row_count", 0)),
        skipped_row_count=int(payload.get("skipped_row_count", 0)),
    )


def validate_normalize_result(
    result: NormalizeResult,
    *,
    expected_source_name: str | None = None,
) -> dict[str, Any]:
    issues: list[str] = []
    source_name = str(result.source_name or "").strip()

    if not source_name:
        issues.append("missing NormalizeResult.source_name")
    elif not is_known(source_name):
        issues.append(f"unknown source_name '{source_name}'")

    if expected_source_name and source_name != expected_source_name:
        issues.append(
            f"source_name '{source_name}' does not match expected '{expected_source_name}'"
        )

    for row_idx, row in enumerate(result.rows, start=1):
        missing = sorted(key for key in _REQUIRED_ROW_KEYS if not str(row.get(key, "")).strip())
        if missing:
            issues.append(f"row {row_idx}: missing required fields: {', '.join(missing)}")

        extras = sorted(set(row) - _ALLOWED_ROW_KEYS)
        if extras:
            issues.append(f"row {row_idx}: unexpected fields: {', '.join(extras)}")

        date_value = str(row.get("Date", "")).strip()
        if date_value:
            try:
                normalize_date(date_value)
            except Exception:
                issues.append(f"row {row_idx}: invalid date '{date_value}'")

        amount_value = str(row.get("Amount", "")).strip()
        if amount_value:
            try:
                _parse_amount(amount_value)
            except ValueError:
                issues.append(f"row {row_idx}: invalid amount '{amount_value}'")

        account_type = str(row.get("Account Type", "")).strip()
        if account_type and account_type not in _ALLOWED_ACCOUNT_TYPES:
            issues.append(f"row {row_idx}: invalid account type '{account_type}'")

        row_source = str(row.get("Source", "")).strip()
        if not row_source:
            issues.append(f"row {row_idx}: missing Source")
        elif expected_source_name and row_source != expected_source_name:
            issues.append(
                f"row {row_idx}: Source '{row_source}' does not match expected '{expected_source_name}'"
            )
        elif not is_known(row_source):
            issues.append(f"row {row_idx}: unknown Source '{row_source}'")

        payment_value = str(row.get("Is Payment", "")).strip().lower()
        if payment_value and payment_value not in {"0", "1", "false", "no", "true", "y", "yes"}:
            issues.append(f"row {row_idx}: invalid Is Payment '{row.get('Is Payment', '')}'")

    return {
        "issues": issues,
        "raw_row_count": result.raw_row_count,
        "row_count": len(result.rows),
        "skipped_row_count": result.skipped_row_count,
        "source_name": source_name,
        "valid": not issues,
        "warning_count": len(result.warnings),
    }


from .normalizers import get_normalizer_loader, normalize_registry_key  # noqa: E402


def detect_csv_institution(file_path: str | Path) -> str | None:
    """Sniff a CSV preamble/header and infer a supported institution key."""
    path = Path(file_path)
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as fh:
            lines = [fh.readline() for _ in range(20)]
    except (OSError, UnicodeError):
        logger.info("CSV institution detection failed file=%s reason=read_error", path)
        return None

    loader = get_normalizer_loader()
    for entry in loader.detection_entries():
        try:
            if entry.detect_fn(lines):
                logger.info("CSV institution detection file=%s institution=%s", path, entry.primary_key)
                return entry.primary_key
        except Exception as exc:
            logger.warning(
                "CSV institution detection module failed file=%s normalizer=%s error=%s",
                path,
                entry.primary_key,
                exc,
            )

    logger.info("CSV institution detection file=%s institution=%s", path, "none")
    return None


def normalize_csv(file_path: str | Path, institution: str) -> NormalizeResult:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    normalized_institution = normalize_registry_key(institution)
    loader = get_normalizer_loader()
    entry = loader.get_entry(normalized_institution)
    if entry is None:
        supported = ", ".join(supported_institutions())
        raise ValueError(
            f"unsupported institution '{institution}'. supported institutions: {supported}"
        )

    result = entry.normalize_fn(path)
    logger.info(
        "CSV normalization complete file=%s institution=%s source=%s rows=%s skipped=%s warnings=%s",
        path,
        normalized_institution,
        result.source_name,
        len(result.rows),
        result.skipped_row_count,
        len(result.warnings),
    )
    return result


def supported_institutions() -> list[str]:
    return get_normalizer_loader().supported_keys()


__all__ = ["NormalizeResult", "detect_csv_institution", "normalize_csv", "supported_institutions"]
