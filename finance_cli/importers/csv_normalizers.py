"""Institution CSV adapters that emit canonical rows for CSV import."""

from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class NormalizeResult:
    rows: list[dict[str, str]]
    source_name: str
    warnings: list[str]
    raw_row_count: int
    skipped_row_count: int


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


def _normalize_apple_card(file_path: Path) -> NormalizeResult:
    rows: list[dict[str, str]] = []
    warnings: list[str] = []
    raw_row_count = 0
    skipped_row_count = 0

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row_num, raw_row in enumerate(reader, start=2):
            raw_row_count += 1
            row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}

            date_value = _row_value(row, "Transaction Date")
            description = _row_value(row, "Description") or _row_value(row, "Merchant")
            amount_raw = _row_value(row, "Amount (USD)", "Amount")
            txn_type = _row_value(row, "Type")
            category = _row_value(row, "Category")

            if not date_value or not description or not amount_raw:
                skipped_row_count += 1
                warnings.append(f"row {row_num}: missing required fields")
                continue

            try:
                normalized_amount = _format_amount(-_parse_amount(amount_raw))
            except ValueError:
                skipped_row_count += 1
                warnings.append(f"row {row_num}: invalid amount '{amount_raw}'")
                continue

            normalized_row = {
                "Date": date_value,
                "Description": description,
                "Amount": normalized_amount,
                "Card Ending": "Apple",
                "Account Type": "credit_card",
                "Source": "Apple Card",
                "Is Payment": "true" if txn_type.lower() == "payment" else "false",
            }
            if category:
                normalized_row["Category"] = category
            rows.append(normalized_row)

    return NormalizeResult(
        rows=rows,
        source_name="Apple Card",
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )


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


def _normalize_barclays(file_path: Path) -> NormalizeResult:
    rows: list[dict[str, str]] = []
    warnings: list[str] = []
    raw_row_count = 0
    skipped_row_count = 0

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        lines = fh.readlines()

    header_idx = -1
    card_ending = ""
    for idx, line in enumerate(lines):
        if line.strip().startswith("Transaction Date"):
            header_idx = idx
            break
        maybe_card_ending = _extract_card_ending(line)
        if maybe_card_ending:
            card_ending = maybe_card_ending

    if header_idx < 0:
        raise ValueError("could not find Barclays transaction header row")

    payload = io.StringIO("".join(lines[header_idx:]))
    reader = csv.DictReader(payload)
    for row_num, raw_row in enumerate(reader, start=header_idx + 2):
        raw_row_count += 1
        row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}

        date_value = _row_value(row, "Transaction Date")
        description = _row_value(row, "Description")
        amount_raw = _row_value(row, "Amount")

        if not date_value or not description or not amount_raw:
            skipped_row_count += 1
            warnings.append(f"row {row_num}: missing required fields")
            continue

        try:
            normalized_amount = _format_amount(_parse_amount(amount_raw))
        except ValueError:
            skipped_row_count += 1
            warnings.append(f"row {row_num}: invalid amount '{amount_raw}'")
            continue

        rows.append(
            {
                "Date": date_value,
                "Description": description,
                "Amount": normalized_amount,
                "Card Ending": card_ending,
                "Account Type": "credit_card",
                "Source": "Barclays",
                "Is Payment": "true" if "payment received" in description.lower() else "false",
            }
        )

    return NormalizeResult(
        rows=rows,
        source_name="Barclays",
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )


def _normalize_chase_credit(file_path: Path) -> NormalizeResult:
    rows: list[dict[str, str]] = []
    warnings: list[str] = []
    raw_row_count = 0
    skipped_row_count = 0

    card_ending_match = re.search(r"Chase(\d{4})_", file_path.name)
    card_ending = card_ending_match.group(1) if card_ending_match else ""
    if not card_ending:
        logger.warning(
            "Could not extract Chase card ending from filename=%s "
            "(expected pattern 'Chase<4digits>_'). Account aliasing may not work.",
            file_path.name,
        )

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row_num, raw_row in enumerate(reader, start=2):
            raw_row_count += 1
            row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}

            date_value = _row_value(row, "Transaction Date")
            description = _row_value(row, "Description")
            amount_raw = _row_value(row, "Amount")
            txn_type = _row_value(row, "Type")
            category = _row_value(row, "Category")

            if not date_value or not description or not amount_raw:
                skipped_row_count += 1
                warnings.append(f"row {row_num}: missing required fields")
                continue

            try:
                normalized_amount = _format_amount(_parse_amount(amount_raw))
            except ValueError:
                skipped_row_count += 1
                warnings.append(f"row {row_num}: invalid amount '{amount_raw}'")
                continue

            normalized_row = {
                "Date": date_value,
                "Description": description,
                "Amount": normalized_amount,
                "Card Ending": card_ending,
                "Account Type": "credit_card",
                "Source": "Chase",
                "Is Payment": "true" if txn_type.lower() == "payment" else "false",
            }
            if category:
                normalized_row["Category"] = category
            rows.append(normalized_row)

    return NormalizeResult(
        rows=rows,
        source_name="Chase",
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )


def _normalize_amex(file_path: Path) -> NormalizeResult:
    rows: list[dict[str, str]] = []
    warnings: list[str] = []
    raw_row_count = 0
    skipped_row_count = 0

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row_num, raw_row in enumerate(reader, start=2):
            raw_row_count += 1
            row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}

            date_value = _row_value(row, "Date")
            description = _row_value(row, "Description")
            amount_raw = _row_value(row, "Amount")
            category = _row_value(row, "Category")
            reference = _row_value(row, "Reference").strip("'")

            if not date_value or not description or not amount_raw:
                skipped_row_count += 1
                warnings.append(f"row {row_num}: missing required fields")
                continue

            try:
                normalized_amount = _format_amount(-_parse_amount(amount_raw))
            except ValueError:
                skipped_row_count += 1
                warnings.append(f"row {row_num}: invalid amount '{amount_raw}'")
                continue

            is_payment = "PAYMENT - THANK YOU" in description.upper()

            normalized_row: dict[str, str] = {
                "Date": date_value,
                "Description": description,
                "Amount": normalized_amount,
                "Card Ending": "Amex",
                "Account Type": "credit_card",
                "Source": "American Express",
                "Is Payment": "true" if is_payment else "false",
            }
            if category:
                normalized_row["Category"] = category
            if reference:
                normalized_row["Transaction ID"] = reference
            rows.append(normalized_row)

    return NormalizeResult(
        rows=rows,
        source_name="American Express",
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )


def _normalize_bofa_checking(file_path: Path) -> NormalizeResult:
    rows: list[dict[str, str]] = []
    warnings: list[str] = []
    raw_row_count = 0
    skipped_row_count = 0

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        lines = fh.readlines()

    header_idx = -1
    for idx, line in enumerate(lines):
        if line.strip().startswith("Date,Description,Amount"):
            header_idx = idx
            break

    if header_idx < 0:
        raise ValueError("could not find BofA Checking transaction header row")

    payload = io.StringIO("".join(lines[header_idx:]))
    reader = csv.DictReader(payload)
    for row_num, raw_row in enumerate(reader, start=header_idx + 2):
        raw_row_count += 1
        row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}

        date_value = _row_value(row, "Date")
        description = _row_value(row, "Description")
        amount_raw = _row_value(row, "Amount")

        if not date_value or not description or not amount_raw:
            skipped_row_count += 1
            warnings.append(f"row {row_num}: missing required fields")
            continue

        try:
            normalized_amount = _format_amount(_parse_amount(amount_raw))
        except ValueError:
            skipped_row_count += 1
            warnings.append(f"row {row_num}: invalid amount '{amount_raw}'")
            continue

        rows.append(
            {
                "Date": date_value,
                "Description": description,
                "Amount": normalized_amount,
                "Card Ending": "",
                "Account Type": "checking",
                "Source": "Bank of America",
                "Is Payment": "false",
            }
        )

    if rows and all(not r.get("Card Ending") for r in rows):
        warnings.append(
            "No card ending available for BofA Checking - "
            "automatic account alias matching will be limited. "
            "Use 'dedup create-alias' to link manually."
        )

    return NormalizeResult(
        rows=rows,
        source_name="Bank of America",
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )


_NORMALIZER_MAP: dict[str, Callable[[Path], NormalizeResult]] = {
    "american_express": _normalize_amex,
    "amex": _normalize_amex,
    "apple_card": _normalize_apple_card,
    "apple": _normalize_apple_card,
    "barclays": _normalize_barclays,
    "bofa_checking": _normalize_bofa_checking,
    "chase_credit": _normalize_chase_credit,
}


def detect_csv_institution(file_path: str | Path) -> str | None:
    """Sniff a CSV preamble/header and infer a supported institution key."""
    path = Path(file_path)
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as fh:
            lines = [fh.readline() for _ in range(20)]
    except (OSError, UnicodeError):
        logger.info("CSV institution detection failed file=%s reason=read_error", path)
        return None

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if "Amount (USD)" in stripped:
            logger.info("CSV institution detection file=%s institution=%s", path, "apple_card")
            return "apple_card"
        if stripped.lower().startswith("barclays"):
            logger.info("CSV institution detection file=%s institution=%s", path, "barclays")
            return "barclays"
        # Chase Credit: header has Transaction Date, Post Date, Type, Amount, Memo
        if (
            "Transaction Date" in stripped
            and "Post Date" in stripped
            and "Type" in stripped
            and "Amount" in stripped
            and "Memo" in stripped
        ):
            logger.info("CSV institution detection file=%s institution=%s", path, "chase_credit")
            return "chase_credit"
        # Amex: header has "Extended Details" and "Appears On Your Statement As"
        if "Extended Details" in stripped and "Appears On Your Statement As" in stripped:
            logger.info("CSV institution detection file=%s institution=%s", path, "amex")
            return "amex"
        # BofA family: preamble contains "Summary Amt."
        if "Summary Amt." in stripped:
            for lookahead in lines:
                if lookahead.strip().startswith("Date,Description,Amount,Running Bal."):
                    logger.info("CSV institution detection file=%s institution=%s", path, "bofa_checking")
                    return "bofa_checking"
            logger.info("CSV institution detection file=%s institution=%s", path, "none")
            return None

    logger.info("CSV institution detection file=%s institution=%s", path, "none")
    return None


def normalize_csv(file_path: str | Path, institution: str) -> NormalizeResult:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    normalized_institution = re.sub(r"[\s-]+", "_", institution.strip().lower())
    normalizer = _NORMALIZER_MAP.get(normalized_institution)
    if normalizer is None:
        supported = ", ".join(supported_institutions())
        raise ValueError(
            f"unsupported institution '{institution}'. supported institutions: {supported}"
        )

    result = normalizer(path)
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
    return sorted(_NORMALIZER_MAP.keys())


__all__ = ["NormalizeResult", "detect_csv_institution", "normalize_csv", "supported_institutions"]
