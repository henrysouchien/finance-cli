from __future__ import annotations

import csv
from pathlib import Path

from ..csv_normalizers import NormalizeResult, _format_amount, _parse_amount, _row_value

PRIMARY_KEY = "amex"
ALIASES = ["american_express"]
SOURCE_NAME = "American Express"


def detect(lines: list[str]) -> bool:
    return any(
        "Extended Details" in line and "Appears On Your Statement As" in line
        for line in lines
        if line.strip()
    )


def normalize(file_path: Path) -> NormalizeResult:
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
                "Card Ending": "",
                "Account Type": "credit_card",
                "Source": SOURCE_NAME,
                "Is Payment": "true" if is_payment else "false",
            }
            if category:
                normalized_row["Category"] = category
            if reference:
                normalized_row["Transaction ID"] = reference
            rows.append(normalized_row)

    return NormalizeResult(
        rows=rows,
        source_name=SOURCE_NAME,
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )
