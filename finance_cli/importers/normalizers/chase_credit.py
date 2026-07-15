from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

from ..csv_normalizers import NormalizeResult, _format_amount, _parse_amount, _row_value

logger = logging.getLogger(__name__)

PRIMARY_KEY = "chase_credit"
ALIASES: list[str] = []
SOURCE_NAME = "Chase"


def detect(lines: list[str]) -> bool:
    return any(
        "Transaction Date" in line
        and "Post Date" in line
        and "Type" in line
        and "Amount" in line
        and "Memo" in line
        for line in lines
        if line.strip()
    )


def normalize(file_path: Path) -> NormalizeResult:
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
                "Source": SOURCE_NAME,
                "Is Payment": "true" if txn_type.lower() == "payment" else "false",
            }
            if category:
                normalized_row["Category"] = category
            rows.append(normalized_row)

    return NormalizeResult(
        rows=rows,
        source_name=SOURCE_NAME,
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )
