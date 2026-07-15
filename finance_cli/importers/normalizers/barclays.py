from __future__ import annotations

import csv
import io
from pathlib import Path

from ..csv_normalizers import (
    NormalizeResult,
    _extract_card_ending,
    _format_amount,
    _parse_amount,
    _row_value,
)

PRIMARY_KEY = "barclays"
ALIASES: list[str] = []
SOURCE_NAME = "Barclays"


def detect(lines: list[str]) -> bool:
    return any(line.strip().lower().startswith("barclays") for line in lines if line.strip())


def normalize(file_path: Path) -> NormalizeResult:
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
                "Source": SOURCE_NAME,
                "Is Payment": "true" if "payment received" in description.lower() else "false",
            }
        )

    return NormalizeResult(
        rows=rows,
        source_name=SOURCE_NAME,
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )
