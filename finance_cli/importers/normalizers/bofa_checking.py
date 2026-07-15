from __future__ import annotations

import csv
import io
from pathlib import Path

from ..csv_normalizers import NormalizeResult, _format_amount, _parse_amount, _row_value

PRIMARY_KEY = "bofa_checking"
ALIASES: list[str] = []
SOURCE_NAME = "Bank of America"


def detect(lines: list[str]) -> bool:
    if not any("Summary Amt." in line for line in lines if line.strip()):
        return False
    return any(line.strip().startswith("Date,Description,Amount,Running Bal.") for line in lines)


def normalize(file_path: Path) -> NormalizeResult:
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
                "Source": SOURCE_NAME,
                "Is Payment": "false",
            }
        )

    if rows and all(not row.get("Card Ending") for row in rows):
        warnings.append(
            "No card ending available for BofA Checking - "
            "automatic account alias matching will be limited. "
            "Use 'dedup create-alias' to link manually."
        )

    return NormalizeResult(
        rows=rows,
        source_name=SOURCE_NAME,
        warnings=warnings,
        raw_row_count=raw_row_count,
        skipped_row_count=skipped_row_count,
    )
