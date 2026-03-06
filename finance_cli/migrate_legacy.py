"""One-time migration utilities from legacy financial_system CSVs."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .importers import import_csv, import_vendor_memory_csv


DEFAULT_TXN_FILES = [
    "tagged_all_credit_combined_cleaned_2024.csv",
    "tagged_checking_cleaned_2024.csv",
]


def migrate_legacy_source(
    conn: sqlite3.Connection,
    source_dir: str | Path,
    dry_run: bool = False,
) -> dict:
    source = Path(source_dir)
    if not source.exists() or not source.is_dir():
        raise FileNotFoundError(f"Legacy source directory not found: {source}")

    summary: dict[str, object] = {
        "vendor_memory": None,
        "transactions": [],
    }

    memory_file = source / "transaction_memory.csv"
    if memory_file.exists():
        summary["vendor_memory"] = import_vendor_memory_csv(conn, memory_file, dry_run=dry_run)

    for name in DEFAULT_TXN_FILES:
        path = source / name
        if not path.exists():
            continue
        source_name = name.replace(".csv", "")
        report = import_csv(conn, path, source_name=source_name, dry_run=dry_run, validate_name=False)
        summary["transactions"].append({
            "file": str(path),
            **report.as_dict(),
        })

    return summary
