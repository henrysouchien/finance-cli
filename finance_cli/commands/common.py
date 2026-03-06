"""Shared CLI helpers and JSON envelope utilities."""

from __future__ import annotations

import json
import sqlite3
from datetime import date

from .. import __version__
from ..models import cents_to_dollars


def success_envelope(
    command: str,
    data: dict,
    summary: dict | None = None,
    cli_report: str | None = None,
) -> dict:
    payload = {
        "status": "success",
        "command": command,
        "version": __version__,
        "data": data,
        "summary": summary or {},
    }
    if cli_report:
        payload["cli_report"] = cli_report
    return payload


def error_envelope(command: str, message: str, cli_report: str | None = None) -> dict:
    payload = {
        "status": "error",
        "command": command,
        "version": __version__,
        "error": message,
    }
    payload["cli_report"] = cli_report or f"Error: {message}"
    return payload


def print_envelope(envelope: dict, output_format: str) -> None:
    if output_format == "cli":
        cli_report = envelope.get("cli_report")
        if cli_report:
            print(cli_report)
            return
    print(json.dumps(envelope, indent=2, sort_keys=False, default=str))


def bool_flag(value: int | bool | None) -> bool:
    return bool(int(value or 0))


def today_iso() -> str:
    return date.today().isoformat()


def txn_row_to_dict(row: dict) -> dict:
    amount_cents = int(row["amount_cents"])
    out = dict(row)
    out["amount"] = cents_to_dollars(amount_cents)
    return out


def fmt_dollars(val: float) -> str:
    """Format dollar amount: $1,234.56 or -$1,234.56"""
    if val < 0:
        return f"-${abs(val):,.2f}"
    return f"${val:,.2f}"


def use_type_filter(view: str) -> str:
    """Return SQL WHERE clause fragment for use_type filtering."""
    if view == "business":
        return "AND t.use_type = 'Business'"
    if view == "personal":
        return "AND (t.use_type = 'Personal' OR t.use_type IS NULL)"
    return ""


def get_category_id_by_name(
    conn: sqlite3.Connection,
    category_name: str | None,
    *,
    required: bool = False,
) -> str | None:
    if not category_name:
        return None
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (category_name,)).fetchone()
    if row:
        return row["id"]
    if required:
        raise ValueError(f"Category '{category_name}' not found")
    return None
