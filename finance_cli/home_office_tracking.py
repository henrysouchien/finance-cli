"""Home-office tax tracking setup helpers."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from finance_cli.exceptions import ValidationError
from finance_cli.models import cents_to_dollars
from finance_cli.user_rules import load_rules

_YEAR_RE = re.compile(r"^\d{4}$")
_SIMPLIFIED_RATE_CENTS_PER_SQFT = 500
_SIMPLIFIED_SQFT_CAP = 300


def _validate_year(year: Any) -> int:
    value = str(year or "").strip()
    if not _YEAR_RE.match(value):
        raise ValidationError("year must be in YYYY format")
    tax_year = int(value)
    if tax_year < 2000 or tax_year > 2100:
        raise ValidationError("year must be between 2000 and 2100")
    return tax_year


def _validate_sqft(value: Any, *, field_name: str) -> int:
    try:
        sqft = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer") from exc
    if sqft <= 0:
        raise ValidationError(f"{field_name} must be greater than 0")
    return sqft


def _get_tax_config(conn: sqlite3.Connection, tax_year: int) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT config_key, config_value
          FROM tax_config
         WHERE tax_year = ?
        """,
        (tax_year,),
    ).fetchall()
    return {str(row["config_key"]): str(row["config_value"]) for row in rows}


def _set_tax_config(conn: sqlite3.Connection, tax_year: int, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO tax_config (tax_year, config_key, config_value, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(tax_year, config_key) DO UPDATE
            SET config_value = excluded.config_value,
                updated_at = datetime('now')
        """,
        (tax_year, key, value),
    )


def _has_home_split_rule_conflict(rules_path: Path | None) -> bool:
    if rules_path is None:
        return False
    try:
        split_rules = load_rules(path=rules_path).split_rules
    except Exception:
        return False
    target_categories = {"rent", "utilities"}
    for rule in split_rules:
        match_category = (rule.match_category or "").strip().lower()
        business_category = (rule.business_category or "").strip().lower()
        if match_category in target_categories or business_category in target_categories:
            return True
    return False


def setup_home_office_tracking(
    conn: sqlite3.Connection,
    *,
    year: Any,
    sqft: Any,
    method: str = "simplified",
    total_sqft: Any = None,
    dry_run: bool = False,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    """Configure existing tax_config keys for simplified home-office tracking."""
    tax_year = _validate_year(year)
    normalized_method = str(method or "simplified").strip().lower()
    if normalized_method != "simplified":
        raise ValidationError(
            "setup_home_office_tracking currently supports method='simplified'; "
            "actual-method Form 8829 expense tracking is not implemented"
        )
    office_sqft = _validate_sqft(sqft, field_name="sqft")
    normalized_total_sqft = None
    if total_sqft not in (None, ""):
        normalized_total_sqft = _validate_sqft(total_sqft, field_name="total_sqft")
        if office_sqft > normalized_total_sqft:
            raise ValidationError("sqft must be less than or equal to total_sqft")
    if _has_home_split_rule_conflict(rules_path):
        raise ValidationError(
            "simplified home-office tracking conflicts with existing Rent/Utilities split rules"
        )

    eligible_sqft = min(office_sqft, _SIMPLIFIED_SQFT_CAP)
    tentative_deduction_cents = eligible_sqft * _SIMPLIFIED_RATE_CENTS_PER_SQFT
    before_config = _get_tax_config(conn, tax_year)
    updates = {
        "home_office_method": normalized_method,
        "home_office_sqft": str(office_sqft),
    }
    if normalized_total_sqft is not None:
        updates["home_total_sqft"] = str(normalized_total_sqft)
    changed_keys = sorted(
        key for key, value in updates.items() if before_config.get(key) != value
    )
    preview_config = {**before_config, **updates}
    data = {
        "tax_year": tax_year,
        "method": normalized_method,
        "office_sqft": office_sqft,
        "eligible_sqft": eligible_sqft,
        "total_sqft": normalized_total_sqft,
        "tentative_deduction_cents": tentative_deduction_cents,
        "tentative_deduction": cents_to_dollars(tentative_deduction_cents),
        "updated_keys": changed_keys,
        "config": preview_config,
        "dry_run": bool(dry_run),
    }
    summary = {
        "tax_year": tax_year,
        "updated_count": 0 if dry_run else len(changed_keys),
        "would_update_count": len(changed_keys),
        "tentative_deduction_cents": tentative_deduction_cents,
        "dry_run": bool(dry_run),
    }
    action = "Would configure" if dry_run else "Configured"
    cli_report = (
        f"{action} simplified home-office tracking for {tax_year}: "
        f"{office_sqft} sqft, tentative deduction ${cents_to_dollars(tentative_deduction_cents):,.2f}"
    )
    if dry_run:
        return {"data": data, "summary": summary, "cli_report": f"[DRY RUN] {cli_report}"}

    for key, value in updates.items():
        _set_tax_config(conn, tax_year, key, value)
    conn.commit()
    data["config"] = _get_tax_config(conn, tax_year)
    return {"data": data, "summary": summary, "cli_report": cli_report}


def handle_setup(
    args,
    conn: sqlite3.Connection,
    *,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    return setup_home_office_tracking(
        conn,
        year=getattr(args, "year", ""),
        sqft=getattr(args, "sqft", None),
        method=getattr(args, "method", "simplified"),
        total_sqft=getattr(args, "total_sqft", None),
        dry_run=bool(getattr(args, "dry_run", False)),
        rules_path=rules_path,
    )
