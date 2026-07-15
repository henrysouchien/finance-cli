"""Bulk business transaction action helpers."""

from __future__ import annotations

import sqlite3
from typing import Any
import uuid

from finance_cli.categorizer import normalize_description
from finance_cli.exceptions import NotFoundError, ValidationError
from finance_cli.models import cents_to_dollars

_MAX_BULK_TRANSACTION_IDS = 200


def _normalize_ids(ids: Any) -> list[str]:
    if isinstance(ids, str):
        raw_values = ids.split(",")
    else:
        raw_values = list(ids or [])
    normalized: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        txn_id = str(value or "").strip()
        if not txn_id or txn_id in seen:
            continue
        seen.add(txn_id)
        normalized.append(txn_id)
    if not normalized:
        raise ValidationError("at least one transaction id is required")
    if len(normalized) > _MAX_BULK_TRANSACTION_IDS:
        raise ValidationError(
            f"bulk actions accept at most {_MAX_BULK_TRANSACTION_IDS} transaction ids"
        )
    return normalized


def _load_transactions(conn: sqlite3.Connection, ids: list[str]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT t.id, t.date, t.description, t.amount_cents, t.use_type,
               t.category_id, c.name AS category_name, t.project_id,
               p.name AS project_name, t.notes
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN projects p ON p.id = t.project_id
         WHERE t.is_active = 1
           AND t.id IN ({placeholders})
        """,
        tuple(ids),
    ).fetchall()
    by_id = {str(row["id"]): row for row in rows}
    missing = [txn_id for txn_id in ids if txn_id not in by_id]
    if missing:
        preview = ", ".join(missing[:10])
        suffix = "..." if len(missing) > 10 else ""
        raise NotFoundError(f"active transaction not found: {preview}{suffix}")
    return [by_id[txn_id] for txn_id in ids]


def _require_expenses(rows: list[sqlite3.Row], *, tool_name: str) -> None:
    non_expense_ids = [str(row["id"]) for row in rows if int(row["amount_cents"] or 0) >= 0]
    if non_expense_ids:
        preview = ", ".join(non_expense_ids[:10])
        raise ValidationError(f"{tool_name} only accepts expense transactions; non-expense ids: {preview}")


def _project_id_by_name(conn: sqlite3.Connection, project_name: str) -> str | None:
    row = conn.execute(
        "SELECT id FROM projects WHERE name = ?",
        (project_name,),
    ).fetchone()
    return None if row is None else str(row["id"])


def _create_or_get_project(conn: sqlite3.Connection, project_name: str) -> str:
    existing = _project_id_by_name(conn, project_name)
    if existing:
        return existing
    project_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO projects (id, name, is_active) VALUES (?, ?, 1)",
        (project_id, project_name),
    )
    return project_id


def _category_id_by_name(conn: sqlite3.Connection, category_name: str | None) -> str | None:
    normalized = str(category_name or "").strip()
    if not normalized:
        return None
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (normalized,)).fetchone()
    if row is None:
        raise NotFoundError(f"Category '{normalized}' not found")
    return str(row["id"])


def _schedule_c_mapping(conn: sqlite3.Connection, category_id: str | None) -> dict[str, Any] | None:
    if not category_id:
        return None
    row = conn.execute(
        """
        SELECT schedule_c_line, line_number, deduction_pct, tax_year
          FROM schedule_c_map
         WHERE category_id = ?
         ORDER BY tax_year DESC
         LIMIT 1
        """,
        (category_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "schedule_c_line": row["schedule_c_line"],
        "line_number": row["line_number"],
        "deduction_pct": row["deduction_pct"],
        "tax_year": row["tax_year"],
    }


def _upsert_business_vendor_memory(
    conn: sqlite3.Connection,
    *,
    description: str,
    category_id: str,
) -> str:
    pattern = normalize_description(description)
    existing = conn.execute(
        "SELECT id FROM vendor_memory WHERE description_pattern = ? AND use_type = 'Business'",
        (pattern,),
    ).fetchone()
    if existing:
        rule_id = str(existing["id"])
        conn.execute(
            """
            UPDATE vendor_memory
               SET category_id = ?,
                   confidence = 1.0,
                   is_enabled = 1,
                   is_confirmed = 1,
                   priority = MAX(priority, 0)
             WHERE id = ?
            """,
            (category_id, rule_id),
        )
        return rule_id

    rule_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO vendor_memory (
            id, description_pattern, category_id, use_type, confidence,
            priority, is_enabled, is_confirmed, match_count
        ) VALUES (?, ?, ?, 'Business', 1.0, 0, 1, 1, 0)
        """,
        (rule_id, pattern, category_id),
    )
    return rule_id


def _row_preview(row: sqlite3.Row) -> dict[str, Any]:
    amount_cents = int(row["amount_cents"] or 0)
    return {
        "id": str(row["id"]),
        "date": row["date"],
        "description": row["description"],
        "amount_cents": amount_cents,
        "amount": cents_to_dollars(amount_cents),
        "use_type": row["use_type"],
        "category_id": row["category_id"],
        "category_name": row["category_name"],
        "project_id": row["project_id"],
        "project_name": row["project_name"],
    }


def _expense_total_cents(rows: list[sqlite3.Row]) -> int:
    return sum(abs(int(row["amount_cents"] or 0)) for row in rows)


def bulk_reclassify_business(
    conn: sqlite3.Connection,
    *,
    ids: Any,
    category: str | None = None,
    remember: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Mark selected expense transactions as Business, optionally recategorizing them."""
    txn_ids = _normalize_ids(ids)
    rows = _load_transactions(conn, txn_ids)
    _require_expenses(rows, tool_name="bulk_reclassify_business")
    category_name = str(category or "").strip()
    category_id = _category_id_by_name(conn, category_name)
    schedule_c_mapping = _schedule_c_mapping(conn, category_id)
    previews = [_row_preview(row) for row in rows]
    changed_use_type = sum(1 for row in rows if row["use_type"] != "Business")
    changed_category = sum(1 for row in rows if category_id and row["category_id"] != category_id)
    remembered_count = len({normalize_description(str(row["description"] or "")) for row in rows}) if (
        remember and category_id
    ) else 0

    data: dict[str, Any] = {
        "transaction_ids": txn_ids,
        "transactions": previews,
        "category": category_name or None,
        "schedule_c_mapping": schedule_c_mapping,
        "remember": bool(remember),
        "remembered_count": 0 if dry_run else remembered_count,
        "would_remember_count": remembered_count,
        "dry_run": bool(dry_run),
    }
    summary = {
        "total_transactions": len(rows),
        "changed_use_type": changed_use_type,
        "changed_category": changed_category,
        "total_expense_cents": _expense_total_cents(rows),
        "dry_run": bool(dry_run),
    }
    action = "Would reclassify" if dry_run else "Reclassified"
    cli_report = f"{action} {len(rows)} expense transaction(s) as Business"
    if category_name:
        cli_report += f" in {category_name}"

    if dry_run:
        return {"data": data, "summary": summary, "cli_report": f"[DRY RUN] {cli_report}"}

    if category_id:
        conn.executemany(
            """
            UPDATE transactions
               SET use_type = 'Business',
                   category_id = ?,
                   category_source = 'user',
                   category_confidence = 1.0,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            [(category_id, txn_id) for txn_id in txn_ids],
        )
        if remember:
            for row in rows:
                _upsert_business_vendor_memory(
                    conn,
                    description=str(row["description"] or ""),
                    category_id=category_id,
                )
    else:
        conn.executemany(
            """
            UPDATE transactions
               SET use_type = 'Business',
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            [(txn_id,) for txn_id in txn_ids],
        )
    conn.commit()
    return {"data": data, "summary": summary, "cli_report": cli_report}


def bulk_tag_billable_expenses(
    conn: sqlite3.Connection,
    *,
    ids: Any,
    project: str,
    overwrite_existing_project: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Attach selected Business expense transactions to a client/project."""
    project_name = str(project or "").strip()
    if not project_name:
        raise ValidationError("project is required")
    txn_ids = _normalize_ids(ids)
    rows = _load_transactions(conn, txn_ids)
    _require_expenses(rows, tool_name="bulk_tag_billable_expenses")
    non_business_ids = [str(row["id"]) for row in rows if row["use_type"] != "Business"]
    if non_business_ids:
        preview = ", ".join(non_business_ids[:10])
        raise ValidationError(
            "bulk_tag_billable_expenses requires transactions already tagged Business; "
            f"non-business ids: {preview}"
        )

    existing_project_id = _project_id_by_name(conn, project_name)
    conflicting_project_ids = [
        str(row["id"])
        for row in rows
        if row["project_id"] is not None
        and (existing_project_id is None or row["project_id"] != existing_project_id)
    ]
    if conflicting_project_ids and not overwrite_existing_project:
        preview = ", ".join(conflicting_project_ids[:10])
        raise ValidationError(
            "some transactions already have a different project tag; "
            f"pass overwrite_existing_project=True to replace them: {preview}"
        )

    previews = [_row_preview(row) for row in rows]
    unchanged = sum(1 for row in rows if existing_project_id and row["project_id"] == existing_project_id)
    data = {
        "transaction_ids": txn_ids,
        "transactions": previews,
        "project": project_name,
        "project_id": existing_project_id,
        "project_would_create": existing_project_id is None,
        "overwrite_existing_project": bool(overwrite_existing_project),
        "dry_run": bool(dry_run),
    }
    summary = {
        "total_transactions": len(rows),
        "updated": len(rows) - unchanged,
        "unchanged": unchanged,
        "total_expense_cents": _expense_total_cents(rows),
        "dry_run": bool(dry_run),
    }
    action = "Would tag" if dry_run else "Tagged"
    cli_report = f"{action} {len(rows)} billable expense transaction(s) with project '{project_name}'"
    if dry_run:
        return {"data": data, "summary": summary, "cli_report": f"[DRY RUN] {cli_report}"}

    project_id = _create_or_get_project(conn, project_name)
    conn.executemany(
        """
        UPDATE transactions
           SET project_id = ?,
               updated_at = datetime('now')
         WHERE id = ?
        """,
        [(project_id, txn_id) for txn_id in txn_ids],
    )
    conn.commit()
    data["project_id"] = project_id
    data["project_would_create"] = False
    return {"data": data, "summary": summary, "cli_report": cli_report}
