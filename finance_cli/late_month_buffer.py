"""Late-month buffer budget helper."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import sqlite3
from typing import Any
import uuid

from finance_cli.budget_engine import find_budget, set_budget
from finance_cli.exceptions import NotFoundError, ValidationError

_DEFAULT_CATEGORY_NAME = "Late-Month Buffer"


def _normalize_name(value: str | None, *, default: str = "") -> str:
    normalized = str(value or "").strip()
    return normalized or default


def _parse_effective_from(value: str | None) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return date.today().replace(day=1).isoformat()
    try:
        parsed = date.fromisoformat(normalized)
    except ValueError as exc:
        raise ValidationError("effective_from must be an ISO date (YYYY-MM-DD)") from exc
    return parsed.isoformat()


def _amount_dollars(amount_cents: int) -> str:
    return str((Decimal(int(amount_cents)) / Decimal("100")).quantize(Decimal("0.01")))


def _find_category_by_name(conn: sqlite3.Connection, name: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, name, parent_id, is_income, is_system
          FROM categories
         WHERE lower(name) = lower(?)
         ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, name ASC
         LIMIT 1
        """,
        (name, name),
    ).fetchone()


def _category_child_count(conn: sqlite3.Connection, category_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM categories WHERE parent_id = ?",
        (category_id,),
    ).fetchone()
    return int(row["n"] or 0)


def _parent_id(conn: sqlite3.Connection, parent_category_name: str | None) -> str | None:
    parent_name = _normalize_name(parent_category_name)
    if not parent_name:
        return None
    row = _find_category_by_name(conn, parent_name)
    if row is None:
        raise NotFoundError(f"Parent category '{parent_name}' not found")
    return str(row["id"])


def add_late_month_buffer_budget(
    conn: sqlite3.Connection,
    *,
    amount_cents: int,
    category_name: str = _DEFAULT_CATEGORY_NAME,
    parent_category_name: str = "",
    effective_from: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create or update a personal monthly late-month buffer budget."""
    amount = int(amount_cents)
    if amount <= 0:
        raise ValidationError("amount_cents must be greater than 0")
    normalized_category_name = _normalize_name(category_name, default=_DEFAULT_CATEGORY_NAME)
    normalized_effective_from = _parse_effective_from(effective_from)
    parent_id = _parent_id(conn, parent_category_name)
    category = _find_category_by_name(conn, normalized_category_name)
    category_created = category is None
    category_id = uuid.uuid4().hex if category is None else str(category["id"])
    if category is not None and _category_child_count(conn, category_id) > 0:
        raise ValidationError(
            f"Cannot set budget on parent category '{category['name']}' - "
            "choose or create a leaf category."
        )

    existing_budget = (
        None
        if category is None
        else find_budget(
            conn,
            category_id=category_id,
            period="monthly",
            use_type="Personal",
        )
    )
    budget_action = "created" if existing_budget is None else "updated"
    preview = {
        "category": {
            "id": category_id,
            "name": normalized_category_name if category is None else str(category["name"]),
            "parent_id": parent_id if category is None else category["parent_id"],
            "created": category_created,
        },
        "budget": {
            "id": None if existing_budget is None else str(existing_budget["id"]),
            "period": "monthly",
            "amount_cents": amount,
            "effective_from": normalized_effective_from,
            "use_type": "Personal",
            "action": budget_action,
        },
        "dry_run": dry_run,
    }
    if dry_run:
        return {
            "data": preview,
            "summary": {
                "configured": 0,
                "dry_run": True,
                "category_created": category_created,
                "budget_action": budget_action,
                "amount_cents": amount,
            },
        }

    if category is None:
        conn.execute(
            """
            INSERT INTO categories (id, name, parent_id, is_system)
            VALUES (?, ?, ?, 0)
            """,
            (category_id, normalized_category_name, parent_id),
        )

    budget_id = set_budget(
        conn,
        category_id=category_id,
        amount_dollars=_amount_dollars(amount),
        period="monthly",
        use_type="Personal",
        effective_from=normalized_effective_from,
    )
    budget = find_budget(
        conn,
        category_id=category_id,
        period="monthly",
        use_type="Personal",
    )
    if budget is None:
        raise RuntimeError("late-month buffer budget was not written")
    return {
        "data": {
            "category": {
                "id": category_id,
                "name": normalized_category_name if category is None else str(category["name"]),
                "parent_id": parent_id if category is None else category["parent_id"],
                "created": category_created,
            },
            "budget": {
                "id": budget_id,
                "period": "monthly",
                "amount_cents": int(budget["amount_cents"]),
                "effective_from": str(budget["effective_from"]),
                "use_type": str(budget["use_type"]),
                "action": budget_action,
            },
            "dry_run": False,
        },
        "summary": {
            "configured": 1,
            "dry_run": False,
            "category_created": category_created,
            "budget_action": budget_action,
            "amount_cents": amount,
            "budget_id": budget_id,
        },
    }
