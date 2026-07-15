"""Canonical category reconciliation helpers."""

from __future__ import annotations

import uuid
from typing import Any

from .user_rules import CANONICAL_CATEGORIES, _CATEGORY_HIERARCHY as USER_RULES_CATEGORY_HIERARCHY

CATEGORY_HIERARCHY: dict[str, list[str]] = USER_RULES_CATEGORY_HIERARCHY

INCOME_NAMES: frozenset[str] = frozenset(
    {
        "Income",
        "Income: Salary",
        "Income: Business",
        "Income: Other",
    }
)


def _fetch_category_row(conn, name: str):
    return conn.execute(
        """
        SELECT id, name, parent_id, level, is_income, is_system
          FROM categories
         WHERE lower(trim(name)) = lower(trim(?))
         ORDER BY rowid ASC
         LIMIT 1
        """,
        (name,),
    ).fetchone()


def _normalize_parent_id(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _category_updates(
    row,
    *,
    expected_parent_id: str | None,
    expected_level: int,
    expected_is_income: int,
) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    if _normalize_parent_id(row["parent_id"]) != _normalize_parent_id(expected_parent_id):
        updates["parent_id"] = expected_parent_id
    if int(row["level"] or 0) != int(expected_level):
        updates["level"] = int(expected_level)
    if int(row["is_income"] or 0) != int(expected_is_income):
        updates["is_income"] = int(expected_is_income)
    if int(row["is_system"] or 0) != 1:
        updates["is_system"] = 1
    return updates


def _reconcile_single_category(
    conn,
    *,
    name: str,
    expected_parent_id: str | None,
    expected_level: int,
    expected_is_income: int,
    dry_run: bool,
) -> tuple[str, str]:
    row = _fetch_category_row(conn, name)
    if row is None:
        new_id = uuid.uuid4().hex
        if not dry_run:
            conn.execute(
                """
                INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
                VALUES (?, ?, ?, ?, ?, 1, 0)
                """,
                (new_id, name, expected_parent_id, expected_level, expected_is_income),
            )
        return "created", new_id

    updates = _category_updates(
        row,
        expected_parent_id=expected_parent_id,
        expected_level=expected_level,
        expected_is_income=expected_is_income,
    )
    category_id = str(row["id"])
    if updates:
        if not dry_run:
            assignments = ", ".join(f"{column} = ?" for column in updates)
            conn.execute(
                f"UPDATE categories SET {assignments} WHERE id = ?",
                (*updates.values(), category_id),
            )
        return "updated", category_id
    return "already_correct", category_id


def seed_canonical_categories(conn, *, dry_run: bool) -> dict[str, Any]:
    expected_names = set(CATEGORY_HIERARCHY.keys())
    for children in CATEGORY_HIERARCHY.values():
        expected_names.update(children)
    if expected_names != set(CANONICAL_CATEGORIES):
        raise ValueError("Canonical category hierarchy is out of sync with CANONICAL_CATEGORIES")

    created = 0
    updated = 0
    already_correct = 0
    parent_ids: dict[str, str] = {}

    for parent_name in CATEGORY_HIERARCHY:
        status, category_id = _reconcile_single_category(
            conn,
            name=parent_name,
            expected_parent_id=None,
            expected_level=0,
            expected_is_income=int(parent_name in INCOME_NAMES),
            dry_run=dry_run,
        )
        parent_ids[parent_name] = category_id
        if status == "created":
            created += 1
        elif status == "updated":
            updated += 1
        else:
            already_correct += 1

    for parent_name, children in CATEGORY_HIERARCHY.items():
        for child_name in children:
            status, _ = _reconcile_single_category(
                conn,
                name=child_name,
                expected_parent_id=parent_ids[parent_name],
                expected_level=1,
                expected_is_income=int(child_name in INCOME_NAMES),
                dry_run=dry_run,
            )
            if status == "created":
                created += 1
            elif status == "updated":
                updated += 1
            else:
                already_correct += 1

    if not dry_run and (created > 0 or updated > 0):
        conn.commit()

    return {
        "dry_run": dry_run,
        "created": 0 if dry_run else created,
        "updated": 0 if dry_run else updated,
        "already_correct": already_correct,
        "would_create": created if dry_run else 0,
        "would_update": updated if dry_run else 0,
        "expected_total": len(CANONICAL_CATEGORIES),
    }
