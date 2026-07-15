from __future__ import annotations

from pathlib import Path
import uuid

import pytest

from finance_cli import late_month_buffer
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_category(conn, name: str, *, parent_id: str | None = None) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, parent_id, level, is_system)
        VALUES (?, ?, ?, 0, 0)
        """,
        (category_id, name, parent_id),
    )
    return category_id


def test_add_late_month_buffer_budget_creates_category_and_budget(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = late_month_buffer.add_late_month_buffer_budget(
            conn,
            amount_cents=7_500,
            effective_from="2099-06-01",
        )
        category = conn.execute("SELECT id, name FROM categories WHERE name = 'Late-Month Buffer'").fetchone()
        budget = conn.execute(
            """
            SELECT id, category_id, amount_cents, period, effective_from, use_type
              FROM budgets
             WHERE category_id = ?
            """,
            (category["id"],),
        ).fetchone()

    assert result["summary"]["configured"] == 1
    assert result["summary"]["category_created"] is True
    assert result["summary"]["budget_action"] == "created"
    assert category["name"] == "Late-Month Buffer"
    assert budget["amount_cents"] == 7_500
    assert budget["period"] == "monthly"
    assert budget["effective_from"] == "2099-06-01"
    assert budget["use_type"] == "Personal"


def test_add_late_month_buffer_budget_is_idempotent_and_updates_amount(db_path: Path) -> None:
    with connect(db_path) as conn:
        first = late_month_buffer.add_late_month_buffer_budget(
            conn,
            amount_cents=7_500,
            effective_from="2099-06-01",
        )
        second = late_month_buffer.add_late_month_buffer_budget(
            conn,
            amount_cents=10_000,
            effective_from="2099-06-01",
        )
        category_count = conn.execute(
            "SELECT COUNT(*) AS n FROM categories WHERE name = 'Late-Month Buffer'"
        ).fetchone()["n"]
        budget_rows = conn.execute("SELECT id, amount_cents FROM budgets").fetchall()

    assert second["summary"]["category_created"] is False
    assert second["summary"]["budget_action"] == "updated"
    assert second["data"]["budget"]["id"] == first["data"]["budget"]["id"]
    assert category_count == 1
    assert len(budget_rows) == 1
    assert budget_rows[0]["amount_cents"] == 10_000


def test_add_late_month_buffer_budget_can_use_parent_category(db_path: Path) -> None:
    with connect(db_path) as conn:
        parent_id = _seed_category(conn, "Spending")
        conn.commit()

        result = late_month_buffer.add_late_month_buffer_budget(
            conn,
            amount_cents=5_000,
            parent_category_name="Spending",
            effective_from="2099-06-01",
        )
        category = conn.execute(
            "SELECT parent_id FROM categories WHERE id = ?",
            (result["data"]["category"]["id"],),
        ).fetchone()

    assert category["parent_id"] == parent_id


def test_add_late_month_buffer_budget_dry_run_does_not_write(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = late_month_buffer.add_late_month_buffer_budget(
            conn,
            amount_cents=7_500,
            dry_run=True,
        )
        category_count = conn.execute("SELECT COUNT(*) AS n FROM categories").fetchone()["n"]
        budget_count = conn.execute("SELECT COUNT(*) AS n FROM budgets").fetchone()["n"]

    assert result["summary"]["configured"] == 0
    assert result["data"]["dry_run"] is True
    assert category_count == 0
    assert budget_count == 0


def test_add_late_month_buffer_budget_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        parent_id = _seed_category(conn, "Parent")
        _seed_category(conn, "Child", parent_id=parent_id)
        conn.commit()

        with pytest.raises(ValueError, match="amount_cents must be greater than 0"):
            late_month_buffer.add_late_month_buffer_budget(conn, amount_cents=0)
        with pytest.raises(ValueError, match="Parent category 'Missing' not found"):
            late_month_buffer.add_late_month_buffer_budget(
                conn,
                amount_cents=7_500,
                parent_category_name="Missing",
            )
        with pytest.raises(ValueError, match="choose or create a leaf category"):
            late_month_buffer.add_late_month_buffer_budget(
                conn,
                amount_cents=7_500,
                category_name="Parent",
            )
        with pytest.raises(ValueError, match="effective_from must be an ISO date"):
            late_month_buffer.add_late_month_buffer_budget(
                conn,
                amount_cents=7_500,
                effective_from="2099-06",
            )


def test_late_month_buffer_tool_is_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    assert "add_late_month_buffer_budget" in gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert "add_late_month_buffer_budget" in DB_WRITE_TOOLS
