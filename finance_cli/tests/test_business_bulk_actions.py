from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import business_bulk_actions
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import NotFoundError, ValidationError


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _category_id(conn, name: str) -> str:
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name, is_income, is_system) VALUES (?, ?, 0, 0)",
        (f"cat-{name.lower().replace(' ', '-').replace('&', 'and')}", name),
    )
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    assert row is not None
    return str(row["id"])


def _project_id(conn, name: str) -> str:
    conn.execute(
        "INSERT INTO projects (id, name, is_active) VALUES (?, ?, 1)",
        (f"project-{name.lower().replace(' ', '-')}", name),
    )
    return f"project-{name.lower().replace(' ', '-')}"


def _seed_txn(
    conn,
    txn_id: str,
    *,
    description: str = "ACME SOFTWARE",
    amount_cents: int = -12_00,
    use_type: str | None = "Personal",
    category_name: str | None = None,
    project_id: str | None = None,
) -> None:
    category_id = _category_id(conn, category_name) if category_name else None
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id, use_type,
            is_active, source, project_id
        ) VALUES (?, '2026-05-01', ?, ?, ?, ?, 1, 'manual', ?)
        """,
        (txn_id, description, amount_cents, category_id, use_type, project_id),
    )


def test_bulk_reclassify_business_updates_use_type_category_and_memory(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_txn(conn, "txn-1", description="ACME SOFTWARE 123", category_name="Personal Expense")
        _seed_txn(conn, "txn-2", description="ACME SOFTWARE 456", use_type=None)
        _category_id(conn, "Software & Subscriptions")
        conn.commit()

        preview = business_bulk_actions.bulk_reclassify_business(
            conn,
            ids=["txn-1", "txn-2"],
            category="Software & Subscriptions",
            remember=True,
            dry_run=True,
        )
        unchanged = conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE use_type = 'Business'"
        ).fetchone()["n"]
        memory_preview_count = conn.execute(
            "SELECT COUNT(*) AS n FROM vendor_memory WHERE use_type = 'Business'"
        ).fetchone()["n"]

        result = business_bulk_actions.bulk_reclassify_business(
            conn,
            ids=["txn-1", "txn-2"],
            category="Software & Subscriptions",
            remember=True,
        )
        rows = conn.execute(
            """
            SELECT t.id, t.use_type, c.name AS category_name
              FROM transactions t
              JOIN categories c ON c.id = t.category_id
             WHERE t.id IN ('txn-1', 'txn-2')
             ORDER BY t.id
            """
        ).fetchall()
        memory_count = conn.execute(
            "SELECT COUNT(*) AS n FROM vendor_memory WHERE use_type = 'Business'"
        ).fetchone()["n"]

    assert preview["summary"]["dry_run"] is True
    assert preview["data"]["would_remember_count"] == 2
    assert unchanged == 0
    assert memory_preview_count == 0
    assert result["summary"]["total_transactions"] == 2
    assert result["summary"]["changed_use_type"] == 2
    assert result["summary"]["changed_category"] == 2
    assert result["summary"]["total_expense_cents"] == 2_400
    assert result["data"]["remembered_count"] == 2
    assert [(row["id"], row["use_type"], row["category_name"]) for row in rows] == [
        ("txn-1", "Business", "Software & Subscriptions"),
        ("txn-2", "Business", "Software & Subscriptions"),
    ]
    assert memory_count == 2


def test_bulk_reclassify_business_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_txn(conn, "expense-1")
        _seed_txn(conn, "income-1", amount_cents=25_00)
        conn.commit()

        with pytest.raises(ValidationError, match="at least one transaction id"):
            business_bulk_actions.bulk_reclassify_business(conn, ids=[])
        with pytest.raises(NotFoundError, match="active transaction not found"):
            business_bulk_actions.bulk_reclassify_business(conn, ids=["missing"])
        with pytest.raises(ValidationError, match="only accepts expense"):
            business_bulk_actions.bulk_reclassify_business(conn, ids=["income-1"])
        with pytest.raises(NotFoundError, match="Category"):
            business_bulk_actions.bulk_reclassify_business(
                conn,
                ids=["expense-1"],
                category="Not Real",
            )


def test_bulk_tag_billable_expenses_creates_project_and_tags_business_expenses(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_txn(conn, "txn-1", description="CLIENT LUNCH", use_type="Business")
        _seed_txn(conn, "txn-2", description="CLIENT TRAIN", use_type="Business")
        conn.commit()

        preview = business_bulk_actions.bulk_tag_billable_expenses(
            conn,
            ids=["txn-1", "txn-2"],
            project="Client Acme",
            dry_run=True,
        )
        project_preview = conn.execute(
            "SELECT COUNT(*) AS n FROM projects WHERE name = 'Client Acme'"
        ).fetchone()["n"]

        result = business_bulk_actions.bulk_tag_billable_expenses(
            conn,
            ids=["txn-1", "txn-2"],
            project="Client Acme",
        )
        repeat = business_bulk_actions.bulk_tag_billable_expenses(
            conn,
            ids=["txn-1", "txn-2"],
            project="Client Acme",
        )
        rows = conn.execute(
            """
            SELECT t.id, p.name AS project_name
              FROM transactions t
              JOIN projects p ON p.id = t.project_id
             WHERE t.id IN ('txn-1', 'txn-2')
             ORDER BY t.id
            """
        ).fetchall()

    assert preview["summary"]["dry_run"] is True
    assert preview["data"]["project_would_create"] is True
    assert project_preview == 0
    assert result["summary"]["updated"] == 2
    assert result["summary"]["total_expense_cents"] == 2_400
    assert repeat["summary"]["updated"] == 0
    assert repeat["summary"]["unchanged"] == 2
    assert [(row["id"], row["project_name"]) for row in rows] == [
        ("txn-1", "Client Acme"),
        ("txn-2", "Client Acme"),
    ]


def test_bulk_tag_billable_expenses_validation_and_overwrite(db_path: Path) -> None:
    with connect(db_path) as conn:
        other_project_id = _project_id(conn, "Other Client")
        _seed_txn(conn, "personal-1", use_type="Personal")
        _seed_txn(conn, "income-1", amount_cents=50_00, use_type="Business")
        _seed_txn(conn, "conflict-1", use_type="Business", project_id=other_project_id)
        conn.commit()

        with pytest.raises(ValidationError, match="already tagged Business"):
            business_bulk_actions.bulk_tag_billable_expenses(
                conn,
                ids=["personal-1"],
                project="Client Acme",
            )
        with pytest.raises(ValidationError, match="only accepts expense"):
            business_bulk_actions.bulk_tag_billable_expenses(
                conn,
                ids=["income-1"],
                project="Client Acme",
            )
        with pytest.raises(ValidationError, match="different project"):
            business_bulk_actions.bulk_tag_billable_expenses(
                conn,
                ids=["conflict-1"],
                project="Client Acme",
            )

        result = business_bulk_actions.bulk_tag_billable_expenses(
            conn,
            ids=["conflict-1"],
            project="Client Acme",
            overwrite_existing_project=True,
        )
        project_name = conn.execute(
            """
            SELECT p.name
              FROM transactions t
              JOIN projects p ON p.id = t.project_id
             WHERE t.id = 'conflict-1'
            """
        ).fetchone()["name"]

    assert result["summary"]["updated"] == 1
    assert project_name == "Client Acme"


def test_business_bulk_tools_are_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    expected = {"bulk_tag_billable_expenses", "bulk_reclassify_business"}
    assert expected <= gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert expected <= DB_WRITE_TOOLS
