from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli.__main__ import build_parser, main
from finance_cli.commands import budget as budget_cmd
from finance_cli.budget_engine import (
    delete_budget,
    find_budget,
    list_budgets,
    monthly_budget_forecast,
    monthly_budget_status,
    reallocate_budget,
    set_budget,
    update_budget,
)
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_category(conn: sqlite3.Connection, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_transaction(
    conn: sqlite3.Connection,
    *,
    category_id: str,
    amount_cents: int,
    txn_date: str,
    use_type: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id, source, use_type, is_payment, is_active
        ) VALUES (?, ?, 'seed', ?, ?, 'manual', ?, 0, 1)
        """,
        (uuid.uuid4().hex, txn_date, amount_cents, category_id, use_type),
    )
    conn.commit()


def _budget_set_args(**kwargs) -> SimpleNamespace:
    defaults = dict(
        category="TestCat",
        amount=100.0,
        period="monthly",
        view="personal",
        dry_run=False,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _seed_budget_and_txn_mix(conn: sqlite3.Connection, month: str = "2026-01") -> str:
    category_id = _seed_category(conn, "Software & Subscriptions")
    set_budget(
        conn,
        category_id=category_id,
        amount_dollars="50",
        period="monthly",
        effective_from=f"{month}-01",
        use_type="Personal",
    )
    set_budget(
        conn,
        category_id=category_id,
        amount_dollars="1100",
        period="monthly",
        effective_from=f"{month}-01",
        use_type="Business",
    )
    _seed_transaction(
        conn,
        category_id=category_id,
        amount_cents=-5_00,
        txn_date=f"{month}-10",
        use_type="Personal",
    )
    _seed_transaction(
        conn,
        category_id=category_id,
        amount_cents=-20_00,
        txn_date=f"{month}-11",
        use_type=None,
    )
    _seed_transaction(
        conn,
        category_id=category_id,
        amount_cents=-1091_00,
        txn_date=f"{month}-12",
        use_type="Business",
    )
    return category_id


def test_set_budget_rejects_negative_amount(db_path):
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestNeg")
        with pytest.raises(ValidationError, match="negative"):
            set_budget(conn, category_id=cat_id, amount_dollars="-100", period="monthly")


def test_set_budget_rejects_negative_subcent(db_path):
    """'-0.004' is negative even though it rounds to 0 cents."""
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestNegSub")
        with pytest.raises(ValidationError, match="negative"):
            set_budget(conn, category_id=cat_id, amount_dollars="-0.004", period="monthly")


def test_set_budget_rejects_negative_exponent_underflow(db_path):
    """'-1e-400' underflows to float -0.0 but is negative in Decimal."""
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestNegExp")
        with pytest.raises(ValidationError, match="negative"):
            set_budget(conn, category_id=cat_id, amount_dollars="-1e-400", period="monthly")


def test_set_budget_rejects_negative_no_db_write(db_path):
    """Rejected set_budget should not insert any row."""
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestNoWrite")
        before = conn.execute("SELECT COUNT(*) AS c FROM budgets").fetchone()["c"]
        try:
            set_budget(conn, category_id=cat_id, amount_dollars="-100", period="monthly")
        except ValidationError:
            pass
        after = conn.execute("SELECT COUNT(*) AS c FROM budgets").fetchone()["c"]
        assert after == before


def test_set_budget_allows_zero_amount(db_path):
    """Zero is a valid sentinel (disables budget alerts)."""
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestZero")
        bid = set_budget(conn, category_id=cat_id, amount_dollars="0", period="monthly")
        assert bid


def test_update_budget_rejects_negative_amount(db_path):
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestUpd")
        bid = set_budget(conn, category_id=cat_id, amount_dollars="100", period="monthly")
        with pytest.raises(ValidationError, match="negative"):
            update_budget(conn, budget_id=bid, amount_dollars="-50")


def test_update_budget_allows_zero_amount(db_path):
    """Zero update is valid (disables budget alerts)."""
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestUpdZero")
        bid = set_budget(conn, category_id=cat_id, amount_dollars="100", period="monthly")
        update_budget(conn, budget_id=bid, amount_dollars="0")
        row = conn.execute("SELECT amount_cents FROM budgets WHERE id = ?", (bid,)).fetchone()
        assert row["amount_cents"] == 0


def test_update_budget_rejects_negative_no_db_change(db_path):
    """Verify rejected update doesn't modify the existing budget."""
    with connect(db_path) as conn:
        cat_id = _seed_category(conn, "TestNoChange")
        bid = set_budget(conn, category_id=cat_id, amount_dollars="200", period="monthly")
        try:
            update_budget(conn, budget_id=bid, amount_dollars="-50")
        except ValidationError:
            pass
        row = conn.execute("SELECT amount_cents FROM budgets WHERE id = ?", (bid,)).fetchone()
        assert row["amount_cents"] == 20000


def test_reallocate_budget_moves_amount_between_two_budgets(db_path):
    with connect(db_path) as conn:
        from_cat_id = _seed_category(conn, "Travel")
        to_cat_id = _seed_category(conn, "Dining")
        from_budget_id = set_budget(conn, category_id=from_cat_id, amount_dollars="1500", period="monthly")
        to_budget_id = set_budget(conn, category_id=to_cat_id, amount_dollars="300", period="monthly")

        result = reallocate_budget(
            conn,
            from_budget_id=from_budget_id,
            to_budget_id=to_budget_id,
            amount_dollars="700",
        )

        assert result["transfer_cents"] == 70_000
        assert result["from_after_cents"] == 80_000
        assert result["to_after_cents"] == 100_000
        rows = {
            str(row["id"]): int(row["amount_cents"])
            for row in conn.execute("SELECT id, amount_cents FROM budgets").fetchall()
        }
        assert rows[from_budget_id] == 80_000
        assert rows[to_budget_id] == 100_000


def test_reallocate_budget_rejects_source_underflow_without_partial_write(db_path):
    with connect(db_path) as conn:
        from_cat_id = _seed_category(conn, "Travel")
        to_cat_id = _seed_category(conn, "Dining")
        from_budget_id = set_budget(conn, category_id=from_cat_id, amount_dollars="100", period="monthly")
        to_budget_id = set_budget(conn, category_id=to_cat_id, amount_dollars="300", period="monthly")

        with pytest.raises(ValidationError, match="source budget negative"):
            reallocate_budget(
                conn,
                from_budget_id=from_budget_id,
                to_budget_id=to_budget_id,
                amount_dollars="700",
            )

        rows = {
            str(row["id"]): int(row["amount_cents"])
            for row in conn.execute("SELECT id, amount_cents FROM budgets").fetchall()
        }
        assert rows[from_budget_id] == 10_000
        assert rows[to_budget_id] == 30_000


def test_reallocate_budget_dry_run_preserves_caller_transaction(db_path):
    with connect(db_path) as conn:
        from_cat_id = _seed_category(conn, "Travel")
        to_cat_id = _seed_category(conn, "Dining")
        pending_cat_id = uuid.uuid4().hex
        from_budget_id = set_budget(conn, category_id=from_cat_id, amount_dollars="1500", period="monthly")
        to_budget_id = set_budget(conn, category_id=to_cat_id, amount_dollars="300", period="monthly")
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Pending', 0)",
            (pending_cat_id,),
        )

        result = reallocate_budget(
            conn,
            from_budget_id=from_budget_id,
            to_budget_id=to_budget_id,
            amount_dollars="700",
            dry_run=True,
        )

        assert result["from_after_cents"] == 80_000
        assert result["to_after_cents"] == 100_000
        assert conn.execute("SELECT id FROM categories WHERE id = ?", (pending_cat_id,)).fetchone() is not None
        rows = {
            str(row["id"]): int(row["amount_cents"])
            for row in conn.execute("SELECT id, amount_cents FROM budgets").fetchall()
        }
        assert rows[from_budget_id] == 150_000
        assert rows[to_budget_id] == 30_000


def test_reallocate_budget_does_not_commit_caller_transaction(db_path):
    with connect(db_path) as conn:
        from_cat_id = _seed_category(conn, "Travel")
        to_cat_id = _seed_category(conn, "Dining")
        pending_cat_id = uuid.uuid4().hex
        from_budget_id = set_budget(conn, category_id=from_cat_id, amount_dollars="1500", period="monthly")
        to_budget_id = set_budget(conn, category_id=to_cat_id, amount_dollars="300", period="monthly")
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Pending', 0)",
            (pending_cat_id,),
        )

        reallocate_budget(
            conn,
            from_budget_id=from_budget_id,
            to_budget_id=to_budget_id,
            amount_dollars="700",
        )
        conn.rollback()

        assert conn.execute("SELECT id FROM categories WHERE id = ?", (pending_cat_id,)).fetchone() is None
        rows = {
            str(row["id"]): int(row["amount_cents"])
            for row in conn.execute("SELECT id, amount_cents FROM budgets").fetchall()
        }
        assert rows[from_budget_id] == 150_000
        assert rows[to_budget_id] == 30_000


def test_handle_set_rejects_negative_float(db_path):
    """MCP path: amount arrives as float."""
    with connect(db_path) as conn:
        _seed_category(conn, "TestCat")
        with pytest.raises(ValidationError, match="negative"):
            budget_cmd.handle_set(_budget_set_args(amount=-50.0), conn)


def test_handle_set_accepts_negative_zero_float(db_path):
    """-0.0 is numerically zero — allowed."""
    with connect(db_path) as conn:
        _seed_category(conn, "TestCat")
        result = budget_cmd.handle_set(_budget_set_args(amount=-0.0), conn)
        assert result["data"]["budget_id"]


def test_handle_update_rejects_negative_float(db_path):
    with connect(db_path) as conn:
        _seed_category(conn, "TestCat")
        result = budget_cmd.handle_set(_budget_set_args(amount=100.0), conn)
        bid = result["data"]["budget_id"]
        with pytest.raises(ValidationError, match="negative"):
            budget_cmd.handle_update(SimpleNamespace(id=bid, amount=-25.0, dry_run=False), conn)


def test_handle_update_accepts_zero_float(db_path):
    with connect(db_path) as conn:
        _seed_category(conn, "TestCat")
        result = budget_cmd.handle_set(_budget_set_args(amount=100.0), conn)
        bid = result["data"]["budget_id"]
        budget_cmd.handle_update(SimpleNamespace(id=bid, amount=0.0, dry_run=False), conn)
        row = conn.execute("SELECT amount_cents FROM budgets WHERE id = ?", (bid,)).fetchone()
        assert row["amount_cents"] == 0


def test_migration_027_applies_and_backfills_existing_rows(tmp_path: Path) -> None:
    db_file = tmp_path / "pre027.db"

    raw = sqlite3.connect(str(db_file))
    raw.executescript(
        """
        CREATE TABLE schema_version (
            version     INTEGER PRIMARY KEY,
            applied_at  TEXT DEFAULT (datetime('now')),
            description TEXT
        );

        CREATE TABLE categories (
            id          TEXT PRIMARY KEY,
            name        TEXT UNIQUE NOT NULL
        );

        CREATE TABLE plaid_items (
            id                  TEXT PRIMARY KEY,
            plaid_item_id       TEXT UNIQUE NOT NULL,
            institution_name    TEXT NOT NULL,
            access_token_ref    TEXT,
            status              TEXT NOT NULL CHECK (status IN ('active', 'error', 'disconnected', 'pending')),
            error_code          TEXT,
            consented_products  TEXT,
            sync_cursor         TEXT,
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE budgets (
            id             TEXT PRIMARY KEY,
            category_id    TEXT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
            period         TEXT NOT NULL CHECK (period IN ('monthly', 'weekly', 'yearly')),
            amount_cents   INTEGER NOT NULL,
            effective_from TEXT NOT NULL,
            effective_to   TEXT
        );

        CREATE TABLE transactions (
            id TEXT PRIMARY KEY
        );

        CREATE TABLE subscriptions (
            id TEXT PRIMARY KEY
        );

        CREATE TRIGGER budgets_no_overlap_insert
        BEFORE INSERT ON budgets
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1
            FROM budgets b
            WHERE b.category_id = NEW.category_id
              AND b.period = NEW.period
              AND date(b.effective_from) <= date(COALESCE(NEW.effective_to, '9999-12-31'))
              AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(NEW.effective_from)
        )
        BEGIN
            SELECT RAISE(ABORT, 'budget range overlap');
        END;

        CREATE TRIGGER budgets_no_overlap_update
        BEFORE UPDATE ON budgets
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1
            FROM budgets b
            WHERE b.id <> OLD.id
              AND b.category_id = NEW.category_id
              AND b.period = NEW.period
              AND date(b.effective_from) <= date(COALESCE(NEW.effective_to, '9999-12-31'))
              AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(NEW.effective_from)
        )
        BEGIN
            SELECT RAISE(ABORT, 'budget range overlap');
        END;
        """
    )

    for version in range(1, 27):
        raw.execute(
            "INSERT INTO schema_version (version, description) VALUES (?, ?)",
            (version, f"{version:03d}_placeholder.sql"),
        )

    category_id = uuid.uuid4().hex
    budget_id = uuid.uuid4().hex
    raw.execute("INSERT INTO categories (id, name) VALUES (?, 'Dining')", (category_id,))
    raw.execute(
        """
        INSERT INTO budgets (id, category_id, period, amount_cents, effective_from, effective_to)
        VALUES (?, ?, 'monthly', 20000, '2026-01-01', NULL)
        """,
        (budget_id, category_id),
    )
    raw.commit()
    raw.close()

    initialize_database(db_file)

    with connect(db_file) as conn:
        cols = conn.execute("PRAGMA table_info(budgets)").fetchall()
        use_type_col = next(row for row in cols if row["name"] == "use_type")
        assert int(use_type_col["notnull"]) == 1
        assert str(use_type_col["dflt_value"]) == "'Personal'"

        row = conn.execute("SELECT use_type FROM budgets WHERE id = ?", (budget_id,)).fetchone()
        assert row["use_type"] == "Personal"

        trigger_sql = conn.execute(
            """
            SELECT sql
              FROM sqlite_master
             WHERE type = 'trigger'
               AND name = 'budgets_no_overlap_insert'
            """
        ).fetchone()["sql"]
        assert "b.use_type = NEW.use_type" in trigger_sql


def test_budgets_use_type_rejects_null(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO budgets (
                    id, category_id, period, amount_cents, effective_from, effective_to, use_type
                ) VALUES (?, ?, 'monthly', 20000, '2026-01-01', NULL, NULL)
                """,
                (uuid.uuid4().hex, category_id),
            )


def test_set_budget_allows_same_category_for_personal_and_business(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="200",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="500",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Business",
        )
        count_row = conn.execute("SELECT COUNT(*) AS n FROM budgets WHERE category_id = ?", (category_id,)).fetchone()
        assert int(count_row["n"]) == 2


def test_set_budget_upserts_same_category_period_and_use_type(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        original_id = set_budget(
            conn,
            category_id=category_id,
            amount_dollars="200",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )
        updated_id = set_budget(
            conn,
            category_id=category_id,
            amount_dollars="250",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )
        rows = conn.execute(
            "SELECT id, amount_cents FROM budgets WHERE category_id = ? AND period = 'monthly' AND use_type = 'Personal'",
            (category_id,),
        ).fetchall()

    assert original_id == updated_id
    assert len(rows) == 1
    assert int(rows[0]["amount_cents"]) == 25_000


def test_status_personal_view_scopes_budget_and_actuals(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_budget_and_txn_mix(conn)
        rows = monthly_budget_status(conn, month="2026-01", view="personal")

    assert len(rows) == 1
    assert rows[0].use_type == "Personal"
    assert rows[0].actual_cents == -2_500


def test_status_business_view_scopes_budget_and_actuals(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_budget_and_txn_mix(conn)
        rows = monthly_budget_status(conn, month="2026-01", view="business")

    assert len(rows) == 1
    assert rows[0].use_type == "Business"
    assert rows[0].actual_cents == -109_100


def test_status_all_view_shows_both_scoped_rows_without_double_counting(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_budget_and_txn_mix(conn)
        rows = monthly_budget_status(conn, month="2026-01", view="all")

    assert len(rows) == 2
    by_type = {row.use_type: row for row in rows}
    assert by_type["Personal"].actual_cents == -2_500
    assert by_type["Business"].actual_cents == -109_100


def test_personal_budget_status_includes_legacy_null_transactions(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="200",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )
        _seed_transaction(
            conn,
            category_id=category_id,
            amount_cents=-4_500,
            txn_date="2026-01-15",
            use_type=None,
        )
        rows = monthly_budget_status(conn, month="2026-01", view="personal")

    assert len(rows) == 1
    assert rows[0].actual_cents == -4_500


def test_forecast_inherits_view_filtering(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_budget_and_txn_mix(conn)
        rows = monthly_budget_forecast(conn, month="2026-01", view="business")

    assert len(rows) == 1
    assert rows[0]["use_type"] == "Business"
    assert rows[0]["actual_cents"] == -109_100


def test_list_budgets_view_filter_returns_expected_subset(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_budget_and_txn_mix(conn)
        personal = list_budgets(conn, view="personal")
        business = list_budgets(conn, view="business")
        all_rows = list_budgets(conn, view="all")

    assert len(personal) == 1
    assert personal[0]["use_type"] == "Personal"
    assert len(business) == 1
    assert business[0]["use_type"] == "Business"
    assert len(all_rows) == 2


def test_budget_set_view_all_is_rejected(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        _seed_category(conn, "Dining")

    code = main(
        [
            "budget",
            "set",
            "--category",
            "Dining",
            "--amount",
            "200",
            "--period",
            "monthly",
            "--view",
            "all",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "error"
    assert "requires --view personal or --view business" in payload["error"]


def test_budget_set_reports_created_then_updated(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        _seed_category(conn, "Dining")

    created_code = main(
        [
            "budget",
            "set",
            "--category",
            "Dining",
            "--amount",
            "200",
            "--period",
            "monthly",
            "--view",
            "personal",
        ]
    )
    created_payload = json.loads(capsys.readouterr().out)
    assert created_code == 0
    assert created_payload["data"]["action"] == "created"
    assert created_payload["data"]["created"] is True

    updated_code = main(
        [
            "budget",
            "set",
            "--category",
            "Dining",
            "--amount",
            "250",
            "--period",
            "monthly",
            "--view",
            "personal",
        ]
    )
    updated_payload = json.loads(capsys.readouterr().out)
    assert updated_code == 0
    assert updated_payload["data"]["action"] == "updated"
    assert updated_payload["data"]["created"] is False
    assert updated_payload["data"]["budget_id"] == created_payload["data"]["budget_id"]


def test_parser_registers_view_on_all_budget_subcommands() -> None:
    parser = build_parser()

    parsed_set = parser.parse_args(
        ["budget", "set", "--category", "Dining", "--amount", "200", "--period", "monthly", "--view", "business"]
    )
    assert parsed_set.view == "business"
    assert parser.parse_args(["budget", "set", "--category", "Dining", "--amount", "200", "--period", "monthly"]).view == "personal"

    parsed_update = parser.parse_args(
        ["budget", "update", "--category", "Dining", "--amount", "250", "--period", "monthly", "--view", "business"]
    )
    assert parsed_update.view == "business"
    assert parser.parse_args(["budget", "update", "--id", "abc123", "--amount", "250"]).id == "abc123"

    parsed_reallocate = parser.parse_args(
        [
            "budget",
            "reallocate",
            "--from-category",
            "Travel",
            "--to-category",
            "Dining",
            "--amount",
            "100",
            "--period",
            "monthly",
            "--view",
            "personal",
        ]
    )
    assert parsed_reallocate.view == "personal"

    parsed_delete = parser.parse_args(["budget", "delete", "--category", "Dining", "--view", "personal"])
    assert parsed_delete.view == "personal"
    assert parser.parse_args(["budget", "delete", "--id", "abc123"]).id == "abc123"

    parsed_list = parser.parse_args(["budget", "list", "--view", "personal"])
    assert parsed_list.view == "personal"

    parsed_status = parser.parse_args(["budget", "status", "--view", "business"])
    assert parsed_status.view == "business"

    parsed_forecast = parser.parse_args(["budget", "forecast", "--view", "all"])
    assert parsed_forecast.view == "all"

    parsed_alerts = parser.parse_args(["budget", "alerts", "--view", "business"])
    assert parsed_alerts.view == "business"

    parsed_suggest = parser.parse_args(["budget", "suggest", "--goal", "savings", "--target", "100", "--view", "personal"])
    assert parsed_suggest.view == "personal"


def test_mcp_budget_tools_accept_view_parameter(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        _seed_category(conn, "Travel")
        _seed_transaction(
            conn,
            category_id=category_id,
            amount_cents=-4_500,
            txn_date=date.today().isoformat(),
            use_type="Business",
        )

    from finance_cli.mcp_server import (
        budget_alerts,
        budget_delete,
        budget_forecast,
        budget_list,
        budget_reallocate,
        budget_set,
        budget_status,
        budget_suggest,
        budget_update,
    )

    budget_set(category="Dining", amount=500, period="monthly", view="business")
    budget_set(category="Travel", amount=1000, period="monthly", view="business")
    listed = budget_list(view="business")
    assert listed["data"]["budgets"]
    assert all(row["use_type"] == "Business" for row in listed["data"]["budgets"])

    month = date.today().strftime("%Y-%m")
    status = budget_status(month=month, view="business")
    assert all(row["use_type"] == "Business" for row in status["data"]["status"])

    forecast = budget_forecast(month=month, view="business")
    assert all(row["use_type"] == "Business" for row in forecast["data"]["forecast"])

    alerts = budget_alerts(month=month, view="business")
    assert "alerts" in alerts["data"]

    suggest = budget_suggest(goal="savings", target=100, view="business")
    assert "suggestions" in suggest["data"]

    updated = budget_update(category="Dining", amount=450, period="monthly", view="business")
    assert updated["data"]["amount"] == 450.0

    reallocated = budget_reallocate(
        from_category="Travel",
        to_category="Dining",
        amount=100,
        period="monthly",
        view="business",
    )
    assert reallocated["data"]["from_amount"] == 900.0
    assert reallocated["data"]["to_amount"] == 550.0

    deleted = budget_delete(category="Dining", period="monthly", view="business")
    assert deleted["data"]["deleted"] is True
    assert all(row["category_name"] != "Dining" for row in budget_list(view="business")["data"]["budgets"])


def test_find_update_delete_budget_engine_helpers(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        budget_id = set_budget(
            conn,
            category_id=category_id,
            amount_dollars="200",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )

        found = find_budget(conn, category_id=category_id, period="monthly", use_type="Personal")
        assert found is not None
        assert found["id"] == budget_id

        update_budget(conn, budget_id=budget_id, amount_dollars="275")
        amount_row = conn.execute("SELECT amount_cents FROM budgets WHERE id = ?", (budget_id,)).fetchone()
        assert int(amount_row["amount_cents"]) == 27_500

        delete_budget(conn, budget_id=budget_id)
        assert conn.execute("SELECT id FROM budgets WHERE id = ?", (budget_id,)).fetchone() is None

        with pytest.raises(ValueError, match="not found"):
            update_budget(conn, budget_id=budget_id, amount_dollars="300")
        with pytest.raises(ValueError, match="not found"):
            delete_budget(conn, budget_id=budget_id)


def test_find_budget_ignores_closed_rows(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Travel")
        budget_id = set_budget(
            conn,
            category_id=category_id,
            amount_dollars="100",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )
        conn.execute(
            "UPDATE budgets SET effective_to = '2026-01-31' WHERE id = ?",
            (budget_id,),
        )
        conn.commit()

        assert find_budget(conn, category_id=category_id, period="monthly", use_type="Personal") is None


def test_budget_update_and_delete_cli_by_category(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        _seed_category(conn, "Dining")

    assert main(["budget", "set", "--category", "Dining", "--amount", "200", "--period", "monthly"]) == 0
    capsys.readouterr()

    code = main(["budget", "update", "--category", "Dining", "--amount", "350", "--period", "monthly"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["command"] == "budget.update"
    assert payload["data"]["category"] == "Dining"
    assert payload["data"]["use_type"] == "Personal"
    assert payload["data"]["amount"] == 350.0

    with connect(db_path) as conn:
        row = conn.execute("SELECT amount_cents FROM budgets").fetchone()
        assert int(row["amount_cents"]) == 35_000

    code = main(["budget", "delete", "--category", "Dining", "--period", "monthly"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["command"] == "budget.delete"
    assert payload["data"]["deleted"] is True

    with connect(db_path) as conn:
        count_row = conn.execute("SELECT COUNT(*) AS n FROM budgets").fetchone()
        assert int(count_row["n"]) == 0


def test_budget_reallocate_cli_by_category(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        travel_id = _seed_category(conn, "Travel")
        set_budget(
            conn,
            category_id=dining_id,
            amount_dollars="300",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )
        set_budget(
            conn,
            category_id=travel_id,
            amount_dollars="1500",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )

    code = main(
        [
            "budget",
            "reallocate",
            "--from-category",
            "Travel",
            "--to-category",
            "Dining",
            "--amount",
            "700",
            "--period",
            "monthly",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["command"] == "budget.reallocate"
    assert payload["data"]["from_category"] == "Travel"
    assert payload["data"]["to_category"] == "Dining"
    assert payload["data"]["from_amount"] == 800.0
    assert payload["data"]["to_amount"] == 1000.0

    with connect(db_path) as conn:
        rows = {
            str(row["name"]): int(row["amount_cents"])
            for row in conn.execute(
                """
                SELECT c.name, b.amount_cents
                  FROM budgets b
                  JOIN categories c ON c.id = b.category_id
                """
            ).fetchall()
        }
        assert rows == {"Dining": 100_000, "Travel": 80_000}


def test_budget_update_and_delete_cli_by_id_takes_precedence(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        budget_id = set_budget(
            conn,
            category_id=category_id,
            amount_dollars="200",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )

    code = main(
        [
            "budget",
            "update",
            "--id",
            budget_id,
            "--category",
            "Missing",
            "--amount",
            "420",
            "--view",
            "all",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["command"] == "budget.update"
    assert payload["data"]["budget_id"] == budget_id
    assert payload["data"]["amount"] == 420.0

    code = main(
        [
            "budget",
            "delete",
            "--id",
            budget_id,
            "--category",
            "Missing",
            "--view",
            "all",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["command"] == "budget.delete"
    assert payload["data"]["budget_id"] == budget_id


def test_budget_update_delete_category_resolution_errors(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        _seed_category(conn, "Dining")

    code = main(["budget", "update", "--category", "Dining", "--amount", "200"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert "no budget found for Dining (Personal, monthly)" in payload["error"]

    code = main(["budget", "delete", "--category", "Missing"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert "Category 'Missing' not found" in payload["error"]


def test_budget_list_output_includes_use_type(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="200",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Personal",
        )
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="500",
            period="monthly",
            effective_from="2026-01-01",
            use_type="Business",
        )

    code = main(["budget", "list", "--view", "all"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert all("use_type" in row for row in payload["data"]["budgets"])
    assert "[Personal]" in payload["cli_report"]
    assert "[Business]" in payload["cli_report"]
