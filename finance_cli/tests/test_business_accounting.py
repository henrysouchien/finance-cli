from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.categorizer import MatchResult, apply_match
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _apply_migrations_up_to(db_path: Path, max_version: int) -> None:
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version     INTEGER PRIMARY KEY,
                applied_at  TEXT DEFAULT (datetime('now')),
                description TEXT
            )
            """
        )
        for path in sorted(migration_dir.glob("*.sql")):
            version = int(path.name.split("_", 1)[0])
            if version > max_version:
                continue
            conn.executescript(path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO schema_version (version, description) VALUES (?, ?)",
                (version, path.name),
            )
        conn.commit()


def _seed_category(
    conn,
    *,
    name: str,
    parent_id: str | None = None,
    level: int = 0,
    is_income: int = 0,
    is_system: int = 1,
) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
        VALUES (?, ?, ?, ?, ?, ?, 0)
        """,
        (category_id, name, parent_id, level, is_income, is_system),
    )
    return category_id


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _write_rules(tmp_path: Path, content: str) -> None:
    (tmp_path / "rules.yaml").write_text(content.strip() + "\n", encoding="utf-8")


def _seed_account(conn, *, is_business: int = 0) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            source, is_active, is_business
        ) VALUES (?, 'Test Bank', 'Checking', 'checking', 'manual', 1, ?)
        """,
        (account_id, is_business),
    )
    return account_id


def _seed_txn(
    conn,
    *,
    account_id: str,
    use_type: str | None,
    is_active: int = 1,
    category_id: str | None = None,
    category_source: str | None = None,
    amount_cents: int = -1000,
    description: str | None = None,
    txn_date: str = "2026-02-01",
    is_reviewed: int = 0,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, source, use_type, is_active,
            category_id, category_source, is_reviewed
        ) VALUES (?, ?, ?, ?, ?, 'manual', ?, ?, ?, ?, ?)
        """,
        (
            txn_id,
            account_id,
            txn_date,
            description or f"txn-{txn_id[:6]}",
            amount_cents,
            use_type,
            is_active,
            category_id,
            category_source,
            is_reviewed,
        ),
    )
    return txn_id


def _category_id(conn, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    return _seed_category(conn, name=name, level=1, is_income=0, is_system=0)


def test_migration_020_schema_categories_and_mapping_seed(tmp_path: Path) -> None:
    db_path = tmp_path / "finance_020.db"
    _apply_migrations_up_to(db_path, max_version=19)

    with connect(db_path) as conn:
        professional_id = _seed_category(conn, name="Professional", level=0)
        housing_id = _seed_category(conn, name="Housing", level=0)
        financial_id = _seed_category(conn, name="Financial", level=0)
        income_id = _seed_category(conn, name="Income", level=0, is_income=1)

        _seed_category(conn, name="Income: Business", parent_id=income_id, level=1, is_income=1)
        _seed_category(conn, name="Software & Subscriptions", parent_id=professional_id, level=1)
        _seed_category(conn, name="Professional Fees", parent_id=professional_id, level=1)
        _seed_category(conn, name="Rent", parent_id=housing_id, level=1)
        _seed_category(conn, name="Utilities", parent_id=housing_id, level=1)
        _seed_category(conn, name="Insurance", parent_id=housing_id, level=1)
        _seed_category(conn, name="Travel", level=1)
        _seed_category(conn, name="Transportation", level=1)
        _seed_category(conn, name="Dining", level=1)
        _seed_category(conn, name="Bank Charges & Fees", parent_id=financial_id, level=1)

        conn.commit()

    initialize_database(db_path)

    with connect(db_path) as conn:
        account_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(accounts)").fetchall()
        }
        assert "is_business" in account_columns

        pl_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(pl_section_map)").fetchall()
        }
        assert {"id", "category_id", "pl_section", "display_order"} <= pl_columns

        schedule_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(schedule_c_map)").fetchall()
        }
        assert {
            "id",
            "category_id",
            "schedule_c_line",
            "line_number",
            "deduction_pct",
            "tax_year",
            "notes",
        } <= schedule_columns

        expected_parents = {
            "Advertising": "Professional",
            "Contract Labor": "Professional",
            "Office Expense": "Housing",
            "Supplies": "Housing",
            "Depreciation": "Financial",
            "Taxes & Licenses": "Financial",
            "Cost of Goods Sold": "Income",
        }
        rows = conn.execute(
            """
            SELECT c.name, c.level, c.is_income, c.is_system, p.name AS parent_name
              FROM categories c
              LEFT JOIN categories p ON p.id = c.parent_id
             WHERE c.name IN (
                 'Advertising', 'Contract Labor', 'Office Expense',
                 'Supplies', 'Depreciation', 'Taxes & Licenses', 'Cost of Goods Sold'
             )
             ORDER BY c.name ASC
            """
        ).fetchall()

        assert len(rows) == 7
        for row in rows:
            assert row["parent_name"] == expected_parents[str(row["name"])]
            assert int(row["level"] or 0) == 1
            assert int(row["is_income"] or 0) == 0
            assert int(row["is_system"] or 0) == 1

        pl_map = {
            row["name"]: row["pl_section"]
            for row in conn.execute(
                """
                SELECT c.name, m.pl_section
                  FROM pl_section_map m
                  JOIN categories c ON c.id = m.category_id
                 WHERE c.name IN ('Income: Business', 'Advertising', 'Cost of Goods Sold')
                """
            ).fetchall()
        }
        assert pl_map == {
            "Income: Business": "revenue",
            "Advertising": "opex_marketing",
            "Cost of Goods Sold": "cogs",
        }

        schedule_map = {
            row["name"]: (row["schedule_c_line"], row["line_number"], float(row["deduction_pct"]), int(row["tax_year"]))
            for row in conn.execute(
                """
                SELECT c.name, m.schedule_c_line, m.line_number, m.deduction_pct, m.tax_year
                  FROM schedule_c_map m
                  JOIN categories c ON c.id = m.category_id
                 WHERE c.name IN ('Advertising', 'Dining', 'Cost of Goods Sold')
                """
            ).fetchall()
        }
        assert schedule_map["Advertising"] == ("Advertising", "8", 1.0, 2025)
        assert schedule_map["Dining"] == ("Deductible meals", "24b", 0.5, 2025)
        assert schedule_map["Cost of Goods Sold"] == ("COGS (Part III)", "42", 1.0, 2025)


def test_account_set_business_marks_account_business(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, is_business=0)
        conn.commit()

    code, payload = _run_cli(["account", "set-business", account_id, "--business"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "account.set_business"
    assert payload["data"]["new_is_business"] == 1

    with connect(db_path) as conn:
        row = conn.execute("SELECT is_business FROM accounts WHERE id = ?", (account_id,)).fetchone()
        assert row["is_business"] == 1


def test_account_set_business_marks_account_personal(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, is_business=1)
        conn.commit()

    code, payload = _run_cli(["account", "set-business", account_id, "--personal"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["new_is_business"] == 0

    with connect(db_path) as conn:
        row = conn.execute("SELECT is_business FROM accounts WHERE id = ?", (account_id,)).fetchone()
        assert row["is_business"] == 0


def test_account_set_business_backfill_only_updates_active_null_use_type(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, is_business=0)
        null_active_1 = _seed_txn(conn, account_id=account_id, use_type=None, is_active=1)
        null_active_2 = _seed_txn(conn, account_id=account_id, use_type=None, is_active=1)
        business_active = _seed_txn(conn, account_id=account_id, use_type="Business", is_active=1)
        personal_active = _seed_txn(conn, account_id=account_id, use_type="Personal", is_active=1)
        null_inactive = _seed_txn(conn, account_id=account_id, use_type=None, is_active=0)
        conn.commit()

    code, payload = _run_cli(["account", "set-business", account_id, "--business", "--backfill"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["backfill"]["enabled"] is True
    assert payload["data"]["backfill"]["transactions_updated"] == 2

    with connect(db_path) as conn:
        rows = {
            row["id"]: row
            for row in conn.execute(
                "SELECT id, use_type, is_active FROM transactions WHERE account_id = ?",
                (account_id,),
            ).fetchall()
        }

    assert rows[null_active_1]["use_type"] == "Business"
    assert rows[null_active_2]["use_type"] == "Business"
    assert rows[business_active]["use_type"] == "Business"
    assert rows[personal_active]["use_type"] == "Personal"
    assert rows[null_inactive]["use_type"] is None


def test_account_set_business_backfill_personal_undoes_business_on_active_rows(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, is_business=1)
        business_active_1 = _seed_txn(conn, account_id=account_id, use_type="Business", is_active=1)
        business_active_2 = _seed_txn(conn, account_id=account_id, use_type="Business", is_active=1)
        personal_active = _seed_txn(conn, account_id=account_id, use_type="Personal", is_active=1)
        business_inactive = _seed_txn(conn, account_id=account_id, use_type="Business", is_active=0)
        conn.commit()

    code, payload = _run_cli(["account", "set-business", account_id, "--personal", "--backfill"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["backfill"]["transactions_updated"] == 2

    with connect(db_path) as conn:
        rows = {
            row["id"]: row
            for row in conn.execute(
                "SELECT id, use_type, is_active FROM transactions WHERE account_id = ?",
                (account_id,),
            ).fetchall()
        }

    assert rows[business_active_1]["use_type"] is None
    assert rows[business_active_2]["use_type"] is None
    assert rows[personal_active]["use_type"] == "Personal"
    assert rows[business_inactive]["use_type"] == "Business"


def test_account_list_includes_business_flag(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        business_account = _seed_account(conn, is_business=1)
        personal_account = _seed_account(conn, is_business=0)
        conn.commit()

    code, payload = _run_cli(["account", "list", "--status", "all"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    by_id = {row["id"]: row for row in payload["data"]["accounts"]}
    assert by_id[business_account]["is_business"] == 1
    assert by_id[personal_account]["is_business"] == 0
    assert "biz" in payload["cli_report"]


def test_mcp_account_set_business_updates_flag_and_backfill(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, is_business=0)
        _seed_txn(conn, account_id=account_id, use_type=None, is_active=1)
        _seed_txn(conn, account_id=account_id, use_type="Personal", is_active=1)
        conn.commit()

    from finance_cli.mcp_server import account_set_business

    result = account_set_business(account_id=account_id, is_business=True, backfill=True)
    assert result["data"]["new_is_business"] == 1
    assert result["data"]["backfill"]["transactions_updated"] == 1

    with connect(db_path) as conn:
        account_row = conn.execute("SELECT is_business FROM accounts WHERE id = ?", (account_id,)).fetchone()
        assert account_row["is_business"] == 1


def test_keyword_rule_use_type_propagates_via_auto_categorize(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        keyword_rules:
          - keywords: ["ACME SAAS"]
            category: "Software & Subscriptions"
            use_type: Business
            priority: 10
        """,
    )

    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _category_id(conn, "Software & Subscriptions")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            use_type=None,
            description="ACME SAAS monthly plan",
            category_id=None,
            category_source=None,
        )
        conn.commit()

    code, payload = _run_cli(["cat", "auto-categorize"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT category_source, use_type FROM transactions WHERE id = ?",
            (txn_id,),
        ).fetchone()
        assert row["category_source"] == "keyword_rule"
        assert row["use_type"] == "Business"


def test_category_override_sets_use_type_when_apply_match_runs(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        category_overrides:
          - categories: [Dining]
            force_use_type: Personal
            note: "force personal for test"
        """,
    )

    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        dining_id = _category_id(conn, "Dining")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            use_type=None,
            description="Generic Dining Charge",
            category_id=None,
            category_source=None,
        )
        conn.commit()

        applied = apply_match(
            conn,
            txn_id,
            MatchResult(
                category_id=dining_id,
                category_source="plaid",
                category_confidence=0.8,
                category_rule_id=None,
            ),
        )
        assert applied is True

        row = conn.execute("SELECT use_type FROM transactions WHERE id = ?", (txn_id,)).fetchone()
        assert row["use_type"] == "Personal"


def test_cat_apply_splits_dry_run_lists_candidates_without_creating_children(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        split_rules:
          - match:
              category: Rent
            business_pct: 25
            business_category: Rent
            personal_category: Rent
            note: "25% home office"
        """,
    )

    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        rent_id = _category_id(conn, "Rent")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            use_type=None,
            category_id=rent_id,
            category_source="user",
            amount_cents=-10000,
            description="Monthly Rent",
        )
        conn.commit()

    code, payload = _run_cli(["cat", "apply-splits"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["candidate_transactions"] == 1
    assert payload["data"]["split_transactions"] == 0
    assert payload["data"]["created_children"] == 0
    assert payload["cli_report"] == "1 transactions would be split"

    with connect(db_path) as conn:
        parent = conn.execute(
            "SELECT is_active, split_group_id FROM transactions WHERE id = ?",
            (txn_id,),
        ).fetchone()
        child_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE parent_transaction_id = ?",
            (txn_id,),
        ).fetchone()["cnt"]
        assert int(parent["is_active"]) == 1
        assert parent["split_group_id"] is None
        assert child_count == 0


def test_cat_apply_splits_commit_creates_children_with_amounts_and_use_type(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        split_rules:
          - match:
              category: Rent
            business_pct: 25
            business_category: Rent
            personal_category: Rent
            note: "25% home office"
        """,
    )

    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        rent_id = _category_id(conn, "Rent")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            use_type=None,
            category_id=rent_id,
            category_source="user",
            amount_cents=-10000,
            description="Monthly Rent",
        )
        conn.commit()

    code, payload = _run_cli(["cat", "apply-splits", "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["split_transactions"] == 1
    assert payload["data"]["created_children"] == 2
    assert payload["cli_report"] == "1 transactions split into 2 children"

    with connect(db_path) as conn:
        parent = conn.execute(
            "SELECT is_active, split_group_id FROM transactions WHERE id = ?",
            (txn_id,),
        ).fetchone()
        children = conn.execute(
            """
            SELECT use_type, amount_cents, split_pct
              FROM transactions
             WHERE parent_transaction_id = ?
             ORDER BY use_type ASC
            """,
            (txn_id,),
        ).fetchall()
        assert int(parent["is_active"]) == 0
        assert parent["split_group_id"] is not None
        assert len(children) == 2
        by_use_type = {str(row["use_type"]): row for row in children}
        assert int(by_use_type["Business"]["amount_cents"]) == -2500
        assert int(by_use_type["Personal"]["amount_cents"]) == -7500
        assert float(by_use_type["Business"]["split_pct"]) == 0.25
        assert float(by_use_type["Personal"]["split_pct"]) == 0.75


def test_view_business_filters_daily_and_weekly_to_business_transactions(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_txn(
            conn,
            account_id=account_id,
            use_type="Business",
            category_id=dining_id,
            category_source="user",
            amount_cents=-2000,
            description="biz-lunch",
            txn_date="2026-02-15",
        )
        _seed_txn(
            conn,
            account_id=account_id,
            use_type="Personal",
            category_id=dining_id,
            category_source="user",
            amount_cents=-3000,
            description="personal-lunch",
            txn_date="2026-02-15",
        )
        conn.commit()

    code, daily_payload = _run_cli(["daily", "--date", "2026-02-15", "--view", "business"], capsys)
    assert code == 0
    assert daily_payload["summary"]["total_transactions"] == 1
    assert daily_payload["data"]["transactions"][0]["description"] == "biz-lunch"

    code, weekly_payload = _run_cli(["weekly", "--week", "2026-W07", "--view", "business"], capsys)
    assert code == 0
    assert sum(int(row["total_cents"]) for row in weekly_payload["data"]["categories"]) == -2000


def test_view_personal_includes_null_use_type_and_view_all_matches_default(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_txn(
            conn,
            account_id=account_id,
            use_type="Business",
            category_id=dining_id,
            category_source="user",
            amount_cents=-2000,
            description="biz-item",
            txn_date="2026-02-15",
        )
        _seed_txn(
            conn,
            account_id=account_id,
            use_type="Personal",
            category_id=dining_id,
            category_source="user",
            amount_cents=-1500,
            description="personal-item",
            txn_date="2026-02-15",
        )
        _seed_txn(
            conn,
            account_id=account_id,
            use_type=None,
            category_id=dining_id,
            category_source="user",
            amount_cents=-500,
            description="null-item",
            txn_date="2026-02-15",
        )
        conn.commit()

    code, personal_payload = _run_cli(["daily", "--date", "2026-02-15", "--view", "personal"], capsys)
    assert code == 0
    assert personal_payload["summary"]["total_transactions"] == 2
    assert {txn["description"] for txn in personal_payload["data"]["transactions"]} == {"personal-item", "null-item"}

    code, default_payload = _run_cli(["daily", "--date", "2026-02-15"], capsys)
    assert code == 0
    code, all_payload = _run_cli(["daily", "--date", "2026-02-15", "--view", "all"], capsys)
    assert code == 0
    assert all_payload["summary"]["total_transactions"] == default_payload["summary"]["total_transactions"]
    assert all_payload["summary"]["total_amount"] == default_payload["summary"]["total_amount"]


def test_cat_classify_use_type_sets_use_type_from_category_override(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        category_overrides:
          - categories: [Dining]
            force_use_type: Personal
            note: "force personal for test"
        """,
    )

    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        dining_id = _category_id(conn, "Dining")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            use_type=None,
            category_id=dining_id,
            category_source="plaid",
            description="Dining purchase",
        )
        conn.commit()

    code, payload = _run_cli(["cat", "classify-use-type", "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["updated"] == 1
    assert payload["data"]["by_reason"]["category_override"] == 1

    with connect(db_path) as conn:
        row = conn.execute("SELECT use_type FROM transactions WHERE id = ?", (txn_id,)).fetchone()
        assert row["use_type"] == "Personal"


def test_cat_classify_use_type_sets_use_type_from_keyword_rule(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        keyword_rules:
          - keywords: ["ACME PAYOUT"]
            category: "Income: Business"
            use_type: Business
            priority: 0
        """,
    )

    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        income_business_id = _category_id(conn, "Income: Business")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            use_type=None,
            category_id=income_business_id,
            category_source="keyword_rule",
            amount_cents=250000,
            description="ACME PAYOUT 2026-02",
        )
        conn.commit()

    code, payload = _run_cli(["cat", "classify-use-type", "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["by_reason"]["keyword_rule"] == 1

    with connect(db_path) as conn:
        row = conn.execute("SELECT use_type FROM transactions WHERE id = ?", (txn_id,)).fetchone()
        assert row["use_type"] == "Business"
