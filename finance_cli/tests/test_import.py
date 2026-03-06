from __future__ import annotations

import uuid
from pathlib import Path
from textwrap import dedent

import pytest

from finance_cli.categorizer import MatchResult, normalize_description
from finance_cli.db import connect, initialize_database
from finance_cli.importers import (
    _account_id_for_source,
    _get_or_create_account,
    backfill_account_aliases,
    import_csv,
    import_income_csv,
    import_vendor_memory_csv,
)
from finance_cli.importers.pdf import ExtractResult, import_extracted_statement
from finance_cli.plaid_client import PlaidConfigStatus, fetch_liabilities
from finance_cli.user_rules import UserRules


def _setup_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(dedent(content).lstrip("\n"), encoding="utf-8")
    return path


def _income_rules(income_sources: dict[str, dict]) -> UserRules:
    return UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources=income_sources,
        ai_categorizer={},
        raw={},
    )


def _insert_account(
    conn,
    *,
    account_id: str,
    institution_name: str,
    account_type: str,
    card_ending: str | None = None,
    plaid_account_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, institution_name, account_name, account_type, card_ending, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (
            account_id,
            plaid_account_id,
            institution_name,
            f"{institution_name} {card_ending or ''}".strip(),
            account_type,
            card_ending,
        ),
    )


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    return category_id


def _seed_plaid_liabilities_item(conn, *, plaid_item_id: str = "item_liab_test") -> None:
    conn.execute(
        """
        INSERT INTO plaid_items (
            id,
            plaid_item_id,
            institution_name,
            access_token_ref,
            status,
            consented_products,
            sync_cursor
        ) VALUES (?, ?, 'Test Bank', 'secret/ref', 'active', '["transactions","liabilities"]', NULL)
        """,
        (uuid.uuid4().hex, plaid_item_id),
    )
    conn.commit()


# -----------------------------------------------------------------------------
# Existing import_csv idempotency coverage
# -----------------------------------------------------------------------------


def test_import_idempotency(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "sample.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2024-01-01,UBER TRIP,-12.50,1234,Chase,Business,Travel,false
        2024-01-02,COFFEE SHOP,-5.00,1234,Chase,Personal,Dining,false
        """,
    )

    with connect(db_path) as conn:
        first = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=False)
        second = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=False)

    assert first.inserted == 2
    assert first.skipped_duplicates == 0
    assert second.inserted == 0
    assert second.skipped_duplicates == 2


def test_same_fingerprint_duplicates_preserved(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "duplicate_rows.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2024-03-01,COFFEE SHOP,-5.00,1234,Chase,Personal,Dining,false
        2024-03-01,COFFEE SHOP,-5.00,1234,Chase,Personal,Dining,false
        """,
    )

    with connect(db_path) as conn:
        first = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=False)
        second = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=False)
        count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]

    assert first.inserted == 2
    assert first.skipped_duplicates == 0
    assert count == 2
    assert second.inserted == 0
    assert second.skipped_duplicates == 2


# -----------------------------------------------------------------------------
# import_vendor_memory_csv
# -----------------------------------------------------------------------------


def test_vendor_memory_insert_and_normalization(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "vendor_memory.csv",
        """
        Description,Use Type,Category,Canonical Name
          UBER   TRIP  ,business,Travel,Uber
        NETFLIX,,Entertainment,Netflix
        """,
    )

    with connect(db_path) as conn:
        report = import_vendor_memory_csv(conn, csv_path)
        rows = conn.execute(
            """
            SELECT description_pattern, use_type, match_count
            FROM vendor_memory
            ORDER BY description_pattern
            """
        ).fetchall()

    assert report == {"inserted": 2, "updated": 0, "errors": 0}
    assert [row["description_pattern"] for row in rows] == [
        normalize_description("NETFLIX"),
        normalize_description("  UBER   TRIP  "),
    ]
    assert [row["use_type"] for row in rows] == ["Any", "Any"]
    assert [row["match_count"] for row in rows] == [0, 0]


def test_vendor_memory_update_existing_row(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "vendor_memory_update.csv",
        """
        Description,Use Type,Category,Canonical Name
        UBER TRIP,any,Travel,Uber Technologies
        """,
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id,
                description_pattern,
                canonical_name,
                category_id,
                use_type,
                confidence,
                priority,
                is_enabled,
                is_confirmed,
                match_count
            ) VALUES ('vm_rule_1', ?, 'Old Name', NULL, 'Any', 0.5, 0, 1, 1, 7)
            """,
            (normalize_description("Uber Trip"),),
        )

        report = import_vendor_memory_csv(conn, csv_path)
        count = conn.execute("SELECT COUNT(*) AS n FROM vendor_memory").fetchone()["n"]
        row = conn.execute(
            """
            SELECT vm.canonical_name, vm.match_count, c.name AS category_name
            FROM vendor_memory vm
            LEFT JOIN categories c ON c.id = vm.category_id
            WHERE vm.id = 'vm_rule_1'
            """
        ).fetchone()

    assert report == {"inserted": 0, "updated": 1, "errors": 0}
    assert count == 1
    assert row["canonical_name"] == "Uber Technologies"
    assert row["category_name"] == "Travel"
    assert row["match_count"] == 7


def test_vendor_memory_dry_run_no_commit(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "vendor_memory_dry_run.csv",
        """
        Description,Use Type,Category,Canonical Name
        UBER TRIP,Any,,Updated Canonical
        COFFEE SHOP,Personal,,Coffee Shop
        """,
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id,
                description_pattern,
                canonical_name,
                use_type,
                confidence,
                priority,
                is_enabled,
                is_confirmed,
                match_count
            ) VALUES ('vm_rule_1', ?, 'Original Canonical', 'Any', 1.0, 0, 1, 1, 0)
            """,
            (normalize_description("UBER TRIP"),),
        )
        conn.commit()

        report = import_vendor_memory_csv(conn, csv_path, dry_run=True)
        count = conn.execute("SELECT COUNT(*) AS n FROM vendor_memory").fetchone()["n"]
        canonical = conn.execute(
            "SELECT canonical_name FROM vendor_memory WHERE id = 'vm_rule_1'"
        ).fetchone()["canonical_name"]

    assert report == {"inserted": 1, "updated": 1, "errors": 0}
    assert count == 1
    assert canonical == "Original Canonical"


def test_vendor_memory_skip_blank_pattern(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "vendor_memory_blank.csv",
        """
        Description,Use Type,Category,Canonical Name
          ,Business,Travel,Ignored
        """,
    )

    with connect(db_path) as conn:
        report = import_vendor_memory_csv(conn, csv_path)
        count = conn.execute("SELECT COUNT(*) AS n FROM vendor_memory").fetchone()["n"]

    assert report == {"inserted": 0, "updated": 0, "errors": 0}
    assert count == 0


# -----------------------------------------------------------------------------
# import_income_csv validation and dry-run rollback behavior
# -----------------------------------------------------------------------------


def test_import_income_csv_dry_run_rolls_back(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "income.csv",
        """
        Date,Revenue (USD),Product
        2026-01-01,100.00,Course A
        2026-01-02,25.50,Course B
        """,
    )
    rules = _income_rules(
        {
            "kartra": {
                "platform": "Kartra",
                "category": "Income: Business",
                "use_type": "Business",
                "csv_columns": {
                    "date": "Date",
                    "amount": "Revenue (USD)",
                    "description": "Product",
                },
            }
        }
    )

    with connect(db_path) as conn:
        report = import_income_csv(conn, csv_path, source_name="kartra", rules=rules, dry_run=True)
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
        category_count = conn.execute("SELECT COUNT(*) AS n FROM categories").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert report.inserted == 2
    assert report.skipped_duplicates == 0
    assert report.errors == 0
    assert txn_count == 0
    assert account_count == 0
    assert category_count == 0
    assert batch_count == 0


def test_import_income_csv_missing_source_config_raises(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "income_missing_source.csv",
        """
        Date,Amount
        2026-01-01,100.00
        """,
    )
    rules = _income_rules({})

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="not configured"):
            import_income_csv(conn, csv_path, source_name="kartra", rules=rules)


def test_import_income_csv_invalid_csv_columns_raises(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "income_invalid_columns.csv",
        """
        Date,Amount
        2026-01-01,100.00
        """,
    )
    rules = _income_rules(
        {
            "kartra": {
                "csv_columns": "not-a-mapping",
            }
        }
    )

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="csv_columns must be a mapping"):
            import_income_csv(conn, csv_path, source_name="kartra", rules=rules)


def test_import_income_csv_missing_date_or_amount_mapping_raises(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "income_missing_required_columns.csv",
        """
        Date,Amount
        2026-01-01,100.00
        """,
    )
    rules = _income_rules(
        {
            "kartra": {
                "csv_columns": {
                    "date": "Date",
                },
            }
        }
    )

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="must define date and amount"):
            import_income_csv(conn, csv_path, source_name="kartra", rules=rules)


# -----------------------------------------------------------------------------
# Core row parsing and tolerance in import_csv/_import_row_iter
# -----------------------------------------------------------------------------


def test_import_csv_parses_truthy_is_payment(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "truthy_is_payment.csv",
        """
        Date,Description,Amount,Card Ending,Source,Is Payment
        2026-01-01,Payment One,-10.00,1111,Test Source,true
        2026-01-02,Payment Two,-20.00,1111,Test Source,1
        2026-01-03,Payment Three,-30.00,1111,Test Source,Yes
        """,
    )

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Test Source", validate_name=False)
        flags = conn.execute(
            "SELECT is_payment FROM transactions ORDER BY date ASC"
        ).fetchall()

    assert report.inserted == 3
    assert report.errors == 0
    assert [row["is_payment"] for row in flags] == [1, 1, 1]


def test_import_csv_parses_parentheses_and_currency_amounts(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "currency_amount.csv",
        """
        Date,Description,Amount,Card Ending,Source,Is Payment
        2026-01-01,Large Purchase,"($1,234.56)",1111,Test Source,false
        """,
    )

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Test Source", validate_name=False)
        row = conn.execute(
            "SELECT amount_cents FROM transactions WHERE description = 'Large Purchase'"
        ).fetchone()

    assert report.inserted == 1
    assert row["amount_cents"] == -123456


def test_import_csv_bad_amount_increments_errors(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "bad_amount.csv",
        """
        Date,Description,Amount,Card Ending,Source,Is Payment
        2026-01-01,Bad Row,not-a-number,1111,Test Source,false
        2026-01-02,Good Row,-5.00,1111,Test Source,false
        """,
    )

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Test Source", validate_name=False)
        count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        kept = conn.execute(
            "SELECT description, amount_cents FROM transactions"
        ).fetchone()

    assert report.inserted == 1
    assert report.errors == 1
    assert count == 1
    assert kept["description"] == "Good Row"
    assert kept["amount_cents"] == -500


def test_import_csv_missing_row_source_falls_back(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "missing_source_column.csv",
        """
        Date,Description,Amount,Card Ending,Is Payment
        2026-01-01,Fallback Source Row,-12.00,0001,false
        """,
    )
    expected_account_id = _account_id_for_source("Fallback Source", "0001")

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Fallback Source", validate_name=False)
        txn = conn.execute(
            "SELECT account_id, source FROM transactions WHERE description = 'Fallback Source Row'"
        ).fetchone()
        account = conn.execute(
            "SELECT id FROM accounts WHERE id = ?",
            (expected_account_id,),
        ).fetchone()

    assert report.inserted == 1
    assert txn["account_id"] == expected_account_id
    assert txn["source"] == "csv_import"
    assert account is not None


def test_import_csv_uses_pipeline_and_preserves_source_category(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "pipeline.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-01-01,ACME PAYROLL,-100.00,0001,Test Source,Personal,Bank Label,false
        """,
    )

    with connect(db_path) as conn:
        income_id = _seed_category(conn, "Income: Salary")
        rule_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id, description_pattern, category_id, use_type, confidence, priority, is_enabled, is_confirmed, match_count
            ) VALUES (?, 'acme payroll', ?, 'Any', 1.0, 0, 1, 1, 0)
            """,
            (rule_id, income_id),
        )
        conn.commit()

    calls: list[tuple[str, str | None, str | None, bool]] = []

    def _fake_match(conn, description, use_type, source_category=None, is_payment=False):
        calls.append((description, use_type, source_category, bool(is_payment)))
        return MatchResult(
            category_id=income_id,
            category_source="vendor_memory",
            category_confidence=1.0,
            category_rule_id=rule_id,
            matched_rule_id=rule_id,
        )

    monkeypatch.setattr("finance_cli.importers.match_transaction", _fake_match)

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Test Source", dry_run=False, validate_name=False)
        row = conn.execute(
            """
            SELECT source_category, category_id, category_source, category_confidence, category_rule_id
              FROM transactions
             WHERE description = 'ACME PAYROLL'
            """
        ).fetchone()

    assert report.inserted == 1
    assert calls == [("ACME PAYROLL", "Personal", "Bank Label", False)]
    assert row["source_category"] == "Bank Label"
    assert row["category_id"] == income_id
    assert row["category_source"] == "vendor_memory"
    assert row["category_confidence"] == 1.0
    assert row["category_rule_id"] == rule_id


def test_import_csv_pipeline_propagates_payment_flag_from_match_result(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "payment_flag.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-01-01,BANK OF AMERICA CREDIT CARD BILL PAYMENT,-100.00,0001,Test Source,Personal,Bank Label,false
        """,
    )

    with connect(db_path) as conn:
        payments_id = _seed_category(conn, "Payments & Transfers")
        conn.commit()

    def _fake_match(conn, description, use_type, source_category=None, is_payment=False):
        return MatchResult(
            category_id=payments_id,
            category_source="keyword_rule",
            category_confidence=0.9,
            category_rule_id=None,
            matched_rule_id=None,
            is_payment=True,
        )

    monkeypatch.setattr("finance_cli.importers.match_transaction", _fake_match)

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Test Source", dry_run=False, validate_name=False)
        row = conn.execute(
            """
            SELECT category_id, category_source, is_payment
              FROM transactions
             WHERE description = 'BANK OF AMERICA CREDIT CARD BILL PAYMENT'
            """
        ).fetchone()

    assert report.inserted == 1
    assert row["category_id"] == payments_id
    assert row["category_source"] == "keyword_rule"
    assert row["is_payment"] == 1


def test_import_csv_dry_run_skips_match_transaction(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "dryrun_skip_match.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-01-01,NO MATCH CALL,-5.00,0001,Test Source,Personal,Bank Label,false
        """,
    )

    def _unexpected(*_args, **_kwargs):
        raise AssertionError("match_transaction should not be called during dry-run imports")

    monkeypatch.setattr("finance_cli.importers.match_transaction", _unexpected)

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Test Source", dry_run=True, validate_name=False)
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]

    assert report.inserted == 1
    assert report.errors == 0
    assert txn_count == 0


def test_import_csv_invalid_rules_skips_pipeline_and_continues(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "rules_invalid.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-01-01,ROW ONE,-10.00,0001,Test Source,Personal,Bank A,false
        2026-01-02,ROW TWO,-20.00,0001,Test Source,Personal,Bank B,false
        """,
    )

    monkeypatch.setattr("finance_cli.importers.load_rules", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad rules")))

    def _unexpected(*_args, **_kwargs):
        raise AssertionError("match_transaction should be skipped when rules preload fails")

    monkeypatch.setattr("finance_cli.importers.match_transaction", _unexpected)

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Test Source", dry_run=False, validate_name=False)
        rows = conn.execute(
            """
            SELECT description, source_category, category_id, category_source, category_confidence, category_rule_id
              FROM transactions
             ORDER BY date
            """
        ).fetchall()

    assert report.inserted == 2
    assert report.errors == 0
    assert [row["source_category"] for row in rows] == ["Bank A", "Bank B"]
    assert all(row["category_id"] is None for row in rows)
    assert all(row["category_source"] is None for row in rows)
    assert all(row["category_confidence"] is None for row in rows)
    assert all(row["category_rule_id"] is None for row in rows)


# -----------------------------------------------------------------------------
# Account alias backfill behavior
# -----------------------------------------------------------------------------


def test_backfill_aliases_unchanged_and_stale_kept(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id="plaid_chase_1234",
            plaid_account_id="plaid_ext_chase_1234",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
        )
        _insert_account(
            conn,
            account_id="hash_chase_1234",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            ("hash_chase_1234", "plaid_chase_1234"),
        )

        _insert_account(
            conn,
            account_id="plaid_other",
            plaid_account_id="plaid_ext_other",
            institution_name="Other Bank",
            account_type="checking",
            card_ending="9999",
        )
        _insert_account(
            conn,
            account_id="hash_stale",
            institution_name="Unknown Bank",
            account_type="loan",
            card_ending="0000",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            ("hash_stale", "plaid_other"),
        )

        report = backfill_account_aliases(conn, dry_run=False)
        aliases = conn.execute(
            "SELECT hash_account_id, canonical_id FROM account_aliases ORDER BY hash_account_id"
        ).fetchall()

    assert report["scanned"] == 2
    assert report["aliased"] == 0
    assert report["removed"] == 0
    assert report["unchanged"] == 2
    assert len(aliases) == 2
    assert aliases[0]["hash_account_id"] == "hash_chase_1234"
    assert aliases[0]["canonical_id"] == "plaid_chase_1234"
    assert aliases[1]["hash_account_id"] == "hash_stale"
    assert aliases[1]["canonical_id"] == "plaid_other"


def test_get_or_create_account_returns_canonical_and_hash_when_alias_found(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id="plaid_chase_1234",
            plaid_account_id="plaid_ext_chase_1234",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
        )

        effective_id, hash_id = _get_or_create_account(
            conn,
            "Chase",
            "1234",
            account_type="credit_card",
        )
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_id,),
        ).fetchone()

    assert effective_id == "plaid_chase_1234"
    assert hash_id == _account_id_for_source("Chase", "1234")
    assert alias is not None
    assert alias["canonical_id"] == "plaid_chase_1234"


def test_get_or_create_account_returns_hash_tuple_when_no_alias(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        effective_id, hash_id = _get_or_create_account(
            conn,
            "Unknown Bank",
            "7777",
            account_type="credit_card",
        )

    assert effective_id == _account_id_for_source("Unknown Bank", "7777")
    assert hash_id == _account_id_for_source("Unknown Bank", "7777")


def test_import_csv_dedupe_key_stays_stable_after_alias_creation(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "alias_stability.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-02-10,SPOTIFY,-11.99,1234,Chase,Personal,Entertainment,false
        """,
    )
    hash_account_id = _account_id_for_source("Chase", "1234")

    with connect(db_path) as conn:
        first = import_csv(conn, csv_path, source_name="Chase", dry_run=False, validate_name=False)
        first_row = conn.execute(
            "SELECT account_id, dedupe_key FROM transactions WHERE description = 'SPOTIFY'"
        ).fetchone()

        _insert_account(
            conn,
            account_id="plaid_chase_1234",
            plaid_account_id="plaid_ext_chase_1234",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
        )
        conn.commit()

        second = import_csv(conn, csv_path, source_name="Chase", dry_run=False, validate_name=False)
        second_row = conn.execute(
            "SELECT account_id, dedupe_key FROM transactions WHERE description = 'SPOTIFY'"
        ).fetchone()
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    assert first.inserted == 1
    assert first.skipped_duplicates == 0
    assert first_row["account_id"] == hash_account_id
    assert second.inserted == 0
    assert second.skipped_duplicates == 1
    assert second_row["account_id"] == "plaid_chase_1234"
    assert second_row["dedupe_key"] == first_row["dedupe_key"]
    assert txn_count == 1
    assert alias is not None
    assert alias["canonical_id"] == "plaid_chase_1234"


def test_pdf_import_uses_canonical_account_when_alias_exists(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-02-10",
                "description": "SPOTIFY",
                "amount_cents": -1199,
                "card_ending": "1234",
                "source": "Chase",
            }
        ],
        extracted_total_cents=-1199,
        reconciled=True,
        warnings=[],
        statement_card_ending="1234",
    )

    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id="plaid_chase_1234",
            plaid_account_id="plaid_ext_chase_1234",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
        )
        conn.commit()

        result = import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="chase_credit",
            validate_name=False,
        )
        row = conn.execute(
            "SELECT account_id FROM transactions WHERE source = 'pdf_import'"
        ).fetchone()

    assert result["inserted"] == 1
    assert row["account_id"] == "plaid_chase_1234"


def test_pdf_import_applies_match_transaction_before_insert(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-02-10",
                "description": "BLOOMINGDALES DES:AUTO PYMT",
                "amount_cents": -1199,
                "card_ending": "1234",
                "source": "Chase",
            }
        ],
        extracted_total_cents=-1199,
        reconciled=True,
        warnings=[],
        statement_card_ending="1234",
    )

    calls: list[tuple[str, str | None, bool]] = []

    with connect(db_path) as conn:
        payments_row = conn.execute(
            "SELECT id FROM categories WHERE name = 'Payments & Transfers'"
        ).fetchone()
        payments_id = str(payments_row["id"]) if payments_row else _seed_category(conn, "Payments & Transfers")
        conn.commit()

    def _fake_match(conn, description, use_type=None, source_category=None, is_payment=False):
        calls.append((description, use_type, bool(is_payment)))
        return MatchResult(
            category_id=payments_id,
            category_source="keyword_rule",
            category_confidence=0.9,
            category_rule_id=None,
            matched_rule_id=None,
            is_payment=True,
        )

    monkeypatch.setattr("finance_cli.importers.pdf.match_transaction", _fake_match)

    with connect(db_path) as conn:
        result = import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="chase_credit",
            validate_name=False,
        )
        row = conn.execute(
            """
            SELECT category_id, category_source, category_confidence, category_rule_id, is_payment
              FROM transactions
             WHERE source = 'pdf_import'
            """
        ).fetchone()

    assert result["inserted"] == 1
    assert calls == [("BLOOMINGDALES DES:AUTO PYMT", None, False)]
    assert row["category_id"] == payments_id
    assert row["category_source"] == "keyword_rule"
    assert row["category_confidence"] == 0.9
    assert row["category_rule_id"] is None
    assert row["is_payment"] == 1


def test_pdf_import_creates_liability_row_for_credit_card_apr(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-02-10",
                "description": "STORE PURCHASE",
                "amount_cents": -1199,
                "card_ending": "1234",
                "source": "Chase",
            }
        ],
        extracted_total_cents=-1199,
        reconciled=True,
        warnings=[],
        statement_card_ending="1234",
        statement_account_type="credit_card",
        statement_period_end="2026-02-28",
        new_balance_cents=120000,
        apr_purchase=24.99,
    )

    with connect(db_path) as conn:
        result = import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="ai:claude",
            validate_name=False,
        )
        row = conn.execute(
            """
            SELECT liability_type,
                   is_active,
                   apr_purchase,
                   apr_balance_transfer,
                   apr_cash_advance,
                   last_statement_balance_cents,
                   last_statement_issue_date
              FROM liabilities
            """
        ).fetchone()

    assert result["inserted"] == 1
    assert row["liability_type"] == "credit"
    assert row["is_active"] == 1
    assert row["apr_purchase"] == 24.99
    assert row["apr_balance_transfer"] is None
    assert row["apr_cash_advance"] is None
    assert row["last_statement_balance_cents"] == 120000
    assert row["last_statement_issue_date"] == "2026-02-28"


def test_pdf_import_liability_upsert_supplements_existing_values(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")
    account_id = "acct_pdf_apr_1"

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-03-10",
                "description": "PAYMENT",
                "amount_cents": 5000,
                "card_ending": "1234",
                "source": "Chase",
            }
        ],
        extracted_total_cents=5000,
        reconciled=True,
        warnings=[],
        statement_card_ending="1234",
        statement_account_type="credit card",
        statement_period_end="2026-03-31",
        new_balance_cents=95000,
        apr_balance_transfer=15.5,
    )

    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id=account_id,
            plaid_account_id="plaid_pdf_apr_1",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
        )
        conn.execute(
            """
            INSERT INTO liabilities (
                id,
                account_id,
                liability_type,
                is_active,
                apr_purchase,
                apr_balance_transfer,
                apr_cash_advance,
                raw_plaid_json
            ) VALUES (?, ?, 'credit', 0, 21.99, 17.25, 29.99, '{}')
            """,
            (uuid.uuid4().hex, account_id),
        )
        conn.commit()

        import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="ai:claude",
            account_id=account_id,
            validate_name=False,
        )
        row = conn.execute(
            """
            SELECT is_active,
                   apr_purchase,
                   apr_balance_transfer,
                   apr_cash_advance,
                   last_statement_balance_cents,
                   last_statement_issue_date
              FROM liabilities
             WHERE account_id = ?
               AND liability_type = 'credit'
            """,
            (account_id,),
        ).fetchone()

    assert row["is_active"] == 1
    assert row["apr_purchase"] == 21.99
    assert row["apr_balance_transfer"] == 15.5
    assert row["apr_cash_advance"] == 29.99
    assert row["last_statement_balance_cents"] == 95000
    assert row["last_statement_issue_date"] == "2026-03-31"


def test_pdf_import_does_not_create_liability_for_non_credit_statement(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-02-10",
                "description": "DIRECT DEPOSIT",
                "amount_cents": 200000,
                "source": "Wells Fargo",
            }
        ],
        extracted_total_cents=200000,
        reconciled=True,
        warnings=[],
        statement_account_type="checking",
        statement_period_end="2026-02-28",
        new_balance_cents=350000,
        apr_purchase=9.99,
    )

    with connect(db_path) as conn:
        import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="ai:claude",
            validate_name=False,
        )
        row = conn.execute("SELECT COUNT(*) AS n FROM liabilities").fetchone()

    assert row["n"] == 0


def test_pdf_import_dry_run_does_not_touch_liabilities(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-02-10",
                "description": "STORE PURCHASE",
                "amount_cents": -1199,
                "card_ending": "1234",
                "source": "Chase",
            }
        ],
        extracted_total_cents=-1199,
        reconciled=True,
        warnings=[],
        statement_card_ending="1234",
        statement_account_type="credit_card",
        statement_period_end="2026-02-28",
        new_balance_cents=120000,
        apr_purchase=24.99,
    )

    with connect(db_path) as conn:
        import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="ai:claude",
            dry_run=True,
            validate_name=False,
        )
        row = conn.execute("SELECT COUNT(*) AS n FROM liabilities").fetchone()

    assert row["n"] == 0


def test_pdf_apr_survives_later_plaid_sync_with_null_apr(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")
    account_id = "acct_cross_source_apr"
    plaid_item_id = "item_cross_source_apr"

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-02-10",
                "description": "STORE PURCHASE",
                "amount_cents": -1199,
                "card_ending": "1234",
                "source": "Chase",
            }
        ],
        extracted_total_cents=-1199,
        reconciled=True,
        warnings=[],
        statement_card_ending="1234",
        statement_account_type="credit_card",
        statement_period_end="2026-02-28",
        new_balance_cents=120000,
        apr_purchase=24.99,
    )

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, _request):
            return _Resp(
                {
                    "accounts": [
                        {
                            "account_id": "cross_source_acct",
                            "name": "Card",
                            "type": "credit",
                            "subtype": "credit card",
                            "balances": {"current": 100.0, "limit": 2000.0, "iso_currency_code": "USD"},
                        }
                    ],
                    "liabilities": {
                        "credit": [
                            {
                                "account_id": "cross_source_acct",
                                "minimum_payment_amount": 25.0,
                                "next_payment_due_date": "2026-03-01",
                                "aprs": [{"apr_type": "purchase_apr", "apr_percentage": None}],
                            }
                        ],
                        "student": [],
                        "mortgage": [],
                    },
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_liabilities_item(conn, plaid_item_id=plaid_item_id)
        conn.execute(
            """
            INSERT INTO accounts (
                id,
                plaid_account_id,
                plaid_item_id,
                institution_name,
                account_name,
                account_type,
                card_ending,
                is_active
            ) VALUES (?, 'cross_source_acct', ?, 'Chase', 'Chase 1234', 'credit_card', '1234', 1)
            """,
            (account_id, plaid_item_id),
        )
        conn.commit()

        import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="ai:claude",
            account_id=account_id,
            validate_name=False,
        )
        before = conn.execute(
            "SELECT apr_purchase FROM liabilities WHERE account_id = ? AND liability_type = 'credit'",
            (account_id,),
        ).fetchone()
        assert before["apr_purchase"] == 24.99

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        fetch_liabilities(conn, item_id=plaid_item_id, force_refresh=True)
        after = conn.execute(
            "SELECT apr_purchase FROM liabilities WHERE account_id = ? AND liability_type = 'credit'",
            (account_id,),
        ).fetchone()

    assert after["apr_purchase"] == 24.99


# -----------------------------------------------------------------------------
# Name validation bypass
# -----------------------------------------------------------------------------


def test_import_csv_bypass_name_validation(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = _write_csv(
        tmp_path / "unknown_source.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-02-10,UBER TRIP,-12.50,1234,Unknown Credit Union,Business,Travel,false
        """,
    )

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Unknown Credit Union", validate_name=False)
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]

    assert report.inserted == 1
    assert report.errors == 0
    assert txn_count == 1
