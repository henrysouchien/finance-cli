from __future__ import annotations

import json
import uuid
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace

import pytest

from finance_cli.__main__ import main
from finance_cli.categorizer import MatchResult, apply_match, match_transaction, normalize_description
from finance_cli.commands.txn import handle_categorize
from finance_cli.db import connect, initialize_database
from finance_cli.importers import import_csv
from finance_cli.importers.pdf import ExtractResult, import_extracted_statement
from finance_cli.plaid_client import apply_sync_updates
from finance_cli.user_rules import KeywordRule, UserRules, load_rules, match_payment_exclusion


def _setup_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _category_id(conn, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE lower(name) = lower(?)", (name,)).fetchone()
    if row:
        return str(row["id"])

    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_vendor_memory(
    conn,
    *,
    description: str,
    category_id: str,
    use_type: str = "Any",
    priority: int = 0,
    confidence: float = 1.0,
    match_count: int = 0,
) -> str:
    rule_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO vendor_memory (
            id,
            description_pattern,
            category_id,
            use_type,
            confidence,
            priority,
            is_enabled,
            is_confirmed,
            match_count
        ) VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?)
        """,
        (
            rule_id,
            normalize_description(description),
            category_id,
            use_type,
            confidence,
            priority,
            match_count,
        ),
    )
    conn.commit()
    return rule_id


def _seed_transaction(
    conn,
    *,
    description: str,
    is_payment: int,
    category_id: str | None = None,
    txn_id: str | None = None,
    use_type: str | None = None,
) -> str:
    transaction_id = txn_id or uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id,
            date,
            description,
            amount_cents,
            category_id,
            source,
            is_active,
            is_payment,
            use_type
        ) VALUES (?, '2026-01-01', ?, -1000, ?, 'manual', 1, ?, ?)
        """,
        (transaction_id, description, category_id, is_payment, use_type),
    )
    conn.commit()
    return transaction_id


def _seed_plaid_item(conn, *, plaid_item_id: str = "item_data002"):
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
        ) VALUES (?, ?, 'Test Bank', 'secret/ref', 'active', '["transactions"]', NULL)
        """,
        (uuid.uuid4().hex, plaid_item_id),
    )
    conn.commit()
    return conn.execute(
        "SELECT * FROM plaid_items WHERE plaid_item_id = ?",
        (plaid_item_id,),
    ).fetchone()


def _rules(
    *,
    keyword_rules: list[KeywordRule] | None = None,
    payment_keywords: list[str] | None = None,
    payment_exclusions: list[str] | None = None,
) -> UserRules:
    return UserRules(
        keyword_rules=keyword_rules or [],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={},
        ai_categorizer={},
        revenue_streams=[],
        payment_keywords=payment_keywords or [],
        payment_exclusions=payment_exclusions or [],
        ai_parser={},
        extractors={},
        raw={},
    )


def _plaid_added_transaction(*, txn_id: str, account_id: str, name: str) -> dict[str, object]:
    return {
        "transaction_id": txn_id,
        "account_id": account_id,
        "date": "2026-01-01",
        "amount": 120.00,
        "name": name,
        "merchant_name": None,
        "payment_channel": "online",
        "pending": False,
        "personal_finance_category": {
            "primary": "TRANSFER_OUT",
            "detailed": "TRANSFER_OUT_ACCOUNT_TRANSFER",
            "confidence_level": "HIGH",
            "version": "v2",
        },
    }


def _plaid_accounts(*, account_id: str) -> list[dict[str, object]]:
    return [
        {
            "account_id": account_id,
            "name": "Checking",
            "type": "depository",
            "subtype": "checking",
            "mask": "1234",
        }
    ]


def test_vendor_memory_overrides_plaid_is_payment(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        rent_id = _category_id(conn, "Rent")
        _category_id(conn, "Payments & Transfers")
        _seed_vendor_memory(conn, description="Check 1143", category_id=rent_id)

        monkeypatch.setattr("finance_cli.categorizer.load_rules", lambda: _rules())
        result = match_transaction(conn, "Check 1143", is_payment=True)

    assert result is not None
    assert result.category_id == rent_id
    assert result.category_source == "vendor_memory"
    assert result.is_payment is False


def test_payment_exclusion_suppresses_plaid_is_payment(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        rent_id = _category_id(conn, "Rent")
        conn.execute(
            """
            INSERT INTO category_mappings (
                id,
                source_category,
                source,
                category_id,
                created_by,
                is_enabled
            ) VALUES (?, ?, NULL, ?, 'system', 1)
            """,
            (uuid.uuid4().hex, "TRANSFER_OUT_ACCOUNT_TRANSFER", rent_id),
        )
        conn.commit()

        monkeypatch.setattr(
            "finance_cli.categorizer.load_rules",
            lambda: _rules(payment_exclusions=["Check "]),
        )
        result = match_transaction(
            conn,
            "Check 1143",
            source_category="TRANSFER_OUT_ACCOUNT_TRANSFER",
            is_payment=True,
        )

    assert result is not None
    assert result.category_id == rent_id
    assert result.category_source == "category_mapping"
    assert result.is_payment is False


def test_payment_keyword_still_wins_over_exclusion(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        payments_id = _category_id(conn, "Payments & Transfers")
        monkeypatch.setattr(
            "finance_cli.categorizer.load_rules",
            lambda: _rules(payment_keywords=["PAYMENT"], payment_exclusions=["Check "]),
        )

        result = match_transaction(conn, "Check 1143 PAYMENT", is_payment=True)

    assert result is not None
    assert result.category_id == payments_id
    assert result.category_source == "keyword_rule"
    assert result.is_payment is True


def test_payment_keyword_still_wins_over_vendor_memory(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        payments_id = _category_id(conn, "Payments & Transfers")
        rent_id = _category_id(conn, "Rent")
        _seed_vendor_memory(conn, description="AUTO PYMT RENT", category_id=rent_id)

        monkeypatch.setattr(
            "finance_cli.categorizer.load_rules",
            lambda: _rules(payment_keywords=["AUTO PYMT"]),
        )
        result = match_transaction(conn, "AUTO PYMT RENT", is_payment=False)

    assert result is not None
    assert result.category_id == payments_id
    assert result.category_source == "keyword_rule"
    assert result.is_payment is True


def test_handle_categorize_clears_is_payment(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        payments_id = _category_id(conn, "Payments & Transfers")
        rent_id = _category_id(conn, "Rent")
        txn_id = _seed_transaction(conn, description="Manual recategorize", is_payment=1, category_id=payments_id)

        handle_categorize(
            SimpleNamespace(
                category="Rent",
                bulk=False,
                txn_id=txn_id,
                remember=False,
                date_from=None,
                date_to=None,
                query=None,
            ),
            conn,
        )
        row = conn.execute("SELECT category_id, is_payment FROM transactions WHERE id = ?", (txn_id,)).fetchone()

    assert row["category_id"] == rent_id
    assert row["is_payment"] == 0


def test_handle_categorize_sets_is_payment_for_payments_category(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        rent_id = _category_id(conn, "Rent")
        _category_id(conn, "Payments & Transfers")
        txn_id = _seed_transaction(conn, description="Manual payment", is_payment=0, category_id=rent_id)

        handle_categorize(
            SimpleNamespace(
                category="Payments & Transfers",
                bulk=False,
                txn_id=txn_id,
                remember=False,
                date_from=None,
                date_to=None,
                query=None,
            ),
            conn,
        )
        row = conn.execute("SELECT is_payment FROM transactions WHERE id = ?", (txn_id,)).fetchone()

    assert row["is_payment"] == 1


def test_bulk_categorize_clears_is_payment(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        payments_id = _category_id(conn, "Payments & Transfers")
        _category_id(conn, "Rent")
        _seed_transaction(conn, description="BULKCLR 1", is_payment=1, category_id=payments_id)
        _seed_transaction(conn, description="BULKCLR 2", is_payment=1, category_id=payments_id)

        result = handle_categorize(
            SimpleNamespace(
                category="Rent",
                bulk=True,
                txn_id=None,
                remember=False,
                date_from=None,
                date_to=None,
                query="BULKCLR%",
            ),
            conn,
        )
        rows = conn.execute(
            "SELECT is_payment FROM transactions WHERE description LIKE 'BULKCLR %' ORDER BY description"
        ).fetchall()

    assert result["data"]["updated"] == 2
    assert [row["is_payment"] for row in rows] == [0, 0]


def test_bulk_categorize_sets_is_payment(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        rent_id = _category_id(conn, "Rent")
        _category_id(conn, "Payments & Transfers")
        _seed_transaction(conn, description="BULKSET 1", is_payment=0, category_id=rent_id)
        _seed_transaction(conn, description="BULKSET 2", is_payment=0, category_id=rent_id)

        result = handle_categorize(
            SimpleNamespace(
                category="Payments & Transfers",
                bulk=True,
                txn_id=None,
                remember=False,
                date_from=None,
                date_to=None,
                query="BULKSET%",
            ),
            conn,
        )
        rows = conn.execute(
            "SELECT is_payment FROM transactions WHERE description LIKE 'BULKSET %' ORDER BY description"
        ).fetchall()

    assert result["data"]["updated"] == 2
    assert [row["is_payment"] for row in rows] == [1, 1]


def test_bulk_categorize_requires_filter_or_ids(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        _category_id(conn, "Rent")
        _seed_transaction(conn, description="NO FILTER", is_payment=0)

        with pytest.raises(ValueError, match="requires at least one filter"):
            handle_categorize(
                SimpleNamespace(
                    category="Rent",
                    bulk=True,
                    txn_id=None,
                    remember=False,
                    ids=None,
                    date_from=None,
                    date_to=None,
                    query=None,
                ),
                conn,
            )


def test_bulk_categorize_by_ids_updates_only_selected_transactions(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        coffee_id = _category_id(conn, "Coffee")
        rent_id = _category_id(conn, "Rent")
        first_id = _seed_transaction(conn, description="IDS BULK 1", is_payment=0, category_id=coffee_id)
        second_id = _seed_transaction(conn, description="IDS BULK 2", is_payment=0, category_id=coffee_id)
        third_id = _seed_transaction(conn, description="IDS BULK 3", is_payment=0, category_id=coffee_id)

        result = handle_categorize(
            SimpleNamespace(
                category="Rent",
                bulk=True,
                txn_id=None,
                remember=False,
                ids=f"{first_id}, {third_id}",
                date_from=None,
                date_to=None,
                query=None,
            ),
            conn,
        )

        rows = conn.execute(
            """
            SELECT id, category_id
              FROM transactions
             WHERE id IN (?, ?, ?)
             ORDER BY description
            """,
            (first_id, second_id, third_id),
        ).fetchall()

    assert result["data"]["updated"] == 2
    category_by_id = {row["id"]: row["category_id"] for row in rows}
    assert category_by_id[first_id] == rent_id
    assert category_by_id[second_id] == coffee_id
    assert category_by_id[third_id] == rent_id


def test_bulk_categorize_remember_dedupes_patterns_by_use_type(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        fee_id = _category_id(conn, "Bank Charges & Fees")
        _seed_transaction(conn, description="PLAN FEE 12345", is_payment=0, use_type="Business")
        _seed_transaction(conn, description="PLAN FEE 67890", is_payment=0, use_type="Business")
        _seed_transaction(conn, description="PLAN FEE 11111", is_payment=0, use_type="Personal")
        _seed_transaction(conn, description="PLAN FEE MONTHLY 22222", is_payment=0)

        result = handle_categorize(
            SimpleNamespace(
                category="Bank Charges & Fees",
                bulk=True,
                txn_id=None,
                remember=True,
                ids=None,
                date_from=None,
                date_to=None,
                query="PLAN FEE%",
            ),
            conn,
        )

        remembered = conn.execute(
            """
            SELECT description_pattern, use_type, category_id
              FROM vendor_memory
             ORDER BY description_pattern, use_type
            """
        ).fetchall()

    assert result["data"]["updated"] == 4
    assert result["data"]["remembered_count"] == 3
    assert [(row["description_pattern"], row["use_type"], row["category_id"]) for row in remembered] == [
        ("plan fee", "Business", fee_id),
        ("plan fee", "Personal", fee_id),
        ("plan fee monthly", "Any", fee_id),
    ]


def test_txn_categorize_cli_supports_bulk_ids(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    with connect(db_path) as conn:
        _category_id(conn, "Rent")
        first_id = _seed_transaction(conn, description="CLI IDS 1", is_payment=0)
        second_id = _seed_transaction(conn, description="CLI IDS 2", is_payment=0)

    code = main(["txn", "categorize", "--ids", f"{first_id},{second_id}", "--category", "Rent"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["updated"] == 2


def test_load_rules_parses_payment_exclusions(tmp_path: Path) -> None:
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        dedent(
            """
            payment_exclusions:
              - "Check "
              - "Wire Out"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    rules = load_rules(rules_path)
    assert rules.payment_exclusions == ["Check", "Wire Out"]


def test_match_payment_exclusion() -> None:
    rules = _rules(payment_exclusions=["Check ", "Wire Out"])

    assert match_payment_exclusion("CHECK 1143", rules) is True
    assert match_payment_exclusion("Utilities payment", rules) is False


def test_plaid_sync_honors_match_result_is_payment_false(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, plaid_item_id="item_honor_false")
        rent_id = _category_id(conn, "Rent")

        def _fake_match(*_args, **_kwargs):
            return MatchResult(
                category_id=rent_id,
                category_source="vendor_memory",
                category_confidence=1.0,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=False,
            )

        monkeypatch.setattr("finance_cli.plaid_client.match_transaction", _fake_match)

        apply_sync_updates(
            conn,
            item,
            [_plaid_added_transaction(txn_id="plaid_false_payment", account_id="acct_false", name="Check 1143")],
            [],
            [],
            _plaid_accounts(account_id="acct_false"),
            next_cursor="cursor_false",
        )

        row = conn.execute(
            "SELECT category_id, is_payment FROM transactions WHERE plaid_txn_id = 'plaid_false_payment'"
        ).fetchone()

    assert row["category_id"] == rent_id
    assert row["is_payment"] == 0


def test_csv_import_honors_match_result_is_payment_false(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    csv_path = tmp_path / "csv_payment_false.csv"
    csv_path.write_text(
        dedent(
            """
            Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
            2026-01-01,Check 1143,-100.00,1234,Test Source,Personal,Transfer Label,true
            """
        ).lstrip("\n"),
        encoding="utf-8",
    )

    with connect(db_path) as conn:
        rent_id = _category_id(conn, "Rent")

    def _fake_match(*_args, **_kwargs):
        return MatchResult(
            category_id=rent_id,
            category_source="vendor_memory",
            category_confidence=1.0,
            category_rule_id=None,
            matched_rule_id=None,
            is_payment=False,
        )

    monkeypatch.setattr("finance_cli.importers.match_transaction", _fake_match)

    with connect(db_path) as conn:
        import_csv(conn, csv_path, source_name="Test Source", dry_run=False, validate_name=False)
        row = conn.execute("SELECT is_payment FROM transactions WHERE description = 'Check 1143'").fetchone()

    assert row["is_payment"] == 0


def test_pdf_import_honors_match_result_is_payment_false(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"pdf")

    with connect(db_path) as conn:
        rent_id = _category_id(conn, "Rent")

    def _fake_match(*_args, **_kwargs):
        return MatchResult(
            category_id=rent_id,
            category_source="vendor_memory",
            category_confidence=1.0,
            category_rule_id=None,
            matched_rule_id=None,
            is_payment=False,
        )

    monkeypatch.setattr("finance_cli.importers.pdf.match_transaction", _fake_match)

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-01-01",
                "description": "PDF Check 1143",
                "amount_cents": -10000,
                "source": "Test Source",
                "is_payment": True,
            }
        ],
        extracted_total_cents=-10000,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_extracted_statement(
            conn,
            extracted=extracted,
            file_path=pdf_path,
            bank_parser="chase_credit",
            validate_name=False,
        )
        row = conn.execute("SELECT is_payment FROM transactions WHERE description = 'PDF Check 1143'").fetchone()

    assert row["is_payment"] == 0


def test_apply_match_preserves_is_payment_on_ambiguous(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        txn_id = _seed_transaction(conn, description="Ambiguous payment", is_payment=1)
        applied = apply_match(
            conn,
            txn_id,
            MatchResult(
                category_id=None,
                category_source="ambiguous",
                category_confidence=None,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=False,
            ),
        )
        row = conn.execute("SELECT is_payment FROM transactions WHERE id = ?", (txn_id,)).fetchone()

    assert applied is True
    assert row["is_payment"] == 1


def test_plaid_ingest_preserves_pfc_is_payment_on_ambiguous(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, plaid_item_id="item_ambiguous")

        def _fake_match(*_args, **_kwargs):
            return MatchResult(
                category_id=None,
                category_source="ambiguous",
                category_confidence=None,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=False,
            )

        monkeypatch.setattr("finance_cli.plaid_client.match_transaction", _fake_match)

        apply_sync_updates(
            conn,
            item,
            [_plaid_added_transaction(txn_id="plaid_ambiguous", account_id="acct_ambiguous", name="Check 9988")],
            [],
            [],
            _plaid_accounts(account_id="acct_ambiguous"),
            next_cursor="cursor_ambiguous",
        )

        row = conn.execute("SELECT is_payment FROM transactions WHERE plaid_txn_id = 'plaid_ambiguous'").fetchone()

    assert row["is_payment"] == 1


def test_vendor_memory_payments_category_sets_is_payment_true(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        payments_id = _category_id(conn, "Payments & Transfers")
        _seed_vendor_memory(conn, description="Internal Transfer", category_id=payments_id)

        monkeypatch.setattr("finance_cli.categorizer.load_rules", lambda: _rules())
        result = match_transaction(conn, "Internal Transfer", is_payment=False)

    assert result is not None
    assert result.category_id == payments_id
    assert result.category_source == "vendor_memory"
    assert result.is_payment is True


def test_keyword_rule_payments_category_sets_is_payment_true(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        payments_id = _category_id(conn, "Payments & Transfers")
        monkeypatch.setattr(
            "finance_cli.categorizer.load_rules",
            lambda: _rules(
                keyword_rules=[
                    KeywordRule(
                        keywords=["MEMBER TRANSFER"],
                        category="Payments & Transfers",
                        use_type=None,
                        priority=0,
                        rule_index=0,
                    )
                ]
            ),
        )

        result = match_transaction(conn, "MEMBER TRANSFER TO SAVINGS", is_payment=False)

    assert result is not None
    assert result.category_id == payments_id
    assert result.category_source == "keyword_rule"
    assert result.is_payment is True


def test_payment_exclusion_no_category_match_returns_not_payment(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        monkeypatch.setattr(
            "finance_cli.categorizer.load_rules",
            lambda: _rules(payment_exclusions=["Check "]),
        )
        result = match_transaction(conn, "Check 1143", is_payment=True)

    assert result is not None
    assert result.category_id is None
    assert result.category_source is None
    assert result.is_payment is False


def test_plaid_ingest_payment_exclusion_no_match_stores_is_payment_0(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, plaid_item_id="item_exclusion_none")

        def _fake_match(*_args, **_kwargs):
            return MatchResult(
                category_id=None,
                category_source=None,
                category_confidence=None,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=False,
            )

        monkeypatch.setattr("finance_cli.plaid_client.match_transaction", _fake_match)

        apply_sync_updates(
            conn,
            item,
            [_plaid_added_transaction(txn_id="plaid_exclusion_none", account_id="acct_exclusion", name="Check 1143")],
            [],
            [],
            _plaid_accounts(account_id="acct_exclusion"),
            next_cursor="cursor_exclusion_none",
        )

        row = conn.execute(
            "SELECT is_payment FROM transactions WHERE plaid_txn_id = 'plaid_exclusion_none'"
        ).fetchone()

    assert row["is_payment"] == 0
