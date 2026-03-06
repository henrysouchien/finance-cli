from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.plaid_client import (
    PlaidConfigStatus,
    PlaidSyncError,
    _INVESTMENT_SUBTYPE_MAP,
    _apr_percentage,
    _apply_investment_transaction,
    _get_access_token_for_item,
    _extract_products_from_item_payload,
    _fetch_investment_transactions,
    _investment_description,
    _item_within_cooldown,
    _selective_raw_investment_json,
    _sync_investment_transactions,
    _touch_item_cooldown,
    apply_sync_updates,
    collect_transactions_sync_pages,
    complete_link_session,
    create_hosted_link_session,
    delete_secret,
    fetch_liabilities,
    refresh_balances,
    resolve_requested_products,
    run_sync,
    secret_name_candidates,
    secret_name_candidates_for_item,
    unlink_item,
)


class _ApiLikeError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.body = json.dumps({"error_code": code, "error_message": message})


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


def _seed_plaid_item(
    conn,
    plaid_item_id: str = "item_abc",
    consented_products: str = '["transactions"]',
    status: str = "active",
):
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
        ) VALUES (?, ?, 'Test Bank', 'secret/ref', ?, ?, NULL)
        """,
        (uuid.uuid4().hex, plaid_item_id, status, consented_products),
    )
    conn.commit()
    return conn.execute("SELECT * FROM plaid_items WHERE plaid_item_id = ?", (plaid_item_id,)).fetchone()


def _set_item_timestamp(conn, plaid_item_id: str, column: str, offset_expr: str) -> None:
    conn.execute(
        f"UPDATE plaid_items SET {column} = datetime('now', ?) WHERE plaid_item_id = ?",
        (offset_expr, plaid_item_id),
    )
    conn.commit()


def _mock_plaid_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        "finance_cli.plaid_client.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )


def _mock_access_token(monkeypatch) -> None:
    monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")


def test_collect_sync_pages_restarts_on_mutation_error() -> None:
    calls: list[str | None] = []
    state = {"n": 0}

    def fetch(cursor):
        calls.append(cursor)
        state["n"] += 1
        if state["n"] == 1:
            return {
                "added": [{"transaction_id": "tx1"}],
                "modified": [],
                "removed": [],
                "accounts": [],
                "next_cursor": "c1",
                "has_more": True,
            }
        if state["n"] == 2:
            raise _ApiLikeError("TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION", "mutation")
        if state["n"] == 3:
            return {
                "added": [{"transaction_id": "tx1"}],
                "modified": [],
                "removed": [],
                "accounts": [],
                "next_cursor": "c1",
                "has_more": True,
            }
        return {
            "added": [{"transaction_id": "tx2"}],
            "modified": [],
            "removed": [],
            "accounts": [],
            "next_cursor": "c2",
            "has_more": False,
        }

    out = collect_transactions_sync_pages(fetch, starting_cursor=None)
    assert [row["transaction_id"] for row in out["added"]] == ["tx1", "tx2"]
    assert out["next_cursor"] == "c2"
    assert calls == [None, "c1", None, "c1"]


def test_resolve_requested_products_includes_transactions_and_dedupes() -> None:
    products = resolve_requested_products(
        requested_products=["liabilities", "transactions", "liabilities"],
        include_balance=True,
    )
    assert products == ["transactions", "liabilities"]


def test_resolve_requested_products_treats_balance_as_implicit() -> None:
    products = resolve_requested_products(
        requested_products=["balance"],
        include_balance=True,
    )
    assert products == ["transactions"]


def test_resolve_requested_products_investments() -> None:
    products = resolve_requested_products(requested_products=["investments"])
    assert products == ["transactions", "investments"]


def test_resolve_requested_products_rejects_unknown_products() -> None:
    with pytest.raises(PlaidSyncError):
        resolve_requested_products(requested_products=["unknown_product"])


def test_create_link_session_update_mode_uses_additional_consented_products(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    captured: dict[str, dict[str, object]] = {}

    class _Client:
        def link_token_create(self, request):
            captured["request"] = request.to_dict()
            return _Resp(
                {
                    "link_token": "link-token-1",
                    "hosted_link_url": "https://plaid.test/link",
                    "expiration": "2030-01-01T00:00:00Z",
                }
            )

    monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr(
        "finance_cli.plaid_client._get_access_token_for_item",
        lambda item, region_name=None: "access-token-1",
    )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_update_1")
        session = create_hosted_link_session(
            conn,
            user_id="user-1",
            update_item_id="item_update_1",
            requested_products=["investments"],
        )

    assert session["requested_products"] == ["transactions", "investments"]
    request_payload = captured["request"]
    assert request_payload["products"] == ["transactions"]
    assert request_payload["additional_consented_products"] == ["investments"]


def test_extract_products_prefers_billed_products_from_item_payload() -> None:
    payload = {
        "item": {
            "available_products": ["auth", "transactions", "liabilities"],
            "billed_products": ["transactions", "liabilities", "transactions"],
        }
    }
    assert _extract_products_from_item_payload(payload) == ["transactions", "liabilities"]


def test_apply_sync_updates_add_modify_remove(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)

        added = [
            {
                "transaction_id": "plaid_txn_1",
                "account_id": "plaid_acct_1",
                "date": "2025-02-10",
                "amount": 12.34,
                "name": "STARBUCKS STORE",
                "merchant_name": "Starbucks",
                "payment_channel": "in store",
                "pending": False,
                "personal_finance_category": {
                    "primary": "FOOD_AND_DRINK",
                    "detailed": "FOOD_AND_DRINK_COFFEE",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        modified = [
            {
                "transaction_id": "plaid_txn_1",
                "account_id": "plaid_acct_1",
                "date": "2025-02-10",
                "amount": 20.00,
                "name": "STARBUCKS STORE 2",
                "merchant_name": "Starbucks",
                "payment_channel": "in store",
                "pending": False,
                "personal_finance_category": {
                    "primary": "FOOD_AND_DRINK",
                    "detailed": "FOOD_AND_DRINK_COFFEE",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        removed = [{"transaction_id": "plaid_txn_1"}]
        accounts = [
            {
                "account_id": "plaid_acct_1",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            }
        ]

        counts_add = apply_sync_updates(conn, item, added, [], [], accounts, next_cursor="cursor_1")
        conn.commit()
        assert counts_add["added"] == 1

        txn = conn.execute("SELECT * FROM transactions WHERE plaid_txn_id = 'plaid_txn_1'").fetchone()
        assert txn is not None
        assert txn["amount_cents"] == -1234
        assert txn["source"] == "plaid"
        assert txn["category_source"] == "plaid"
        assert txn["category_confidence"] == pytest.approx(0.3)

        counts_mod = apply_sync_updates(conn, item, [], modified, [], accounts, next_cursor="cursor_2")
        conn.commit()
        assert counts_mod["modified"] >= 1

        txn2 = conn.execute("SELECT * FROM transactions WHERE plaid_txn_id = 'plaid_txn_1'").fetchone()
        assert txn2["amount_cents"] == -2000

        counts_rm = apply_sync_updates(conn, item, [], [], removed, accounts, next_cursor="cursor_3")
        conn.commit()
        assert counts_rm["removed"] == 1

        txn3 = conn.execute("SELECT * FROM transactions WHERE plaid_txn_id = 'plaid_txn_1'").fetchone()
        assert txn3["is_active"] == 0

        item_row = conn.execute("SELECT sync_cursor FROM plaid_items WHERE plaid_item_id = 'item_abc'").fetchone()
        assert item_row["sync_cursor"] == "cursor_3"


def test_apply_sync_updates_prefers_keyword_pipeline_over_pfc(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)
        software_category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Software & Subscriptions', 1)",
            (software_category_id,),
        )
        conn.commit()

        added = [
            {
                "transaction_id": "plaid_txn_keyword",
                "account_id": "plaid_acct_keyword",
                "date": "2025-02-10",
                "amount": 42.00,
                "name": "OPENAI API",
                "merchant_name": "OpenAI",
                "payment_channel": "online",
                "pending": False,
                "personal_finance_category": {
                    "primary": "FOOD_AND_DRINK",
                    "detailed": "FOOD_AND_DRINK_RESTAURANT",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        accounts = [
            {
                "account_id": "plaid_acct_keyword",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            }
        ]

        counts = apply_sync_updates(conn, item, added, [], [], accounts, next_cursor="cursor_keyword")
        conn.commit()
        assert counts["added"] == 1

        txn = conn.execute(
            """
            SELECT category_id, category_source, category_confidence, source_category
              FROM transactions
             WHERE plaid_txn_id = 'plaid_txn_keyword'
            """
        ).fetchone()
        assert txn is not None
        assert txn["category_id"] == software_category_id
        assert txn["category_source"] == "keyword_rule"
        assert txn["category_confidence"] == pytest.approx(0.9)
        assert txn["source_category"] == "FOOD_AND_DRINK_RESTAURANT"


def test_investment_dividend_transfer_in_categorized_as_income(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)

        added = [
            {
                "transaction_id": "plaid_txn_investment_dividend",
                "account_id": "plaid_acct_investment_dividend",
                "date": "2025-02-10",
                "amount": -125.00,
                "name": "BLACKROCK DIVIDEND PAY DATE",
                "merchant_name": None,
                "payment_channel": "other",
                "pending": False,
                "personal_finance_category": {
                    "primary": "TRANSFER_IN",
                    "detailed": "TRANSFER_IN_CASH_ADVANCES_AND_LOANS",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        accounts = [
            {
                "account_id": "plaid_acct_investment_dividend",
                "name": "Brokerage",
                "type": "investment",
                "subtype": "brokerage",
                "mask": "1234",
            }
        ]

        counts = apply_sync_updates(conn, item, added, [], [], accounts, next_cursor="cursor_investment_dividend")
        conn.commit()
        assert counts["added"] == 1

        txn = conn.execute(
            """
            SELECT c.name AS category_name, t.is_payment
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE t.plaid_txn_id = 'plaid_txn_investment_dividend'
            """
        ).fetchone()
        assert txn is not None
        assert txn["category_name"] == "Income: Other"
        assert txn["is_payment"] == 0


def test_investment_reinvestment_stays_as_payment(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)
        conn.execute(
            "INSERT OR IGNORE INTO categories (id, name, is_system) VALUES (?, 'Payments & Transfers', 1)",
            (uuid.uuid4().hex,),
        )
        conn.commit()

        added = [
            {
                "transaction_id": "plaid_txn_investment_reinvestment",
                "account_id": "plaid_acct_investment_reinvestment",
                "date": "2025-02-10",
                "amount": -125.00,
                "name": "PRINCIPAL REINVESTMENT",
                "merchant_name": None,
                "payment_channel": "other",
                "pending": False,
                "personal_finance_category": {
                    "primary": "TRANSFER_IN",
                    "detailed": "TRANSFER_IN_INVESTMENT_AND_RETIREMENT_FUNDS",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        accounts = [
            {
                "account_id": "plaid_acct_investment_reinvestment",
                "name": "Brokerage",
                "type": "investment",
                "subtype": "brokerage",
                "mask": "1234",
            }
        ]

        counts = apply_sync_updates(conn, item, added, [], [], accounts, next_cursor="cursor_investment_reinvestment")
        conn.commit()
        assert counts["added"] == 1

        txn = conn.execute(
            """
            SELECT c.name AS category_name, t.is_payment
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE t.plaid_txn_id = 'plaid_txn_investment_reinvestment'
            """
        ).fetchone()
        assert txn is not None
        assert txn["category_name"] == "Payments & Transfers"
        assert txn["is_payment"] == 1


def test_non_investment_transfer_in_stays_as_payment(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)

        added = [
            {
                "transaction_id": "plaid_txn_non_investment_transfer_in",
                "account_id": "plaid_acct_non_investment_transfer_in",
                "date": "2025-02-10",
                "amount": -125.00,
                "name": "TRANSFER IN TEST",
                "merchant_name": None,
                "payment_channel": "other",
                "pending": False,
                "personal_finance_category": {
                    "primary": "TRANSFER_IN",
                    "detailed": "TRANSFER_IN_CASH_ADVANCES_AND_LOANS",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        accounts = [
            {
                "account_id": "plaid_acct_non_investment_transfer_in",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            }
        ]

        counts = apply_sync_updates(conn, item, added, [], [], accounts, next_cursor="cursor_non_investment_transfer_in")
        conn.commit()
        assert counts["added"] == 1

        txn = conn.execute(
            """
            SELECT is_payment
              FROM transactions
             WHERE plaid_txn_id = 'plaid_txn_non_investment_transfer_in'
            """
        ).fetchone()
        assert txn is not None
        assert txn["is_payment"] == 1


def test_investment_dividend_account_map_miss_falls_back_safely(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)

        added = [
            {
                "transaction_id": "plaid_txn_investment_dividend_account_map_miss",
                "account_id": "plaid_acct_investment_dividend_account_map_miss",
                "date": "2025-02-10",
                "amount": -125.00,
                "name": "BLACKROCK DIVIDEND PAY DATE",
                "merchant_name": None,
                "payment_channel": "other",
                "pending": False,
                "personal_finance_category": {
                    "primary": "TRANSFER_IN",
                    "detailed": "TRANSFER_IN_CASH_ADVANCES_AND_LOANS",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]

        counts = apply_sync_updates(
            conn,
            item,
            added,
            [],
            [],
            [],
            next_cursor="cursor_investment_dividend_account_map_miss",
        )
        conn.commit()
        assert counts["added"] == 1

        txn = conn.execute(
            """
            SELECT is_payment
              FROM transactions
             WHERE plaid_txn_id = 'plaid_txn_investment_dividend_account_map_miss'
            """
        ).fetchone()
        assert txn is not None
        assert txn["is_payment"] == 1


def test_investment_dividend_modify_updates_is_payment(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)
        conn.execute(
            "INSERT OR IGNORE INTO categories (id, name, is_system) VALUES (?, 'Payments & Transfers', 1)",
            (uuid.uuid4().hex,),
        )
        conn.commit()

        added = [
            {
                "transaction_id": "plaid_txn_investment_dividend_modify",
                "account_id": "plaid_acct_investment_dividend_modify",
                "date": "2025-02-10",
                "amount": -125.00,
                "name": "BLACKROCK DIVIDEND PAY DATE",
                "merchant_name": None,
                "payment_channel": "other",
                "pending": False,
                "personal_finance_category": {
                    "primary": "TRANSFER_IN",
                    "detailed": "TRANSFER_IN_CASH_ADVANCES_AND_LOANS",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        modified = [
            {
                "transaction_id": "plaid_txn_investment_dividend_modify",
                "account_id": "plaid_acct_investment_dividend_modify",
                "date": "2025-02-11",
                "amount": -125.00,
                "name": "BLACKROCK DIVIDEND PAY DATE UPDATED",
                "merchant_name": None,
                "payment_channel": "other",
                "pending": False,
                "personal_finance_category": {
                    "primary": "TRANSFER_IN",
                    "detailed": "TRANSFER_IN_CASH_ADVANCES_AND_LOANS",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        checking_accounts = [
            {
                "account_id": "plaid_acct_investment_dividend_modify",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            }
        ]
        investment_accounts = [
            {
                "account_id": "plaid_acct_investment_dividend_modify",
                "name": "Brokerage",
                "type": "investment",
                "subtype": "brokerage",
                "mask": "1234",
            }
        ]

        counts_add = apply_sync_updates(conn, item, added, [], [], checking_accounts, next_cursor="cursor_before_modify")
        conn.commit()
        assert counts_add["added"] == 1

        before = conn.execute(
            """
            SELECT c.name AS category_name, t.category_source, t.is_payment
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE t.plaid_txn_id = 'plaid_txn_investment_dividend_modify'
            """
        ).fetchone()
        assert before is not None
        assert before["category_name"] == "Payments & Transfers"
        assert before["category_source"] == "keyword_rule"
        assert before["is_payment"] == 1

        counts_mod = apply_sync_updates(
            conn,
            item,
            [],
            modified,
            [],
            investment_accounts,
            next_cursor="cursor_after_modify",
        )
        conn.commit()
        assert counts_mod["modified"] >= 1

        after = conn.execute(
            """
            SELECT c.name AS category_name, t.category_source, t.is_payment
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE t.plaid_txn_id = 'plaid_txn_investment_dividend_modify'
            """
        ).fetchone()
        assert after is not None
        assert after["category_name"] == "Payments & Transfers"
        assert after["category_source"] == "keyword_rule"
        assert after["is_payment"] == 0


def test_investment_subtype_map_values() -> None:
    assert _INVESTMENT_SUBTYPE_MAP
    for subtype, entry in _INVESTMENT_SUBTYPE_MAP.items():
        assert isinstance(subtype, str)
        assert isinstance(entry, tuple)
        assert len(entry) == 2
        category_name, is_payment = entry
        assert isinstance(category_name, str)
        assert category_name.strip()
        assert isinstance(is_payment, bool)


def test_investment_description_with_ticker() -> None:
    inv_txn = {
        "type": "cash",
        "subtype": "dividend",
        "security_id": "sec_1",
        "name": "Apple Inc Dividend Payment",
    }
    securities_map = {"sec_1": {"ticker_symbol": "AAPL", "name": "Apple Inc"}}

    description = _investment_description(inv_txn, securities_map)
    assert description == "DIVIDEND - AAPL - Apple Inc Dividend Payment"


def test_investment_description_no_security() -> None:
    inv_txn = {
        "type": "fee",
        "subtype": "account fee",
        "name": "Monthly advisory fee",
    }
    description = _investment_description(inv_txn, {})
    assert description == "ACCOUNT FEE - Monthly advisory fee"


def test_apply_investment_transaction_buy(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, consented_products='["transactions", "investments"]')
        status = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_buy_1",
                "account_id": "inv_acct_1",
                "date": "2025-03-01",
                "amount": 10.00,
                "type": "buy",
                "subtype": "buy",
                "security_id": "sec_1",
                "name": "Buy Apple",
            },
            securities_map={"sec_1": {"security_id": "sec_1", "ticker_symbol": "AAPL", "name": "Apple Inc"}},
            account_map={
                "inv_acct_1": {
                    "account_id": "inv_acct_1",
                    "name": "Brokerage",
                    "type": "investment",
                    "subtype": "brokerage",
                }
            },
            local_account_ids={},
            consumed_crossfeed_ids=set(),
        )
        conn.commit()
        assert status == "added"

        row = conn.execute(
            """
            SELECT t.amount_cents, t.is_payment, t.source_category, c.name AS category_name
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE t.plaid_txn_id = 'inv_buy_1'
            """
        ).fetchone()
        assert row is not None
        assert row["amount_cents"] == -1000
        assert row["category_name"] == "Payments & Transfers"
        assert row["is_payment"] == 1
        assert row["source_category"] == "investment:buy:buy"


def test_apply_investment_transaction_dividend(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, consented_products='["transactions", "investments"]')
        status = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_div_1",
                "account_id": "inv_acct_div",
                "date": "2025-03-02",
                "amount": -5.00,
                "type": "cash",
                "subtype": "dividend",
                "security_id": "sec_div_1",
                "name": "Dividend Payment",
            },
            securities_map={"sec_div_1": {"security_id": "sec_div_1", "ticker_symbol": "VOO", "name": "Vanguard S&P 500 ETF"}},
            account_map={
                "inv_acct_div": {
                    "account_id": "inv_acct_div",
                    "name": "Brokerage",
                    "type": "investment",
                    "subtype": "brokerage",
                }
            },
            local_account_ids={},
            consumed_crossfeed_ids=set(),
        )
        conn.commit()
        assert status == "added"

        row = conn.execute(
            """
            SELECT t.amount_cents, t.is_payment, c.name AS category_name
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE t.plaid_txn_id = 'inv_div_1'
            """
        ).fetchone()
        assert row is not None
        assert row["amount_cents"] == 500
        assert row["category_name"] == "Income: Other"
        assert row["is_payment"] == 0


def test_apply_investment_transaction_update_preserves_user_category(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, consented_products='["transactions", "investments"]')
        account_map = {
            "inv_acct_user": {
                "account_id": "inv_acct_user",
                "name": "Brokerage",
                "type": "investment",
                "subtype": "brokerage",
            }
        }
        securities_map = {"sec_user_1": {"security_id": "sec_user_1", "ticker_symbol": "MSFT", "name": "Microsoft Corp"}}
        local_account_ids: dict[str, str | None] = {}

        first_status = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_user_1",
                "account_id": "inv_acct_user",
                "date": "2025-03-03",
                "amount": 7.50,
                "type": "buy",
                "subtype": "buy",
                "security_id": "sec_user_1",
                "name": "Initial Buy",
            },
            securities_map,
            account_map,
            local_account_ids,
            consumed_crossfeed_ids=set(),
        )
        assert first_status == "added"

        custom_category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'User Custom Investment', 0)",
            (custom_category_id,),
        )
        conn.execute(
            """
            UPDATE transactions
               SET category_id = ?,
                   category_source = 'user',
                   category_confidence = 1.0
             WHERE plaid_txn_id = 'inv_user_1'
            """,
            (custom_category_id,),
        )

        second_status = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_user_1",
                "account_id": "inv_acct_user",
                "date": "2025-03-04",
                "amount": 9.00,
                "type": "buy",
                "subtype": "buy",
                "security_id": "sec_user_1",
                "name": "Updated Buy",
            },
            securities_map,
            account_map,
            local_account_ids,
            consumed_crossfeed_ids=set(),
        )
        conn.commit()
        assert second_status == "modified"

        row = conn.execute(
            """
            SELECT category_id, category_source, amount_cents
              FROM transactions
             WHERE plaid_txn_id = 'inv_user_1'
            """
        ).fetchone()
        assert row is not None
        assert row["category_id"] == custom_category_id
        assert row["category_source"] == "user"
        assert row["amount_cents"] == -900


def test_apply_investment_transaction_cross_feed_dedup(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, consented_products='["transactions", "investments"]')
        accounts = [
            {
                "account_id": "cross_acct_1",
                "name": "Brokerage",
                "type": "investment",
                "subtype": "brokerage",
            }
        ]
        apply_sync_updates(
            conn,
            item,
            [
                {
                    "transaction_id": "regular_cross_1",
                    "account_id": "cross_acct_1",
                    "date": "2025-03-05",
                    "amount": 10.00,
                    "name": "Regular feed txn",
                    "merchant_name": "Regular",
                    "payment_channel": "other",
                    "pending": False,
                    "personal_finance_category": {
                        "primary": "FOOD_AND_DRINK",
                        "detailed": "FOOD_AND_DRINK_RESTAURANT",
                        "confidence_level": "HIGH",
                        "version": "v2",
                    },
                }
            ],
            [],
            [],
            accounts,
            next_cursor="cursor_cross_1",
        )
        status = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_cross_1",
                "account_id": "cross_acct_1",
                "date": "2025-03-05",
                "amount": 10.00,
                "type": "cash",
                "subtype": "deposit",
                "name": "Investment cash movement",
            },
            securities_map={},
            account_map={"cross_acct_1": accounts[0]},
            local_account_ids={},
            consumed_crossfeed_ids=set(),
        )
        conn.commit()
        assert status == "skipped"
        row = conn.execute(
            "SELECT id FROM transactions WHERE plaid_txn_id = 'inv_cross_1'"
        ).fetchone()
        assert row is None


def test_apply_investment_transaction_no_false_dedup(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, consented_products='["transactions", "investments"]')
        account_map = {
            "no_dup_acct_1": {
                "account_id": "no_dup_acct_1",
                "name": "Brokerage",
                "type": "investment",
                "subtype": "brokerage",
            }
        }
        consumed_crossfeed_ids: set[str] = set()
        first = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_no_dup_1",
                "account_id": "no_dup_acct_1",
                "date": "2025-03-06",
                "amount": 4.00,
                "type": "cash",
                "subtype": "deposit",
                "name": "Cash movement one",
            },
            securities_map={},
            account_map=account_map,
            local_account_ids={},
            consumed_crossfeed_ids=consumed_crossfeed_ids,
        )
        second = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_no_dup_2",
                "account_id": "no_dup_acct_1",
                "date": "2025-03-06",
                "amount": 4.00,
                "type": "cash",
                "subtype": "deposit",
                "name": "Cash movement two",
            },
            securities_map={},
            account_map=account_map,
            local_account_ids={},
            consumed_crossfeed_ids=consumed_crossfeed_ids,
        )
        conn.commit()
        assert first == "added"
        assert second == "added"

        row = conn.execute(
            """
            SELECT COUNT(*) AS n
              FROM transactions
             WHERE plaid_txn_id IN ('inv_no_dup_1', 'inv_no_dup_2')
            """
        ).fetchone()
        assert row["n"] == 2


def test_apply_investment_transaction_one_to_one_dedup(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, consented_products='["transactions", "investments"]')
        account_payload = {
            "account_id": "one_to_one_acct_1",
            "name": "Brokerage",
            "type": "investment",
            "subtype": "brokerage",
        }
        apply_sync_updates(
            conn,
            item,
            [
                {
                    "transaction_id": "regular_one_to_one_1",
                    "account_id": "one_to_one_acct_1",
                    "date": "2025-03-07",
                    "amount": 8.00,
                    "name": "Regular feed tx",
                    "merchant_name": "Regular",
                    "payment_channel": "other",
                    "pending": False,
                    "personal_finance_category": {
                        "primary": "FOOD_AND_DRINK",
                        "detailed": "FOOD_AND_DRINK_RESTAURANT",
                        "confidence_level": "HIGH",
                        "version": "v2",
                    },
                }
            ],
            [],
            [],
            [account_payload],
            next_cursor="cursor_one_to_one",
        )

        consumed_crossfeed_ids: set[str] = set()
        first = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_one_to_one_1",
                "account_id": "one_to_one_acct_1",
                "date": "2025-03-07",
                "amount": 8.00,
                "type": "cash",
                "subtype": "deposit",
                "name": "Investment txn one",
            },
            securities_map={},
            account_map={"one_to_one_acct_1": account_payload},
            local_account_ids={},
            consumed_crossfeed_ids=consumed_crossfeed_ids,
        )
        second = _apply_investment_transaction(
            conn,
            item,
            {
                "investment_transaction_id": "inv_one_to_one_2",
                "account_id": "one_to_one_acct_1",
                "date": "2025-03-07",
                "amount": 8.00,
                "type": "cash",
                "subtype": "deposit",
                "name": "Investment txn two",
            },
            securities_map={},
            account_map={"one_to_one_acct_1": account_payload},
            local_account_ids={},
            consumed_crossfeed_ids=consumed_crossfeed_ids,
        )
        conn.commit()

        assert first == "skipped"
        assert second == "added"
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
              FROM transactions
             WHERE plaid_txn_id IN ('inv_one_to_one_1', 'inv_one_to_one_2')
            """
        ).fetchone()
        assert row["n"] == 1


def test_fetch_investment_transactions_pagination() -> None:
    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def __init__(self):
            self.offsets: list[int] = []

        def investments_transactions_get(self, request):
            request_payload = request.to_dict()
            offset = int(request_payload.get("options", {}).get("offset", 0))
            self.offsets.append(offset)
            if offset == 0:
                first_page = [
                    {
                        "investment_transaction_id": f"inv_page_1_{idx}",
                        "account_id": "acct_page",
                        "date": "2025-02-01",
                        "amount": float(idx + 1),
                        "type": "cash",
                        "subtype": "deposit",
                    }
                    for idx in range(100)
                ]
                return _Resp(
                    {
                        "investment_transactions": first_page,
                        "securities": [{"security_id": "sec_1", "ticker_symbol": "AAPL"}],
                        "accounts": [{"account_id": "acct_1", "name": "Brokerage One"}],
                        "total_investment_transactions": 101,
                    }
                )
            return _Resp(
                {
                    "investment_transactions": [
                        {
                            "investment_transaction_id": "inv_page_2_1",
                            "account_id": "acct_page",
                            "date": "2025-02-02",
                            "amount": 1.0,
                            "type": "cash",
                            "subtype": "deposit",
                        }
                    ],
                    "securities": [{"security_id": "sec_2", "ticker_symbol": "MSFT"}],
                    "accounts": [{"account_id": "acct_2", "name": "Brokerage Two"}],
                    "total_investment_transactions": 101,
                }
            )

    client = _Client()
    out = _fetch_investment_transactions(
        client,
        "access-token",
        date(2025, 2, 1),
        date(2025, 2, 28),
    )

    assert client.offsets == [0, 100]
    assert out["total"] == 101
    assert len(out["investment_transactions"]) == 101
    assert set(out["securities"].keys()) == {"sec_1", "sec_2"}
    assert out["securities"]["sec_2"]["ticker_symbol"] == "MSFT"
    assert set(out["accounts"].keys()) == {"acct_1", "acct_2"}


def test_selective_raw_investment_json() -> None:
    raw = _selective_raw_investment_json(
        {
            "investment_transaction_id": "inv_raw_1",
            "type": "buy",
            "subtype": "buy",
            "quantity": 3.5,
            "price": 250.1,
            "fees": 1.25,
            "security_id": "sec_raw_1",
        },
        {
            "ticker_symbol": "NVDA",
            "name": "NVIDIA Corporation",
            "type": "equity",
            "close_price": 1020.22,
            "cusip": "67066G104",
        },
    )
    payload = json.loads(raw)
    assert payload["type"] == "buy"
    assert payload["subtype"] == "buy"
    assert payload["quantity"] == 3.5
    assert payload["price"] == 250.1
    assert payload["fees"] == 1.25
    assert payload["security"]["ticker_symbol"] == "NVDA"


def test_sync_investment_transactions_skips_without_product(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, plaid_item_id="item_no_investments", consented_products='["transactions"]')
        plaid_item_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(plaid_items)").fetchall()
        }

        out = _sync_investment_transactions(
            conn,
            client=object(),
            item=item,
            plaid_item_columns=plaid_item_columns,
            force_refresh=False,
            region_name=None,
        )
        assert out["status"] == "skipped_no_product"
        assert out["added"] == 0
        assert out["modified"] == 0
        assert out["skipped"] == 0


def test_sync_investment_transactions_failure_nonfatal(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_investment_failure", consented_products='["transactions", "investments"]')
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr(
            "finance_cli.plaid_client._fetch_sync_page",
            lambda client, access_token, cursor, days_requested: {
                "added": [
                    {
                        "transaction_id": "sync_regular_1",
                        "account_id": "sync_regular_acct_1",
                        "date": "2025-03-10",
                        "amount": 12.00,
                        "name": "Regular Sync Transaction",
                        "merchant_name": "Regular",
                        "payment_channel": "other",
                        "pending": False,
                        "personal_finance_category": {
                            "primary": "FOOD_AND_DRINK",
                            "detailed": "FOOD_AND_DRINK_RESTAURANT",
                            "confidence_level": "HIGH",
                            "version": "v2",
                        },
                    }
                ],
                "modified": [],
                "removed": [],
                "accounts": [
                    {
                        "account_id": "sync_regular_acct_1",
                        "name": "Checking",
                        "type": "depository",
                        "subtype": "checking",
                    }
                ],
                "next_cursor": "cursor_regular_1",
                "has_more": False,
            },
        )
        monkeypatch.setattr(
            "finance_cli.plaid_client._sync_investment_transactions",
            lambda conn, client, item, plaid_item_columns, force_refresh, region_name: (_ for _ in ()).throw(
                RuntimeError("investment endpoint failed")
            ),
        )

        out = run_sync(conn, item_id="item_investment_failure")
        assert out["items_synced"] == 1
        assert out["items_failed"] == 0
        assert out["added"] == 1

        item_out = out["items"][0]
        assert item_out["status"] == "synced"
        assert "investment_error" in item_out
        assert "investment endpoint failed" in item_out["investment_error"]

        txn = conn.execute(
            "SELECT id FROM transactions WHERE plaid_txn_id = 'sync_regular_1'"
        ).fetchone()
        assert txn is not None


def test_apply_sync_updates_sets_payment_flag_from_keyword_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)
        payments_category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Payments & Transfers', 1)",
            (payments_category_id,),
        )
        conn.commit()

        added = [
            {
                "transaction_id": "plaid_txn_payment_keyword",
                "account_id": "plaid_acct_payment",
                "date": "2025-02-10",
                "amount": 300.00,
                "name": "BANK OF AMERICA CREDIT CARD BILL PAYMENT",
                "merchant_name": None,
                "payment_channel": "online",
                "pending": False,
                "personal_finance_category": {
                    "primary": "FOOD_AND_DRINK",
                    "detailed": "FOOD_AND_DRINK_RESTAURANT",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        accounts = [
            {
                "account_id": "plaid_acct_payment",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            }
        ]

        counts = apply_sync_updates(conn, item, added, [], [], accounts, next_cursor="cursor_payment_keyword")
        conn.commit()
        assert counts["added"] == 1

        txn = conn.execute(
            """
            SELECT category_id, category_source, is_payment
              FROM transactions
             WHERE plaid_txn_id = 'plaid_txn_payment_keyword'
            """
        ).fetchone()
        assert txn is not None
        assert txn["category_id"] == payments_category_id
        assert txn["category_source"] == "keyword_rule"
        assert txn["is_payment"] == 1


def test_apply_sync_updates_preserves_user_category_on_modify(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)
        custom_category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Custom Category', 0)",
            (custom_category_id,),
        )
        conn.commit()

        added = [
            {
                "transaction_id": "plaid_txn_keep_user",
                "account_id": "plaid_acct_1",
                "date": "2025-02-10",
                "amount": 12.34,
                "name": "STARBUCKS STORE",
                "merchant_name": "Starbucks",
                "payment_channel": "in store",
                "pending": False,
                "personal_finance_category": {
                    "primary": "FOOD_AND_DRINK",
                    "detailed": "FOOD_AND_DRINK_COFFEE",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        modified = [
            {
                "transaction_id": "plaid_txn_keep_user",
                "account_id": "plaid_acct_1",
                "date": "2025-02-10",
                "amount": 14.00,
                "name": "STARBUCKS STORE UPDATED",
                "merchant_name": "Starbucks",
                "payment_channel": "in store",
                "pending": False,
                "personal_finance_category": {
                    "primary": "TRANSPORTATION",
                    "detailed": "TRANSPORTATION_TAXIS_AND_RIDE_SHARES",
                    "confidence_level": "HIGH",
                    "version": "v2",
                },
            }
        ]
        accounts = [
            {
                "account_id": "plaid_acct_1",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            }
        ]

        apply_sync_updates(conn, item, added, [], [], accounts, next_cursor="cursor_1")
        conn.execute(
            """
            UPDATE transactions
               SET category_id = ?,
                   category_source = 'user',
                   category_confidence = 1.0
             WHERE plaid_txn_id = 'plaid_txn_keep_user'
            """,
            (custom_category_id,),
        )
        conn.commit()

        apply_sync_updates(conn, item, [], modified, [], accounts, next_cursor="cursor_2")
        conn.commit()

        txn = conn.execute(
            """
            SELECT category_id, category_source, category_confidence, amount_cents
              FROM transactions
             WHERE plaid_txn_id = 'plaid_txn_keep_user'
            """
        ).fetchone()
        assert txn["category_id"] == custom_category_id
        assert txn["category_source"] == "user"
        assert txn["category_confidence"] == 1.0
        assert txn["amount_cents"] == -1400


def test_apply_sync_updates_captures_balances_and_updates_same_day_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)
        accounts = [
            {
                "account_id": "acct_bal_1",
                "name": "Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "4444",
                "balances": {
                    "current": 100.25,
                    "available": 96.10,
                    "limit": None,
                    "iso_currency_code": "USD",
                },
            }
        ]

        apply_sync_updates(conn, item, [], [], [], accounts, next_cursor="cursor_1")
        conn.commit()

        apply_sync_updates(
            conn,
            item,
            [],
            [],
            [],
            [
                {
                    **accounts[0],
                    "balances": {
                        "current": 120.50,
                        "available": 111.00,
                        "limit": None,
                        "iso_currency_code": "USD",
                    },
                }
            ],
            next_cursor="cursor_2",
        )
        conn.commit()

        account = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, iso_currency_code, balance_updated_at
              FROM accounts
             WHERE plaid_account_id = 'acct_bal_1'
            """
        ).fetchone()
        assert account["balance_current_cents"] == 12050
        assert account["balance_available_cents"] == 11100
        assert account["iso_currency_code"] == "USD"
        assert account["balance_updated_at"] is not None

        snapshots = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents
              FROM balance_snapshots
             WHERE source = 'sync'
            """
        ).fetchall()
        assert len(snapshots) == 1
        assert snapshots[0]["balance_current_cents"] == 12050
        assert snapshots[0]["balance_available_cents"] == 11100


def test_apply_sync_updates_clears_stale_balances_when_plaid_returns_null(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn)
        apply_sync_updates(
            conn,
            item,
            [],
            [],
            [],
            [
                {
                    "account_id": "acct_clear_1",
                    "name": "Checking",
                    "type": "depository",
                    "subtype": "checking",
                    "balances": {
                        "current": 80.12,
                        "available": 79.00,
                        "limit": None,
                        "iso_currency_code": "USD",
                    },
                }
            ],
            next_cursor="cursor_1",
        )
        conn.commit()

        apply_sync_updates(
            conn,
            item,
            [],
            [],
            [],
            [
                {
                    "account_id": "acct_clear_1",
                    "name": "Checking",
                    "type": "depository",
                    "subtype": "checking",
                    "balances": {
                        "current": None,
                        "available": None,
                        "limit": None,
                        "iso_currency_code": None,
                        "unofficial_currency_code": None,
                    },
                }
            ],
            next_cursor="cursor_2",
        )
        conn.commit()

        row = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, iso_currency_code
              FROM accounts
             WHERE plaid_account_id = 'acct_clear_1'
            """
        ).fetchone()
        assert row["balance_current_cents"] is None
        assert row["balance_available_cents"] is None
        assert row["iso_currency_code"] is None


def test_run_sync_uses_pagination_and_updates_db(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync")

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        state = {"n": 0}

        def fake_fetch_page(client, access_token, cursor, days_requested):
            state["n"] += 1
            if state["n"] == 1:
                return {
                    "added": [
                        {
                            "transaction_id": "sync_txn_1",
                            "account_id": "sync_acct_1",
                            "date": "2025-03-01",
                            "amount": 40.00,
                            "name": "UBER TRIP",
                            "merchant_name": "Uber",
                            "payment_channel": "online",
                            "pending": False,
                            "personal_finance_category": {
                                "primary": "TRANSPORTATION",
                                "detailed": "TRANSPORTATION_TAXIS_AND_RIDE_SHARES",
                                "confidence_level": "HIGH",
                                "version": "v2",
                            },
                        }
                    ],
                    "modified": [],
                    "removed": [],
                    "accounts": [
                        {
                            "account_id": "sync_acct_1",
                            "name": "Main Checking",
                            "type": "depository",
                            "subtype": "checking",
                            "mask": "9999",
                        }
                    ],
                    "next_cursor": "cursor_a",
                    "has_more": True,
                }
            return {
                "added": [],
                "modified": [],
                "removed": [],
                "accounts": [],
                "next_cursor": "cursor_b",
                "has_more": False,
            }

        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", fake_fetch_page)

        result = run_sync(conn, days=730, item_id="item_sync")
        assert result["items_synced"] == 1
        assert result["added"] == 1
        assert result["removed"] == 0
        assert result["items"][0]["elapsed_ms"] >= 0
        assert result["total_elapsed_ms"] == result["items"][0]["elapsed_ms"]

        txn = conn.execute("SELECT * FROM transactions WHERE plaid_txn_id = 'sync_txn_1'").fetchone()
        assert txn is not None
        assert txn["amount_cents"] == -4000

        cursor_row = conn.execute("SELECT sync_cursor FROM plaid_items WHERE plaid_item_id = 'item_sync'").fetchone()
        assert cursor_row["sync_cursor"] == "cursor_b"


def test_run_sync_updates_dormant_accounts_from_accounts_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_dormant")

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        def fake_fetch_page(client, access_token, cursor, days_requested):
            return {
                "added": [],
                "modified": [],
                "removed": [],
                "accounts": [
                    {
                        "account_id": "dormant_acct_1",
                        "name": "Dormant Savings",
                        "type": "depository",
                        "subtype": "savings",
                        "balances": {
                            "current": 777.77,
                            "available": 777.77,
                            "iso_currency_code": "USD",
                        },
                    }
                ],
                "next_cursor": "cursor_dormant",
                "has_more": False,
            }

        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", fake_fetch_page)

        out = run_sync(conn, item_id="item_dormant")
        assert out["items_synced"] == 1
        assert out["added"] == 0

        acct = conn.execute(
            """
            SELECT balance_current_cents, account_type
              FROM accounts
             WHERE plaid_account_id = 'dormant_acct_1'
            """
        ).fetchone()
        assert acct is not None
        assert acct["balance_current_cents"] == 77777
        assert acct["account_type"] == "savings"

        snap = conn.execute(
            "SELECT COUNT(*) AS n FROM balance_snapshots WHERE source = 'sync'"
        ).fetchone()
        assert snap["n"] == 1


def test_run_sync_mutation_retry_exhaustion_marks_item_error(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_mut")

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        def always_mutation(client, access_token, cursor, days_requested):
            raise _ApiLikeError("TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION", "mutation loop")

        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", always_mutation)

        result = run_sync(conn, item_id="item_mut")
        assert result["items_failed"] == 1
        assert result["items_synced"] == 0
        assert result["items"][0]["status"] == "failed"
        assert result["items"][0]["elapsed_ms"] == 0
        assert result["total_elapsed_ms"] == 0

        item = conn.execute("SELECT status, error_code FROM plaid_items WHERE plaid_item_id = 'item_mut'").fetchone()
        assert item["status"] == "error"
        assert "MUTATION_DURING_PAGINATION" in (item["error_code"] or "")


def test_cooldown_helpers_noop_when_columns_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _apply_migrations_up_to(db_path, max_version=3)

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_legacy_cooldown")
        within, stamp = _item_within_cooldown(conn, "item_legacy_cooldown", "sync", cooldown_seconds=300)
        assert within is False
        assert stamp is None

        _touch_item_cooldown(conn, "item_legacy_cooldown", "sync")
        conn.commit()

        row = conn.execute(
            "SELECT plaid_item_id FROM plaid_items WHERE plaid_item_id = 'item_legacy_cooldown'"
        ).fetchone()
        assert row is not None


def test_run_sync_without_cooldown_columns_does_not_error(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    _apply_migrations_up_to(db_path, max_version=3)

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_legacy", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_legacy")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_legacy")
        assert out["items_synced"] == 1
        assert out["items_failed"] == 0
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_sync_cooldown_skips_fresh_item(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_1", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_fresh")
        _set_item_timestamp(conn, "item_sync_fresh", "last_sync_at", "-60 seconds")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_fresh")
        assert out["items_synced"] == 0
        assert out["items_skipped"] == 1
        assert out["items_failed"] == 0
        assert out["items"][0]["status"] == "skipped_cooldown"
        assert out["items"][0]["last_sync_at"] is not None
        assert out["items"][0]["elapsed_ms"] == 0
        assert out["total_elapsed_ms"] == 0
        assert calls["n"] == 0


def test_sync_cooldown_allows_stale_item(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_1", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_stale")
        _set_item_timestamp(conn, "item_sync_stale", "last_sync_at", "-10 minutes")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_stale")
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_sync_cooldown_allows_null_timestamp(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_1", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_null")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_null")
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_sync_force_refresh_bypasses_cooldown(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_1", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_force")
        _set_item_timestamp(conn, "item_sync_force", "last_sync_at", "-60 seconds")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_force", force_refresh=True)
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_cooldown_env_var_override(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    monkeypatch.setenv("PLAID_SYNC_COOLDOWN", "0")

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_1", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_env_zero")
        _set_item_timestamp(conn, "item_sync_env_zero", "last_sync_at", "-60 seconds")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_env_zero")
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_cooldown_env_var_invalid_fallback(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    monkeypatch.setenv("PLAID_SYNC_COOLDOWN", "abc")

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_1", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_env_invalid")
        _set_item_timestamp(conn, "item_sync_env_invalid", "last_sync_at", "-60 seconds")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_env_invalid")
        assert out["items_synced"] == 0
        assert out["items_skipped"] == 1
        assert calls["n"] == 0


def test_error_status_item_bypasses_cooldown(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    def _fetch_page(client, access_token, cursor, days_requested):
        calls["n"] += 1
        return {"added": [], "modified": [], "removed": [], "accounts": [], "next_cursor": "cursor_1", "has_more": False}

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_sync_error", status="error")
        _set_item_timestamp(conn, "item_sync_error", "last_sync_at", "-60 seconds")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr("finance_cli.plaid_client._fetch_sync_page", _fetch_page)

        out = run_sync(conn, item_id="item_sync_error")
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_sync_zero_items_returns_items_skipped(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _mock_plaid_ready(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        out = run_sync(conn)
        assert out["items_requested"] == 0
        assert out["items_skipped"] == 0
        assert out["total_elapsed_ms"] == 0


def test_sync_total_elapsed_is_sum(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_elapsed_a")
        _seed_plaid_item(conn, plaid_item_id="item_elapsed_b")

        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        monkeypatch.setattr(
            "finance_cli.plaid_client._fetch_sync_page",
            lambda client, access_token, cursor, days_requested: {
                "added": [],
                "modified": [],
                "removed": [],
                "accounts": [],
                "next_cursor": "cursor_elapsed",
                "has_more": False,
            },
        )
        ticks = iter([10.0, 10.125, 20.0, 20.25])
        monkeypatch.setattr("finance_cli.plaid_client.time.perf_counter", lambda: next(ticks))

        out = run_sync(conn)
        assert out["items_synced"] == 2
        assert [item["elapsed_ms"] for item in out["items"]] == [125, 250]
        assert out["total_elapsed_ms"] == 375


def test_refresh_balances_updates_accounts_and_snapshots(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def accounts_balance_get(self, request):
            return _Resp(
                {
                    "accounts": [
                        {
                            "account_id": "acct_refresh_1",
                            "name": "Refresh Checking",
                            "type": "depository",
                            "subtype": "checking",
                            "balances": {
                                "current": 321.45,
                                "available": 300.00,
                                "iso_currency_code": "USD",
                            },
                        }
                    ]
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_refresh")

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        out = refresh_balances(conn, item_id="item_refresh")
        assert out["items_refreshed"] == 1
        assert out["accounts_updated"] == 1
        assert out["snapshots_updated"] == 1

        account = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents
              FROM accounts
             WHERE plaid_account_id = 'acct_refresh_1'
            """
        ).fetchone()
        assert account["balance_current_cents"] == 32145
        assert account["balance_available_cents"] == 30000

        snap = conn.execute(
            """
            SELECT source, balance_current_cents
              FROM balance_snapshots
             WHERE source = 'refresh'
            """
        ).fetchone()
        assert snap["source"] == "refresh"
        assert snap["balance_current_cents"] == 32145


def test_balance_cooldown_skips_fresh_item(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def accounts_balance_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": []})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_balance_fresh")
        _set_item_timestamp(conn, "item_balance_fresh", "last_balance_refresh_at", "-60 seconds")
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = refresh_balances(conn, item_id="item_balance_fresh")
        assert out["items_refreshed"] == 0
        assert out["items_skipped"] == 1
        assert out["items_failed"] == 0
        assert out["items"][0]["status"] == "skipped_cooldown"
        assert calls["n"] == 0


def test_balance_cooldown_allows_stale_item(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def accounts_balance_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": []})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_balance_stale")
        _set_item_timestamp(conn, "item_balance_stale", "last_balance_refresh_at", "-20 minutes")
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = refresh_balances(conn, item_id="item_balance_stale")
        assert out["items_refreshed"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_balance_cooldown_allows_null_timestamp(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def accounts_balance_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": []})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_balance_null")
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = refresh_balances(conn, item_id="item_balance_null")
        assert out["items_refreshed"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_balance_force_refresh_bypasses_cooldown(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def accounts_balance_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": []})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_balance_force")
        _set_item_timestamp(conn, "item_balance_force", "last_balance_refresh_at", "-60 seconds")
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = refresh_balances(conn, item_id="item_balance_force", force_refresh=True)
        assert out["items_refreshed"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_balance_zero_items_returns_items_skipped(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _mock_plaid_ready(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        out = refresh_balances(conn)
        assert out["items_requested"] == 0
        assert out["items_skipped"] == 0


def test_apr_percentage_supports_plaid_suffix_alias_and_legacy_types() -> None:
    aprs = [
        {"apr_type": "purchase_apr", "apr_percentage": 24.99},
        {"apr_type": "balance_transfer_apr", "apr_percentage": 14.99},
        {"apr_type": "cash_apr", "apr_percentage": 29.99},
    ]
    assert _apr_percentage(aprs, "purchase") == 24.99
    assert _apr_percentage(aprs, "balance_transfer") == 14.99
    assert _apr_percentage(aprs, "cash_advance") == 29.99
    assert _apr_percentage([{"apr_type": "purchase", "apr_percentage": 19.99}], "purchase") == 19.99


def test_apr_percentage_handles_edge_cases() -> None:
    assert _apr_percentage(None, "purchase") is None
    assert _apr_percentage([], "purchase") is None
    assert _apr_percentage([{}], "purchase") is None
    assert _apr_percentage([{"apr_type": "purchase_apr"}], "purchase") is None
    assert _apr_percentage([{"apr_type": "purchase_apr", "apr_percentage": "bad"}], "purchase") is None
    assert _apr_percentage([{"apr_type": "cash_apr", "apr_percentage": 0.0}], "cash_advance") == 0.0


def test_fetch_liabilities_upserts_and_deactivates_missing_rows(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            return _Resp(
                {
                    "accounts": [
                        {
                            "account_id": "liab_acct_1",
                            "name": "Credit Card",
                            "type": "credit",
                            "subtype": "credit card",
                            "balances": {
                                "current": 1200.00,
                                "limit": 5000.00,
                                "iso_currency_code": "USD",
                            },
                        }
                    ],
                    "liabilities": {
                        "credit": [
                            {
                                "account_id": "liab_acct_1",
                                "is_overdue": False,
                                "last_payment_amount": 90.25,
                                "last_payment_date": "2026-01-31",
                                "minimum_payment_amount": 45.00,
                                "next_payment_due_date": "2026-02-28",
                                "aprs": [
                                    {"apr_type": "purchase_apr", "apr_percentage": 24.99},
                                ],
                            }
                        ],
                        "student": [],
                        "mortgage": [],
                    },
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_liab", consented_products='["transactions","liabilities"]')

        stale_account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_account_id, plaid_item_id, institution_name, account_type, is_active)
            VALUES (?, 'liab_stale_acct', 'item_liab', 'Test Bank', 'loan', 1)
            """,
            (stale_account_id,),
        )
        conn.execute(
            """
            INSERT INTO liabilities (id, account_id, liability_type, is_active, last_seen_at, raw_plaid_json)
            VALUES (?, ?, 'student', 1, '2000-01-01 00:00:00', '{}')
            """,
            (uuid.uuid4().hex, stale_account_id),
        )
        conn.commit()

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        out = fetch_liabilities(conn, item_id="item_liab")
        assert out["items_synced"] == 1
        assert out["liabilities_upserted"] == 1
        assert out["liabilities_deactivated"] >= 1

        active_credit = conn.execute(
            """
            SELECT l.liability_type, l.is_active, l.minimum_payment_cents, l.apr_purchase
              FROM liabilities l
              JOIN accounts a ON a.id = l.account_id
             WHERE a.plaid_account_id = 'liab_acct_1'
            """
        ).fetchone()
        assert active_credit["liability_type"] == "credit"
        assert active_credit["is_active"] == 1
        assert active_credit["minimum_payment_cents"] == 4500
        assert active_credit["apr_purchase"] == 24.99

        stale = conn.execute(
            "SELECT is_active FROM liabilities WHERE account_id = ? AND liability_type = 'student'",
            (stale_account_id,),
        ).fetchone()
        assert stale["is_active"] == 0


def test_fetch_liabilities_idempotent_upsert(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            return _Resp(
                {
                    "accounts": [
                        {
                            "account_id": "liab_idem_1",
                            "name": "Card",
                            "type": "credit",
                            "subtype": "credit card",
                            "balances": {"current": 50.0, "limit": 1000.0, "iso_currency_code": "USD"},
                        }
                    ],
                    "liabilities": {
                        "credit": [
                            {
                                "account_id": "liab_idem_1",
                                "minimum_payment_amount": 20.0,
                                "next_payment_due_date": "2026-03-01",
                                "aprs": [{"apr_type": "purchase_apr", "apr_percentage": 10.5}],
                            }
                        ],
                        "student": [],
                        "mortgage": [],
                    },
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_idem", consented_products='["transactions","liabilities"]')
        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        first = fetch_liabilities(conn, item_id="item_idem")
        second = fetch_liabilities(conn, item_id="item_idem", force_refresh=True)
        assert first["liabilities_upserted"] == 1
        assert second["liabilities_upserted"] == 1

        row = conn.execute(
            "SELECT COUNT(*) AS n FROM liabilities"
        ).fetchone()
        assert row["n"] == 1


def test_fetch_liabilities_null_apr_does_not_wipe_existing_apr(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            return _Resp(
                {
                    "accounts": [
                        {
                            "account_id": "liab_null_apr_1",
                            "name": "Card",
                            "type": "credit",
                            "subtype": "credit card",
                            "balances": {"current": 100.0, "limit": 2000.0, "iso_currency_code": "USD"},
                        }
                    ],
                    "liabilities": {
                        "credit": [
                            {
                                "account_id": "liab_null_apr_1",
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
        _seed_plaid_item(conn, plaid_item_id="item_null_apr", consented_products='["transactions","liabilities"]')
        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_account_id, plaid_item_id, institution_name, account_type, is_active)
            VALUES (?, 'liab_null_apr_1', 'item_null_apr', 'Test Bank', 'credit_card', 1)
            """,
            (account_id,),
        )
        conn.execute(
            """
            INSERT INTO liabilities (
                id,
                account_id,
                liability_type,
                is_active,
                last_seen_at,
                apr_purchase,
                raw_plaid_json
            ) VALUES (?, ?, 'credit', 1, '2000-01-01 00:00:00', 21.99, '{}')
            """,
            (uuid.uuid4().hex, account_id),
        )
        conn.commit()

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        out = fetch_liabilities(conn, item_id="item_null_apr", force_refresh=True)
        assert out["items_synced"] == 1
        assert out["liabilities_upserted"] == 1

        row = conn.execute(
            """
            SELECT apr_purchase
              FROM liabilities
             WHERE account_id = ?
               AND liability_type = 'credit'
            """,
            (account_id,),
        ).fetchone()
        assert row["apr_purchase"] == 21.99


def test_fetch_liabilities_deactivates_when_account_and_liability_disappear(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            return _Resp(
                {
                    "accounts": [],
                    "liabilities": {
                        "credit": [],
                        "student": [],
                        "mortgage": [],
                    },
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_disappear", consented_products='["transactions","liabilities"]')
        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_account_id, plaid_item_id, institution_name, account_type, is_active)
            VALUES (?, 'disappear_acct', 'item_disappear', 'Test Bank', 'loan', 1)
            """,
            (account_id,),
        )
        conn.execute(
            """
            INSERT INTO liabilities (id, account_id, liability_type, is_active, last_seen_at, raw_plaid_json)
            VALUES (?, ?, 'student', 1, '2000-01-01 00:00:00', '{}')
            """,
            (uuid.uuid4().hex, account_id),
        )
        conn.commit()

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        out = fetch_liabilities(conn, item_id="item_disappear")
        assert out["items_synced"] == 1
        assert out["liabilities_upserted"] == 0
        assert out["liabilities_deactivated"] >= 1

        stale = conn.execute(
            "SELECT is_active FROM liabilities WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        assert stale["is_active"] == 0


def test_fetch_liabilities_deactivation_skips_pdf_created_rows(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            return _Resp(
                {
                    "accounts": [],
                    "liabilities": {
                        "credit": [],
                        "student": [],
                        "mortgage": [],
                    },
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_pdf_guard", consented_products='["transactions","liabilities"]')
        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_account_id, plaid_item_id, institution_name, account_type, is_active)
            VALUES (?, 'pdf_guard_acct', 'item_pdf_guard', 'Test Bank', 'credit_card', 1)
            """,
            (account_id,),
        )
        conn.execute(
            """
            INSERT INTO liabilities (id, account_id, liability_type, is_active, last_seen_at, raw_plaid_json)
            VALUES (?, ?, 'credit', 1, '2000-01-01 00:00:00', NULL)
            """,
            (uuid.uuid4().hex, account_id),
        )
        conn.commit()

        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        out = fetch_liabilities(conn, item_id="item_pdf_guard", force_refresh=True)
        assert out["items_synced"] == 1
        assert out["liabilities_deactivated"] == 0

        row = conn.execute(
            "SELECT is_active FROM liabilities WHERE account_id = ? AND liability_type = 'credit'",
            (account_id,),
        ).fetchone()
        assert row["is_active"] == 1


def test_fetch_liabilities_serializes_date_and_datetime_in_raw_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            return _Resp(
                {
                    "accounts": [
                        {
                            "account_id": "liab_dates_1",
                            "name": "Card",
                            "type": "credit",
                            "subtype": "credit card",
                            "balances": {"current": 300.0, "limit": 5000.0, "iso_currency_code": "USD"},
                        }
                    ],
                    "liabilities": {
                        "credit": [
                            {
                                "account_id": "liab_dates_1",
                                "minimum_payment_amount": 50.0,
                                "next_payment_due_date": date(2026, 3, 1),
                                "last_payment_date": datetime(2026, 2, 10, 9, 30, 0),
                                "property_address": {
                                    "city": "New York",
                                    "captured_on": date(2026, 2, 1),
                                },
                                "metadata": {"generated_at": datetime(2026, 2, 10, 9, 30, 0)},
                            }
                        ],
                        "student": [],
                        "mortgage": [],
                    },
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_dates", consented_products='["transactions","liabilities"]')
        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "access-token")

        out = fetch_liabilities(conn, item_id="item_dates")
        assert out["items_synced"] == 1
        assert out["liabilities_upserted"] == 1

        row = conn.execute(
            """
            SELECT raw_plaid_json, property_address_json
              FROM liabilities
             WHERE liability_type = 'credit'
            """
        ).fetchone()
        assert row is not None
        raw_payload = json.loads(row["raw_plaid_json"])
        prop_payload = json.loads(row["property_address_json"])
        assert raw_payload["next_payment_due_date"] == "2026-03-01"
        assert raw_payload["last_payment_date"] == "2026-02-10 09:30:00"
        assert raw_payload["metadata"]["generated_at"] == "2026-02-10 09:30:00"
        assert prop_payload["captured_on"] == "2026-02-01"


def test_liabilities_cooldown_skips_fresh_item(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": [], "liabilities": {"credit": [], "student": [], "mortgage": []}})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_liab_fresh", consented_products='["transactions","liabilities"]')
        _set_item_timestamp(conn, "item_liab_fresh", "last_liabilities_fetch_at", "-60 seconds")
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = fetch_liabilities(conn, item_id="item_liab_fresh")
        assert out["items_synced"] == 0
        assert out["items_skipped"] == 1
        assert out["items_failed"] == 0
        assert out["items"][0]["status"] == "skipped_cooldown"
        assert calls["n"] == 0


def test_liabilities_cooldown_allows_stale_item(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": [], "liabilities": {"credit": [], "student": [], "mortgage": []}})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_liab_stale", consented_products='["transactions","liabilities"]')
        _set_item_timestamp(conn, "item_liab_stale", "last_liabilities_fetch_at", "-2 hours")
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = fetch_liabilities(conn, item_id="item_liab_stale")
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_liabilities_cooldown_allows_null_timestamp(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": [], "liabilities": {"credit": [], "student": [], "mortgage": []}})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_liab_null", consented_products='["transactions","liabilities"]')
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = fetch_liabilities(conn, item_id="item_liab_null")
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_liabilities_force_refresh_bypasses_cooldown(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    calls = {"n": 0}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def liabilities_get(self, request):
            calls["n"] += 1
            return _Resp({"accounts": [], "liabilities": {"credit": [], "student": [], "mortgage": []}})

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_liab_force", consented_products='["transactions","liabilities"]')
        _set_item_timestamp(conn, "item_liab_force", "last_liabilities_fetch_at", "-60 seconds")
        _mock_plaid_ready(monkeypatch)
        _mock_access_token(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())

        out = fetch_liabilities(conn, item_id="item_liab_force", force_refresh=True)
        assert out["items_synced"] == 1
        assert out["items_skipped"] == 0
        assert calls["n"] == 1


def test_liabilities_zero_items_returns_items_skipped(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _mock_plaid_ready(monkeypatch)
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: object())
        out = fetch_liabilities(conn)
        assert out["items_requested"] == 0
        assert out["items_skipped"] == 0


def test_complete_link_session_stores_products_from_item_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def item_public_token_exchange(self, request):
            return _Resp({"access_token": "access-token-1", "item_id": "item_link_1"})

        def item_get(self, request):
            return _Resp(
                {
                    "item": {
                        "item_id": "item_link_1",
                        "institution_id": "ins_123",
                        "billed_products": ["transactions", "liabilities"],
                    }
                }
            )

        def institutions_get_by_id(self, request):
            return _Resp({"institution": {"name": "Link Bank"}})

    captured: dict[str, list[str]] = {}
    monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr("finance_cli.plaid_client.wait_for_public_token", lambda *args, **kwargs: "public-token-1")

    def _fake_store_plaid_token(**kwargs):
        captured["secret_names"] = list(kwargs.get("secret_names") or [])
        return "secret/link-bank"

    monkeypatch.setattr("finance_cli.plaid_client.store_plaid_token", _fake_store_plaid_token)

    with connect(db_path) as conn:
        out = complete_link_session(
            conn,
            user_id="user-1",
            link_token="link-token-1",
            requested_products=["transactions"],
        )
        assert out["consented_products"] == ["transactions", "liabilities"]

        row = conn.execute(
            "SELECT consented_products, institution_name, access_token_ref FROM plaid_items WHERE plaid_item_id = 'item_link_1'"
        ).fetchone()
        assert row is not None
        assert json.loads(row["consented_products"]) == ["transactions", "liabilities"]
        assert row["institution_name"] == "Link Bank"
        assert row["access_token_ref"] == "secret/link-bank"
        assert captured["secret_names"][0] == "plaid_token_user-1_item_item-link-1"
        assert captured["secret_names"][1] == "plaid/access_token/user-1/item/item-link-1"


def test_complete_link_session_fallback_logs_warning(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def item_public_token_exchange(self, request):
            return _Resp({"access_token": "access-token-1", "item_id": "item_link_warn"})

        def item_get(self, request):
            return _Resp({"item": {"item_id": "item_link_warn", "institution_id": "ins_123"}})

        def institutions_get_by_id(self, request):
            return _Resp({"institution": {"name": "Link Bank"}})

    monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr("finance_cli.plaid_client.wait_for_public_token", lambda *args, **kwargs: "public-token-1")
    monkeypatch.setattr("finance_cli.plaid_client.store_plaid_token", lambda **kwargs: "secret/link-bank")
    warnings: list[tuple[str, tuple[object, ...]]] = []

    def _fake_warning(message, *args) -> None:
        warnings.append((str(message), args))

    monkeypatch.setattr("finance_cli.plaid_client.logger.warning", _fake_warning)

    with connect(db_path) as conn:
        out = complete_link_session(
            conn,
            user_id="user-1",
            link_token="link-token-1",
            requested_products=["investments"],
        )

        row = conn.execute(
            "SELECT consented_products FROM plaid_items WHERE plaid_item_id = 'item_link_warn'"
        ).fetchone()

    assert out["consented_products"] == ["transactions", "investments"]
    assert row is not None
    assert json.loads(row["consented_products"]) == ["transactions", "investments"]
    assert warnings
    assert "falling back to requested products (may overreport)" in warnings[0][0]
    assert warnings[0][1] == (["transactions", "investments"],)


def test_complete_link_session_blocks_duplicate_institution_without_allow_flag(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def item_public_token_exchange(self, request):
            return _Resp({"access_token": "access-token-new", "item_id": "item_link_new"})

        def item_get(self, request):
            return _Resp({"item": {"item_id": "item_link_new", "institution_id": "ins_123"}})

        def institutions_get_by_id(self, request):
            return _Resp({"institution": {"name": "Link Bank"}})

    stored: list[str] = []
    monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr("finance_cli.plaid_client.wait_for_public_token", lambda *args, **kwargs: "public-token-1")
    monkeypatch.setattr(
        "finance_cli.plaid_client.store_plaid_token",
        lambda **kwargs: stored.append("called") or "secret/link-bank-new",
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_link_existing', 'Link Bank', 'secret/link-bank-existing', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        with pytest.raises(PlaidSyncError) as exc:
            complete_link_session(
                conn,
                user_id="user-1",
                link_token="link-token-1",
                requested_products=["transactions"],
            )

        assert "Duplicate institution link blocked" in str(exc.value)
        assert "item_link_existing" in str(exc.value)
        assert stored == []

        inserted = conn.execute(
            "SELECT 1 FROM plaid_items WHERE plaid_item_id = 'item_link_new'"
        ).fetchone()
        assert inserted is None


def test_complete_link_session_blocks_duplicate_by_institution_id_even_if_name_differs(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def item_public_token_exchange(self, request):
            return _Resp({"access_token": "access-token-new", "item_id": "item_link_new"})

        def item_get(self, request):
            return _Resp({"item": {"item_id": "item_link_new", "institution_id": "ins_123"}})

        def institutions_get_by_id(self, request):
            return _Resp({"institution": {"name": "Link Bank Renamed"}})

    stored: list[str] = []
    monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr("finance_cli.plaid_client.wait_for_public_token", lambda *args, **kwargs: "public-token-1")
    monkeypatch.setattr(
        "finance_cli.plaid_client.store_plaid_token",
        lambda **kwargs: stored.append("called") or "secret/link-bank-new",
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_link_existing', 'ins_123', 'Legacy Link Bank Name', 'secret/link-bank-existing', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        with pytest.raises(PlaidSyncError) as exc:
            complete_link_session(
                conn,
                user_id="user-1",
                link_token="link-token-1",
                requested_products=["transactions"],
            )

        assert "Duplicate institution link blocked" in str(exc.value)
        assert "item_link_existing" in str(exc.value)
        assert stored == []


def test_complete_link_session_does_not_block_duplicates_for_unknown_institution(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def item_public_token_exchange(self, request):
            return _Resp({"access_token": "access-token-new", "item_id": "item_link_new"})

        def item_get(self, request):
            # No institution_id means we cannot reliably dedupe by institution.
            return _Resp({"item": {"item_id": "item_link_new"}})

    monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr("finance_cli.plaid_client.wait_for_public_token", lambda *args, **kwargs: "public-token-1")
    monkeypatch.setattr(
        "finance_cli.plaid_client.store_plaid_token",
        lambda **kwargs: "secret/item-link-new",
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_link_existing', 'Unknown Institution', 'secret/existing', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        out = complete_link_session(
            conn,
            user_id="user-1",
            link_token="link-token-1",
            requested_products=["transactions"],
        )
        assert out["plaid_item_id"] == "item_link_new"
        assert out["institution_name"] == "Unknown Institution"

        inserted = conn.execute(
            "SELECT institution_name FROM plaid_items WHERE plaid_item_id = 'item_link_new'"
        ).fetchone()
        assert inserted is not None
        assert inserted["institution_name"] == "Unknown Institution"


def test_secret_name_candidates_include_both_formats() -> None:
    names = secret_name_candidates("user@example.com", "Chase Credit")
    assert names[0].startswith("plaid_token_user@example.com_")
    assert names[1] == "plaid/access_token/user@example.com/chase-credit"


def test_secret_name_candidates_for_item_include_item_scoped_formats() -> None:
    names = secret_name_candidates_for_item("user@example.com", "item_abc123")
    assert names[0] == "plaid_token_user@example.com_item_item-abc123"
    assert names[1] == "plaid/access_token/user@example.com/item/item-abc123"


def test_get_access_token_for_item_rejects_secret_item_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        "finance_cli.plaid_client.get_secret_payload",
        lambda secret_name, region_name=None: {
            "access_token": "access-token",
            "item_id": "item_other",
        },
    )

    with pytest.raises(PlaidSyncError) as exc:
        _get_access_token_for_item(
            {"plaid_item_id": "item_expected", "access_token_ref": "secret/shared"},
        )
    assert "secret token item mismatch" in str(exc.value)


def test_delete_secret_raises_on_invalid_request(monkeypatch) -> None:
    class _FakeSecretsClient:
        def delete_secret(self, SecretId, ForceDeleteWithoutRecovery):
            class _Err(Exception):
                pass

            err = _Err("invalid request")
            err.response = {"Error": {"Code": "InvalidRequestException"}}
            raise err

    monkeypatch.setattr("finance_cli.plaid_client._boto_secrets_client", lambda region_name=None: _FakeSecretsClient())

    with pytest.raises(Exception):
        delete_secret("secret-name")


def test_unlink_item_invokes_secret_cleanup(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    cleaned: list[str] = []
    monkeypatch.setattr("finance_cli.plaid_client.delete_secret", lambda name, region_name=None: cleaned.append(name))

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_unlink', 'Test Bank', 'secret/to/delete', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        ok = unlink_item(conn, "item_unlink")
        assert ok is True

    assert cleaned == ["secret/to/delete"]


def test_unlink_item_deactivates_transactions_for_item_accounts(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    monkeypatch.setattr("finance_cli.plaid_client.delete_secret", lambda *_args, **_kwargs: None)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_unlink_txn', 'Test Bank', 'secret/to/delete', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO accounts (
                id, plaid_account_id, plaid_item_id, institution_name, account_name, account_type, is_active
            ) VALUES ('acct_unlink_txn', 'plaid_acct_unlink_txn', 'item_unlink_txn', 'Test Bank', 'Checking', 'checking', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, source, is_active
            ) VALUES ('txn_unlink_txn', 'acct_unlink_txn', '2026-02-19', 'Coffee', -420, 'plaid', 1)
            """
        )
        conn.commit()

        ok = unlink_item(conn, "item_unlink_txn")
        assert ok is True

        account_row = conn.execute("SELECT is_active FROM accounts WHERE id = 'acct_unlink_txn'").fetchone()
        txn_row = conn.execute("SELECT is_active FROM transactions WHERE id = 'txn_unlink_txn'").fetchone()

    assert account_row["is_active"] == 0
    assert txn_row["is_active"] == 0


def test_unlink_item_skips_secret_cleanup_when_token_ref_shared_by_active_item(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    cleaned: list[str] = []
    remote_removed: list[str] = []
    monkeypatch.setattr("finance_cli.plaid_client.delete_secret", lambda name, region_name=None: cleaned.append(name))
    monkeypatch.setattr(
        "finance_cli.plaid_client.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.plaid_client._get_access_token_for_item",
        lambda item, region_name=None: "access-token-shared",
    )
    monkeypatch.setattr("finance_cli.plaid_client._remove_remote_item", lambda access_token: remote_removed.append(access_token))

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_unlink_1', 'Test Bank', 'secret/shared', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_unlink_2', 'Test Bank', 'secret/shared', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        ok = unlink_item(conn, "item_unlink_1")
        assert ok is True

        first = conn.execute(
            "SELECT status FROM plaid_items WHERE plaid_item_id = 'item_unlink_1'"
        ).fetchone()
        second = conn.execute(
            "SELECT status FROM plaid_items WHERE plaid_item_id = 'item_unlink_2'"
        ).fetchone()
        assert first["status"] == "disconnected"
        assert second["status"] == "active"

    assert cleaned == []
    assert remote_removed == []


def test_unlink_item_calls_remote_remove_when_token_ref_not_shared(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    cleaned: list[str] = []
    remote_removed: list[str] = []
    monkeypatch.setattr("finance_cli.plaid_client.delete_secret", lambda name, region_name=None: cleaned.append(name))
    monkeypatch.setattr(
        "finance_cli.plaid_client.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.plaid_client._get_access_token_for_item",
        lambda item, region_name=None: "access-token-single",
    )
    monkeypatch.setattr("finance_cli.plaid_client._remove_remote_item", lambda access_token: remote_removed.append(access_token))

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_unlink_single', 'Test Bank', 'secret/single', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        ok = unlink_item(conn, "item_unlink_single")
        assert ok is True

        row = conn.execute(
            "SELECT status FROM plaid_items WHERE plaid_item_id = 'item_unlink_single'"
        ).fetchone()
        assert row["status"] == "disconnected"

    assert remote_removed == ["access-token-single"]
    assert cleaned == ["secret/single"]
