from __future__ import annotations

from pathlib import Path

from finance_cli.db import connect, initialize_database
import finance_cli.plaid_client as plaid_client
from finance_cli.plaid_client import PlaidConfigStatus, refresh_balances, run_sync


def _seed_plaid_item(conn, *, plaid_item_id: str, consented_products: str = '["transactions"]') -> None:
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
        ) VALUES (?, ?, 'Test Bank', 'secret/token', 'active', ?, NULL)
        """,
        (f"row-{plaid_item_id}", plaid_item_id, consented_products),
    )
    conn.commit()


def _set_last_balance_refresh_at(conn, plaid_item_id: str, modifier: str) -> None:
    conn.execute(
        f"""
        UPDATE plaid_items
           SET last_balance_refresh_at = datetime('now', '{modifier}')
         WHERE plaid_item_id = ?
        """,
        (plaid_item_id,),
    )
    conn.commit()


def _mock_plaid_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        plaid_client,
        "config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )


def _mock_access_token(monkeypatch) -> None:
    monkeypatch.setattr(plaid_client, "_get_access_token_for_item", lambda item, region_name=None, **kwargs: "access-token")


def test_refresh_balances_records_cost_ledger_row_on_success(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def to_dict(self):
            return {"accounts": []}

    class _Client:
        def accounts_balance_get(self, request):
            return _Resp()

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item-balance-success")
        result = refresh_balances(conn, item_id="item-balance-success")

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT provider, operation, cost_usd6, is_estimated
            FROM cost_ledger
            """
        ).fetchone()

    assert result["items_refreshed"] == 1
    assert row["provider"] == "plaid"
    assert row["operation"] == "accounts_balance_get"
    assert row["cost_usd6"] == 100_000
    assert row["is_estimated"] == 1


def test_refresh_balances_records_zero_cost_row_on_failure(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Client:
        def accounts_balance_get(self, request):
            raise RuntimeError("balance boom")

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item-balance-failure")
        result = refresh_balances(conn, item_id="item-balance-failure")

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT provider, operation, cost_usd6, is_estimated
            FROM cost_ledger
            """
        ).fetchone()

    assert result["items_failed"] == 1
    assert row["provider"] == "plaid"
    assert row["operation"] == "accounts_balance_get"
    assert row["cost_usd6"] == 0
    assert row["is_estimated"] == 1


def test_refresh_balances_cooldown_skip_records_no_ledger_row(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    client_calls = {"count": 0}

    class _Client:
        def accounts_balance_get(self, request):
            client_calls["count"] += 1
            raise AssertionError("accounts_balance_get should not run during cooldown skip")

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item-balance-cooldown")
        _set_last_balance_refresh_at(conn, "item-balance-cooldown", "-60 seconds")
        result = refresh_balances(conn, item_id="item-balance-cooldown")

    with connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM cost_ledger").fetchone()

    assert result["items_skipped"] == 1
    assert client_calls["count"] == 0
    assert row["count"] == 0


def test_run_sync_records_zero_cost_row_for_transactions_sync(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def to_dict(self):
            return {
                "added": [],
                "modified": [],
                "removed": [],
                "accounts": [],
                "next_cursor": "cursor-1",
                "has_more": False,
            }

    class _Client:
        def transactions_sync(self, request):
            return _Resp()

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr(
        plaid_client,
        "apply_sync_updates",
        lambda conn, **kwargs: {"added": 0, "modified": 0, "removed": 0},
    )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item-sync-success")
        result = run_sync(conn, item_id="item-sync-success")

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT provider, operation, cost_usd6, is_estimated
            FROM cost_ledger
            """
        ).fetchone()

    assert result["items_synced"] == 1
    assert row["provider"] == "plaid"
    assert row["operation"] == "transactions_sync"
    assert row["cost_usd6"] == 0
    assert row["is_estimated"] == 1
