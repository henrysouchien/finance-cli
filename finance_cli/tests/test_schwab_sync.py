from __future__ import annotations

from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.schwab_client import (
    _extract_portfolio_value,
    _schwab_account_id,
    sync_schwab_balances,
)


class _FakeSchwabClient:
    def __init__(self, account_rows: list[dict], account_payloads: dict[str, dict]):
        self._account_rows = account_rows
        self._account_payloads = account_payloads

    def get_account_numbers(self):
        return self._account_rows

    def get_account(self, account_hash: str, fields=None):  # noqa: ANN001
        del fields
        payload = self._account_payloads[account_hash]
        return {"securitiesAccount": payload}


def test_schwab_account_id_deterministic() -> None:
    account_number = "1234567890"
    first = _schwab_account_id(account_number)
    second = _schwab_account_id(account_number)
    assert first == second
    assert len(first) == 32
    int(first, 16)


def test_extract_portfolio_value_liquidation() -> None:
    payload = {"currentBalances": {"liquidationValue": 150432.18}}
    assert _extract_portfolio_value(payload) == 150432.18


def test_extract_portfolio_value_fallback() -> None:
    payload = {
        "currentBalances": {"cashBalance": 50.25},
        "positions": [{"marketValue": 100.00}, {"marketValue": 249.75}],
    }
    assert _extract_portfolio_value(payload) == 400.0


def test_extract_portfolio_value_empty() -> None:
    assert _extract_portfolio_value({}) is None
    assert _extract_portfolio_value({"currentBalances": {}}) is None


def test_sync_upserts_account_and_snapshot(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    client = _FakeSchwabClient(
        account_rows=[{"accountNumber": "12345678", "hashValue": "hash_1"}],
        account_payloads={"hash_1": {"currentBalances": {"liquidationValue": 150432.18}}},
    )
    monkeypatch.setattr("finance_cli.schwab_client._client_from_token_file", lambda: client)

    with connect(db_path) as conn:
        result = sync_schwab_balances(conn)
        account = conn.execute(
            """
            SELECT institution_name, account_name, account_type, source, balance_current_cents, is_active
              FROM accounts
             WHERE id = ?
            """,
            (_schwab_account_id("12345678"),),
        ).fetchone()
        snapshot = conn.execute(
            """
            SELECT balance_current_cents, source, snapshot_date
              FROM balance_snapshots
             WHERE account_id = ?
               AND source = 'refresh'
               AND snapshot_date = date('now')
            """,
            (_schwab_account_id("12345678"),),
        ).fetchone()

    assert result["accounts_synced"] == 1
    assert result["snapshots_upserted"] == 1
    assert result["accounts_failed"] == 0
    assert account is not None
    assert account["institution_name"] == "Charles Schwab"
    assert account["account_name"] == "Brokerage ****5678"
    assert account["account_type"] == "investment"
    assert account["source"] == "schwab"
    assert account["balance_current_cents"] == 15043218
    assert account["is_active"] == 1
    assert snapshot is not None
    assert snapshot["balance_current_cents"] == 15043218
    assert snapshot["source"] == "refresh"


def test_sync_idempotent(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    client = _FakeSchwabClient(
        account_rows=[{"accountNumber": "12345678", "hashValue": "hash_1"}],
        account_payloads={"hash_1": {"currentBalances": {"liquidationValue": 10.00}}},
    )
    monkeypatch.setattr("finance_cli.schwab_client._client_from_token_file", lambda: client)

    with connect(db_path) as conn:
        first = sync_schwab_balances(conn)
        second = sync_schwab_balances(conn)
        account_count = conn.execute(
            "SELECT COUNT(*) AS n FROM accounts WHERE id = ?",
            (_schwab_account_id("12345678"),),
        ).fetchone()["n"]
        snapshot_count = conn.execute(
            """
            SELECT COUNT(*) AS n
              FROM balance_snapshots
             WHERE account_id = ?
               AND source = 'refresh'
               AND snapshot_date = date('now')
            """,
            (_schwab_account_id("12345678"),),
        ).fetchone()["n"]

    assert first["accounts_synced"] == 1
    assert second["accounts_synced"] == 1
    assert account_count == 1
    assert snapshot_count == 1


def test_sync_updates_balance_current_cents(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    first_client = _FakeSchwabClient(
        account_rows=[{"accountNumber": "12345678", "hashValue": "hash_1"}],
        account_payloads={"hash_1": {"currentBalances": {"liquidationValue": 100.00}}},
    )
    second_client = _FakeSchwabClient(
        account_rows=[{"accountNumber": "12345678", "hashValue": "hash_1"}],
        account_payloads={"hash_1": {"currentBalances": {"liquidationValue": 250.00}}},
    )

    with connect(db_path) as conn:
        monkeypatch.setattr("finance_cli.schwab_client._client_from_token_file", lambda: first_client)
        sync_schwab_balances(conn)
        first_balance = conn.execute(
            "SELECT balance_current_cents FROM accounts WHERE id = ?",
            (_schwab_account_id("12345678"),),
        ).fetchone()["balance_current_cents"]

        monkeypatch.setattr("finance_cli.schwab_client._client_from_token_file", lambda: second_client)
        sync_schwab_balances(conn)
        updated = conn.execute(
            "SELECT balance_current_cents, balance_updated_at FROM accounts WHERE id = ?",
            (_schwab_account_id("12345678"),),
        ).fetchone()

    assert first_balance == 10000
    assert updated["balance_current_cents"] == 25000
    assert updated["balance_updated_at"] is not None

