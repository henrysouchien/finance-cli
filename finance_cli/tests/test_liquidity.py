from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.liquidity import liquidity_snapshot


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_account(
    conn,
    *,
    account_type: str,
    balance_cents: int,
    name: str,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, source
        ) VALUES (?, 'Test Bank', ?, ?, ?, 1, 'manual')
        """,
        (account_id, name, account_type, balance_cents),
    )
    return account_id


def test_liquidity_snapshot_excludes_hash_alias_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        canonical_checking = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            name="Canonical Checking",
        )
        hash_checking = _seed_account(conn, account_type="checking", balance_cents=25_000, name="Hash Checking")
        canonical_credit = _seed_account(
            conn,
            account_type="credit_card",
            balance_cents=-30_000,
            name="Canonical Credit",
        )
        hash_credit = _seed_account(conn, account_type="credit_card", balance_cents=-5_000, name="Hash Credit")
        conn.commit()

        no_alias = liquidity_snapshot(conn)
        assert no_alias["liquid_balance_cents"] == 125_000
        assert no_alias["credit_owed_cents"] == 35_000

        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_checking, canonical_checking),
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_credit, canonical_credit),
        )
        conn.commit()

        with_alias = liquidity_snapshot(conn)
        assert with_alias["liquid_balance_cents"] == 100_000
        assert with_alias["credit_owed_cents"] == 30_000
