from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import biz_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_business_account(conn, *, name: str, balance_cents: int) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business, source
        ) VALUES (?, 'Test Bank', ?, 'checking', ?, 1, 1, 'manual')
        """,
        (account_id, name, balance_cents),
    )
    return account_id


def test_handle_cashflow_excludes_hash_alias_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        canonical_id = _seed_business_account(conn, name="Canonical Checking", balance_cents=100_000)
        hash_id = _seed_business_account(conn, name="Hash Checking", balance_cents=50_000)
        conn.commit()

        args = Namespace(month="2026-01", quarter=None, year=None, format="json")
        no_alias = biz_cmd.handle_cashflow(args, conn)
        no_alias_ids = {row["id"] for row in no_alias["data"]["business_accounts"]}
        assert no_alias_ids == {canonical_id, hash_id}
        assert sum(row["balance_current_cents"] for row in no_alias["data"]["business_accounts"]) == 150_000

        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_id, canonical_id),
        )
        conn.commit()

        with_alias = biz_cmd.handle_cashflow(args, conn)
        with_alias_ids = {row["id"] for row in with_alias["data"]["business_accounts"]}
        assert with_alias_ids == {canonical_id}
        assert sum(row["balance_current_cents"] for row in with_alias["data"]["business_accounts"]) == 100_000
