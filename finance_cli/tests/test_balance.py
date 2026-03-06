from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import balance_cmd
from finance_cli.db import connect, initialize_database


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


def test_handle_show_excludes_hash_alias_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        canonical_id = _seed_account(conn, account_type="checking", balance_cents=100_000, name="Canonical Checking")
        hash_id = _seed_account(conn, account_type="checking", balance_cents=25_000, name="Hash Checking")
        conn.commit()

        args = Namespace(type=None, show_all=False, view="all")
        no_alias = balance_cmd.handle_show(args, conn)
        no_alias_ids = {row["id"] for row in no_alias["data"]["accounts"]}
        assert no_alias_ids == {canonical_id, hash_id}
        assert no_alias["data"]["total_assets_cents"] == 125_000

        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_id, canonical_id),
        )
        conn.commit()

        with_alias = balance_cmd.handle_show(args, conn)
        with_alias_ids = {row["id"] for row in with_alias["data"]["accounts"]}
        assert with_alias_ids == {canonical_id}
        assert with_alias["data"]["total_assets_cents"] == 100_000


def test_handle_net_worth_excludes_hash_alias_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        canonical_checking = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            name="Canonical Checking",
        )
        hash_checking = _seed_account(conn, account_type="checking", balance_cents=40_000, name="Hash Checking")
        canonical_credit = _seed_account(
            conn,
            account_type="credit_card",
            balance_cents=-50_000,
            name="Canonical Credit",
        )
        hash_credit = _seed_account(conn, account_type="credit_card", balance_cents=-20_000, name="Hash Credit")
        conn.commit()

        args = Namespace(exclude_investments=False, view="all")
        no_alias = balance_cmd.handle_net_worth(args, conn)
        assert no_alias["data"]["assets_cents"] == 140_000
        assert no_alias["data"]["liabilities_cents"] == 70_000
        assert no_alias["data"]["net_worth_cents"] == 70_000

        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_checking, canonical_checking),
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_credit, canonical_credit),
        )
        conn.commit()

        with_alias = balance_cmd.handle_net_worth(args, conn)
        assert with_alias["data"]["assets_cents"] == 100_000
        assert with_alias["data"]["liabilities_cents"] == 50_000
        assert with_alias["data"]["net_worth_cents"] == 50_000

        by_type = {row["account_type"]: row["balance_cents"] for row in with_alias["data"]["breakdown"]}
        assert by_type == {"checking": 100_000, "credit_card": -50_000}
