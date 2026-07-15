from __future__ import annotations

import json
import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.__main__ import main
from finance_cli.commands import balance_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError


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
    available_cents: int | None = None,
    limit_cents: int | None = None,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, balance_available_cents,
            balance_limit_cents, is_active, source
        ) VALUES (?, 'Test Bank', ?, ?, ?, ?, ?, 1, 'manual')
        """,
        (account_id, name, account_type, balance_cents, available_cents, limit_cents),
    )
    return account_id


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


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


def test_balance_update_records_manual_snapshot_and_updates_account(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            name="Manual Checking",
        )
        conn.commit()

    code, payload = _run_cli(
        [
            "balance",
            "update",
            "--account",
            account_id,
            "--current",
            "1234.56",
            "--available",
            "1200",
            "--date",
            "2026-06-23",
        ],
        capsys,
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "balance.update"
    assert payload["data"]["account_id"] == account_id
    assert payload["data"]["snapshot"]["source"] == "manual"
    assert payload["data"]["snapshot"]["snapshot_date"] == "2026-06-23"
    assert payload["data"]["snapshot"]["balance_current_cents"] == 123_456
    assert payload["data"]["snapshot"]["balance_available_cents"] == 120_000

    with connect(db_path) as conn:
        account = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, balance_updated_at
              FROM accounts
             WHERE id = ?
            """,
            (account_id,),
        ).fetchone()
        assert account["balance_current_cents"] == 123_456
        assert account["balance_available_cents"] == 120_000
        assert account["balance_updated_at"] is not None
        snapshot = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, source, snapshot_date
              FROM balance_snapshots
             WHERE account_id = ?
            """,
            (account_id,),
        ).fetchone()
        assert dict(snapshot) == {
            "balance_current_cents": 123_456,
            "balance_available_cents": 120_000,
            "source": "manual",
            "snapshot_date": "2026-06-23",
        }


def test_balance_update_snapshot_omits_unprovided_fields(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            available_cents=50_000,
            limit_cents=200_000,
            name="Stale Available Checking",
        )
        conn.commit()

    code, payload = _run_cli(
        [
            "balance",
            "update",
            "--account",
            account_id,
            "--current",
            "1234.56",
            "--date",
            "2026-06-23",
        ],
        capsys,
    )

    assert code == 0
    assert payload["data"]["account"]["balance_available_cents"] == 50_000
    assert payload["data"]["account"]["balance_limit_cents"] == 200_000
    assert payload["data"]["snapshot"]["balance_current_cents"] == 123_456
    assert payload["data"]["snapshot"]["balance_available_cents"] is None
    assert payload["data"]["snapshot"]["balance_limit_cents"] is None

    with connect(db_path) as conn:
        account = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, balance_limit_cents
              FROM accounts
             WHERE id = ?
            """,
            (account_id,),
        ).fetchone()
        snapshot = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, balance_limit_cents
              FROM balance_snapshots
             WHERE account_id = ?
               AND snapshot_date = '2026-06-23'
               AND source = 'manual'
            """,
            (account_id,),
        ).fetchone()

    assert dict(account) == {
        "balance_current_cents": 123_456,
        "balance_available_cents": 50_000,
        "balance_limit_cents": 200_000,
    }
    assert dict(snapshot) == {
        "balance_current_cents": 123_456,
        "balance_available_cents": None,
        "balance_limit_cents": None,
    }


def test_balance_update_reuses_manual_snapshot_for_same_date(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            account_type="savings",
            balance_cents=100_000,
            name="Manual Savings",
        )
        conn.commit()

    first_code, first_payload = _run_cli(
        [
            "balance",
            "update",
            "--account",
            account_id,
            "--current",
            "1000",
            "--date",
            "2026-06-23",
        ],
        capsys,
    )
    assert first_code == 0

    second_code, second_payload = _run_cli(
        [
            "balance",
            "update",
            "--account",
            account_id,
            "--current",
            "1100",
            "--date",
            "2026-06-23",
        ],
        capsys,
    )
    assert second_code == 0
    assert second_payload["data"]["updated_existing_snapshot"] is True
    assert second_payload["data"]["snapshot"]["id"] == first_payload["data"]["snapshot"]["id"]

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT balance_current_cents
              FROM balance_snapshots
             WHERE account_id = ?
            """,
            (account_id,),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["balance_current_cents"] == 110_000


def test_balance_update_accumulates_same_day_explicit_fields(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            available_cents=50_000,
            limit_cents=200_000,
            name="Partial Manual Checking",
        )
        conn.commit()

    first_code, first_payload = _run_cli(
        [
            "balance",
            "update",
            "--account",
            account_id,
            "--current",
            "1000",
            "--date",
            "2026-06-23",
        ],
        capsys,
    )
    assert first_code == 0
    assert first_payload["data"]["snapshot"]["balance_current_cents"] == 100_000
    assert first_payload["data"]["snapshot"]["balance_available_cents"] is None
    assert first_payload["data"]["snapshot"]["balance_limit_cents"] is None

    second_code, second_payload = _run_cli(
        [
            "balance",
            "update",
            "--account",
            account_id,
            "--available",
            "750",
            "--date",
            "2026-06-23",
        ],
        capsys,
    )
    assert second_code == 0
    assert second_payload["data"]["updated_existing_snapshot"] is True
    assert second_payload["data"]["snapshot"]["id"] == first_payload["data"]["snapshot"]["id"]
    assert second_payload["data"]["snapshot"]["balance_current_cents"] == 100_000
    assert second_payload["data"]["snapshot"]["balance_available_cents"] == 75_000
    assert second_payload["data"]["snapshot"]["balance_limit_cents"] is None

    with connect(db_path) as conn:
        account = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, balance_limit_cents
              FROM accounts
             WHERE id = ?
            """,
            (account_id,),
        ).fetchone()
        snapshot = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents, balance_limit_cents
              FROM balance_snapshots
             WHERE account_id = ?
               AND snapshot_date = '2026-06-23'
               AND source = 'manual'
            """,
            (account_id,),
        ).fetchone()

    assert dict(account) == {
        "balance_current_cents": 100_000,
        "balance_available_cents": 75_000,
        "balance_limit_cents": 200_000,
    }
    assert dict(snapshot) == {
        "balance_current_cents": 100_000,
        "balance_available_cents": 75_000,
        "balance_limit_cents": None,
    }


def test_balance_update_dry_run_rolls_back(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            name="Dry Run Checking",
        )
        conn.commit()

        result = balance_cmd.handle_update(
            Namespace(
                account=account_id,
                current="2000",
                available=None,
                balance_limit=None,
                snapshot_date="2026-06-23",
                dry_run=True,
            ),
            conn,
        )

        assert result["data"]["dry_run"] is True
        account = conn.execute(
            "SELECT balance_current_cents FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        snapshot_count = conn.execute(
            "SELECT COUNT(*) AS n FROM balance_snapshots WHERE account_id = ?",
            (account_id,),
        ).fetchone()["n"]
        assert account["balance_current_cents"] == 100_000
        assert snapshot_count == 0


def test_balance_update_rejects_alias_source_account(db_path: Path) -> None:
    with connect(db_path) as conn:
        canonical_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            name="Canonical Checking",
        )
        hash_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=50_000,
            name="Hash Checking",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_id, canonical_id),
        )
        conn.commit()

        with pytest.raises(ValidationError, match="canonical account"):
            balance_cmd.handle_update(
                Namespace(
                    account=hash_id,
                    current="750",
                    available=None,
                    balance_limit=None,
                    snapshot_date="2026-06-23",
                    dry_run=False,
                ),
                conn,
            )


def test_balance_update_requires_at_least_one_balance_field(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            name="Manual Checking",
        )
        conn.commit()

    code, payload = _run_cli(["balance", "update", "--account", account_id], capsys)

    assert code == 1
    assert payload["status"] == "error"
    assert "at least one" in payload["error"]
