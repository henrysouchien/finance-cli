from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import hysa_transfer_flags
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
    account_id: str = "checking-1",
    *,
    account_type: str = "checking",
    balance_cents: int = 820_000,
    is_active: int = 1,
    is_business: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business
        ) VALUES (?, 'Cash Bank', 'Checking', ?, ?, ?, ?)
        """,
        (account_id, account_type, balance_cents, is_active, is_business),
    )


def _seed_snapshots(
    conn,
    account_id: str = "checking-1",
    balances: list[tuple[str, int]] | None = None,
) -> None:
    rows = balances or [
        ("2026-02-20", 800_000),
        ("2026-03-15", 810_000),
        ("2026-05-26", 820_000),
    ]
    for index, (snapshot_date, balance_cents) in enumerate(rows, start=1):
        conn.execute(
            """
            INSERT INTO balance_snapshots (
                id, account_id, balance_current_cents, source, snapshot_date
            ) VALUES (?, ?, ?, 'manual', ?)
            """,
            (f"snap-{account_id}-{index}", account_id, balance_cents, snapshot_date),
        )


def test_flag_account_for_hysa_transfer_is_idempotent_and_snapshots_evidence(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_snapshots(
            conn,
            balances=[
                ("2026-02-20", 820_000),
                ("2026-03-15", 820_000),
                ("2026-05-26", 820_000),
            ],
        )
        conn.commit()

        preview = hysa_transfer_flags.flag_account_for_hysa_transfer(
            conn,
            account_id="checking-1",
            suggested_transfer_cents=600_000,
            retained_buffer_cents=200_000,
            current_apy_bps=1,
            hysa_apy_bps=450,
            as_of="2026-05-26",
            reason="Surplus checking balance for 90 days.",
            dry_run=True,
        )
        preview_count = conn.execute(
            "SELECT COUNT(*) AS n FROM hysa_transfer_flags"
        ).fetchone()["n"]

        first = hysa_transfer_flags.flag_account_for_hysa_transfer(
            conn,
            account_id="checking-1",
            suggested_transfer_cents=600_000,
            retained_buffer_cents=200_000,
            current_apy_bps=1,
            hysa_apy_bps=450,
            as_of="2026-05-26",
            reason="Surplus checking balance for 90 days.",
        )
        second = hysa_transfer_flags.flag_account_for_hysa_transfer(
            conn,
            account_id="checking-1",
            suggested_transfer_cents=650_000,
            retained_buffer_cents=170_000,
            current_apy_bps=1,
            hysa_apy_bps=500,
            as_of="2026-05-26",
            reason="Updated transfer amount.",
            source="user",
        )
        rows = conn.execute(
            """
            SELECT id, suggested_transfer_cents, retained_buffer_cents,
                   minimum_balance_cents, hysa_apy_bps, estimated_annual_yield_cents,
                   reason, source
              FROM hysa_transfer_flags
            """
        ).fetchall()

    assert preview["summary"]["flagged"] == 0
    assert preview["data"]["flag"]["snapshot"]["evidence_points"] == 3
    assert preview_count == 0
    assert first["summary"]["flagged"] == 1
    assert first["summary"]["estimated_annual_yield_cents"] == 26_940
    assert first["data"]["flag"]["snapshot"]["observed_since"] == "2026-02-20"
    assert second["data"]["flag"]["id"] == first["data"]["flag"]["id"]
    assert len(rows) == 1
    assert rows[0]["suggested_transfer_cents"] == 650_000
    assert rows[0]["retained_buffer_cents"] == 170_000
    assert rows[0]["minimum_balance_cents"] == 820_000
    assert rows[0]["hysa_apy_bps"] == 500
    assert rows[0]["estimated_annual_yield_cents"] == 32_435
    assert rows[0]["reason"] == "Updated transfer amount."
    assert rows[0]["source"] == "user"


def test_flag_account_for_hysa_transfer_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, "checking-1", balance_cents=820_000)
        _seed_account(conn, "savings-1", account_type="savings", balance_cents=820_000)
        _seed_account(conn, "business-checking", balance_cents=820_000, is_business=1)
        _seed_account(conn, "canonical-checking", balance_cents=820_000)
        _seed_account(conn, "hash-checking", balance_cents=820_000)
        _seed_account(conn, "small-checking", balance_cents=150_000)
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            ("hash-checking", "canonical-checking"),
        )
        _seed_snapshots(conn, "checking-1")
        _seed_snapshots(conn, "small-checking", balances=[("2026-02-20", 150_000)])
        conn.commit()

        with pytest.raises(ValidationError, match="account not found"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="missing",
                suggested_transfer_cents=600_000,
                hysa_apy_bps=450,
            )
        with pytest.raises(ValidationError, match="checking account"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="savings-1",
                suggested_transfer_cents=600_000,
                hysa_apy_bps=450,
            )
        with pytest.raises(ValidationError, match="personal checking account"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="business-checking",
                suggested_transfer_cents=600_000,
                hysa_apy_bps=450,
            )
        with pytest.raises(ValidationError, match="canonical account"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="hash-checking",
                suggested_transfer_cents=600_000,
                hysa_apy_bps=450,
            )
        with pytest.raises(ValidationError, match="below minimum_balance_cents"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="small-checking",
                suggested_transfer_cents=100_000,
                hysa_apy_bps=450,
                as_of="2026-05-26",
            )
        with pytest.raises(ValidationError, match="exceeds current balance"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="checking-1",
                suggested_transfer_cents=700_000,
                retained_buffer_cents=200_000,
                hysa_apy_bps=450,
                as_of="2026-05-26",
            )
        with pytest.raises(ValidationError, match="greater than current_apy_bps"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="checking-1",
                suggested_transfer_cents=600_000,
                current_apy_bps=450,
                hysa_apy_bps=450,
                as_of="2026-05-26",
            )
        with pytest.raises(ValidationError, match="YYYY-MM-DD"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="checking-1",
                suggested_transfer_cents=600_000,
                hysa_apy_bps=450,
                as_of="05/26/2026",
            )


def test_flag_account_for_hysa_transfer_requires_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, "checking-1", balance_cents=820_000)
        _seed_account(conn, "new-checking", balance_cents=820_000)
        _seed_account(conn, "stale-checking", balance_cents=820_000)
        _seed_snapshots(conn, "checking-1", balances=[("2026-05-01", 820_000)])
        _seed_snapshots(
            conn,
            "new-checking",
            balances=[
                ("2026-02-20", 820_000),
                ("2026-03-15", 150_000),
                ("2026-05-26", 820_000),
            ],
        )
        _seed_snapshots(
            conn,
            "stale-checking",
            balances=[
                ("2026-02-20", 820_000),
                ("2026-03-15", 820_000),
            ],
        )
        conn.commit()

        with pytest.raises(ValidationError, match="on or before"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="checking-1",
                suggested_transfer_cents=600_000,
                hysa_apy_bps=450,
                as_of="2026-05-26",
            )
        with pytest.raises(ValidationError, match="dropped below minimum_balance_cents"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="new-checking",
                suggested_transfer_cents=600_000,
                hysa_apy_bps=450,
                as_of="2026-05-26",
            )
        with pytest.raises(ValidationError, match="recent snapshot"):
            hysa_transfer_flags.flag_account_for_hysa_transfer(
                conn,
                account_id="stale-checking",
                suggested_transfer_cents=600_000,
                retained_buffer_cents=200_000,
                hysa_apy_bps=450,
                as_of="2026-05-26",
            )


def test_hysa_transfer_flag_tool_is_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    assert "flag_account_for_hysa_transfer" in gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert "flag_account_for_hysa_transfer" in DB_WRITE_TOOLS
