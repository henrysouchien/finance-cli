from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import debt_cmd
from finance_cli.db import connect, initialize_database


def _seed_credit_account(
    conn,
    *,
    institution_name: str,
    account_name: str,
    card_ending: str,
    balance_current_cents: int,
    balance_limit_cents: int | None = None,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            card_ending, balance_current_cents, balance_limit_cents, is_active
        ) VALUES (?, ?, ?, 'credit_card', ?, ?, ?, 1)
        """,
        (
            account_id,
            institution_name,
            account_name,
            card_ending,
            balance_current_cents,
            balance_limit_cents,
        ),
    )
    conn.commit()
    return account_id


def _seed_credit_liability(
    conn,
    *,
    account_id: str,
    apr_purchase: float | None,
    minimum_payment_cents: int | None = None,
    next_monthly_payment_cents: int | None = None,
) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active,
            apr_purchase, minimum_payment_cents, next_monthly_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, ?, ?)
        """,
        (liability_id, account_id, apr_purchase, minimum_payment_cents, next_monthly_payment_cents),
    )
    conn.commit()
    return liability_id


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def test_debt_dashboard_returns_expected_structure(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Freedom",
            card_ending="1234",
            balance_current_cents=-50_000,
            balance_limit_cents=100_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=19.99, minimum_payment_cents=2_000)

        result = debt_cmd.handle_dashboard(
            Namespace(include_zero_balance=False, sort="balance", format="json"),
            conn,
        )

    assert "data" in result
    assert "summary" in result
    assert "cli_report" in result
    assert result["summary"]["total_cards"] == 1
    assert result["data"]["cards"][0]["apr"] == pytest.approx(19.99)


def test_debt_interest_months_six_returns_projection(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Barclays",
            account_name="View",
            card_ending="9876",
            balance_current_cents=-25_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=24.24, minimum_payment_cents=1_000)

        result = debt_cmd.handle_interest(Namespace(months=6, format="json"), conn)

    assert result["summary"]["months"] == 6
    assert len(result["data"]["schedule"]) == 6
    assert result["data"]["total_interest_cents"] > 0


def test_debt_simulate_compare_returns_both_strategies(db_path: Path) -> None:
    with connect(db_path) as conn:
        a1 = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Sapphire",
            card_ending="1111",
            balance_current_cents=-40_000,
        )
        a2 = _seed_credit_account(
            conn,
            institution_name="Amex",
            account_name="Gold",
            card_ending="2222",
            balance_current_cents=-10_000,
        )
        _seed_credit_liability(conn, account_id=a1, apr_purchase=29.99, minimum_payment_cents=1_000)
        _seed_credit_liability(conn, account_id=a2, apr_purchase=8.99, minimum_payment_cents=500)

        result = debt_cmd.handle_simulate(
            Namespace(extra=500.0, strategy="compare", format="json"),
            conn,
        )

    assert "avalanche" in result["data"]
    assert "snowball" in result["data"]
    assert "baseline" in result["data"]


def test_debt_commands_graceful_on_empty_database(db_path: Path) -> None:
    with connect(db_path) as conn:
        dashboard = debt_cmd.handle_dashboard(
            Namespace(include_zero_balance=False, sort="balance", format="json"),
            conn,
        )
        interest = debt_cmd.handle_interest(Namespace(months=6, format="json"), conn)
        compare = debt_cmd.handle_simulate(Namespace(extra=500.0, strategy="compare", format="json"), conn)
        avalanche = debt_cmd.handle_simulate(Namespace(extra=500.0, strategy="avalanche", format="json"), conn)
        snowball = debt_cmd.handle_simulate(Namespace(extra=500.0, strategy="snowball", format="json"), conn)

    assert dashboard["data"]["cards"] == []
    assert dashboard["summary"]["total_cards"] == 0
    assert interest["data"]["total_interest_cents"] == 0
    assert compare["data"]["baseline"]["months"] == 0
    assert avalanche["data"]["months_to_payoff"] == 0
    assert snowball["data"]["months_to_payoff"] == 0


def test_missing_liability_data_is_unknown_apr(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_credit_account(
            conn,
            institution_name="Apple",
            account_name="Apple Card",
            card_ending="3333",
            balance_current_cents=-12_345,
        )

        dashboard = debt_cmd.handle_dashboard(
            Namespace(include_zero_balance=False, sort="balance", format="json"),
            conn,
        )

    assert dashboard["summary"]["apr_unknown_count"] == 1
    assert dashboard["data"]["cards"][0]["apr"] is None


def test_handler_validation_for_sort_and_strategy(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="sort"):
            debt_cmd.handle_dashboard(
                Namespace(include_zero_balance=False, sort="bad-sort", format="json"),
                conn,
            )

        with pytest.raises(ValueError, match="strategy"):
            debt_cmd.handle_simulate(
                Namespace(extra=100.0, strategy="not-real", format="json"),
                conn,
            )
