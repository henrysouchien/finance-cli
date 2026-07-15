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
    intro_apr_end_date: str | None = None,
) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active,
            apr_purchase, minimum_payment_cents, next_monthly_payment_cents, intro_apr_end_date
        ) VALUES (?, ?, 'credit', 1, ?, ?, ?, ?)
        """,
        (
            liability_id,
            account_id,
            apr_purchase,
            minimum_payment_cents,
            next_monthly_payment_cents,
            intro_apr_end_date,
        ),
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
        _seed_credit_liability(
            conn,
            account_id=account_id,
            apr_purchase=19.99,
            minimum_payment_cents=2_000,
            intro_apr_end_date="2026-12-31",
        )

        result = debt_cmd.handle_dashboard(
            Namespace(include_zero_balance=False, sort="balance", format="json"),
            conn,
        )

    assert "data" in result
    assert "summary" in result
    assert "cli_report" in result
    assert result["summary"]["total_cards"] == 1
    assert result["data"]["cards"][0]["card_id"] == account_id
    assert result["data"]["cards"][0]["apr"] == pytest.approx(19.99)
    assert result["data"]["cards"][0]["intro_apr_end_date"] == "2026-12-31"


def test_debt_set_apr_updates_existing_credit_liability(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Freedom",
            card_ending="1234",
            balance_current_cents=-50_000,
        )
        liability_id = _seed_credit_liability(
            conn,
            account_id=account_id,
            apr_purchase=19.99,
            minimum_payment_cents=2_000,
        )

        result = debt_cmd.handle_set_apr(
            Namespace(account=account_id, apr=24.49, dry_run=False, format="json"),
            conn,
        )
        row = conn.execute(
            "SELECT id, apr_purchase FROM liabilities WHERE account_id = ?",
            (account_id,),
        ).fetchone()

    assert result["data"]["liability_id"] == liability_id
    assert result["data"]["previous_apr"] == pytest.approx(19.99)
    assert result["data"]["apr"] == pytest.approx(24.49)
    assert result["data"]["created"] is False
    assert row["id"] == liability_id
    assert row["apr_purchase"] == pytest.approx(24.49)


def test_debt_set_apr_creates_credit_liability_when_missing(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Store",
            account_name="Card",
            card_ending="9999",
            balance_current_cents=-10_000,
        )

        result = debt_cmd.handle_set_apr(
            Namespace(account=account_id, apr=31.24, dry_run=False, format="json"),
            conn,
        )
        row = conn.execute(
            """
            SELECT account_id, liability_type, is_active, apr_purchase
              FROM liabilities
             WHERE id = ?
            """,
            (result["data"]["liability_id"],),
        ).fetchone()

    assert result["data"]["previous_apr"] is None
    assert result["data"]["created"] is True
    assert row["account_id"] == account_id
    assert row["liability_type"] == "credit"
    assert row["is_active"] == 1
    assert row["apr_purchase"] == pytest.approx(31.24)


def test_debt_set_apr_dry_run_does_not_write(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Amex",
            account_name="Gold",
            card_ending="0005",
            balance_current_cents=-15_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=18.99)

        result = debt_cmd.handle_set_apr(
            Namespace(account=account_id, apr=27.99, dry_run=True, format="json"),
            conn,
        )
        row = conn.execute(
            "SELECT apr_purchase FROM liabilities WHERE account_id = ?",
            (account_id,),
        ).fetchone()

    assert result["data"]["dry_run"] is True
    assert result["data"]["previous_apr"] == pytest.approx(18.99)
    assert row["apr_purchase"] == pytest.approx(18.99)


def test_debt_set_apr_rejects_non_credit_card_account(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type,
                balance_current_cents, is_active
            ) VALUES (?, 'Bank', 'Checking', 'checking', 10000, 1)
            """,
            (account_id,),
        )
        conn.commit()

        with pytest.raises(ValueError, match="credit_card account"):
            debt_cmd.handle_set_apr(
                Namespace(account=account_id, apr=24.99, dry_run=False, format="json"),
                conn,
            )


def test_debt_portion_add_persists_and_dashboard_uses_portion(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Amex",
            account_name="Gold",
            card_ending="0005",
            balance_current_cents=-100_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=20.49)

        result = debt_cmd.handle_portion_add(
            Namespace(
                account=account_id,
                label="Plan It",
                principal=1000.0,
                apr=10.0,
                monthly_payment=89.6,
                portion_type="installment",
                promo_end_date=None,
                expected_payoff_date="2028-03-11",
                notes="March plan",
                dry_run=False,
                format="json",
            ),
            conn,
        )
        row = conn.execute(
            """
            SELECT account_id, label, principal_cents, apr_pct,
                   monthly_payment_cents, promo_end_date, notes, is_active
              FROM debt_balance_portions
             WHERE id = ?
            """,
            (result["data"]["id"],),
        ).fetchone()
        dashboard = debt_cmd.handle_dashboard(
            Namespace(include_zero_balance=False, sort="apr", format="json"),
            conn,
        )

    assert result["data"]["principal_cents"] == 100_000
    assert row["account_id"] == account_id
    assert row["label"] == "Plan It"
    assert row["principal_cents"] == 100_000
    assert row["apr_pct"] == pytest.approx(10.0)
    assert row["monthly_payment_cents"] == 8_960
    assert row["promo_end_date"] == "2028-03-11"
    assert row["notes"] == "March plan"
    assert row["is_active"] == 1
    assert dashboard["data"]["cards"][0]["portion_id"] == result["data"]["id"]
    assert dashboard["data"]["cards"][0]["apr"] == pytest.approx(10.0)


def test_debt_portion_add_dry_run_and_alias_rejection(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Amex",
            account_name="Gold",
            card_ending="0005",
            balance_current_cents=-100_000,
        )

        dry_run = debt_cmd.handle_portion_add(
            Namespace(
                account=account_id,
                label="Plan It",
                principal=1000.0,
                apr=10.0,
                monthly_payment=None,
                portion_type="installment",
                promo_end_date=None,
                expected_payoff_date=None,
                notes=None,
                dry_run=True,
                format="json",
            ),
            conn,
        )
        count = conn.execute("SELECT COUNT(*) AS count FROM debt_balance_portions").fetchone()["count"]

        alias_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type,
                balance_current_cents, is_active
            ) VALUES (?, 'Alias Bank', 'Alias Card', 'credit_card', -100000, 1)
            """,
            (alias_id,),
        )
        conn.execute(
            """
            INSERT INTO account_aliases (hash_account_id, canonical_id)
            VALUES (?, ?)
            """,
            (alias_id, account_id),
        )
        conn.commit()

        with pytest.raises(ValueError, match="canonical credit-card account"):
            debt_cmd.handle_portion_add(
                Namespace(
                    account=alias_id,
                    label="Hidden Plan",
                    principal=100.0,
                    apr=10.0,
                    monthly_payment=None,
                    portion_type="installment",
                    promo_end_date=None,
                    expected_payoff_date=None,
                    notes=None,
                    dry_run=False,
                    format="json",
                ),
                conn,
            )

    assert dry_run["data"]["dry_run"] is True
    assert count == 0


def test_debt_portion_list_update_and_deactivate(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Amex",
            account_name="Gold",
            card_ending="0005",
            balance_current_cents=-100_000,
        )
        created = debt_cmd.handle_portion_add(
            Namespace(
                account=account_id,
                label="Plan It",
                principal=1000.0,
                apr=10.0,
                monthly_payment=89.6,
                portion_type="installment",
                promo_end_date="2028-03-11",
                expected_payoff_date=None,
                notes="March plan",
                dry_run=False,
                format="json",
            ),
            conn,
        )
        portion_id = created["data"]["id"]

        dry_update = debt_cmd.handle_portion_update(
            Namespace(
                portion_id=portion_id,
                label="Dry Label",
                principal=None,
                apr=None,
                monthly_payment=None,
                clear_monthly_payment=False,
                portion_type=None,
                promo_end_date=None,
                expected_payoff_date=None,
                clear_promo_end_date=False,
                notes=None,
                clear_notes=False,
                dry_run=True,
                format="json",
            ),
            conn,
        )
        row_after_dry = conn.execute(
            "SELECT label FROM debt_balance_portions WHERE id = ?",
            (portion_id,),
        ).fetchone()

        update = debt_cmd.handle_portion_update(
            Namespace(
                portion_id=portion_id,
                label="Plan It Revised",
                principal=900.0,
                apr=9.5,
                monthly_payment=None,
                clear_monthly_payment=True,
                portion_type="promotional",
                promo_end_date=None,
                expected_payoff_date=None,
                clear_promo_end_date=True,
                notes=None,
                clear_notes=True,
                dry_run=False,
                format="json",
            ),
            conn,
        )
        updated_row = conn.execute(
            """
            SELECT label, principal_cents, apr_pct, monthly_payment_cents,
                   portion_type, promo_end_date, notes
              FROM debt_balance_portions
             WHERE id = ?
            """,
            (portion_id,),
        ).fetchone()

        active_list = debt_cmd.handle_portion_list(
            Namespace(account=account_id, include_inactive=False, format="json"),
            conn,
        )
        dry_deactivate = debt_cmd.handle_portion_deactivate(
            Namespace(portion_id=portion_id, dry_run=True, format="json"),
            conn,
        )
        row_after_dry_deactivate = conn.execute(
            "SELECT is_active FROM debt_balance_portions WHERE id = ?",
            (portion_id,),
        ).fetchone()
        deactivate = debt_cmd.handle_portion_deactivate(
            Namespace(portion_id=portion_id, dry_run=False, format="json"),
            conn,
        )
        inactive_list = debt_cmd.handle_portion_list(
            Namespace(account=account_id, include_inactive=False, format="json"),
            conn,
        )
        all_list = debt_cmd.handle_portion_list(
            Namespace(account=account_id, include_inactive=True, format="json"),
            conn,
        )
        second_deactivate = debt_cmd.handle_portion_deactivate(
            Namespace(portion_id=portion_id, dry_run=False, format="json"),
            conn,
        )

    assert dry_update["data"]["dry_run"] is True
    assert row_after_dry["label"] == "Plan It"
    assert update["summary"]["fields_changed"] == 7
    assert updated_row["label"] == "Plan It Revised"
    assert updated_row["principal_cents"] == 90_000
    assert updated_row["apr_pct"] == pytest.approx(9.5)
    assert updated_row["monthly_payment_cents"] is None
    assert updated_row["portion_type"] == "promotional"
    assert updated_row["promo_end_date"] is None
    assert updated_row["notes"] is None
    assert active_list["summary"]["total_count"] == 1
    assert dry_deactivate["data"]["dry_run"] is True
    assert row_after_dry_deactivate["is_active"] == 1
    assert deactivate["data"]["is_active"] is False
    assert inactive_list["summary"]["total_count"] == 0
    assert all_list["summary"]["total_count"] == 1
    assert second_deactivate["data"]["no_changes"] is True


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
