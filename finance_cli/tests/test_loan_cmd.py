from __future__ import annotations

import json
import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import balance_cmd, debt_cmd, liability_cmd, loan_cmd, summary_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.debt_calculator import load_debt_cards


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _ns(**kwargs) -> Namespace:
    return Namespace(format="json", **kwargs)


def _extract_loan_id(result: dict) -> str:
    data = result["data"]
    if isinstance(data.get("loan"), dict):
        value = data["loan"].get("id") or data["loan"].get("loan_id")
        if value:
            return str(value)
    for key in ("loan_id", "id"):
        value = data.get(key)
        if value:
            return str(value)
    raise AssertionError(f"Could not find loan id in result payload: {result}")


def _loan_items(result: dict) -> list[dict]:
    data = result["data"]
    items = data.get("loans")
    if isinstance(items, list):
        return items
    items = data.get("items")
    if isinstance(items, list):
        return items
    raise AssertionError(f"Could not find loan list in payload: {result}")


def _seed_transaction(
    conn,
    *,
    amount_cents: int = -25_000,
    date_str: str = "2026-01-15",
    description: str = "Loan payment",
    use_type: str | None = "Personal",
    is_active: int = 1,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id,
            date,
            description,
            amount_cents,
            use_type,
            is_active,
            source
        ) VALUES (?, ?, ?, ?, ?, ?, 'manual')
        """,
        (txn_id, date_str, description, amount_cents, use_type, is_active),
    )
    conn.commit()
    return txn_id


def _add_loan(
    conn,
    *,
    creditor: str = "Mom",
    amount: float = 1_000.0,
    start_date: str = "2026-01-01",
    rate: float = 0.0,
    interest_type: str | None = None,
    monthly_payment: float | None = 100.0,
    due_day: int | None = 1,
    expected_payoff: str | None = "2026-12-01",
    use_type: str = "Personal",
    description: str | None = "Family loan",
) -> tuple[str, dict]:
    result = loan_cmd.handle_add(
        _ns(
            creditor=creditor,
            amount=amount,
            start_date=start_date,
            rate=rate,
            interest_type=interest_type,
            monthly_payment=monthly_payment,
            due_day=due_day,
            expected_payoff=expected_payoff,
            use_type=use_type,
            description=description,
        ),
        conn,
    )
    return _extract_loan_id(result), result


def _loan_row(conn, loan_id: str):
    row = conn.execute("SELECT * FROM manual_loans WHERE id = ?", (loan_id,)).fetchone()
    assert row is not None
    return row


def _schedule(
    conn,
    loan_id: str,
    *,
    months: int = 0,
    summary_only: bool = False,
) -> dict:
    return loan_cmd.handle_schedule(
        _ns(loan_id=loan_id, months=months, summary_only=summary_only),
        conn,
    )


def _event_rows(conn, loan_id: str) -> list:
    return conn.execute(
        """
        SELECT event_type, details
         FROM loan_events
         WHERE loan_id = ?
         ORDER BY rowid ASC
        """,
        (loan_id,),
    ).fetchall()


def test_add_creates_loan_and_initial_disbursement_with_auto_interest_type(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, result = _add_loan(
            conn,
            creditor="Mom",
            amount=1_234.56,
            start_date="2026-01-10",
            rate=0.0,
            interest_type=None,
            monthly_payment=250.25,
            due_day=15,
            expected_payoff="2026-12-15",
            description="House down payment help",
        )

        row = _loan_row(conn, loan_id)
        disbursement = conn.execute(
            "SELECT * FROM loan_disbursements WHERE loan_id = ?",
            (loan_id,),
        ).fetchone()
        assert disbursement is not None

        second_loan_id, _ = _add_loan(
            conn,
            creditor="Friend Bob",
            amount=500.0,
            start_date="2026-02-01",
            rate=3.5,
            interest_type=None,
            monthly_payment=None,
            due_day=None,
            expected_payoff=None,
            description=None,
        )
        second_row = _loan_row(conn, second_loan_id)

    assert "data" in result
    assert "summary" in result
    assert row["creditor_name"] == "Mom"
    assert row["total_disbursed_cents"] == 123_456
    assert row["current_balance_cents"] == 123_456
    assert row["monthly_payment_cents"] == 25_025
    assert row["interest_type"] == "none"
    assert disbursement["amount_cents"] == 123_456
    assert disbursement["disbursement_date"] == "2026-01-10"
    assert second_row["interest_type"] == "simple"


def test_list_shows_active_only_unless_include_inactive(db_path: Path) -> None:
    with connect(db_path) as conn:
        active_loan_id, _ = _add_loan(conn, creditor="Active Loan", amount=800.0)
        closed_loan_id, _ = _add_loan(conn, creditor="Closed Loan", amount=200.0)
        loan_cmd.handle_close(_ns(loan_id=closed_loan_id, forgiven=True), conn)

        active_only = loan_cmd.handle_list(_ns(include_inactive=False), conn)
        all_loans = loan_cmd.handle_list(_ns(include_inactive=True), conn)

    active_ids = {str(item["id"]) for item in _loan_items(active_only)}
    all_ids = {str(item["id"]) for item in _loan_items(all_loans)}

    assert active_loan_id in active_ids
    assert closed_loan_id not in active_ids
    assert {active_loan_id, closed_loan_id}.issubset(all_ids)


def test_show_returns_loan_details_disbursements_payments_and_events(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Show Loan", amount=1_000.0, start_date="2026-01-01")
        loan_cmd.handle_disburse(
            _ns(loan_id=loan_id, amount=250.0, date="2026-01-20", notes="Extra advance"),
            conn,
        )
        loan_cmd.handle_payment(
            _ns(loan_id=loan_id, amount=100.0, date="2026-01-25", transaction=None, notes="Manual payment"),
            conn,
        )
        loan_cmd.handle_adjust(
            _ns(
                loan_id=loan_id,
                rate=2.5,
                interest_type="simple",
                monthly_payment=150.0,
                due_day=20,
                expected_payoff="2026-10-01",
                balance=None,
                description="Updated terms",
            ),
            conn,
        )
        loan_cmd.handle_close(_ns(loan_id=loan_id, forgiven=True), conn)

        result = loan_cmd.handle_show(_ns(loan_id=loan_id), conn)

    data = result["data"]
    loan_payload = data["loan"]
    disbursements = data["disbursements"]
    payments = data["payments"]
    events = data["events"]

    assert loan_payload["id"] == loan_id
    assert len(disbursements) == 2
    assert len(payments) == 1
    assert [event["event_type"] for event in events] == ["adjust", "forgive", "close"]


def test_payment_decrements_balance_clamps_overpayment_and_auto_closes(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Payment Loan", amount=1_000.0)

        loan_cmd.handle_payment(
            _ns(loan_id=loan_id, amount=300.0, date="2026-01-10", transaction=None, notes=None),
            conn,
        )
        mid_row = _loan_row(conn, loan_id)

        loan_cmd.handle_payment(
            _ns(loan_id=loan_id, amount=900.0, date="2026-01-11", transaction=None, notes="Final"),
            conn,
        )
        final_row = _loan_row(conn, loan_id)
        payment_rows = conn.execute(
            """
            SELECT amount_cents
              FROM loan_payments
             WHERE loan_id = ?
             ORDER BY rowid ASC
            """,
            (loan_id,),
        ).fetchall()

    assert mid_row["current_balance_cents"] == 70_000
    assert final_row["current_balance_cents"] == 0
    assert final_row["is_active"] == 0
    assert [int(row["amount_cents"]) for row in payment_rows] == [30_000, 70_000]


def test_payment_transaction_link_defaults_to_transaction_date(db_path: Path, monkeypatch) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Linked Payment", amount=500.0)
        txn_id = _seed_transaction(conn, amount_cents=-12_345, date_str="2026-02-14")
        monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2099-12-31")

        loan_cmd.handle_payment(
            _ns(loan_id=loan_id, amount=123.45, date=None, transaction=txn_id, notes="Matched txn"),
            conn,
        )

        payment = conn.execute(
            """
            SELECT payment_date, transaction_id, amount_cents
              FROM loan_payments
             WHERE loan_id = ?
            """,
            (loan_id,),
        ).fetchone()
        assert payment is not None

    assert payment["payment_date"] == "2026-02-14"
    assert payment["transaction_id"] == txn_id
    assert payment["amount_cents"] == 12_345


def test_payment_uniqueness_rejects_reusing_transaction_id(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_a, _ = _add_loan(conn, creditor="Loan A", amount=400.0)
        loan_b, _ = _add_loan(conn, creditor="Loan B", amount=400.0)
        txn_id = _seed_transaction(conn, amount_cents=-50_00, date_str="2026-02-01")

        loan_cmd.handle_payment(
            _ns(loan_id=loan_a, amount=50.0, date=None, transaction=txn_id, notes=None),
            conn,
        )

        with pytest.raises(Exception):
            loan_cmd.handle_payment(
                _ns(loan_id=loan_b, amount=50.0, date=None, transaction=txn_id, notes=None),
                conn,
            )

        count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM loan_payments WHERE transaction_id = ?",
            (txn_id,),
        ).fetchone()["cnt"]

    assert count == 1


def test_disburse_increments_balance_and_reopens_inactive_loan(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Disburse Loan", amount=1_000.0)
        loan_cmd.handle_close(_ns(loan_id=loan_id, forgiven=True), conn)

        loan_cmd.handle_disburse(
            _ns(loan_id=loan_id, amount=250.0, date="2026-03-01", notes="Borrowed more"),
            conn,
        )

        row = _loan_row(conn, loan_id)
        events = _event_rows(conn, loan_id)

    assert row["current_balance_cents"] == 25_000
    assert row["total_disbursed_cents"] == 125_000
    assert row["is_active"] == 1
    assert [row["event_type"] for row in events][-1] == "reopen"


def test_adjust_updates_fields_logs_before_after_and_balance_zero_auto_closes(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Adjust Loan",
            amount=900.0,
            rate=0.0,
            monthly_payment=90.0,
            due_day=10,
            expected_payoff="2026-11-01",
            description="Old terms",
        )

        loan_cmd.handle_adjust(
            _ns(
                loan_id=loan_id,
                rate=4.5,
                interest_type="simple",
                monthly_payment=120.25,
                due_day=5,
                expected_payoff="2026-09-01",
                balance=0.0,
                description="New terms",
            ),
            conn,
        )

        row = _loan_row(conn, loan_id)
        events = _event_rows(conn, loan_id)
        adjust_details = json.loads(events[0]["details"])

    assert row["interest_rate_pct"] == pytest.approx(4.5)
    assert row["interest_type"] == "simple"
    assert row["monthly_payment_cents"] == 12_025
    assert row["payment_due_day"] == 5
    assert row["expected_payoff_date"] == "2026-09-01"
    assert row["current_balance_cents"] == 0
    assert row["is_active"] == 0
    assert adjust_details["before"]["interest_rate_pct"] == pytest.approx(0.0)
    assert adjust_details["after"]["interest_rate_pct"] == pytest.approx(4.5)
    assert [row["event_type"] for row in events] == ["adjust", "close"]


def test_close_requires_zero_balance_without_forgiven(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Strict Close", amount=750.0)

        with pytest.raises(ValueError, match="balance"):
            loan_cmd.handle_close(_ns(loan_id=loan_id, forgiven=False), conn)

        row = _loan_row(conn, loan_id)

    assert row["current_balance_cents"] == 75_000
    assert row["is_active"] == 1


def test_close_with_forgiven_zeros_balance_and_logs_forgive(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Forgiven Loan", amount=800.0)

        loan_cmd.handle_close(_ns(loan_id=loan_id, forgiven=True), conn)

        row = _loan_row(conn, loan_id)
        events = _event_rows(conn, loan_id)
        forgive_details = json.loads(events[0]["details"])

    assert row["current_balance_cents"] == 0
    assert row["is_active"] == 0
    assert forgive_details["forgiven_amount_cents"] == 80_000
    assert [row["event_type"] for row in events] == ["forgive", "close"]


def test_close_then_disburse_reopens_loan(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Close Reopen", amount=600.0)
        conn.execute(
            """
            UPDATE manual_loans
               SET current_balance_cents = 0,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (loan_id,),
        )
        conn.commit()
        loan_cmd.handle_close(_ns(loan_id=loan_id, forgiven=False), conn)

        loan_cmd.handle_disburse(
            _ns(loan_id=loan_id, amount=150.0, date="2026-04-10", notes="Need more time"),
            conn,
        )

        row = _loan_row(conn, loan_id)
        events = _event_rows(conn, loan_id)

    assert row["current_balance_cents"] == 15_000
    assert row["total_disbursed_cents"] == 75_000
    assert row["is_active"] == 1
    assert [row["event_type"] for row in events] == ["close", "reopen"]


def test_validation_rejects_negative_amounts_negative_rates_and_rate_type_contradictions(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError):
            loan_cmd.handle_add(
                _ns(
                    creditor="Bad Loan",
                    amount=-1.0,
                    start_date="2026-01-01",
                    rate=0.0,
                    interest_type=None,
                    monthly_payment=None,
                    due_day=None,
                    expected_payoff=None,
                    use_type="Personal",
                    description=None,
                ),
                conn,
            )

        with pytest.raises(ValueError):
            loan_cmd.handle_add(
                _ns(
                    creditor="Bad Rate",
                    amount=100.0,
                    start_date="2026-01-01",
                    rate=-1.0,
                    interest_type=None,
                    monthly_payment=None,
                    due_day=None,
                    expected_payoff=None,
                    use_type="Personal",
                    description=None,
                ),
                conn,
            )

        with pytest.raises(ValueError):
            loan_cmd.handle_add(
                _ns(
                    creditor="Contradiction One",
                    amount=100.0,
                    start_date="2026-01-01",
                    rate=0.0,
                    interest_type="simple",
                    monthly_payment=None,
                    due_day=None,
                    expected_payoff=None,
                    use_type="Personal",
                    description=None,
                ),
                conn,
            )

        with pytest.raises(ValueError):
            loan_cmd.handle_add(
                _ns(
                    creditor="Contradiction Two",
                    amount=100.0,
                    start_date="2026-01-01",
                    rate=5.0,
                    interest_type="none",
                    monthly_payment=None,
                    due_day=None,
                    expected_payoff=None,
                    use_type="Personal",
                    description=None,
                ),
                conn,
            )


def test_event_log_records_adjust_close_forgive_and_reopen(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Event Loan", amount=700.0)

        loan_cmd.handle_adjust(
            _ns(
                loan_id=loan_id,
                rate=1.5,
                interest_type="simple",
                monthly_payment=None,
                due_day=None,
                expected_payoff=None,
                balance=None,
                description="Adjusted",
            ),
            conn,
        )
        loan_cmd.handle_close(_ns(loan_id=loan_id, forgiven=True), conn)
        loan_cmd.handle_disburse(
            _ns(loan_id=loan_id, amount=50.0, date="2026-05-01", notes="Reopened"),
            conn,
        )

        events = _event_rows(conn, loan_id)

    assert [row["event_type"] for row in events] == ["adjust", "forgive", "close", "reopen"]


def test_schedule_zero_interest_projects_principal_paydown_and_payoff_date(
    db_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2025-12-15")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Zero Interest",
            amount=1_000.0,
            start_date="2026-01-01",
            monthly_payment=250.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id)

    schedule = result["data"]["schedule"]
    summary = result["data"]["summary"]

    assert len(schedule) == 4
    assert schedule[0] == {
        "month": 1,
        "payment_cents": 25_000,
        "principal_cents": 25_000,
        "interest_cents": 0,
        "remaining_balance_cents": 75_000,
    }
    assert schedule[-1] == {
        "month": 4,
        "payment_cents": 25_000,
        "principal_cents": 25_000,
        "interest_cents": 0,
        "remaining_balance_cents": 0,
    }
    assert summary["total_payments_cents"] == 100_000
    assert summary["total_principal_cents"] == 100_000
    assert summary["total_interest_cents"] == 0
    assert summary["months_to_payoff"] == 4
    assert summary["payoff_date"] == "2026-04-01"


def test_schedule_zero_interest_exact_payoff_two_months(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2025-12-15")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Exact Payoff",
            amount=1_000.0,
            start_date="2026-01-01",
            monthly_payment=500.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id)

    schedule = result["data"]["schedule"]
    summary = result["data"]["summary"]

    assert [row["payment_cents"] for row in schedule] == [50_000, 50_000]
    assert summary["months_to_payoff"] == 2
    assert summary["payoff_date"] == "2026-02-01"


def test_schedule_zero_interest_remainder_adjusts_final_payment(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2025-12-15")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Remainder Payoff",
            amount=1_000.0,
            start_date="2026-01-01",
            monthly_payment=300.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id)

    schedule = result["data"]["schedule"]
    summary = result["data"]["summary"]

    assert [row["payment_cents"] for row in schedule] == [30_000, 30_000, 30_000, 10_000]
    assert schedule[-1]["principal_cents"] == 10_000
    assert summary["months_to_payoff"] == 4
    assert summary["payoff_date"] == "2026-04-01"


def test_schedule_simple_interest_keeps_interest_constant(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2025-12-15")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Simple Interest",
            amount=1_200.0,
            start_date="2026-01-01",
            rate=12.0,
            interest_type="simple",
            monthly_payment=200.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id)

    schedule = result["data"]["schedule"]

    assert schedule[0]["interest_cents"] == 1_200
    assert schedule[1]["interest_cents"] == 1_200
    assert schedule[2]["interest_cents"] == 1_200
    assert schedule[0]["principal_cents"] == 18_800
    assert schedule[1]["principal_cents"] == 18_800
    assert schedule[-1]["payment_cents"] == 8_400


def test_schedule_compound_interest_decreases_interest_each_month(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2025-12-15")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Compound Interest",
            amount=1_200.0,
            start_date="2026-01-01",
            rate=12.0,
            interest_type="compound",
            monthly_payment=200.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id)

    schedule = result["data"]["schedule"]

    assert schedule[0]["interest_cents"] == 1_200
    assert schedule[1]["interest_cents"] == 1_012
    assert schedule[2]["interest_cents"] < schedule[1]["interest_cents"]
    assert schedule[-1]["remaining_balance_cents"] == 0


def test_schedule_no_fixed_payment_returns_message(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Flexible Loan",
            amount=900.0,
            monthly_payment=None,
            due_day=None,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id)

    assert result["data"]["message"] == "No fixed payment schedule. Record payments as they occur."
    assert result["data"]["schedule"] == []


def test_schedule_zero_balance_returns_fully_paid_message(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(conn, creditor="Paid Loan", amount=500.0, expected_payoff=None)
        conn.execute(
            """
            UPDATE manual_loans
               SET current_balance_cents = 0,
                   is_active = 0,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (loan_id,),
        )
        conn.commit()

        result = _schedule(conn, loan_id)

    assert result["data"]["message"] == "Loan is fully paid off."
    assert result["data"]["summary"]["fully_paid_off"] is True
    assert result["data"]["schedule"] == []


def test_schedule_negative_amortization_emits_warning(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Negative Am Loan",
            amount=1_000.0,
            rate=24.0,
            interest_type="simple",
            monthly_payment=10.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id, months=3)

    schedule = result["data"]["schedule"]
    warnings = result["data"]["warnings"]

    assert warnings
    assert "Balance will grow." in warnings[0]
    assert schedule[0]["interest_cents"] == 2_000
    assert schedule[0]["principal_cents"] == -1_000
    assert schedule[0]["remaining_balance_cents"] == 101_000


def test_schedule_auto_projection_caps_at_120_months(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2025-12-15")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Slow Loan",
            amount=1_000.0,
            start_date="2026-01-01",
            monthly_payment=5.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id)

    schedule = result["data"]["schedule"]
    summary = result["data"]["summary"]
    warnings = result["data"]["warnings"]

    assert len(schedule) == 120
    assert summary["fully_paid_off"] is False
    assert summary["months_to_payoff"] == 120
    assert summary["balance_remaining_cents"] == 40_000
    assert summary["payoff_date"] is None
    assert warnings[-1] == "Projection capped at 120 months; balance remaining: $400.00"


def test_schedule_summary_only_truncates_middle_rows(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loan_cmd, "today_iso", lambda: "2025-12-15")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Summary Loan",
            amount=2_000.0,
            start_date="2026-01-01",
            monthly_payment=100.0,
            due_day=1,
            expected_payoff=None,
        )
        result = _schedule(conn, loan_id, summary_only=True)

    schedule = result["data"]["schedule"]
    summary = result["data"]["summary"]

    assert len(schedule) == 13
    assert schedule[0]["month"] == 1
    assert schedule[5]["month"] == 6
    assert schedule[6] == "..."
    assert schedule[7]["month"] == 15
    assert schedule[-1]["month"] == 20
    assert summary["months_to_payoff"] == 20


def test_load_debt_cards_includes_manual_loans(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Aunt May",
            amount=2_500.0,
            rate=4.25,
            monthly_payment=175.0,
            due_day=12,
            expected_payoff=None,
        )

        cards = load_debt_cards(conn)

    loan_card = next(card for card in cards if card.card_id == loan_id)
    assert loan_card.label == "Loan: Aunt May"
    assert loan_card.balance_cents == 250_000
    assert loan_card.apr == pytest.approx(4.25)
    assert loan_card.min_payment_cents == 17_500
    assert loan_card.limit_cents is None


def test_load_debt_cards_preserves_zero_apr_for_manual_loans(db_path: Path) -> None:
    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Dad",
            amount=900.0,
            rate=0.0,
            interest_type="none",
            monthly_payment=90.0,
            due_day=5,
            expected_payoff=None,
        )

        cards = load_debt_cards(conn)

    loan_card = next(card for card in cards if card.card_id == loan_id)
    assert loan_card.apr == 0.0


def test_handle_net_worth_includes_manual_loans(db_path: Path) -> None:
    with connect(db_path) as conn:
        _add_loan(
            conn,
            creditor="Family Loan",
            amount=1_200.0,
            monthly_payment=100.0,
            due_day=1,
            expected_payoff=None,
        )

        result = balance_cmd.handle_net_worth(_ns(exclude_investments=False, view="all"), conn)

    assert result["data"]["manual_loans_cents"] == 120_000
    assert result["data"]["liabilities_cents"] == 120_000
    assert result["data"]["net_worth_cents"] == -120_000
    breakdown = {row["account_type"]: row["balance_cents"] for row in result["data"]["breakdown"]}
    assert breakdown["manual_loans"] == -120_000


def test_manual_loan_view_filtering_applies_to_net_worth_and_summary(db_path: Path) -> None:
    with connect(db_path) as conn:
        _add_loan(
            conn,
            creditor="Personal Loan",
            amount=1_000.0,
            monthly_payment=100.0,
            due_day=3,
            expected_payoff=None,
            use_type="Personal",
        )
        _add_loan(
            conn,
            creditor="Business Loan",
            amount=2_500.0,
            monthly_payment=250.0,
            due_day=9,
            expected_payoff=None,
            use_type="Business",
        )

        personal_worth = balance_cmd.handle_net_worth(_ns(exclude_investments=False, view="personal"), conn)
        business_worth = balance_cmd.handle_net_worth(_ns(exclude_investments=False, view="business"), conn)
        personal_summary = summary_cmd.handle_summary(_ns(view="personal"), conn)
        business_summary = summary_cmd.handle_summary(_ns(view="business"), conn)

    assert personal_worth["data"]["manual_loans_cents"] == 100_000
    assert personal_worth["data"]["liabilities_cents"] == 100_000
    assert personal_worth["data"]["net_worth_cents"] == -100_000
    assert business_worth["data"]["manual_loans_cents"] == 250_000
    assert business_worth["data"]["liabilities_cents"] == 250_000
    assert business_worth["data"]["net_worth_cents"] == -250_000

    assert personal_summary["data"]["manual_loans_cents"] == 100_000
    assert personal_summary["data"]["total_debt_cents"] == 100_000
    assert personal_summary["data"]["debt_minimums_cents"] == 10_000
    assert personal_summary["data"]["fixed_obligations_cents"] == 10_000
    assert business_summary["data"]["manual_loans_cents"] == 250_000
    assert business_summary["data"]["total_debt_cents"] == 250_000
    assert business_summary["data"]["debt_minimums_cents"] == 25_000
    assert business_summary["data"]["fixed_obligations_cents"] == 25_000


def test_liability_obligations_include_manual_loans(db_path: Path) -> None:
    with connect(db_path) as conn:
        _add_loan(
            conn,
            creditor="Bridge Loan",
            amount=3_000.0,
            monthly_payment=275.0,
            due_day=18,
            expected_payoff=None,
        )

        result = liability_cmd.handle_obligations(_ns(), conn)

    data = result["data"]
    assert data["manual_loan_total_cents"] == 27_500
    assert data["grand_total_cents"] == 27_500
    assert data["manual_loans"][0]["creditor_name"] == "Bridge Loan"
    assert "Manual Loans:" in result["cli_report"]
    assert "Bridge Loan" in result["cli_report"]


def test_liability_upcoming_includes_manual_loans_with_clamped_due_dates(
    db_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(liability_cmd, "today_iso", lambda: "2026-02-10")

    with connect(db_path) as conn:
        loan_id, _ = _add_loan(
            conn,
            creditor="Mom",
            amount=1_500.0,
            monthly_payment=150.0,
            due_day=31,
            expected_payoff=None,
        )

        result = liability_cmd.handle_upcoming(_ns(days=30, type=None), conn)

    upcoming = result["data"]["upcoming"]
    loan_item = next(item for item in upcoming if item.get("loan_id") == loan_id)
    assert loan_item["next_payment_due_date"] == "2026-02-28"
    assert loan_item["payment_due_cents"] == 15_000
    assert loan_item["institution_name"] == "Manual Loan"
    assert loan_item["account_name"] == "Mom"
    assert result["data"]["total_due_cents"] == 15_000


def test_debt_dashboard_cli_uses_generic_text_for_manual_loans(db_path: Path) -> None:
    with connect(db_path) as conn:
        _add_loan(
            conn,
            creditor="Cousin",
            amount=800.0,
            rate=6.0,
            interest_type="simple",
            monthly_payment=80.0,
            due_day=15,
            expected_payoff=None,
        )

        result = debt_cmd.handle_dashboard(_ns(include_zero_balance=False, sort="balance"), conn)

    cli_report = result["cli_report"].lower()
    assert "loan: cousin" in cli_report
    assert "credit card" not in cli_report
