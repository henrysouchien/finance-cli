from __future__ import annotations

import uuid
from pathlib import Path

from finance_cli.budget_engine import set_budget
from finance_cli.db import connect, initialize_database
from finance_cli.models import cents_to_dollars, dollars_to_cents, normalize_date


def test_cents_round_trip() -> None:
    value = "1234.56"
    cents = dollars_to_cents(value)
    dollars = cents_to_dollars(cents)

    assert cents == 123456
    assert dollars == 1234.56


def test_set_budget_upserts_existing_active_budget(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        cat_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Housing', 0)", (cat_id,))
        conn.commit()

        first_id = set_budget(conn, category_id=cat_id, amount_dollars="2000", period="monthly", effective_from="2025-01-01")
        second_id = set_budget(conn, category_id=cat_id, amount_dollars="2100", period="monthly", effective_from="2025-01-01")

        assert first_id == second_id
        row = conn.execute("SELECT amount_cents FROM budgets WHERE id = ?", (first_id,)).fetchone()
        assert int(row["amount_cents"]) == 210_000


def test_normalize_date_accepts_single_digit_month_day() -> None:
    assert normalize_date("1/5/2025") == "2025-01-05"


def test_normalize_date_accepts_two_digit_year() -> None:
    assert normalize_date("11/20/25") == "2025-11-20"
