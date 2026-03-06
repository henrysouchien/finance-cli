from __future__ import annotations

import json
import uuid
from datetime import date, timedelta
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> dict:
    code = main(args)
    assert code == 0
    output = capsys.readouterr().out
    return json.loads(output)


def test_subscriptions_detect_total_and_cancel(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        cat_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Streaming', 0)", (cat_id,))
        for days_ago in (90, 60, 30):
            txn_date = (date.today() - timedelta(days=days_ago)).isoformat()
            conn.execute(
                """
                INSERT INTO transactions (id, date, description, amount_cents, category_id, source)
                VALUES (?, ?, 'NETFLIX.COM', -1500, ?, 'manual')
                """,
                (uuid.uuid4().hex, txn_date, cat_id),
            )
        conn.commit()

    detect = _run_cli(["subs", "detect"], capsys)
    assert detect["status"] == "success"
    assert detect["data"]["detected"] >= 1

    listed = _run_cli(["subs", "list"], capsys)
    assert listed["status"] == "success"
    assert len(listed["data"]["subscriptions"]) >= 1

    total = _run_cli(["subs", "total"], capsys)
    assert total["status"] == "success"
    assert total["data"]["monthly_burn_cents"] > 0

    sub_id = listed["data"]["subscriptions"][0]["id"]
    canceled = _run_cli(["subs", "cancel", sub_id], capsys)
    assert canceled["status"] == "success"


def test_plan_create_show_review(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    current_month = date.today().strftime("%Y-%m")

    with connect(db_path) as conn:
        # Seed prior months so plan create has history.
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, source) VALUES (?, date('now', '-1 month', '+5 day'), 'Salary', 500000, 'manual')",
            (uuid.uuid4().hex,),
        )
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, source) VALUES (?, date('now', '-1 month', '+10 day'), 'Rent', -200000, 'manual')",
            (uuid.uuid4().hex,),
        )
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, source) VALUES (?, date('now', '-2 month', '+5 day'), 'Salary', 500000, 'manual')",
            (uuid.uuid4().hex,),
        )
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, source) VALUES (?, date('now', '-2 month', '+10 day'), 'Rent', -210000, 'manual')",
            (uuid.uuid4().hex,),
        )
        conn.commit()

    created = _run_cli(["plan", "create", "--month", current_month], capsys)
    assert created["status"] == "success"

    shown = _run_cli(["plan", "show", "--month", current_month], capsys)
    assert shown["status"] == "success"
    assert shown["data"]["plan"]["month"] == current_month

    reviewed = _run_cli(["plan", "review"], capsys)
    assert reviewed["status"] == "success"
    assert reviewed["data"]["review"]["month"] == current_month


def test_liquidity_snapshot_command(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, source) VALUES (?, date('now', '-20 day'), 'Paycheck', 300000, 'manual')",
            (uuid.uuid4().hex,),
        )
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, source) VALUES (?, date('now', '-10 day'), 'Groceries', -45000, 'manual')",
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO subscriptions (id, vendor_name, amount_cents, frequency, is_active)
            VALUES (?, 'Music', 1099, 'monthly', 1)
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    payload = _run_cli(["liquidity", "--forecast", "30"], capsys)
    assert payload["status"] == "success"
    assert payload["command"] == "liquidity"
    assert "projected_net_cents" in payload["data"]
    assert "liquid_balance_cents" in payload["data"]
    assert "credit_owed_cents" in payload["data"]
    assert "upcoming_liability_payments_cents" in payload["data"]
