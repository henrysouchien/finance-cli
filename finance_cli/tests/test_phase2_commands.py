from __future__ import annotations

import json
import uuid
from datetime import date, timedelta
from pathlib import Path

import pytest

from finance_cli.__main__ import main
from finance_cli.budget_engine import set_budget
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> dict:
    code = main(args)
    assert code == 0
    output = capsys.readouterr().out
    return json.loads(output)


def _seed_category(
    conn,
    name: str,
    *,
    parent_id: str | None = None,
) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, parent_id, is_system) VALUES (?, ?, ?, 0)",
        (category_id, name, parent_id),
    )
    return category_id


def _seed_transaction(
    conn,
    *,
    txn_date: str,
    description: str,
    amount_cents: int,
    category_id: str,
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (id, date, description, amount_cents, category_id, source)
        VALUES (?, ?, ?, ?, ?, 'manual')
        """,
        (uuid.uuid4().hex, txn_date, description, amount_cents, category_id),
    )


def test_budget_commands_flow(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    _run_cli(["cat", "add", "Dining"], capsys)
    _run_cli(["txn", "add", "--date", date.today().isoformat(), "--description", "Lunch", "--amount", "-45.00", "--category", "Dining"], capsys)
    _run_cli(["budget", "set", "--category", "Dining", "--amount", "200", "--period", "monthly"], capsys)

    month = date.today().strftime("%Y-%m")
    status = _run_cli(["budget", "status", "--month", month], capsys)
    assert status["command"] == "budget.status"
    assert status["status"] == "success"
    assert len(status["data"]["status"]) == 1
    assert status["data"]["status"][0]["category_name"] == "Dining"
    assert status["data"]["status"][0]["group_name"] == "Dining"
    assert status["cli_report"] == "Dining [Personal]: spent=-45.00 budget=200.00 remaining=155.00"

    forecast = _run_cli(["budget", "forecast", "--month", month], capsys)
    assert forecast["status"] == "success"
    assert len(forecast["data"]["forecast"]) == 1
    assert forecast["data"]["forecast"][0]["group_name"] == "Dining"
    assert forecast["cli_report"] == (
        f"Dining: forecast={forecast['data']['forecast'][0]['forecast']:.2f} budget=200.00"
    )

    alerts = _run_cli(["budget", "alerts", "--month", month], capsys)
    assert alerts["command"] == "budget.alerts"
    assert "alerts" in alerts["data"]
    assert "ok_count" in alerts["summary"]

    suggest = _run_cli(["budget", "suggest", "--goal", "savings", "--target", "100"], capsys)
    assert suggest["status"] == "success"
    assert suggest["data"]["target_cents"] == 10000


def test_weekly_compare(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        cat_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Groceries', 0)", (cat_id,))

        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, source)
            VALUES (?, '2025-01-01', 'Market A', -3000, ?, 'manual')
            """,
            (uuid.uuid4().hex, cat_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, source)
            VALUES (?, '2024-12-26', 'Market B', -2000, ?, 'manual')
            """,
            (uuid.uuid4().hex, cat_id),
        )
        conn.commit()

    payload = _run_cli(["weekly", "--week", "2025-W01", "--compare"], capsys)
    assert payload["status"] == "success"
    assert payload["command"] == "weekly"
    assert payload["data"]["categories"][0]["category_name"] == "Groceries"
    assert payload["data"]["categories"][0]["delta_cents"] == -1000


def test_weekly_current_week_appends_budget_alerts(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        _seed_transaction(
            conn,
            txn_date=date.today().isoformat(),
            description="Lunch",
            amount_cents=-15_000,
            category_id=dining_id,
        )
        conn.commit()

    with connect(db_path) as conn:
        set_budget(
            conn,
            category_id=dining_id,
            amount_dollars="100",
            period="monthly",
            effective_from=date.today().replace(day=1).isoformat(),
            use_type="Personal",
        )

    payload = _run_cli(["weekly"], capsys)
    assert payload["status"] == "success"
    assert "budget_alerts" in payload["data"]
    assert any(row["category_name"] == "Dining" for row in payload["data"]["budget_alerts"])
    assert "--- Budget Alerts ---" in payload["cli_report"]


def test_weekly_historical_week_omits_budget_alerts(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        _seed_transaction(
            conn,
            txn_date=date.today().isoformat(),
            description="Lunch",
            amount_cents=-15_000,
            category_id=dining_id,
        )
        conn.commit()

    with connect(db_path) as conn:
        set_budget(
            conn,
            category_id=dining_id,
            amount_dollars="100",
            period="monthly",
            effective_from=date.today().replace(day=1).isoformat(),
            use_type="Personal",
        )

    previous_week = date.today() - timedelta(days=7)
    previous_week_str = f"{previous_week.strftime('%G')}-W{previous_week.strftime('%V')}"
    payload = _run_cli(["weekly", "--week", previous_week_str], capsys)
    assert payload["status"] == "success"
    assert "budget_alerts" not in payload["data"]
    assert "--- Budget Alerts ---" not in payload["cli_report"]


def test_export_commands(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        cat_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Travel', 0)", (cat_id,))
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, source)
            VALUES (?, '2025-02-10', 'Airline', -50000, ?, 'manual')
            """,
            (uuid.uuid4().hex, cat_id),
        )
        conn.commit()

    out_csv = tmp_path / "txns.csv"
    out_summary = tmp_path / "summary.csv"

    result_csv = _run_cli(["export", "csv", "--output", str(out_csv)], capsys)
    assert result_csv["status"] == "success"
    assert out_csv.exists()
    assert out_csv.read_text(encoding="utf-8").count("\n") >= 2

    result_summary = _run_cli(["export", "summary", "--month", "2025-02", "--output", str(out_summary)], capsys)
    assert result_summary["status"] == "success"
    assert out_summary.exists()
    assert "Travel" in out_summary.read_text(encoding="utf-8")


def test_cat_tree(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        parent_id = uuid.uuid4().hex
        child1_id = uuid.uuid4().hex
        child2_id = uuid.uuid4().hex
        standalone_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Food & Drink', 1)", (parent_id,))
        conn.execute("INSERT INTO categories (id, name, is_system, parent_id) VALUES (?, 'Dining', 1, ?)", (child1_id, parent_id))
        conn.execute("INSERT INTO categories (id, name, is_system, parent_id) VALUES (?, 'Groceries', 1, ?)", (child2_id, parent_id))
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Transportation', 1)", (standalone_id,))
        for i in range(3):
            conn.execute(
                "INSERT INTO transactions (id, date, description, amount_cents, category_id, source) VALUES (?, '2025-01-01', 'Tx', -100, ?, 'manual')",
                (uuid.uuid4().hex, child1_id),
            )
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, category_id, source) VALUES (?, '2025-01-01', 'Tx', -100, ?, 'manual')",
            (uuid.uuid4().hex, standalone_id),
        )
        conn.commit()

    result = _run_cli(["cat", "tree"], capsys)
    assert result["status"] == "success"
    tree = result["data"]["tree"]

    # Food & Drink parent with 2 children
    food = next(n for n in tree if n["name"] == "Food & Drink")
    assert food["txn_count"] == 3
    assert len(food["children"]) == 2

    # Standalone has no children
    transport = next(n for n in tree if n["name"] == "Transportation")
    assert transport["txn_count"] == 1
    assert len(transport["children"]) == 0

    # CLI report has tree connectors
    cli = result["cli_report"]
    assert "├── Dining (3)" in cli
    assert "└── Groceries (0)" in cli
    assert "Transportation (1)" in cli


def test_budget_status_and_forecast_grouped_rollup_output(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        food_group_id = _seed_category(conn, "Food & Drink")
        dining_id = _seed_category(conn, "Dining", parent_id=food_group_id)
        groceries_id = _seed_category(conn, "Groceries", parent_id=food_group_id)
        _seed_transaction(
            conn,
            txn_date="2025-01-10",
            description="Lunch",
            amount_cents=-4500,
            category_id=dining_id,
        )
        _seed_transaction(
            conn,
            txn_date="2025-01-12",
            description="Market",
            amount_cents=-35000,
            category_id=groceries_id,
        )
        conn.commit()

    with connect(db_path) as conn:
        set_budget(
            conn,
            category_id=dining_id,
            amount_dollars="200",
            period="monthly",
            effective_from="2025-01-01",
        )
        set_budget(
            conn,
            category_id=groceries_id,
            amount_dollars="400",
            period="monthly",
            effective_from="2025-01-01",
        )

    status = _run_cli(["budget", "status", "--month", "2025-01"], capsys)
    status_rows = status["data"]["status"]
    assert len(status_rows) == 2
    assert {row["group_name"] for row in status_rows} == {"Food & Drink"}
    assert status["cli_report"].splitlines() == [
        "Food & Drink: spent=-395.00 budget=600.00 remaining=205.00",
        "  Groceries [Personal]: spent=-350.00 budget=400.00 remaining=50.00",
        "  Dining [Personal]: spent=-45.00 budget=200.00 remaining=155.00",
    ]

    forecast = _run_cli(["budget", "forecast", "--month", "2025-01"], capsys)
    forecast_rows = forecast["data"]["forecast"]
    assert len(forecast_rows) == 2
    assert {row["group_name"] for row in forecast_rows} == {"Food & Drink"}
    assert forecast["cli_report"].splitlines() == [
        "Food & Drink: forecast=395.00 budget=600.00",
        "  Groceries: forecast=350.00 budget=400.00",
        "  Dining: forecast=45.00 budget=200.00",
    ]


def test_budget_rollup_standalone_category_not_indented(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        travel_id = _seed_category(conn, "Travel")
        _seed_transaction(
            conn,
            txn_date="2025-02-05",
            description="Train",
            amount_cents=-6000,
            category_id=travel_id,
        )
        conn.commit()

    with connect(db_path) as conn:
        set_budget(
            conn,
            category_id=travel_id,
            amount_dollars="100",
            period="monthly",
            effective_from="2025-02-01",
        )

    status = _run_cli(["budget", "status", "--month", "2025-02"], capsys)
    assert status["data"]["status"][0]["group_name"] == "Travel"
    assert status["cli_report"].splitlines() == [
        "Travel [Personal]: spent=-60.00 budget=100.00 remaining=40.00",
    ]

    forecast = _run_cli(["budget", "forecast", "--month", "2025-02"], capsys)
    assert forecast["data"]["forecast"][0]["group_name"] == "Travel"
    assert forecast["cli_report"].splitlines() == [
        "Travel: forecast=60.00 budget=100.00",
    ]


def test_set_budget_rejects_parent_categories(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        parent_id = _seed_category(conn, "Food & Drink")
        _seed_category(conn, "Dining", parent_id=parent_id)
        conn.commit()

        with pytest.raises(ValueError, match="Cannot set budget on parent category 'Food & Drink'"):
            set_budget(
                conn,
                category_id=parent_id,
                amount_dollars="500",
                period="monthly",
                effective_from="2025-01-01",
            )


def test_set_budget_allows_standalone_leaf_category(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        travel_id = _seed_category(conn, "Travel")
        conn.commit()

        budget_id = set_budget(
            conn,
            category_id=travel_id,
            amount_dollars="150",
            period="monthly",
            effective_from="2025-01-01",
        )
        saved = conn.execute("SELECT id FROM budgets WHERE id = ?", (budget_id,)).fetchone()
        assert saved is not None


def test_budget_rollup_ordering_is_deterministic_for_ties(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        alpha_group_id = _seed_category(conn, "Alpha Group")
        beta_group_id = _seed_category(conn, "Beta Group")
        alpha_leaf_id = _seed_category(conn, "Alpha Leaf", parent_id=alpha_group_id)
        beta_leaf_id = _seed_category(conn, "Beta Leaf", parent_id=beta_group_id)
        _seed_transaction(
            conn,
            txn_date="2025-01-03",
            description="Alpha spend",
            amount_cents=-1000,
            category_id=alpha_leaf_id,
        )
        _seed_transaction(
            conn,
            txn_date="2025-01-04",
            description="Beta spend",
            amount_cents=-1000,
            category_id=beta_leaf_id,
        )
        conn.commit()

    with connect(db_path) as conn:
        set_budget(
            conn,
            category_id=alpha_leaf_id,
            amount_dollars="20",
            period="monthly",
            effective_from="2025-01-01",
        )
        set_budget(
            conn,
            category_id=beta_leaf_id,
            amount_dollars="20",
            period="monthly",
            effective_from="2025-01-01",
        )

    status = _run_cli(["budget", "status", "--month", "2025-01"], capsys)
    assert status["cli_report"].splitlines() == [
        "Alpha Group: spent=-10.00 budget=20.00 remaining=10.00",
        "  Alpha Leaf [Personal]: spent=-10.00 budget=20.00 remaining=10.00",
        "Beta Group: spent=-10.00 budget=20.00 remaining=10.00",
        "  Beta Leaf [Personal]: spent=-10.00 budget=20.00 remaining=10.00",
    ]

    forecast = _run_cli(["budget", "forecast", "--month", "2025-01"], capsys)
    assert forecast["cli_report"].splitlines() == [
        "Alpha Group: forecast=10.00 budget=20.00",
        "  Alpha Leaf: forecast=10.00 budget=20.00",
        "Beta Group: forecast=10.00 budget=20.00",
        "  Beta Leaf: forecast=10.00 budget=20.00",
    ]
