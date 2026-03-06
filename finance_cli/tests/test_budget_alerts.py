from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pytest
import finance_cli.budget_engine as budget_engine
from finance_cli.budget_engine import set_budget
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_txn(conn, *, category_id: str, txn_date: str, amount_cents: int, use_type: str | None = "Personal") -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id, source, use_type, is_payment, is_active
        ) VALUES (?, ?, 'seed', ?, ?, 'manual', ?, 0, 1)
        """,
        (uuid.uuid4().hex, txn_date, amount_cents, category_id, use_type),
    )
    conn.commit()


def _freeze_today(monkeypatch, year: int, month: int, day: int) -> None:
    class _FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(year, month, day)

    monkeypatch.setattr(budget_engine, "date", _FrozenDate)


def test_budget_alerts_all_ok(db_path: Path, monkeypatch) -> None:
    _freeze_today(monkeypatch, 2026, 3, 15)
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="100",
            period="monthly",
            effective_from="2026-03-01",
            use_type="Personal",
        )
        _seed_txn(conn, category_id=category_id, txn_date="2026-03-05", amount_cents=-1_000)

        result = budget_engine.budget_alerts(conn, month="2026-03")

    assert result["alerts"] == []
    assert result["ok_count"] == 1
    assert result["over_count"] == 0
    assert result["alert_count"] == 0
    assert result["warn_count"] == 0


def test_budget_alerts_over_budget(db_path: Path, monkeypatch) -> None:
    _freeze_today(monkeypatch, 2026, 3, 15)
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Shopping")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="100",
            period="monthly",
            effective_from="2026-03-01",
            use_type="Personal",
        )
        _seed_txn(conn, category_id=category_id, txn_date="2026-03-06", amount_cents=-12_000)

        result = budget_engine.budget_alerts(conn, month="2026-03")

    assert result["over_count"] == 1
    assert result["alert_count"] == 0
    assert result["warn_count"] == 0
    assert result["ok_count"] == 0
    assert len(result["alerts"]) == 1
    assert result["alerts"][0]["severity"] == "over"


def test_budget_alerts_early_month_low_confidence(db_path: Path, monkeypatch) -> None:
    _freeze_today(monkeypatch, 2026, 3, 2)
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Coffee")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="100",
            period="monthly",
            effective_from="2026-03-01",
            use_type="Personal",
        )
        _seed_txn(conn, category_id=category_id, txn_date="2026-03-02", amount_cents=-1_000)

        result = budget_engine.budget_alerts(conn, month="2026-03")

    assert result["days_elapsed"] == 2
    assert result["low_confidence"] is True
    assert result["alert_count"] == 1
    assert result["alerts"][0]["severity"] == "alert"


def test_budget_alerts_past_month_uses_full_month_elapsed(db_path: Path, monkeypatch) -> None:
    _freeze_today(monkeypatch, 2026, 3, 15)
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Groceries")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="100",
            period="monthly",
            effective_from="2026-02-01",
            use_type="Personal",
        )
        _seed_txn(conn, category_id=category_id, txn_date="2026-02-20", amount_cents=-9_000)

        result = budget_engine.budget_alerts(conn, month="2026-02")

    assert result["days_elapsed"] == 28
    assert result["days_remaining"] == 0
    assert result["low_confidence"] is False
    assert result["warn_count"] == 1
    assert result["alerts"][0]["severity"] == "warn"


def test_budget_alerts_skips_zero_budget_rows(db_path: Path, monkeypatch) -> None:
    _freeze_today(monkeypatch, 2026, 3, 15)
    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Travel")
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="0",
            period="monthly",
            effective_from="2026-03-01",
            use_type="Personal",
        )
        _seed_txn(conn, category_id=category_id, txn_date="2026-03-10", amount_cents=-5_000)

        result = budget_engine.budget_alerts(conn, month="2026-03")

    assert result["alerts"] == []
    assert result["ok_count"] == 0
    assert result["over_count"] == 0
    assert result["alert_count"] == 0
    assert result["warn_count"] == 0


def test_budget_alerts_validates_threshold_order(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="0 < warn_pct < alert_pct"):
            budget_engine.budget_alerts(conn, warn_pct=1.0, alert_pct=1.0)
