from __future__ import annotations

import json
import uuid
from argparse import Namespace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import finance_cli.budget_engine as budget_engine

from finance_cli.__main__ import main
from finance_cli.budget_engine import budget_alerts, set_budget
from finance_cli.commands import notify_cmd
from finance_cli.db import connect, initialize_database


def _init_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _args(**kwargs) -> Namespace:
    return Namespace(**kwargs)


def _freeze_today(monkeypatch, year: int, month: int, day: int) -> None:
    class _FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(year, month, day)

    monkeypatch.setattr(budget_engine, "date", _FrozenDate)


def _seed_budget_spend(
    conn,
    *,
    category_name: str,
    budget_dollars: str,
    spent_cents: int,
    txn_date: str,
    use_type: str = "Personal",
) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, category_name),
    )
    set_budget(
        conn,
        category_id=category_id,
        amount_dollars=budget_dollars,
        period="monthly",
        effective_from=f"{txn_date[:7]}-01",
        use_type=use_type,
    )
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id, source, use_type, is_payment, is_active
        ) VALUES (?, ?, ?, ?, ?, 'manual', ?, 0, 1)
        """,
        (uuid.uuid4().hex, txn_date, f"{category_name} seed", -abs(spent_cents), category_id, use_type),
    )
    conn.commit()
    return category_id


def test_full_pipeline_main_budget_alerts_non_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _init_db(tmp_path, monkeypatch)
    _freeze_today(monkeypatch, 2026, 4, 15)
    sent: dict[str, object] = {}

    with connect(db_path) as conn:
        _seed_budget_spend(
            conn,
            category_name="Dining",
            budget_dollars="100",
            spent_cents=15_000,
            txn_date="2026-04-10",
        )
        notify_cmd.handle_channel_set(
            _args(channel="telegram", config='{"chat_id":"chat-123"}', label="primary"),
            conn,
        )

    def fake_send(message: str, *, channel: str, **creds) -> dict[str, object]:
        sent["message"] = message
        sent["channel"] = channel
        sent["creds"] = creds
        return {"ok": True, "provider": "fake"}

    monkeypatch.setattr(notify_cmd, "_HAS_ALERTS", True)
    monkeypatch.setattr(notify_cmd, "alerts", SimpleNamespace(send=fake_send))

    code = main(
        [
            "notify",
            "budget-alerts",
            "--channel",
            "telegram",
            "--view",
            "personal",
            "--month",
            "2026-04",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "notify.budget_alerts"
    assert payload["summary"]["over_count"] >= 1
    assert sent["channel"] == "telegram"
    assert sent["creds"] == {"chat_id": "chat-123"}
    assert "OVER BUDGET:" in str(sent["message"])


def test_mixed_severity_threshold_boundaries(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path, monkeypatch)
    _freeze_today(monkeypatch, 2026, 4, 15)

    with connect(db_path) as conn:
        _seed_budget_spend(
            conn,
            category_name="Okay",
            budget_dollars="100",
            spent_cents=3_950,
            txn_date="2026-04-05",
        )
        _seed_budget_spend(
            conn,
            category_name="Warning Boundary",
            budget_dollars="100",
            spent_cents=4_000,
            txn_date="2026-04-05",
        )
        _seed_budget_spend(
            conn,
            category_name="Alert Boundary",
            budget_dollars="100",
            spent_cents=5_000,
            txn_date="2026-04-05",
        )
        _seed_budget_spend(
            conn,
            category_name="Over Budget",
            budget_dollars="100",
            spent_cents=12_000,
            txn_date="2026-04-05",
        )

        result = budget_alerts(conn, month="2026-04", view="personal")

    severities = {row["category_name"]: row["severity"] for row in result["alerts"]}
    assert result["ok_count"] == 1
    assert severities == {
        "Warning Boundary": "warn",
        "Alert Boundary": "alert",
        "Over Budget": "over",
    }

    message = notify_cmd.format_budget_alert(result)
    assert "OVER BUDGET:" in message
    assert "AT RISK:" in message
    assert "WARNING:" in message


def test_mcp_channel_lifecycle_envelope(tmp_path: Path, monkeypatch) -> None:
    _init_db(tmp_path, monkeypatch)

    from finance_cli.mcp_server import (
        notify_channel_list,
        notify_channel_remove,
        notify_channel_set,
    )

    created = notify_channel_set(channel="telegram", config='{"chat_id":"123"}', label="test")
    assert created["data"]["updated"] is True
    assert created["summary"]["updated"] is True
    assert "cli_report" not in created

    listed = notify_channel_list()
    assert listed["summary"]["count"] == 1
    assert listed["data"]["channels"][0]["channel"] == "telegram"
    assert listed["data"]["channels"][0]["config"] == {"chat_id": "123"}

    removed = notify_channel_remove(channel="telegram")
    assert removed["data"]["deleted"] is True
    assert "cli_report" not in removed

    listed_after = notify_channel_list()
    assert listed_after["summary"]["count"] == 0
    assert listed_after["data"]["channels"] == []
