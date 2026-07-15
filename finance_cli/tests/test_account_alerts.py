from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli import account_alerts
from finance_cli.db import connect, initialize_database


def _seed_account(conn, *, account_id: str = "checking-1", account_type: str = "checking", balance_cents: int = 40_000) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Test Bank', 'Checking', ?, ?, 1)
        """,
        (account_id, account_type, balance_cents),
    )
    conn.commit()


def test_set_low_balance_alert_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_account(conn)
        first = account_alerts.set_low_balance_alert(
            conn,
            account_id="checking-1",
            threshold_cents=50_000,
            channel="telegram",
        )
        second = account_alerts.set_low_balance_alert(
            conn,
            account_id="checking-1",
            threshold_cents=75_000,
            channel="telegram",
            label="Rent buffer",
        )
        rows = conn.execute("SELECT threshold_cents, label FROM account_alert_rules").fetchall()

    assert first["summary"]["configured"] == 1
    assert second["summary"]["id"] == first["summary"]["id"]
    assert len(rows) == 1
    assert rows[0]["threshold_cents"] == 75_000
    assert rows[0]["label"] == "Rent buffer"


def test_low_balance_alert_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_account(conn, account_id="credit-1", account_type="credit_card", balance_cents=-10_000)
        with pytest.raises(ValueError, match="greater than 0"):
            account_alerts.set_low_balance_alert(conn, account_id="credit-1", threshold_cents=0)
        with pytest.raises(ValueError, match="checking or savings"):
            account_alerts.set_low_balance_alert(conn, account_id="credit-1", threshold_cents=50_000)
        with pytest.raises(ValueError, match="Unsupported notification channel"):
            account_alerts.set_low_balance_alert(conn, account_id="credit-1", threshold_cents=50_000, channel="slack")


def test_evaluate_account_alert_rules_dry_run_does_not_mark_triggered(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_account(conn, balance_cents=40_000)
        account_alerts.set_low_balance_alert(conn, account_id="checking-1", threshold_cents=50_000)

        result = account_alerts.evaluate_account_alert_rules(
            conn,
            now="2026-05-26T10:00:00",
            dry_run=True,
        )
        row = conn.execute("SELECT last_triggered_at FROM account_alert_rules").fetchone()

    assert result["checked_count"] == 1
    assert len(result["previews"]) == 1
    assert result["sent"] == []
    assert row["last_triggered_at"] is None


def test_evaluate_account_alert_rules_sends_and_respects_cooldown(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    sent_messages: list[dict[str, object]] = []

    def fake_send(message: str, *, channel: str, **kwargs):
        sent_messages.append({"message": message, "channel": channel, "kwargs": kwargs})
        return {"ok": True}

    monkeypatch.setattr(account_alerts, "_HAS_ALERTS", True)
    monkeypatch.setattr(account_alerts, "alerts", SimpleNamespace(send=fake_send))

    with connect(db_path) as conn:
        _seed_account(conn, balance_cents=40_000)
        conn.execute(
            """
            INSERT INTO notification_channels (channel, config, label)
            VALUES ('telegram', ?, 'primary')
            """,
            (json.dumps({"chat_id": "123"}),),
        )
        account_alerts.set_low_balance_alert(
            conn,
            account_id="checking-1",
            threshold_cents=50_000,
            label="Rent buffer",
        )

        first = account_alerts.evaluate_account_alert_rules(
            conn,
            now="2026-05-26T10:00:00",
            dry_run=False,
        )
        second = account_alerts.evaluate_account_alert_rules(
            conn,
            now="2026-05-26T11:00:00",
            dry_run=False,
        )
        row = conn.execute("SELECT last_triggered_at, last_error FROM account_alert_rules").fetchone()

    assert len(first["sent"]) == 1
    assert first["sent"][0]["message"] == "Rent buffer: current balance is $400, below your $500 alert threshold."
    assert second["sent"] == []
    assert second["skipped"] == [{"id": first["sent"][0]["id"], "reason": "cooldown"}]
    assert row["last_triggered_at"] == "2026-05-26 10:00:00"
    assert row["last_error"] is None
    assert sent_messages == [
        {
            "message": "Rent buffer: current balance is $400, below your $500 alert threshold.",
            "channel": "telegram",
            "kwargs": {"chat_id": "123"},
        }
    ]


def test_low_balance_alert_tools_are_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS, NO_SYNC_TOOLS

    assert "low_balance_alerts_list" in gateway_tools.READ_ONLY_TOOLS
    assert "low_balance_alerts_list" not in gateway_tools.BRIDGE_TOOLS
    assert "low_balance_alerts_list" in NO_SYNC_TOOLS
    assert {"set_low_balance_alert", "low_balance_alerts_check"} <= gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert {"set_low_balance_alert", "low_balance_alerts_check"} <= DB_WRITE_TOOLS
