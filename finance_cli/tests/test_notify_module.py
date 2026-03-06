from __future__ import annotations

import json
import subprocess
import uuid
from argparse import Namespace
from datetime import date
from pathlib import Path

import pytest

try:
    import notify
    from notify import config as notify_config
    from notify.channels import imessage, telegram

    _HAS_NOTIFY = True
except ImportError:
    _HAS_NOTIFY = False

pytestmark = pytest.mark.skipif(not _HAS_NOTIFY, reason="notify module not installed")

from finance_cli.__main__ import build_parser, main
from finance_cli.budget_engine import set_budget
from finance_cli.commands import notify_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


class _FakeHTTPResponse:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_send_telegram_posts_expected_payload(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_urlopen(req, timeout: int):
        seen["url"] = req.full_url
        seen["method"] = req.get_method()
        seen["timeout"] = timeout
        seen["payload"] = json.loads(req.data.decode("utf-8"))
        return _FakeHTTPResponse('{"ok": true, "result": {"message_id": 321}}')

    monkeypatch.setattr(telegram.request, "urlopen", fake_urlopen)

    result = telegram.send_telegram("hello", "bot-token", "chat-123")

    assert seen["url"] == "https://api.telegram.org/botbot-token/sendMessage"
    assert seen["method"] == "POST"
    assert seen["timeout"] == 10
    assert seen["payload"] == {"chat_id": "chat-123", "text": "hello"}
    assert result["ok"] is True
    assert result["message_id"] == 321


def test_send_imessage_requires_binary(monkeypatch) -> None:
    monkeypatch.setattr(imessage.os.path, "exists", lambda _path: False)

    with pytest.raises(FileNotFoundError, match="imsg binary not found"):
        imessage.send_imessage("hi", "+15551234567")


def test_send_imessage_invokes_rpc(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_run(cmd, *, input, text, capture_output, timeout, check):
        seen["cmd"] = cmd
        seen["input"] = input
        seen["text"] = text
        seen["capture_output"] = capture_output
        seen["timeout"] = timeout
        seen["check"] = check
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout='{"jsonrpc":"2.0","id":1,"result":{}}',
            stderr="",
        )

    monkeypatch.setattr(imessage.os.path, "exists", lambda _path: True)
    monkeypatch.setattr(imessage.subprocess, "run", fake_run)

    result = imessage.send_imessage("hello there", "+15551234567")

    payload = json.loads(str(seen["input"]))
    assert seen["cmd"] == [imessage.IMSG_PATH, "rpc"]
    assert seen["timeout"] == 15
    assert payload["method"] == "send"
    assert payload["params"]["to"] == "+15551234567"
    assert payload["params"]["service"] == "imessage"
    assert result["ok"] is True


def test_notify_config_requires_environment(monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("IMESSAGE_TARGET", raising=False)

    with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
        notify_config.get_telegram_config()

    with pytest.raises(ValueError, match="IMESSAGE_TARGET"):
        notify_config.get_imessage_target()


def test_notify_send_dispatches_telegram_with_env(monkeypatch) -> None:
    seen: dict[str, str] = {}

    monkeypatch.setattr(notify, "get_telegram_config", lambda: ("tok", "chat"))

    def fake_send_telegram(message: str, token: str, chat_id: str) -> dict:
        seen["message"] = message
        seen["token"] = token
        seen["chat_id"] = chat_id
        return {"ok": True, "message_id": 1}

    monkeypatch.setattr(notify, "send_telegram", fake_send_telegram)

    result = notify.send("ping", channel="telegram")

    assert result["ok"] is True
    assert seen == {"message": "ping", "token": "tok", "chat_id": "chat"}


def test_notify_send_dispatches_imessage_with_kwargs(monkeypatch) -> None:
    seen: dict[str, str] = {}

    def fake_send_imessage(message: str, target: str, service: str = "imessage") -> dict:
        seen["message"] = message
        seen["target"] = target
        seen["service"] = service
        return {"ok": True}

    monkeypatch.setattr(notify, "send_imessage", fake_send_imessage)

    result = notify.send("ping", channel="imessage", target="person@example.com", service="sms")

    assert result["ok"] is True
    assert seen == {"message": "ping", "target": "person@example.com", "service": "sms"}


def test_notify_send_rejects_unknown_channel() -> None:
    with pytest.raises(ValueError, match="Unknown channel"):
        notify.send("hello", channel="slack")


def test_notify_cli_parser_registers_subcommands() -> None:
    parser = build_parser()

    parsed_test = parser.parse_args(["notify", "test", "--channel", "imessage", "--dry-run"])
    assert parsed_test.command == "notify"
    assert parsed_test.notify_command == "test"
    assert parsed_test.channel == "imessage"
    assert parsed_test.dry_run is True

    parsed_alerts = parser.parse_args(["notify", "budget-alerts", "--view", "business", "--month", "2026-03"])
    assert parsed_alerts.notify_command == "budget-alerts"
    assert parsed_alerts.view == "business"
    assert parsed_alerts.month == "2026-03"


def _seed_over_budget_data(db_path: Path, month: str) -> None:
    with connect(db_path) as conn:
        category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Dining', 0)",
            (category_id,),
        )
        set_budget(
            conn,
            category_id=category_id,
            amount_dollars="100",
            period="monthly",
            effective_from=f"{month}-01",
            use_type="Personal",
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, source, use_type, is_payment, is_active
            ) VALUES (?, ?, 'seed', -15000, ?, 'manual', 'Personal', 0, 1)
            """,
            (uuid.uuid4().hex, f"{month}-10", category_id),
        )
        conn.commit()


def test_notify_test_command_dry_run_cli(db_path: Path, capsys) -> None:
    code = main(["notify", "test", "--dry-run"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "notify.test"
    assert payload["data"]["dry_run"] is True


def test_notify_budget_alerts_command_dry_run_cli(db_path: Path, capsys) -> None:
    month = date.today().strftime("%Y-%m")
    _seed_over_budget_data(db_path, month)

    code = main(["notify", "budget-alerts", "--month", month, "--dry-run"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "notify.budget_alerts"
    assert payload["summary"]["over_count"] >= 1
    assert "OVER BUDGET:" in payload["data"]["message"]


def test_notify_cmd_handle_test_calls_notify_send(monkeypatch) -> None:
    seen: dict[str, str] = {}

    def fake_send(message: str, channel: str = "telegram", **_kwargs) -> dict:
        seen["message"] = message
        seen["channel"] = channel
        return {"ok": True}

    monkeypatch.setattr(notify_cmd.notify, "send", fake_send)

    result = notify_cmd.handle_test(Namespace(channel="telegram", dry_run=False), None)

    assert seen["channel"] == "telegram"
    assert "connection OK" in seen["message"]
    assert result["data"]["delivery"]["ok"] is True


def test_notify_mcp_tools_support_dry_run(db_path: Path) -> None:
    from finance_cli.mcp_server import notify_budget_alerts, notify_test

    test_result = notify_test(dry_run=True)
    alerts_result = notify_budget_alerts(dry_run=True)

    assert test_result["data"]["dry_run"] is True
    assert alerts_result["data"]["dry_run"] is True
    assert "message" in alerts_result["data"]
