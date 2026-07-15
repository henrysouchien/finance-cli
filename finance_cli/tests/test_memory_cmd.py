from __future__ import annotations

import importlib
import sys
from contextlib import nullcontext
from datetime import date as real_date
from datetime import datetime as real_datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli.commands import memory_cmd


@pytest.fixture()
def memory_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "agent_memory.md"
    monkeypatch.setattr(memory_cmd, "_memory_path", lambda data_dir=None: path)
    return path


@pytest.fixture()
def sessions_dir(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "sessions"
    path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(memory_cmd, "_sessions_dir", lambda data_dir=None: path)
    return path


def _ns(**kwargs) -> SimpleNamespace:
    return SimpleNamespace(**kwargs)


def _import_mcp_server(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    sys.modules.pop("finance_cli.mcp_server", None)
    return importlib.import_module("finance_cli.mcp_server")


def test_handle_read_when_missing(memory_path: Path) -> None:
    result = memory_cmd.handle_read(_ns(), conn=None)

    assert result == {
        "data": {"content": "", "exists": False},
        "summary": {"message": "No memory stored yet."},
    }
    assert not memory_path.exists()


def test_handle_update_then_read(memory_path: Path) -> None:
    content = "# Preferences\n- dining: 400\n"

    update_result = memory_cmd.handle_update(_ns(content=content), conn=None)
    read_result = memory_cmd.handle_read(_ns(), conn=None)

    assert update_result == {
        "data": {"ok": True, "bytes": len(content.encode("utf-8"))},
        "summary": {"message": f"Memory updated ({len(content.encode('utf-8'))} bytes)."},
    }
    assert read_result == {
        "data": {"content": content, "exists": True},
        "summary": {"message": f"Memory loaded ({len(content)} bytes)."},
    }


def test_handle_update_overwrites_existing_content(memory_path: Path) -> None:
    memory_cmd.handle_update(_ns(content="old"), conn=None)
    memory_cmd.handle_update(_ns(content="new"), conn=None)

    assert memory_path.read_text(encoding="utf-8") == "new"


def test_handle_update_rejects_content_over_max_bytes(memory_path: Path) -> None:
    oversized = "a" * (memory_cmd.MAX_BYTES + 1)

    with pytest.raises(ValueError, match=f"Content exceeds {memory_cmd.MAX_BYTES} byte limit\\."):
        memory_cmd.handle_update(_ns(content=oversized), conn=None)

    assert not memory_path.exists()


def test_handle_update_rejects_content_over_max_lines(memory_path: Path) -> None:
    oversized = "\n".join(["line"] * (memory_cmd.MAX_LINES + 1))

    with pytest.raises(ValueError, match=f"Content exceeds {memory_cmd.MAX_LINES} line limit\\."):
        memory_cmd.handle_update(_ns(content=oversized), conn=None)

    assert not memory_path.exists()


def test_handle_update_atomic_write_leaves_no_tmp_file(memory_path: Path) -> None:
    memory_cmd.handle_update(_ns(content="persisted"), conn=None)

    assert memory_path.exists()
    assert not memory_path.with_suffix(".md.tmp").exists()


def test_handle_session_write_appends_timestamped_entry(sessions_dir: Path, monkeypatch) -> None:
    class FakeDate(real_date):
        @classmethod
        def today(cls) -> FakeDate:
            return cls(2026, 3, 10)

    class FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None) -> FakeDateTime:
            del tz
            return cls(2026, 3, 10, 14, 30)

    monkeypatch.setattr(memory_cmd, "date", FakeDate)
    monkeypatch.setattr(memory_cmd, "datetime", FakeDateTime)

    result = memory_cmd.handle_session_write(_ns(content="- Reviewed subscriptions"), conn=None)
    expected_entry = "\n## 14:30\n- Reviewed subscriptions\n"

    assert result == {
        "data": {"ok": True, "date": "2026-03-10", "bytes": len(expected_entry.encode("utf-8"))},
        "summary": {"message": "Session note saved (2026-03-10)."},
    }
    assert (sessions_dir / "2026-03-10.md").read_text(encoding="utf-8") == expected_entry


def test_handle_session_search_returns_matching_section_excerpt(
    sessions_dir: Path, monkeypatch
) -> None:
    class FakeDate(real_date):
        @classmethod
        def today(cls) -> FakeDate:
            return cls(2026, 3, 10)

    monkeypatch.setattr(memory_cmd, "date", FakeDate)
    (sessions_dir / "2026-03-10.md").write_text(
        "## 09:00\nReviewed subscriptions and the Claude billing spike.\n\n"
        "## 16:00\nFollow up on debt plan.\n",
        encoding="utf-8",
    )
    (sessions_dir / "2026-03-09.md").write_text(
        "## 08:00\nTalked about travel only.\n",
        encoding="utf-8",
    )

    result = memory_cmd.handle_session_search(_ns(query="subscriptions", days=30), conn=None)

    assert result == {
        "data": {
            "results": [
                {
                    "date": "2026-03-10",
                    "excerpt": "## 09:00\nReviewed subscriptions and the Claude billing spike.",
                }
            ],
            "count": 1,
        },
        "summary": {"message": "Found 1 session notes matching 'subscriptions'."},
    }


def test_handle_session_read_defaults_to_today(sessions_dir: Path, monkeypatch) -> None:
    class FakeDate(real_date):
        @classmethod
        def today(cls) -> FakeDate:
            return cls(2026, 3, 10)

    monkeypatch.setattr(memory_cmd, "date", FakeDate)
    content = "\n## 14:30\nCaptured note\n"
    (sessions_dir / "2026-03-10.md").write_text(content, encoding="utf-8")

    result = memory_cmd.handle_session_read(_ns(), conn=None)

    assert result == {
        "data": {"content": content, "exists": True},
        "summary": {"message": f"Session notes for 2026-03-10 ({len(content)} bytes)."},
    }


def test_handle_session_read_rejects_invalid_date(sessions_dir: Path) -> None:
    result = memory_cmd.handle_session_read(_ns(date="2026-02-30"), conn=None)

    assert result == {
        "data": {"content": "", "exists": False},
        "summary": {"message": "Invalid date format: '2026-02-30'. Use YYYY-MM-DD."},
    }


def test_handle_session_read_rejects_path_traversal(sessions_dir: Path) -> None:
    result = memory_cmd.handle_session_read(_ns(date="../../etc/passwd"), conn=None)

    assert result == {
        "data": {"content": "", "exists": False},
        "summary": {"message": "Invalid date format: '../../etc/passwd'. Use YYYY-MM-DD."},
    }


def test_mcp_memory_tools_call_expected_handlers(tmp_path: Path, monkeypatch) -> None:
    mcp_server = _import_mcp_server(tmp_path, monkeypatch)
    calls: list[tuple[str, dict[str, object], Path]] = []
    data_dir = tmp_path / "user-data"
    data_dir.mkdir(parents=True, exist_ok=True)

    def make_handler(name: str):
        def _handler(args, conn, data_dir=None):
            del conn
            assert data_dir is not None
            calls.append((name, vars(args), data_dir))
            return {"data": {"ok": True}, "summary": {"message": "ok"}}

        return _handler

    monkeypatch.setattr(mcp_server, "_get_conn", lambda: nullcontext(None))
    monkeypatch.setattr(mcp_server, "_get_data_dir", lambda: data_dir)
    monkeypatch.setattr(mcp_server.memory_cmd, "handle_read", make_handler("read"))
    monkeypatch.setattr(mcp_server.memory_cmd, "handle_update", make_handler("update"))
    monkeypatch.setattr(mcp_server.memory_cmd, "handle_session_write", make_handler("session_write"))
    monkeypatch.setattr(mcp_server.memory_cmd, "handle_session_search", make_handler("session_search"))
    monkeypatch.setattr(mcp_server.memory_cmd, "handle_session_read", make_handler("session_read"))

    read_result = mcp_server.agent_memory_read()
    update_result = mcp_server.agent_memory_update(content="# Saved")
    session_write_result = mcp_server.agent_session_write(content="- Saved")
    session_search_result = mcp_server.agent_session_search(query="spike")
    session_read_result = mcp_server.agent_session_read()

    assert read_result == {"data": {"ok": True}, "summary": {"message": "ok"}}
    assert update_result == {"data": {"ok": True}, "summary": {"message": "ok"}}
    assert session_write_result == {"data": {"ok": True}, "summary": {"message": "ok"}}
    assert session_search_result == {"data": {"ok": True}, "summary": {"message": "ok"}}
    assert session_read_result == {"data": {"ok": True}, "summary": {"message": "ok"}}
    assert calls == [
        ("read", {"format": "json", "verbose": False}, data_dir),
        ("update", {"format": "json", "verbose": False, "content": "# Saved"}, data_dir),
        ("session_write", {"format": "json", "verbose": False, "content": "- Saved"}, data_dir),
        ("session_search", {"format": "json", "verbose": False, "query": "spike", "days": 30}, data_dir),
        ("session_read", {"format": "json", "verbose": False, "date": ""}, data_dir),
    ]


def test_agent_allowlist_includes_memory_tools() -> None:
    from finance_cli.telegram_bot.approval import (
        _APPROVAL_REQUIRED_TOOLS,
        _READ_ONLY_TOOLS,
        needs_approval,
    )

    assert "agent_memory_read" in _READ_ONLY_TOOLS
    assert "agent_session_search" in _READ_ONLY_TOOLS
    assert "agent_session_read" in _READ_ONLY_TOOLS
    assert "agent_session_write" in _APPROVAL_REQUIRED_TOOLS
    assert needs_approval("agent_memory_update") is True
    assert needs_approval("agent_session_write") is True
    assert needs_approval("agent_session_search") is False
