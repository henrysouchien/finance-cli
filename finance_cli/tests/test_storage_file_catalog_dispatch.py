from __future__ import annotations

import json
import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock

from finance_cli.commands import memory_cmd, rules


FINANCE_WEB_ROOT = Path(__file__).resolve().parents[2] / "finance-web"
if str(FINANCE_WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(FINANCE_WEB_ROOT))


def test_agent_memory_remote_uses_storage_file_and_local_writes_disk(tmp_path: Path, monkeypatch) -> None:
    write_file = Mock()
    monkeypatch.setattr(memory_cmd.storage_files, "write_file", write_file)
    monkeypatch.setattr(memory_cmd, "_remote_target_for_data_dir", lambda _data_dir: ("target", "alice"))

    memory_cmd.handle_update(Namespace(content="remote memory"), conn=None, data_dir=tmp_path)

    write_file.assert_called_once_with(
        "target",
        user_id="alice",
        product="finance_cli",
        relative_path="agent_memory.md",
        content=b"remote memory",
    )

    write_file.reset_mock()
    monkeypatch.setattr(memory_cmd, "_remote_target_for_data_dir", lambda _data_dir: (None, None))
    memory_cmd.handle_update(Namespace(content="local memory"), conn=None, data_dir=tmp_path)

    write_file.assert_not_called()
    assert (tmp_path / "agent_memory.md").read_text(encoding="utf-8") == "local memory"


def test_session_note_remote_uses_storage_file_and_local_appends_disk(tmp_path: Path, monkeypatch) -> None:
    write_file = Mock()
    monkeypatch.setattr(memory_cmd.storage_files, "write_file", write_file)
    monkeypatch.setattr(memory_cmd.storage_files, "list_files", lambda *args, **kwargs: [])
    monkeypatch.setattr(memory_cmd, "_remote_target_for_data_dir", lambda _data_dir: ("target", "alice"))

    result = memory_cmd.handle_session_write(Namespace(content="remote note"), conn=None, data_dir=tmp_path)

    assert result["data"]["ok"] is True
    assert write_file.call_args.kwargs["user_id"] == "alice"
    assert write_file.call_args.kwargs["relative_path"].startswith("sessions/")
    assert b"remote note" in write_file.call_args.kwargs["content"]

    write_file.reset_mock()
    monkeypatch.setattr(memory_cmd, "_remote_target_for_data_dir", lambda _data_dir: (None, None))
    memory_cmd.handle_session_write(Namespace(content="local note"), conn=None, data_dir=tmp_path)

    write_file.assert_not_called()
    session_files = list((tmp_path / "sessions").glob("*.md"))
    assert len(session_files) == 1
    assert "local note" in session_files[0].read_text(encoding="utf-8")


def test_sync_engine_session_commit_remote_uses_storage_file_and_local_moves_disk(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from finance_cli.sync import engine as sync_engine

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "finance.db"
    db_path.write_bytes(b"old")
    monkeypatch.setattr(sync_engine, "CASHNERD_DATA_DIR", data_dir)
    monkeypatch.setattr(sync_engine, "CASHNERD_DB_PATH", db_path)
    monkeypatch.setattr(sync_engine, "CASHNERD_DIR", tmp_path)

    staging = tmp_path / "staging"
    (staging / "sessions").mkdir(parents=True)
    (staging / "sessions" / "2026-05-01.md").write_text("remote sync note", encoding="utf-8")
    (staging / "finance.db").write_bytes(b"new")
    write_file = Mock()
    monkeypatch.setattr(sync_engine.storage_files, "write_file", write_file)
    monkeypatch.setattr(sync_engine.storage_dispatch, "remote_file_target_for_user", lambda _user_id: "target")

    sync_engine.SyncEngine._commit_staged_files_sync(object(), staging, user_id="alice")

    write_file.assert_called_once()
    assert write_file.call_args.kwargs["relative_path"] == "sessions/2026-05-01.md"
    assert write_file.call_args.kwargs["content"] == b"remote sync note"

    staging = tmp_path / "staging-local"
    (staging / "sessions").mkdir(parents=True)
    (staging / "sessions" / "2026-05-02.md").write_text("local sync note", encoding="utf-8")
    (staging / "finance.db").write_bytes(b"newer")
    write_file.reset_mock()
    monkeypatch.setattr(sync_engine.storage_dispatch, "remote_file_target_for_user", lambda _user_id: None)

    sync_engine.SyncEngine._commit_staged_files_sync(object(), staging, user_id="alice")

    write_file.assert_not_called()
    assert (data_dir / "sessions" / "2026-05-02.md").read_text(encoding="utf-8") == "local sync note"


def test_skill_state_remote_uses_storage_file_and_local_writes_disk(tmp_path: Path, monkeypatch) -> None:
    from finance_cli import mcp_server

    write_file = Mock()
    monkeypatch.setattr(mcp_server, "_get_data_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server.storage_dispatch, "user_id_from_data_dir", lambda _data_dir: "alice")
    monkeypatch.setattr(mcp_server.storage_dispatch, "remote_file_target_for_user", lambda _user_id: "target")
    monkeypatch.setattr(mcp_server.storage_files, "list_files", lambda *args, **kwargs: [])
    monkeypatch.setattr(mcp_server.storage_files, "write_file", write_file)

    mcp_server.skill_state_set("onboarding", {"step": 2})

    write_file.assert_called_once()
    assert write_file.call_args.kwargs["relative_path"] == "skill_state.json"
    assert json.loads(write_file.call_args.kwargs["content"]) == {"onboarding": {"step": 2}}

    write_file.reset_mock()
    monkeypatch.setattr(mcp_server.storage_dispatch, "remote_file_target_for_user", lambda _user_id: None)
    mcp_server.skill_state_set("onboarding", {"step": 3})

    write_file.assert_not_called()
    assert json.loads((tmp_path / "skill_state.json").read_text(encoding="utf-8")) == {
        "onboarding": {"step": 3}
    }


def test_telegram_router_store_token_uses_vault_and_cleans_legacy_payload(tmp_path: Path, monkeypatch) -> None:
    from server.routers import telegram_router

    payload = {"bot_token": "123:ABC", "user_id": "alice"}
    store_bot_token = Mock(return_value="vault://alice/telegram/bot_token")
    delete_legacy_token_payload = Mock()
    session_manager = object()
    monkeypatch.setattr(telegram_router.telegram_secrets, "store_bot_token", store_bot_token)
    monkeypatch.setattr(
        telegram_router.telegram_secrets,
        "delete_legacy_token_payload",
        delete_legacy_token_payload,
    )

    ref = telegram_router._store_token(tmp_path, payload, session_manager=session_manager)

    assert ref == "vault://alice/telegram/bot_token"
    store_bot_token.assert_called_once_with("alice", "123:ABC", data_root=tmp_path.parent)
    delete_legacy_token_payload.assert_called_once_with(
        tmp_path,
        user_id="alice",
        session_manager=session_manager,
    )


def test_rules_yaml_remote_uses_storage_file_and_local_writes_disk(tmp_path: Path, monkeypatch) -> None:
    write_file = Mock()
    rules_path = tmp_path / "rules.yaml"
    payload = {"keyword_rules": [], "split_rules": []}
    monkeypatch.setattr(rules.storage_files, "write_file", write_file)
    monkeypatch.setattr(rules.storage_dispatch, "user_id_from_user_file_path", lambda _path: "alice")
    monkeypatch.setattr(rules.storage_dispatch, "remote_file_target_for_user", lambda _user_id: "target")

    rules._write_raw_rules_yaml(rules_path, payload)

    write_file.assert_called_once()
    assert write_file.call_args.kwargs["relative_path"] == "rules.yaml"
    assert b"keyword_rules: []" in write_file.call_args.kwargs["content"]

    write_file.reset_mock()
    monkeypatch.setattr(rules.storage_dispatch, "remote_file_target_for_user", lambda _user_id: None)
    rules._write_raw_rules_yaml(rules_path, payload)

    write_file.assert_not_called()
    assert "keyword_rules: []" in rules_path.read_text(encoding="utf-8")
