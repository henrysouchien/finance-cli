from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from finance_cli.sync import config as sync_config


def _patch_cashnerd_paths(monkeypatch, base_dir: Path) -> None:
    monkeypatch.setattr(sync_config, "CASHNERD_DIR", base_dir)
    monkeypatch.setattr(sync_config, "CASHNERD_CONFIG_PATH", base_dir / "config.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AUTH_DIR", base_dir / "auth")
    monkeypatch.setattr(sync_config, "CASHNERD_TOKEN_PATH", base_dir / "auth" / "token.json")
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", base_dir / "data")
    monkeypatch.setattr(sync_config, "CASHNERD_DB_PATH", base_dir / "data" / "finance.db")
    monkeypatch.setattr(sync_config, "CASHNERD_RULES_PATH", base_dir / "data" / "rules.yaml")
    monkeypatch.setattr(sync_config, "CASHNERD_UPLOADS_DIR", base_dir / "data" / "uploads")
    monkeypatch.setattr(sync_config, "CASHNERD_SKILL_STATE_PATH", base_dir / "data" / "skill_state.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AGENT_MEMORY_PATH", base_dir / "data" / "agent_memory.md")
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_DIR", base_dir / "sync")
    monkeypatch.setattr(sync_config, "CASHNERD_PENDING_CHANGESET_PATH", base_dir / "sync" / "pending_changeset.json")
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_LOG_PATH", base_dir / "sync" / "sync_log.json")


def test_ensure_dirs_creates_expected_layout(monkeypatch, tmp_path: Path) -> None:
    base_dir = tmp_path / ".cashnerd"
    _patch_cashnerd_paths(monkeypatch, base_dir)

    sync_config.ensure_dirs()

    assert sync_config.CASHNERD_DIR.is_dir()
    assert sync_config.CASHNERD_AUTH_DIR.is_dir()
    assert sync_config.CASHNERD_DATA_DIR.is_dir()
    assert sync_config.CASHNERD_UPLOADS_DIR.is_dir()
    assert sync_config.CASHNERD_SYNC_DIR.is_dir()
    assert sync_config.CASHNERD_DIR.stat().st_mode & 0o777 == 0o700


def test_load_and_save_config_round_trip(monkeypatch, tmp_path: Path) -> None:
    base_dir = tmp_path / ".cashnerd"
    _patch_cashnerd_paths(monkeypatch, base_dir)

    initial = sync_config.load_config()
    assert initial.user_id is None
    assert initial.server_url == "https://cashnerd.ai"

    config = sync_config.SyncConfig(
        user_id="42",
        server_url="https://cashnerd.example",
        last_sync_ts="2026-04-16T12:00:00Z",
        schema_version=59,
        install_id="install-123",
    )
    sync_config.save_config(config)

    payload = json.loads(sync_config.CASHNERD_CONFIG_PATH.read_text(encoding="utf-8"))
    assert payload["user_id"] == "42"
    assert payload["schema_version"] == 59
    assert payload["install_id"] == "install-123"
    assert sync_config.CASHNERD_CONFIG_PATH.stat().st_mode & 0o777 == 0o600

    loaded = sync_config.load_config()
    assert loaded == config


@pytest.mark.parametrize(
    "server_url",
    [
        "https://cashnerd.ai",
        "http://localhost:8000",
        "http://127.0.0.1",
        "http://[::1]:8000",
    ],
)
def test_sync_config_accepts_https_and_loopback_http(server_url: str) -> None:
    config = sync_config.SyncConfig(server_url=server_url)

    assert config.server_url == server_url


@pytest.mark.parametrize(
    "server_url",
    [
        "http://evil.com",
        "http://cashnerd.local",
        "http://192.168.1.1",
        "ftp://host",
    ],
)
def test_sync_config_rejects_non_loopback_insecure_urls(server_url: str) -> None:
    with pytest.raises(ValidationError, match="CASHNERD_SERVER_URL must use https://"):
        sync_config.SyncConfig(server_url=server_url)


def test_sync_config_json_round_trip() -> None:
    config = sync_config.SyncConfig(
        user_id="x",
        server_url="https://y",
        last_sync_ts="z",
        schema_version=59,
        install_id="install-123",
    )

    reloaded = sync_config.SyncConfig(**config.model_dump(mode="json"))

    assert reloaded == config


def test_load_config_rejects_malicious_env_seed(monkeypatch, tmp_path: Path) -> None:
    base_dir = tmp_path / ".cashnerd"
    _patch_cashnerd_paths(monkeypatch, base_dir)
    monkeypatch.setenv("CASHNERD_SERVER_URL", "http://evil.com")

    with pytest.raises(ValidationError, match="CASHNERD_SERVER_URL must use https://"):
        sync_config.load_config()


def test_load_config_rejects_malicious_config_json(monkeypatch, tmp_path: Path) -> None:
    base_dir = tmp_path / ".cashnerd"
    _patch_cashnerd_paths(monkeypatch, base_dir)
    sync_config.ensure_dirs()
    sync_config.CASHNERD_CONFIG_PATH.write_text(
        json.dumps({"server_url": "http://evil.com"}),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="CASHNERD_SERVER_URL must use https://"):
        sync_config.load_config()
