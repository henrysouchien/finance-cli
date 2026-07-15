from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.sync import config as sync_config
from finance_cli.sync.subscriber import ChangeFeedSubscriber
from finance_cli.user_provisioning import provision_user, user_db_path

FINANCE_WEB_ROOT = Path(__file__).resolve().parents[2] / "finance-web"
if str(FINANCE_WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(FINANCE_WEB_ROOT))

from server.sync_service import write_rules_yaml  # noqa: E402


def _patch_paths(monkeypatch, base_dir: Path) -> None:
    import finance_cli.sync.subscriber as subscriber_module

    monkeypatch.setattr(sync_config, "CASHNERD_DIR", base_dir)
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", base_dir / "data")
    monkeypatch.setattr(sync_config, "CASHNERD_DB_PATH", base_dir / "data" / "finance.db")
    monkeypatch.setattr(sync_config, "CASHNERD_RULES_PATH", base_dir / "data" / "rules.yaml")
    monkeypatch.setattr(subscriber_module, "CASHNERD_DATA_DIR", sync_config.CASHNERD_DATA_DIR)
    monkeypatch.setattr(subscriber_module, "CASHNERD_DB_PATH", sync_config.CASHNERD_DB_PATH)


class FakeEngine:
    def __init__(self, db_path: Path) -> None:
        self.install_id = "install-123"
        self.server_url = "https://cashnerd.example"
        self.schema_version = 59
        self._db_path = db_path

    @property
    def last_applied_op_id(self) -> int:
        with sqlite3.connect(str(self._db_path)) as conn:
            row = conn.execute("SELECT last_applied_op_id FROM sync_state WHERE id = 0").fetchone()
        return int(row[0] or 0)

    async def get_sync_token(self) -> str:
        return "token"

    async def refresh_credentials(self) -> None:
        return None

    def bump_last_applied(self, op_id: int) -> None:
        with connect(self._db_path) as conn:
            conn.execute("UPDATE sync_state SET last_applied_op_id = ? WHERE id = 0", (op_id,))
            conn.commit()

    def mark_subscriber_degraded(self) -> None:
        return None

    def release_install_subscriber_lock(self) -> None:
        return None

    async def fetch_sidecar_content(self, key: str, sha256: str) -> bytes | None:
        raise AssertionError(f"fetch_sidecar_content should not be called for deleted {key} {sha256}")


def test_write_rules_yaml_updates_meta_state_row(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    template_rules = Path(__file__).resolve().parents[1] / "data" / "rules_template.yaml"
    provision_user(
        data_root=data_root,
        user_id="alice",
        template_rules_path=template_rules.resolve(),
    )

    write_rules_yaml("alice", data_root, "keyword_rules: []\n")

    with connect(user_db_path(data_root, "alice"), expected_user_id="alice") as conn:
        row = conn.execute(
            "SELECT key, sha256 FROM _meta_state WHERE key = 'rules.yaml'"
        ).fetchone()
    assert row is not None
    assert row["key"] == "rules.yaml"
    assert isinstance(row["sha256"], str) and len(row["sha256"]) == 64


def test_subscriber_meta_state_with_null_sha_deletes_local_file(monkeypatch, tmp_path: Path) -> None:
    _patch_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.CASHNERD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    initialize_database(sync_config.CASHNERD_DB_PATH)
    sync_config.CASHNERD_RULES_PATH.write_text("keyword_rules: []\n", encoding="utf-8")

    subscriber = ChangeFeedSubscriber(FakeEngine(sync_config.CASHNERD_DB_PATH))
    payload = {
        "id": 5,
        "table": "_meta_state",
        "op": "UPDATE",
        "pk_json": json.dumps({"key": "rules.yaml"}),
        "old_json": json.dumps({"key": "rules.yaml", "sha256": "abc", "updated_at": "2026-04-16T00:00:00Z"}),
        "new_json": json.dumps({"key": "rules.yaml", "sha256": None, "updated_at": "2026-04-16T00:00:01Z"}),
        "origin_session_id": "",
    }

    import asyncio

    asyncio.run(subscriber._apply_op(payload))

    assert not sync_config.CASHNERD_RULES_PATH.exists()
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        row = conn.execute("SELECT sha256 FROM _meta_state WHERE key = 'rules.yaml'").fetchone()
        cursor = conn.execute("SELECT last_applied_op_id FROM sync_state WHERE id = 0").fetchone()
    assert row[0] is None
    assert cursor[0] == 5
