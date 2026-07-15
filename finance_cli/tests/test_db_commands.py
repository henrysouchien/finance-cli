from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from moto import mock_aws

from finance_cli import secrets_backend
from finance_cli import db as db_module
import finance_cli.backup as backup_module
from finance_cli.__main__ import main
from finance_cli.commands import db_cmd
from finance_cli.db import backup_database, connect, initialize_database, wipe_runtime_data
from finance_cli.preferences import export_preferences
from finance_cli.sync.exceptions import SubscriberActiveError
from finance_cli.sync.subscriber_lock import InstallSubscriberLock


@pytest.fixture(autouse=True)
def _mock_backup_secrets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    secrets_backend._client = None
    with mock_aws():
        yield
    secrets_backend._client = None


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", "default")
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _reset_backups_dir(db_path: Path) -> Path:
    backup_dir = db_path.parent / "backups"
    shutil.rmtree(backup_dir, ignore_errors=True)
    backup_dir.mkdir()
    return backup_dir


def _seed_preferences_bundle(workspace: Path) -> Path:
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    (workspace / "agent_memory.md").write_text("# Memory\n", encoding="utf-8")
    sessions_dir = workspace / "sessions"
    sessions_dir.mkdir(exist_ok=True)
    (sessions_dir / "2026-03-10.md").write_text("Session note\n", encoding="utf-8")

    db_path = workspace / "finance.db"
    initialize_database(db_path)
    with connect(db_path) as conn:
        category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'TestDining', 0, 0)",
            (category_id,),
        )
        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, institution_name, account_name, card_ending, account_type, is_active, is_business)
            VALUES (?, 'Test Bank', 'Checking', '1234', 'checking', 1, 1)
            """,
            (account_id,),
        )
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id, description_pattern, category_id, use_type, confidence, priority, is_enabled, is_confirmed, match_count
            ) VALUES (?, 'STARBUCKS', ?, 'Any', 0.95, 0, 1, 1, 1)
            """,
            (uuid.uuid4().hex, category_id),
        )
        conn.execute(
            """
            INSERT INTO subscriptions (
                id, vendor_name, category_id, amount_cents, frequency, next_expected,
                account_id, is_active, use_type, is_auto_detected, sub_type
            ) VALUES (?, 'Netflix', ?, 1599, 'monthly', '2026-04-01', ?, 1, 'Personal', 1, 'fixed')
            """,
            (uuid.uuid4().hex, category_id, account_id),
        )
        conn.commit()
        return export_preferences(
            conn,
            data_dir=workspace,
            rules_path=workspace / "rules.yaml",
        ).bundle_path


def test_db_backup_creates_timestamped_bundle(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    code, payload = _run_cli(["db", "backup"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.backup"

    backup_path = Path(payload["data"]["backup_path"])
    assert backup_path.exists()
    assert backup_path != db_path
    assert backup_path.parent == db_path.parent / "backups"
    assert backup_path.name.startswith("finance_backup_")
    assert payload["data"]["bundle_path"] == str(backup_path)
    assert payload["data"]["file_count"] >= 1
    assert backup_path.suffix == ".bundle"


def test_db_backup_list_returns_created_bundle(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)

    backup_code, backup_payload = _run_cli(["db", "backup"], capsys)
    assert backup_code == 0
    bundle_path = backup_payload["data"]["bundle_path"]

    code, payload = _run_cli(["db", "backup-list"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.backup-list"
    assert isinstance(payload["data"], list)
    assert any(item["bundle_path"] == bundle_path for item in payload["data"])


def test_db_verify_backup_reports_valid_bundle(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    _, backup_payload = _run_cli(["db", "backup"], capsys)

    code, payload = _run_cli(["db", "verify-backup", backup_payload["data"]["bundle_path"]], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.verify-backup"
    assert payload["data"]["valid"] is True
    assert payload["summary"]["valid"] is True


def test_db_restore_defaults_to_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    _, backup_payload = _run_cli(["db", "backup"], capsys)

    code, payload = _run_cli(["db", "restore", "--file", backup_payload["data"]["bundle_path"]], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.restore"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["restored"] is False


def test_db_backup_prune_defaults_to_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    _run_cli(["db", "backup"], capsys)

    code, payload = _run_cli(["db", "backup-prune"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.backup-prune"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["scheduled_key_deletions"] == 0


def test_db_export_preferences_creates_bundle(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    (db_path.parent / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    (db_path.parent / "agent_memory.md").write_text("# Memory\n", encoding="utf-8")
    sessions_dir = db_path.parent / "sessions"
    sessions_dir.mkdir()
    (sessions_dir / "2026-03-10.md").write_text("Session note\n", encoding="utf-8")

    with connect(db_path) as conn:
        category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'TestDining', 0, 0)",
            (category_id,),
        )
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id, description_pattern, category_id, use_type, confidence, priority, is_enabled, is_confirmed, match_count
            ) VALUES (?, 'STARBUCKS', ?, 'Any', 0.95, 0, 1, 1, 1)
            """,
            (uuid.uuid4().hex, category_id),
        )
        conn.commit()

    code, payload = _run_cli(["db", "export-preferences"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.export-preferences"
    assert Path(payload["data"]["bundle_path"]).exists()
    assert payload["summary"]["total_rows"] >= 1


def test_db_import_preferences_defaults_to_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    source_dir = tmp_path / "source"
    bundle_path = _seed_preferences_bundle(source_dir)

    _setup_db(tmp_path / "target", monkeypatch)
    code, payload = _run_cli(["db", "import-preferences", "--file", str(bundle_path)], capsys)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.import-preferences"
    assert payload["data"]["dry_run"] is True
    assert "total_imported" in payload["summary"]


def test_db_import_preferences_overwrite_requires_yes(tmp_path: Path, monkeypatch, capsys) -> None:
    source_dir = tmp_path / "source"
    bundle_path = _seed_preferences_bundle(source_dir)

    _setup_db(tmp_path / "target", monkeypatch)
    code, payload = _run_cli(
        ["db", "import-preferences", "--file", str(bundle_path), "--mode", "overwrite"],
        capsys,
    )

    assert code == 1
    assert payload["status"] == "error"
    assert payload["command"] == "db.import-preferences"
    assert "--yes" in payload["error"]


def test_connect_applies_busy_timeout_pragma(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    with connect(db_path, busy_timeout=4321) as conn:
        pragma_value = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])

    assert pragma_value == 4321


def test_backup_path_adapter_returns_string_for_local_connection(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    backup_path = tmp_path / "adapter-backup.db"

    with connect(db_path) as conn:
        result = db_module._backup_sqlite_connection_to_path(conn, str(backup_path))

    assert isinstance(result, str)
    assert Path(result) == backup_path
    assert backup_path.exists()


def test_backup_database_uses_remote_path_returning_api() -> None:
    class _RemoteConnection(db_module.StorageConnection):
        def __init__(self) -> None:
            pass

        def backup(self, target_path: str | None = None) -> str:
            assert target_path is None
            return "/remote/backups/finance_backup.db"

    result = db_module.backup_database(conn=_RemoteConnection())

    assert str(result) == "/remote/backups/finance_backup.db"


def test_backup_retention_keeps_latest_generated_backups(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    monkeypatch.setenv("FINANCE_CLI_BACKUP_RETENTION", "2")
    backup_dir = _reset_backups_dir(db_path)

    first = backup_database(db_path=db_path)
    second = backup_database(db_path=db_path)
    third = backup_database(db_path=db_path)

    backups = sorted(backup_dir.glob("finance_backup_*.db"))
    assert len(backups) == 2
    assert not first.exists()
    assert second.exists()
    assert third.exists()


def test_backup_retention_removes_sidecars_for_pruned_backups(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    monkeypatch.setenv("FINANCE_CLI_BACKUP_RETENTION", "1")

    backup_dir = _reset_backups_dir(db_path)
    old_backup = backup_dir / "finance_backup_20260101_010101.db"
    old_backup.write_text("old backup", encoding="utf-8")
    old_wal = old_backup.with_name(f"{old_backup.name}-wal")
    old_shm = old_backup.with_name(f"{old_backup.name}-shm")
    old_wal.write_text("", encoding="utf-8")
    old_shm.write_text("", encoding="utf-8")

    new_backup = backup_database(db_path=db_path)

    assert new_backup.exists()
    assert not old_backup.exists()
    assert not old_wal.exists()
    assert not old_shm.exists()


def test_backup_retention_does_not_prune_legacy_root_backups(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    monkeypatch.setenv("FINANCE_CLI_BACKUP_RETENTION", "1")
    backup_dir = _reset_backups_dir(db_path)

    legacy_backup = db_path.parent / "finance_backup_20260101_010101.db"
    legacy_backup.write_text("legacy backup", encoding="utf-8")

    backup_database(db_path=db_path)
    backup_database(db_path=db_path)

    backups = sorted(backup_dir.glob("finance_backup_*.db"))
    assert legacy_backup.exists()
    assert len(backups) == 1


def test_db_reset_requires_yes_flag(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    code, payload = _run_cli(["db", "reset"], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert payload["command"] == "db.reset"
    assert "--yes" in payload["error"]


def test_db_reset_preserves_plaid_items_and_resets_sync_metadata(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, access_token_ref, status,
                sync_cursor, last_sync_at, last_balance_refresh_at, last_liabilities_fetch_at
            ) VALUES (?, 'item_keep', 'Test Bank', 'secret/ref', 'active', ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                "cursor_123",
                "2026-02-20 10:00:00",
                "2026-02-20 10:00:00",
                "2026-02-20 10:00:00",
            ),
        )
        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_item_id, institution_name, account_name, account_type, is_active)
            VALUES (?, 'item_keep', 'Test Bank', 'Checking', 'checking', 1)
            """,
            (account_id,),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
            VALUES (?, ?, '2026-02-20', 'Coffee', -500, 'plaid', 1)
            """,
            (uuid.uuid4().hex, account_id),
        )
        conn.execute(
            """
            INSERT INTO import_batches (
                id, source_type, file_path, file_hash_sha256, bank_parser,
                extracted_count, imported_count, skipped_count, reconcile_status
            ) VALUES (?, 'pdf', '/tmp/stmt.pdf', ?, 'ai:openai', 1, 1, 0, 'no_totals')
            """,
            (uuid.uuid4().hex, uuid.uuid4().hex),
        )
        conn.commit()

    code, payload = _run_cli(["db", "reset", "--yes"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["preserve_plaid_items"] is True
    assert Path(payload["data"]["backup_path"]).exists()

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
        item = conn.execute(
            """
            SELECT sync_cursor, last_sync_at, last_balance_refresh_at, last_liabilities_fetch_at
              FROM plaid_items
             WHERE plaid_item_id = 'item_keep'
            """
        ).fetchone()

    assert txn_count == 0
    assert account_count == 0
    assert batch_count == 0
    assert item is not None
    assert item["sync_cursor"] is None
    assert item["last_sync_at"] is None
    assert item["last_balance_refresh_at"] is None
    assert item["last_liabilities_fetch_at"] is None


def test_db_reset_drop_plaid_items_removes_all_items(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_drop', 'Test Bank', 'secret/ref', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    code, payload = _run_cli(["db", "reset", "--yes", "--drop-plaid-items"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["preserve_plaid_items"] is False

    with connect(db_path) as conn:
        item_count = conn.execute("SELECT COUNT(*) AS n FROM plaid_items").fetchone()["n"]
    assert item_count == 0


def test_wipe_runtime_data_full_uses_schema_introspection_and_keep_tables(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        system_category_id = uuid.uuid4().hex
        user_category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'System Category', 0, 1)",
            (system_category_id,),
        )
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'User Category', 0, 0)",
            (user_category_id,),
        )
        conn.execute(
            """
            INSERT INTO pl_section_map (id, category_id, pl_section, display_order)
            VALUES (?, ?, 'opex_other', 1)
            """,
            (uuid.uuid4().hex, system_category_id),
        )
        conn.execute(
            """
            INSERT INTO schedule_c_map (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year)
            VALUES (?, ?, 'Office expense', '18', 1.0, 2026)
            """,
            (uuid.uuid4().hex, system_category_id),
        )
        conn.execute(
            """
            INSERT INTO bot_requests (request_id, session_id, model)
            VALUES ('req_1', 'session_1', 'gpt-test')
            """
        )
        conn.execute(
            """
            INSERT INTO bot_tool_calls (request_id, tool_name, duration_ms)
            VALUES ('req_1', 'search', 12)
            """
        )
        conn.execute(
            """
            INSERT INTO telegram_config (id, bot_token_ref, bot_username)
            VALUES (1, 'file', 'cashnerd_bot')
            """
        )
        conn.execute("INSERT INTO telegram_processed_updates (update_id) VALUES (42)")
        mileage_rates_before = conn.execute("SELECT COUNT(*) AS n FROM mileage_rates").fetchone()["n"]
        conn.commit()

        report = wipe_runtime_data(conn, full=True)

        remaining_categories = [
            (row["name"], row["is_system"])
            for row in conn.execute("SELECT name, is_system FROM categories ORDER BY name").fetchall()
        ]
        bot_requests_count = conn.execute("SELECT COUNT(*) AS n FROM bot_requests").fetchone()["n"]
        bot_tool_calls_count = conn.execute("SELECT COUNT(*) AS n FROM bot_tool_calls").fetchone()["n"]
        telegram_config_count = conn.execute("SELECT COUNT(*) AS n FROM telegram_config").fetchone()["n"]
        processed_updates_count = conn.execute("SELECT COUNT(*) AS n FROM telegram_processed_updates").fetchone()["n"]
        mileage_rates_after = conn.execute("SELECT COUNT(*) AS n FROM mileage_rates").fetchone()["n"]
        pl_section_map_count = conn.execute("SELECT COUNT(*) AS n FROM pl_section_map").fetchone()["n"]
        schedule_c_map_count = conn.execute("SELECT COUNT(*) AS n FROM schedule_c_map").fetchone()["n"]
        foreign_keys = int(conn.execute("PRAGMA foreign_keys").fetchone()[0])

    assert report["bot_requests"] == 1
    assert report["bot_tool_calls"] == 1
    assert report["telegram_config"] == 1
    assert report["telegram_processed_updates"] == 1
    assert report["categories"] == 1
    assert "txn_fts" not in report
    assert "mileage_rates" not in report
    assert "pl_section_map" not in report
    assert "schedule_c_map" not in report
    assert remaining_categories == [("System Category", 1)]
    assert bot_requests_count == 0
    assert bot_tool_calls_count == 0
    assert telegram_config_count == 0
    assert processed_updates_count == 0
    assert mileage_rates_after == mileage_rates_before
    assert pl_section_map_count == 1
    assert schedule_c_map_count == 1
    assert foreign_keys == 1


def test_wipe_runtime_data_full_quotes_introspected_table_names(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    table_name = 'runtime odd"; DROP TABLE categories; --'
    quoted_table_name = '"' + table_name.replace('"', '""') + '"'

    with connect(db_path) as conn:
        conn.execute(f"CREATE TABLE {quoted_table_name} (id INTEGER PRIMARY KEY)")
        conn.execute(f"INSERT INTO {quoted_table_name} (id) VALUES (1)")
        conn.commit()

        report = wipe_runtime_data(conn, full=True)

        crafted_table_count = conn.execute(
            f"SELECT COUNT(*) AS n FROM {quoted_table_name}"
        ).fetchone()["n"]
        categories_table = conn.execute(
            """
            SELECT name
              FROM sqlite_master
             WHERE type = 'table'
               AND name = 'categories'
            """
        ).fetchone()

    assert report[table_name] == 1
    assert crafted_table_count == 0
    assert categories_table is not None


def test_db_reset_full_clears_introspected_tables_and_keeps_seed_tables(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        system_category_id = uuid.uuid4().hex
        user_category_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'System Category', 0, 1)",
            (system_category_id,),
        )
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'User Category', 0, 0)",
            (user_category_id,),
        )
        conn.execute(
            """
            INSERT INTO contractors (id, name, entity_type)
            VALUES (?, 'Contractor One', 'individual')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, access_token_ref, status,
                sync_cursor, last_sync_at, last_balance_refresh_at, last_liabilities_fetch_at
            ) VALUES (?, 'item_full', 'Test Bank', 'secret/ref', 'active', ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                "cursor_456",
                "2026-02-20 10:00:00",
                "2026-02-20 10:00:00",
                "2026-02-20 10:00:00",
            ),
        )
        mileage_rates_before = conn.execute("SELECT COUNT(*) AS n FROM mileage_rates").fetchone()["n"]
        conn.commit()

    code, payload = _run_cli(["db", "reset", "--yes", "--full"], capsys)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["full"] is True
    assert payload["data"]["wipe_report"]["contractors"] == 1
    assert payload["data"]["wipe_report"]["categories"] == 1
    assert payload["data"]["wipe_report"]["plaid_items_reset"] == 1

    with connect(db_path) as conn:
        contractor_count = conn.execute("SELECT COUNT(*) AS n FROM contractors").fetchone()["n"]
        remaining_categories = [
            (row["name"], row["is_system"])
            for row in conn.execute("SELECT name, is_system FROM categories ORDER BY name").fetchall()
        ]
        plaid_item = conn.execute(
            """
            SELECT sync_cursor, last_sync_at, last_balance_refresh_at, last_liabilities_fetch_at
              FROM plaid_items
             WHERE plaid_item_id = 'item_full'
            """
        ).fetchone()
        mileage_rates_after = conn.execute("SELECT COUNT(*) AS n FROM mileage_rates").fetchone()["n"]

    assert contractor_count == 0
    assert remaining_categories == [("System Category", 1)]
    assert plaid_item is not None
    assert plaid_item["sync_cursor"] is None
    assert plaid_item["last_sync_at"] is None
    assert plaid_item["last_balance_refresh_at"] is None
    assert plaid_item["last_liabilities_fetch_at"] is None
    assert mileage_rates_after == mileage_rates_before


def test_db_reset_blocks_on_active_subscriber_lock_for_canonical_install_db(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    monkeypatch.setattr(backup_module, "canonical_install_db_path", lambda: db_path)

    category_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'User Category', 0, 0)",
            (category_id,),
        )
        conn.commit()

    lock = InstallSubscriberLock(backup_module.install_subscriber_lock_path())
    assert lock.try_acquire() is True

    backup_dir = db_path.parent / "backups"
    backup_count_before = len(list(backup_dir.iterdir())) if backup_dir.exists() else 0
    conn = connect(db_path)
    try:
        with pytest.raises(SubscriberActiveError) as excinfo:
            db_cmd.handle_reset(
                SimpleNamespace(
                    yes=True,
                    drop_plaid_items=False,
                    full=True,
                    no_backup=False,
                    backup_output=None,
                ),
                conn,
            )
    finally:
        lock.release()
        conn.close()

    assert excinfo.value.user_message == (
        "Cannot reset: another CashNerd local MCP process is running. "
        "Stop it (e.g., close Claude Code or kill mcp_local) and retry."
    )
    backup_count_after = len(list(backup_dir.iterdir())) if backup_dir.exists() else 0
    assert backup_count_after == backup_count_before

    with connect(db_path) as conn:
        remaining = conn.execute(
            "SELECT COUNT(*) AS n FROM categories WHERE id = ?",
            (category_id,),
        ).fetchone()["n"]
    assert remaining == 1


def test_db_reset_against_scratch_db_skips_subscriber_lock(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    canonical_db_path = tmp_path / "install" / ".cashnerd" / "data" / "finance.db"
    monkeypatch.setattr(backup_module, "canonical_install_db_path", lambda: canonical_db_path)

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO categories (id, name, level, is_system) VALUES (?, 'User Category', 0, 0)",
            (uuid.uuid4().hex,),
        )
        conn.commit()

    lock = InstallSubscriberLock(backup_module.install_subscriber_lock_path())
    assert lock.try_acquire() is True
    try:
        code, payload = _run_cli(["db", "reset", "--yes", "--full", "--no-backup"], capsys)
    finally:
        lock.release()

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["full"] is True
    assert payload["data"]["wipe_report"]["categories"] == 1


def test_db_status_returns_snapshot(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        dining_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Dining', 0)", (dining_id,))

        active_account = uuid.uuid4().hex
        inactive_account = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO accounts (id, institution_name, account_type, is_active) VALUES (?, 'Test Bank', 'checking', 1)",
            (active_account,),
        )
        conn.execute(
            "INSERT INTO accounts (id, institution_name, account_type, is_active) VALUES (?, 'Old Bank', 'checking', 0)",
            (inactive_account,),
        )

        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, category_id, category_source, source, is_active, is_payment
            ) VALUES (?, ?, '2026-02-01', 'Lunch', -1500, ?, 'keyword_rule', 'manual', 1, 0)
            """,
            (uuid.uuid4().hex, active_account, dining_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, category_source, source, is_active, is_payment
            ) VALUES (?, ?, '2026-02-10', 'Transfer', -2500, NULL, 'manual', 1, 1)
            """,
            (uuid.uuid4().hex, active_account),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, category_id, category_source, source, is_active, is_payment
            ) VALUES (?, ?, '2026-01-15', 'Old Txn', -300, ?, 'user', 'manual', 0, 0)
            """,
            (uuid.uuid4().hex, inactive_account, dining_id),
        )

        conn.execute(
            """
            INSERT INTO import_batches (id, source_type, file_path, created_at)
            VALUES (?, 'csv', '/tmp/older.csv', '2026-01-01 09:00:00')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO import_batches (id, source_type, file_path, created_at)
            VALUES (?, 'pdf', '/tmp/latest.pdf', '2026-02-20 08:30:00')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    code, payload = _run_cli(["db", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.status"

    data = payload["data"]
    assert data["transaction_counts"] == {"total": 3, "active": 2, "inactive": 1}
    assert data["date_range"] == {"earliest": "2026-02-01", "latest": "2026-02-10"}
    assert data["active_account_count"] == 1
    assert data["uncategorized_count"] == 1
    assert data["payment_count"] == 1
    assert data["last_import_at"] == "2026-02-20 08:30:00"

    source_dist = {row["category_source"]: row["count"] for row in data["category_source_distribution"]}
    assert source_dist["keyword_rule"] == 1
    assert source_dist["uncategorized"] == 1

    top_categories = {row["category_name"]: row["count"] for row in data["top_categories"]}
    assert top_categories["Dining"] == 1
    assert top_categories["Uncategorized"] == 1
