from __future__ import annotations

import json
from types import SimpleNamespace
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.category_seed import CATEGORY_HIERARCHY, INCOME_NAMES
from finance_cli.commands import setup_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.plaid_client import PlaidConfigStatus
from finance_cli.user_rules import CANONICAL_CATEGORIES


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _configure_paths(tmp_path: Path, monkeypatch, *, disable_dotenv: bool = True) -> tuple[Path, Path, Path]:
    db_path = tmp_path / "finance.db"
    env_path = tmp_path / ".env"
    rules_path = tmp_path / "rules.yaml"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_ENV_FILE", str(env_path))
    if disable_dotenv:
        monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    else:
        monkeypatch.delenv("FINANCE_CLI_DISABLE_DOTENV", raising=False)
    return db_path, env_path, rules_path


def _check_by_id(payload: dict, check_id: str) -> dict:
    for check in payload["data"]["checks"]:
        if check["id"] == check_id:
            return check
    raise AssertionError(f"check not found: {check_id}")


def test_setup_init_seeds_all_categories_with_hierarchy(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path, env_path, rules_path = _configure_paths(tmp_path, monkeypatch)

    code, payload = _run_cli(["setup", "init"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "setup.init"
    assert payload["data"]["categories"]["expected_total"] == len(CANONICAL_CATEGORIES)
    assert env_path.exists()
    assert rules_path.exists()

    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, name, parent_id, level, is_income, is_system FROM categories ORDER BY name ASC"
        ).fetchall()

    assert len(rows) == len(CANONICAL_CATEGORIES)
    by_name = {str(row["name"]): row for row in rows}

    for parent_name, children in CATEGORY_HIERARCHY.items():
        parent_row = by_name[parent_name]
        assert parent_row["parent_id"] is None
        assert int(parent_row["level"]) == 0
        assert int(parent_row["is_system"]) == 1
        expected_is_income = int(parent_name in INCOME_NAMES)
        assert int(parent_row["is_income"]) == expected_is_income

        for child_name in children:
            child_row = by_name[child_name]
            assert str(child_row["parent_id"]) == str(parent_row["id"])
            assert int(child_row["level"]) == 1
            assert int(child_row["is_system"]) == 1
            expected_child_income = int(child_name in INCOME_NAMES)
            assert int(child_row["is_income"]) == expected_child_income


def test_setup_init_is_idempotent(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)

    first_code, _ = _run_cli(["setup", "init"], capsys)
    assert first_code == 0

    second_code, second_payload = _run_cli(["setup", "init"], capsys)
    assert second_code == 0
    assert second_payload["data"]["categories"]["created"] == 0
    assert second_payload["data"]["categories"]["updated"] == 0


def test_setup_init_reconciles_existing_wrong_category_state(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path, _, _ = _configure_paths(tmp_path, monkeypatch)
    initialize_database(db_path)

    with connect(db_path) as conn:
        other_id = uuid.uuid4().hex
        income_id = uuid.uuid4().hex
        salary_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO categories (id, name, parent_id, level, is_income, is_system) VALUES (?, 'Other', NULL, 0, 0, 0)",
            (other_id,),
        )
        conn.execute(
            "INSERT INTO categories (id, name, parent_id, level, is_income, is_system) VALUES (?, 'Income', ?, 1, 0, 0)",
            (income_id, other_id),
        )
        conn.execute(
            "INSERT INTO categories (id, name, parent_id, level, is_income, is_system) VALUES (?, 'Income: Salary', ?, 0, 0, 0)",
            (salary_id, other_id),
        )
        conn.commit()

    code, payload = _run_cli(["setup", "init"], capsys)
    assert code == 0
    assert payload["data"]["categories"]["updated"] >= 2

    with connect(db_path) as conn:
        income = conn.execute(
            "SELECT id, parent_id, level, is_income, is_system FROM categories WHERE name = 'Income'"
        ).fetchone()
        salary = conn.execute(
            "SELECT parent_id, level, is_income, is_system FROM categories WHERE name = 'Income: Salary'"
        ).fetchone()

    assert income is not None
    assert salary is not None
    assert income["parent_id"] is None
    assert int(income["level"]) == 0
    assert int(income["is_income"]) == 1
    assert int(income["is_system"]) == 1
    assert str(salary["parent_id"]) == str(income["id"])
    assert int(salary["level"]) == 1
    assert int(salary["is_income"]) == 1
    assert int(salary["is_system"]) == 1


def test_setup_init_dry_run_makes_no_changes(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path, env_path, rules_path = _configure_paths(tmp_path, monkeypatch)

    code, payload = _run_cli(["setup", "init", "--dry-run"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["categories"]["would_create"] == len(CANONICAL_CATEGORIES)
    assert env_path.exists() is False
    assert rules_path.exists() is False

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM categories").fetchone()["n"]
    assert count == 0


def test_setup_init_does_not_overwrite_existing_env(tmp_path: Path, monkeypatch, capsys) -> None:
    _, env_path, _ = _configure_paths(tmp_path, monkeypatch)
    original = "EXISTING=1\n"
    env_path.write_text(original, encoding="utf-8")

    code, payload = _run_cli(["setup", "init"], capsys)
    assert code == 0
    assert payload["data"]["env_template"]["created"] is False
    assert env_path.read_text(encoding="utf-8") == original


def test_handle_init_gateway_skips_env_and_rules(tmp_path: Path, monkeypatch) -> None:
    db_path, env_path, rules_path = _configure_paths(tmp_path, monkeypatch)
    initialize_database(db_path)

    with connect(db_path) as conn:
        result = setup_cmd.handle_init(SimpleNamespace(dry_run=False, gateway=True), conn)

    assert result["data"]["env_template"]["skipped"] is True
    assert result["data"]["rules_file"]["skipped"] is True
    assert result["data"]["env_template"]["path"] is None
    assert result["data"]["rules_file"]["path"] is None
    assert env_path.exists() is False
    assert rules_path.exists() is False

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM categories").fetchone()["n"]

    assert count == len(CANONICAL_CATEGORIES)


def test_handle_init_gateway_preserves_response_schema(tmp_path: Path, monkeypatch) -> None:
    db_path, _, _ = _configure_paths(tmp_path, monkeypatch)
    initialize_database(db_path)

    with connect(db_path) as conn:
        result = setup_cmd.handle_init(SimpleNamespace(dry_run=True, gateway=True), conn)

    assert set(result["data"]["env_template"]) == {"path", "created", "would_create", "dry_run", "skipped"}
    assert set(result["data"]["rules_file"]) == {
        "path",
        "created",
        "source_path",
        "would_create",
        "dry_run",
        "skipped",
    }
    assert result["data"]["env_template"]["path"] is None
    assert result["data"]["env_template"]["created"] is False
    assert result["data"]["env_template"]["would_create"] is False
    assert result["data"]["env_template"]["dry_run"] is True
    assert result["data"]["rules_file"]["path"] is None
    assert result["data"]["rules_file"]["created"] is False
    assert result["data"]["rules_file"]["source_path"] is None
    assert result["data"]["rules_file"]["would_create"] is False
    assert result["data"]["rules_file"]["dry_run"] is True


def test_handle_init_cli_unchanged(tmp_path: Path, monkeypatch) -> None:
    db_path, env_path, rules_path = _configure_paths(tmp_path, monkeypatch)
    initialize_database(db_path)

    with connect(db_path) as conn:
        result = setup_cmd.handle_init(SimpleNamespace(dry_run=False), conn)

    assert result["data"]["env_template"]["created"] is True
    assert result["data"]["env_template"]["path"] == str(env_path.resolve())
    assert "skipped" not in result["data"]["env_template"]
    assert result["data"]["rules_file"]["created"] is True
    assert result["data"]["rules_file"]["path"] == str(rules_path.resolve())
    assert "skipped" not in result["data"]["rules_file"]
    assert env_path.exists() is True
    assert rules_path.exists() is True


def test_setup_check_reports_missing_plaid_as_fail(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=False, has_sdk=False, missing_env=["PLAID_CLIENT_ID"], env=None),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )

    code, payload = _run_cli(["setup", "check"], capsys)
    assert code == 0
    plaid_check = _check_by_id(payload, "plaid")
    assert plaid_check["status"] == "FAIL"


def test_setup_check_warns_when_aws_missing(tmp_path: Path, monkeypatch, capsys) -> None:
    _, env_path, _ = _configure_paths(tmp_path, monkeypatch)
    env_path.write_text("PLAID_CLIENT_ID=a\n", encoding="utf-8")
    _run_cli(["setup", "init"], capsys)

    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": False, "region": None, "region_source": None, "error": "AWS region missing"},
    )

    code, payload = _run_cli(["setup", "check"], capsys)
    assert code == 0
    aws_check = _check_by_id(payload, "aws")
    assert aws_check["status"] == "WARN"


def test_setup_check_reports_env_path_when_dotenv_disabled(tmp_path: Path, monkeypatch, capsys) -> None:
    _, env_path, _ = _configure_paths(tmp_path, monkeypatch, disable_dotenv=True)
    env_path.write_text("PLAID_CLIENT_ID=a\n", encoding="utf-8")

    code, payload = _run_cli(["setup", "check"], capsys)
    assert code == 0
    dotenv_check = _check_by_id(payload, "dotenv")
    assert str(env_path.resolve()) in dotenv_check["detail"]


def test_setup_connect_preflight_failure_does_not_open_browser(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    opened: list[str] = []
    monkeypatch.setattr("finance_cli.commands.setup_cmd.webbrowser.open", lambda url: opened.append(url))
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=False, has_sdk=False, missing_env=["PLAID_CLIENT_ID"], env=None),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.create_hosted_link_session",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not create session")),
    )

    code, payload = _run_cli(["setup", "connect", "--open-browser"], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert opened == []


def test_setup_connect_preflight_failure_does_not_send_alerts(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    alert_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "finance_cli.error_capture.alerts",
        SimpleNamespace(send=lambda body, channel: alert_calls.append((body, channel))),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=False, has_sdk=False, missing_env=["PLAID_CLIENT_ID"], env=None),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )

    code, payload = _run_cli(["setup", "connect", "--open-browser"], capsys)

    assert code == 1
    assert payload["status"] == "error"
    assert alert_calls == []


def test_setup_connect_aws_preflight_failure_does_not_open_browser(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    opened: list[str] = []
    monkeypatch.setattr("finance_cli.commands.setup_cmd.webbrowser.open", lambda url: opened.append(url))
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": False, "region": None, "region_source": None, "error": "AWS region missing"},
    )

    code, payload = _run_cli(["setup", "connect", "--open-browser"], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert opened == []


def test_setup_connect_success_scopes_sync_and_balance_to_new_item(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    opened: list[str] = []
    captured: dict[str, str] = {}
    monkeypatch.setattr("finance_cli.commands.setup_cmd.webbrowser.open", lambda url: opened.append(url))
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.create_hosted_link_session",
        lambda conn, user_id, include_balance=True, include_liabilities=False: {
            "link_token": "link-token",
            "hosted_link_url": "https://plaid.test/link",
            "requested_products": ["transactions"],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.complete_link_session",
        lambda conn, user_id, link_token, timeout_seconds, requested_products=None: {
            "plaid_item_id": "item_new_123",
            "institution_name": "Test Bank",
        },
    )

    def _fake_sync(conn, days=None, item_id=None, force_refresh=False, region_name=None):
        captured["sync_item_id"] = str(item_id)
        return {
            "items_requested": 1,
            "items_synced": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "added": 5,
            "modified": 0,
            "removed": 0,
            "total_elapsed_ms": 10,
            "errors": [],
            "items": [],
        }

    def _fake_refresh(conn, item_id=None, force_refresh=False, region_name=None):
        captured["refresh_item_id"] = str(item_id)
        return {
            "items_requested": 1,
            "items_refreshed": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "accounts_updated": 2,
            "snapshots_updated": 2,
            "errors": [],
            "items": [],
        }

    monkeypatch.setattr("finance_cli.commands.setup_cmd.run_sync", _fake_sync)
    monkeypatch.setattr("finance_cli.commands.setup_cmd.refresh_balances", _fake_refresh)

    code, payload = _run_cli(["setup", "connect"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["session"]["hosted_link_url"] == "https://plaid.test/link"
    assert captured["sync_item_id"] == "item_new_123"
    assert captured["refresh_item_id"] == "item_new_123"
    assert opened == []


def test_setup_connect_skip_sync_skips_sync_and_balance(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.create_hosted_link_session",
        lambda conn, user_id, include_balance=True, include_liabilities=False: {
            "link_token": "link-token",
            "hosted_link_url": "https://plaid.test/link",
            "requested_products": ["transactions"],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.complete_link_session",
        lambda conn, user_id, link_token, timeout_seconds, requested_products=None: {
            "plaid_item_id": "item_new_123",
            "institution_name": "Test Bank",
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.run_sync",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sync should be skipped")),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.refresh_balances",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("refresh should be skipped")),
    )

    code, payload = _run_cli(["setup", "connect", "--skip-sync"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["post_link"]["skipped"] is True


def test_setup_connect_open_browser_flag_opens_browser(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    opened: list[str] = []
    monkeypatch.setattr("finance_cli.commands.setup_cmd.webbrowser.open", lambda url: opened.append(url))
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.create_hosted_link_session",
        lambda conn, user_id, include_balance=True, include_liabilities=False: {
            "link_token": "link-token",
            "hosted_link_url": "https://plaid.test/link",
            "requested_products": ["transactions"],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.complete_link_session",
        lambda conn, user_id, link_token, timeout_seconds, requested_products=None: {
            "plaid_item_id": "item_new_123",
            "institution_name": "Test Bank",
        },
    )

    code, payload = _run_cli(["setup", "connect", "--skip-sync", "--open-browser"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert opened == ["https://plaid.test/link"]


def test_setup_connect_reports_partial_success_when_sync_fails(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.create_hosted_link_session",
        lambda conn, user_id, include_balance=True, include_liabilities=False: {
            "link_token": "link-token",
            "hosted_link_url": "https://plaid.test/link",
            "requested_products": ["transactions"],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.complete_link_session",
        lambda conn, user_id, link_token, timeout_seconds, requested_products=None: {
            "plaid_item_id": "item_new_123",
            "institution_name": "Test Bank",
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.run_sync",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("sync exploded")),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.refresh_balances",
        lambda conn, item_id=None, force_refresh=False, region_name=None: {
            "items_requested": 1,
            "items_refreshed": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "accounts_updated": 1,
            "snapshots_updated": 1,
            "errors": [],
            "items": [],
        },
    )

    code, payload = _run_cli(["setup", "connect"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["partial_success"] is True
    assert "sync failed" in payload["data"]["partial_errors"][0]


def test_setup_connect_timeout_returns_error(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.create_hosted_link_session",
        lambda conn, user_id, include_balance=True, include_liabilities=False: {
            "link_token": "link-token",
            "hosted_link_url": "https://plaid.test/link",
            "requested_products": ["transactions"],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.complete_link_session",
        lambda conn, user_id, link_token, timeout_seconds, requested_products=None: (_ for _ in ()).throw(
            RuntimeError("Timed out waiting for Plaid Link completion")
        ),
    )

    code, payload = _run_cli(["setup", "connect"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["hosted_link_url"] == "https://plaid.test/link"
    assert "Timed out" in payload["data"]["error"]
    assert payload["summary"]["linked"] is False


def test_setup_status_delegates_to_db_and_plaid_handlers(tmp_path: Path, monkeypatch, capsys) -> None:
    _configure_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._run_env_checks",
        lambda conn, db_path=None, rules_path=None: {
            "ready": True,
            "checks": [],
            "counts": {"ok": 1, "warn": 0, "fail": 0},
            "next_steps": [],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.db_cmd.handle_status",
        lambda args, conn: {"data": {"transaction_counts": {"active": 7}}},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.plaid_cmd.handle_status",
        lambda args, conn: {
            "data": {
                "items": [
                    {
                        "plaid_item_id": "item_1",
                        "access_token_ref": "secret/item_1",
                        "sync_cursor": "cursor-secret",
                        "has_token_ref": True,
                    }
                ],
                "active_count": 1,
            }
        },
    )

    code, payload = _run_cli(["setup", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["db"] == {"transaction_counts": {"active": 7}}
    assert payload["data"]["plaid"] == {
        "items": [{"plaid_item_id": "item_1", "has_token_ref": True}],
        "active_count": 1,
        "token_missing_count": 0,
    }
    assert "access_token_ref" not in payload["data"]["plaid"]["items"][0]
    assert "sync_cursor" not in payload["data"]["plaid"]["items"][0]


def test_setup_status_env_check_uses_explicit_rules_path(tmp_path: Path, monkeypatch) -> None:
    db_path, env_path, default_rules_path = _configure_paths(tmp_path, monkeypatch)
    env_path.write_text("PLAID_CLIENT_ID=test\n", encoding="utf-8")
    explicit_rules_path = tmp_path / "user" / "rules.yaml"
    explicit_rules_path.parent.mkdir()
    explicit_rules_path.write_text("{}\n", encoding="utf-8")
    assert default_rules_path.exists() is False
    initialize_database(db_path)

    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._aws_readiness",
        lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.db_cmd.handle_status",
        lambda args, conn: {"data": {"transaction_counts": {"active": 1}}},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.plaid_cmd.handle_status",
        lambda args, conn: {"data": {"items": [{"status": "active"}], "active_count": 1}},
    )

    with connect(db_path) as conn:
        setup_cmd._seed_canonical_categories(conn, dry_run=False)
        conn.commit()

        result = setup_cmd.handle_status(
            SimpleNamespace(),
            conn,
            db_path=db_path,
            rules_path=explicit_rules_path,
        )

    rules_check = next(check for check in result["data"]["env"]["checks"] if check["id"] == "rules")
    assert result["summary"]["ready"] is True
    assert rules_check["status"] == "OK"
    assert str(explicit_rules_path.resolve()) in rules_check["detail"]
    assert result["data"]["rules"] == {
        "path": str(explicit_rules_path.resolve()),
        "exists": True,
    }
    assert "rules.yaml missing" not in "\n".join(result["data"]["next_steps"])


def test_setup_status_warns_when_active_plaid_item_missing_token_ref(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _, _, rules_path = _configure_paths(tmp_path, monkeypatch)
    rules_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._run_env_checks",
        lambda conn, db_path=None, rules_path=None: {
            "ready": True,
            "checks": [],
            "counts": {"ok": 1, "warn": 0, "fail": 0},
            "next_steps": [],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.db_cmd.handle_status",
        lambda args, conn: {"data": {"transaction_counts": {"active": 7}}},
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd._category_coverage",
        lambda conn: {
            "expected_total": 2,
            "present_count": 2,
            "missing_count": 0,
            "missing": [],
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.setup_cmd.plaid_cmd.handle_status",
        lambda args, conn: {
            "data": {
                "items": [
                    {
                        "plaid_item_id": "item_missing_token",
                        "institution_name": "Missing Token Bank",
                        "status": "active",
                        "access_token_ref": None,
                        "sync_cursor": "cursor-secret",
                        "has_token_ref": False,
                        "token_missing": True,
                    }
                ],
                "active_count": 1,
                "token_missing_count": 1,
            }
        },
    )

    code, payload = _run_cli(["setup", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["plaid"]["token_missing_count"] == 1
    assert payload["data"]["plaid"]["items"] == [
        {
            "plaid_item_id": "item_missing_token",
            "institution_name": "Missing Token Bank",
            "status": "active",
            "has_token_ref": False,
            "token_missing": True,
        }
    ]
    assert payload["summary"]["plaid_token_missing_count"] == 1
    assert payload["data"]["next_steps"] == [
        "One Plaid item is active but missing its token reference; "
        "run `finance_cli plaid unlink --item item_missing_token` and reconnect."
    ]
    assert "Plaid Items:   1 total, 1 active, 1 token missing" in payload["cli_report"]
