from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from pathlib import Path

from finance_cli.__main__ import build_parser, main
from finance_cli.db import connect, initialize_database
from finance_cli.plaid_client import PlaidConfigStatus


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    out = capsys.readouterr().out
    payload = json.loads(out)
    return code, payload


def test_cli_parser_accepts_investments_product() -> None:
    parser = build_parser()
    args = parser.parse_args(["plaid", "link", "--product", "investments"])
    assert args.product == ["investments"]


def test_plaid_status_reports_configuration(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.delenv("PLAID_CLIENT_ID", raising=False)
    monkeypatch.delenv("PLAID_SECRET", raising=False)
    monkeypatch.delenv("PLAID_ENV", raising=False)

    code, payload = _run_cli(["plaid", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "plaid.status"
    assert payload["data"]["configured"] is False
    assert isinstance(payload["data"]["items"], list)


def test_plaid_status_highlights_error_items(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, status, last_sync_at
            ) VALUES (?, 'item_amex', 'Amex', 'active', '2026-02-21T10:30:00')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, status, error_code, last_sync_at
            ) VALUES (?, 'item_paypal', 'PayPal', 'error', ?, NULL)
            """,
            (
                uuid.uuid4().hex,
                "ITEM_LOGIN_REQUIRED session expired and needs relink because credentials changed at institution",
            ),
        )
        conn.commit()

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )

    code, payload = _run_cli(["plaid", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "plaid.status"
    assert payload["data"]["active_count"] == 1
    assert payload["data"]["error_count"] == 1
    assert payload["summary"]["error_count"] == 1
    assert payload["cli_report"] is not None
    assert "items=2 active=1 errors=1 configured=True sdk=True" in payload["cli_report"]
    assert "Amex: status=active last_sync=2026-02-21T10:30:00" in payload["cli_report"]
    assert "PayPal: status=error last_sync=never error=ITEM_LOGIN_REQUIRED" in payload["cli_report"]
    assert "-> Fix: finance plaid link --update --item item_paypal --wait --user-id default" in payload["cli_report"]


def test_plaid_unlink_marks_item_disconnected(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    plaid_item_id = "item_test_123"
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, status)
            VALUES (?, ?, 'Test Bank', 'active')
            """,
            (uuid.uuid4().hex, plaid_item_id),
        )
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_item_id, institution_name, account_type, is_active)
            VALUES (?, ?, 'Test Bank', 'checking', 1)
            """,
            (uuid.uuid4().hex, plaid_item_id),
        )
        conn.commit()

    code, payload = _run_cli(["plaid", "unlink", "--item", plaid_item_id], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert Path(payload["data"]["backup_path"]).exists()

    with connect(db_path) as conn:
        item = conn.execute("SELECT status FROM plaid_items WHERE plaid_item_id = ?", (plaid_item_id,)).fetchone()
        account = conn.execute("SELECT is_active FROM accounts WHERE plaid_item_id = ?", (plaid_item_id,)).fetchone()

    # Items with no access_token_ref are deleted rather than marked disconnected
    assert item is None
    assert account["is_active"] == 0


def test_plaid_link_wait_success(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.create_hosted_link_session",
        lambda conn, user_id, update_item_id=None, include_balance=False, include_liabilities=False, requested_products=None: {
            "link_token": "link-token-123",
            "hosted_link_url": "https://plaid.test/link",
            "expiration": "2030-01-01T00:00:00Z",
            "requested_products": ["transactions"],
            "update_item_id": update_item_id,
        },
    )
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.complete_link_session",
        lambda conn, user_id, link_token, timeout_seconds, poll_seconds, requested_products=None, allow_duplicate_institution=False: {
            "id": "local-item-id",
            "plaid_item_id": "item_live_123",
            "institution_name": "Test Bank",
            "access_token_ref": "plaid_token_user_test-bank",
            "status": "active",
        },
    )

    code, payload = _run_cli(["plaid", "link", "--user-id", "user", "--wait"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["session"]["link_token"] == "link-token-123"
    assert payload["data"]["linked_item"]["plaid_item_id"] == "item_live_123"
    assert Path(payload["data"]["backup_path"]).exists()


def test_plaid_link_wait_failure_returns_error_envelope(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.create_hosted_link_session",
        lambda conn, user_id, update_item_id=None, include_balance=False, include_liabilities=False, requested_products=None: {
            "link_token": "link-token-123",
            "hosted_link_url": "https://plaid.test/link",
            "expiration": "2030-01-01T00:00:00Z",
            "requested_products": ["transactions"],
            "update_item_id": update_item_id,
        },
    )

    def _fail_complete(
        conn,
        user_id,
        link_token,
        timeout_seconds,
        poll_seconds,
        requested_products=None,
        allow_duplicate_institution=False,
    ):
        raise RuntimeError("public token exchange failed")

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.complete_link_session", _fail_complete)

    code = main(["plaid", "link", "--user-id", "user", "--wait"])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "error"
    assert payload["command"] == "plaid.link"
    assert "public token exchange failed" in payload["error"]


def test_plaid_products_backfill_success(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.backfill_item_products",
        lambda conn, item_id=None: {
            "items_requested": 1,
            "items_updated": 1,
            "items_failed": 0,
            "errors": [],
            "items": [{"plaid_item_id": "item_abc", "consented_products": ["transactions", "liabilities"]}],
        },
    )

    code, payload = _run_cli(["plaid", "products-backfill"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "plaid.products_backfill"
    assert payload["summary"]["items_updated"] == 1


def test_plaid_link_sanitizes_email_user_id_for_link_session(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    captured: dict[str, str] = {}
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )

    def _fake_create_hosted_link_session(
        conn,
        user_id,
        update_item_id=None,
        include_balance=False,
        include_liabilities=False,
        requested_products=None,
    ):
        captured["user_id"] = user_id
        return {
            "link_token": "link-token-123",
            "hosted_link_url": "https://plaid.test/link",
            "expiration": "2030-01-01T00:00:00Z",
            "requested_products": ["transactions"],
            "update_item_id": update_item_id,
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.create_hosted_link_session", _fake_create_hosted_link_session)

    code, payload = _run_cli(["plaid", "link", "--user-id", "user@example.com"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["user_id_sanitized"] is True
    assert payload["data"]["client_user_id"].startswith("user_")
    assert captured["user_id"] == payload["data"]["client_user_id"]


def test_plaid_link_wait_passes_allow_duplicate_flag(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    captured: dict[str, bool] = {}
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.create_hosted_link_session",
        lambda conn, user_id, update_item_id=None, include_balance=False, include_liabilities=False, requested_products=None: {
            "link_token": "link-token-123",
            "hosted_link_url": "https://plaid.test/link",
            "expiration": "2030-01-01T00:00:00Z",
            "requested_products": ["transactions"],
            "update_item_id": update_item_id,
        },
    )

    def _fake_complete_link_session(
        conn,
        user_id,
        link_token,
        timeout_seconds,
        poll_seconds,
        requested_products=None,
        allow_duplicate_institution=False,
    ):
        captured["allow_duplicate_institution"] = allow_duplicate_institution
        return {
            "id": "local-item-id",
            "plaid_item_id": "item_live_123",
            "institution_name": "Test Bank",
            "access_token_ref": "plaid_token_user_test-bank",
            "status": "active",
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.complete_link_session", _fake_complete_link_session)

    code, payload = _run_cli(["plaid", "link", "--user-id", "user", "--wait", "--allow-duplicate"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert captured["allow_duplicate_institution"] is True


def test_plaid_link_wait_passes_sanitized_user_id_to_completion(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    captured: dict[str, str] = {}
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )

    def _fake_create_hosted_link_session(
        conn,
        user_id,
        update_item_id=None,
        include_balance=False,
        include_liabilities=False,
        requested_products=None,
    ):
        captured["create_user_id"] = user_id
        return {
            "link_token": "link-token-123",
            "hosted_link_url": "https://plaid.test/link",
            "expiration": "2030-01-01T00:00:00Z",
            "requested_products": ["transactions"],
            "update_item_id": update_item_id,
        }

    def _fake_complete_link_session(
        conn,
        user_id,
        link_token,
        timeout_seconds,
        poll_seconds,
        requested_products=None,
        allow_duplicate_institution=False,
    ):
        captured["complete_user_id"] = user_id
        return {
            "id": "local-item-id",
            "plaid_item_id": "item_live_123",
            "institution_name": "Test Bank",
            "access_token_ref": "plaid_token_user_test-bank",
            "status": "active",
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.create_hosted_link_session", _fake_create_hosted_link_session)
    monkeypatch.setattr("finance_cli.commands.plaid_cmd.complete_link_session", _fake_complete_link_session)

    code, payload = _run_cli(["plaid", "link", "--user-id", "john doe", "--wait"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["user_id_sanitized"] is True
    assert captured["create_user_id"] == "john-doe"
    assert captured["complete_user_id"] == "john-doe"


def test_plaid_balance_refresh_success(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.refresh_balances",
        lambda conn, item_id=None, **kwargs: {
            "items_requested": 1,
            "items_refreshed": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "accounts_updated": 2,
            "snapshots_updated": 2,
            "errors": [],
            "items": [],
        },
    )

    code, payload = _run_cli(["plaid", "balance-refresh"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "plaid.balance_refresh"
    assert payload["summary"]["accounts_updated"] == 2


def test_plaid_liabilities_sync_success(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.fetch_liabilities",
        lambda conn, item_id=None, **kwargs: {
            "items_requested": 1,
            "items_synced": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "liabilities_upserted": 3,
            "liabilities_deactivated": 1,
            "errors": [],
            "items": [],
        },
    )

    code, payload = _run_cli(["plaid", "liabilities-sync"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "plaid.liabilities_sync"
    assert payload["summary"]["liabilities_upserted"] == 3


def test_plaid_liabilities_sync_serializes_date_and_datetime_payload(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.fetch_liabilities",
        lambda conn, item_id=None, **kwargs: {
            "items_requested": 1,
            "items_synced": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "liabilities_upserted": 1,
            "liabilities_deactivated": 0,
            "errors": [],
            "items": [{"as_of": date(2026, 2, 18), "generated_at": datetime(2026, 2, 18, 10, 15, 0)}],
        },
    )

    code, payload = _run_cli(["plaid", "liabilities-sync"], capsys)
    assert code == 0
    assert payload["data"]["items"][0]["as_of"] == "2026-02-18"
    assert payload["data"]["items"][0]["generated_at"] == "2026-02-18 10:15:00"


def test_sync_force_flag_forwards_to_run_sync(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    captured: dict[str, bool] = {}

    def _fake_run_sync(conn, days=None, item_id=None, **kwargs):
        captured["force_refresh"] = bool(kwargs.get("force_refresh"))
        return {
            "items_requested": 1,
            "items_synced": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "added": 0,
            "modified": 0,
            "removed": 0,
            "total_elapsed_ms": 0,
            "errors": [],
            "items": [],
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.run_sync", _fake_run_sync)

    code, payload = _run_cli(["plaid", "sync", "--force"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert captured["force_refresh"] is True


def test_plaid_sync_cli_includes_elapsed(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.run_sync",
        lambda conn, days=None, item_id=None, **kwargs: {
            "items_requested": 1,
            "items_synced": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "added": 2,
            "modified": 3,
            "removed": 1,
            "total_elapsed_ms": 321,
            "errors": [],
            "items": [],
        },
    )

    code = main(["plaid", "sync", "--format", "cli"])
    output = capsys.readouterr().out
    assert code == 0
    assert "elapsed=321ms" in output


def test_balance_force_flag_forwards(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    captured: dict[str, bool] = {}

    def _fake_refresh(conn, item_id=None, **kwargs):
        captured["force_refresh"] = bool(kwargs.get("force_refresh"))
        return {
            "items_requested": 1,
            "items_refreshed": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "accounts_updated": 0,
            "snapshots_updated": 0,
            "errors": [],
            "items": [],
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.refresh_balances", _fake_refresh)

    code, payload = _run_cli(["plaid", "balance-refresh", "--force"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert captured["force_refresh"] is True


def test_liabilities_force_flag_forwards(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    captured: dict[str, bool] = {}

    def _fake_liabilities(conn, item_id=None, **kwargs):
        captured["force_refresh"] = bool(kwargs.get("force_refresh"))
        return {
            "items_requested": 1,
            "items_synced": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "liabilities_upserted": 0,
            "liabilities_deactivated": 0,
            "errors": [],
            "items": [],
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.fetch_liabilities", _fake_liabilities)

    code, payload = _run_cli(["plaid", "liabilities-sync", "--force"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert captured["force_refresh"] is True
