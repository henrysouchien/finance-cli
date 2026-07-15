from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

from finance_cli.__main__ import build_parser, main
from finance_cli.commands import plaid_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.plaid_client import PlaidConfigStatus
from finance_cli.storage_client import StorageConnection


class _NoDatabaseListStorageConnection(StorageConnection):
    def __init__(self, *, user_id: str) -> None:
        self._user_id = user_id

    def execute(self, sql, params=None):
        raise AssertionError("Plaid link guardrails must not resolve StorageConnection paths via PRAGMA database_list")


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    out = capsys.readouterr().out
    payload = json.loads(out)
    return code, payload


def test_cli_parser_accepts_investments_product() -> None:
    parser = build_parser()
    args = parser.parse_args(["plaid", "link", "--product", "investments"])
    assert args.product == ["investments"]


def test_plaid_link_storage_connection_guardrail_uses_user_db_path(tmp_path: Path, monkeypatch) -> None:
    web_root = tmp_path / "web-data"
    observed: dict[str, object] = {}

    def _check_cost_limit(db_path_arg, provider, projected_cost_usd6=0, source="api"):
        observed["db_path"] = db_path_arg
        observed["provider"] = provider
        observed["projected_cost_usd6"] = projected_cost_usd6
        observed["source"] = source
        return True, None

    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(web_root))
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr("finance_cli.commands.plaid_cmd.check_cost_limit", _check_cost_limit)
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.create_hosted_link_session",
        lambda *args, **kwargs: {"link_token": "link-token", "hosted_link_url": None, "requested_products": []},
    )

    args = SimpleNamespace(
        user_id="default",
        wait=False,
        item=None,
        update=False,
        include_balance=False,
        include_liabilities=False,
        product=[],
        open_browser=False,
        timeout=300,
        poll_seconds=10,
        allow_duplicate=False,
    )

    result = plaid_cmd.handle_link(args, _NoDatabaseListStorageConnection(user_id="42"))

    assert result["summary"] == {"ready": True, "waited": False}
    assert observed == {
        "db_path": web_root.resolve() / "42" / "finance.db",
        "provider": "plaid",
        "projected_cost_usd6": plaid_cmd.PLAID_ITEM_MONTHLY_USD6,
        "source": "cli",
    }


def test_plaid_link_storage_connection_missing_data_root_blocks_before_session(monkeypatch) -> None:
    def _check_cost_limit(*args, **kwargs):
        raise AssertionError("check_cost_limit must not run without a resolved cost DB path")

    def _create_session(*args, **kwargs):
        raise AssertionError("Plaid link session must not be created when the cost guardrail path is unavailable")

    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)
    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )
    monkeypatch.setattr("finance_cli.commands.plaid_cmd.check_cost_limit", _check_cost_limit)
    monkeypatch.setattr("finance_cli.commands.plaid_cmd.create_hosted_link_session", _create_session)

    args = SimpleNamespace(
        user_id="default",
        wait=False,
        item=None,
        update=False,
        include_balance=False,
        include_liabilities=False,
        product=[],
        open_browser=False,
        timeout=300,
        poll_seconds=10,
        allow_duplicate=False,
    )

    result = plaid_cmd.handle_link(args, _NoDatabaseListStorageConnection(user_id="42"))

    assert result == {
        "data": {
            "blocked": True,
            "reason": plaid_cmd._plaid_cost_path_unavailable_reason(),
        },
        "summary": {"ready": True, "blocked": 1},
        "cli_report": f"Plaid link blocked by cost guardrail: {plaid_cmd._plaid_cost_path_unavailable_reason()}",
    }


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
                id, plaid_item_id, institution_name, status, access_token_ref,
                sync_cursor, last_sync_at
            ) VALUES (?, 'item_amex', 'Amex', 'active', 'secret/item_amex',
                'cursor-secret', '2026-02-21T10:30:00')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, status, access_token_ref,
                error_code, last_sync_at
            ) VALUES (?, 'item_paypal', 'PayPal', 'error', 'secret/item_paypal',
                ?, NULL)
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
    assert payload["data"]["token_missing_count"] == 0
    assert payload["summary"]["error_count"] == 1
    assert payload["summary"]["token_missing_count"] == 0
    assert all("access_token_ref" not in item for item in payload["data"]["items"])
    assert all("sync_cursor" not in item for item in payload["data"]["items"])
    active_item = next(item for item in payload["data"]["items"] if item["plaid_item_id"] == "item_amex")
    assert active_item["has_token_ref"] is True
    assert payload["cli_report"] is not None
    assert "items=2 active=1 errors=1 configured=True sdk=True" in payload["cli_report"]
    assert "Amex: status=active last_sync=2026-02-21T10:30:00" in payload["cli_report"]
    assert "PayPal: status=error last_sync=never error=ITEM_LOGIN_REQUIRED" in payload["cli_report"]
    assert "-> Fix: finance plaid link --update --item item_paypal --wait --user-id default" in payload["cli_report"]


def test_plaid_status_reports_active_items_missing_token_refs(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, status, access_token_ref,
                last_sync_at
            ) VALUES (?, 'item_missing_token', 'Missing Token Bank', 'active',
                NULL, '2026-02-21T10:30:00')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    monkeypatch.setattr(
        "finance_cli.commands.plaid_cmd.config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )

    code, payload = _run_cli(["plaid", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["active_count"] == 1
    assert payload["data"]["error_count"] == 0
    assert payload["data"]["token_missing_count"] == 1
    assert payload["summary"]["token_missing_count"] == 1

    item = payload["data"]["items"][0]
    assert item["plaid_item_id"] == "item_missing_token"
    assert item["has_token_ref"] is False
    assert item["token_missing"] is True
    assert "access_token_ref" not in item
    assert "sync_cursor" not in item

    assert "token_missing=1" in payload["cli_report"]
    assert "Missing Token Bank: status=active last_sync=2026-02-21T10:30:00" in payload["cli_report"]
    assert "token=missing" in payload["cli_report"]
    assert "finance plaid unlink --item item_missing_token" in payload["cli_report"]


def test_plaid_sync_threads_explicit_rules_path(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text("{}\n", encoding="utf-8")

    captured: dict[str, object] = {}

    def fake_run_sync(
        conn,
        days=None,
        item_id=None,
        force_refresh=False,
        backfill=False,
        region_name=None,
        rules_path=None,
    ):
        del conn, backfill, region_name
        captured["days"] = days
        captured["item_id"] = item_id
        captured["force_refresh"] = force_refresh
        captured["rules_path"] = rules_path
        return {
            "items_requested": 1,
            "items_synced": 1,
            "items_skipped": 0,
            "items_failed": 0,
            "added": 2,
            "modified": 0,
            "removed": 0,
            "total_elapsed_ms": 5,
            "errors": [],
            "items": [],
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.run_sync", fake_run_sync)

    with connect(db_path) as conn:
        result = plaid_cmd.handle_sync(
            SimpleNamespace(days=30, item="item_123", force=True),
            conn,
            rules_path=rules_path,
        )

    assert captured == {
        "days": 30,
        "item_id": "item_123",
        "force_refresh": True,
        "rules_path": rules_path,
    }
    assert result["summary"]["items_synced"] == 1
    assert result["summary"]["added"] == 2


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


def test_handle_plaid_exchange_completes_link_session(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    captured: dict[str, object] = {}

    def _fake_complete_link_session(
        conn,
        user_id,
        link_token,
        timeout_seconds,
        poll_seconds,
        requested_products=None,
        allow_duplicate_institution=False,
    ):
        captured["conn"] = conn
        captured["user_id"] = user_id
        captured["link_token"] = link_token
        captured["timeout_seconds"] = timeout_seconds
        captured["poll_seconds"] = poll_seconds
        captured["requested_products"] = requested_products
        captured["allow_duplicate_institution"] = allow_duplicate_institution
        return {
            "plaid_item_id": "item_live_123",
            "institution_name": "Test Bank",
            "status": "active",
        }

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.complete_link_session", _fake_complete_link_session)

    with connect(db_path) as conn:
        result = plaid_cmd.handle_plaid_exchange(
            SimpleNamespace(link_token="link-token-123", requested_products=["transactions", "liabilities"]),
            conn,
        )

    assert captured["user_id"] == "finance-cli-user"
    assert captured["link_token"] == "link-token-123"
    assert captured["timeout_seconds"] == 300
    assert captured["poll_seconds"] == 10
    assert captured["requested_products"] == ["transactions", "liabilities"]
    assert captured["allow_duplicate_institution"] is False
    assert result["data"]["plaid_item_id"] == "item_live_123"
    assert result["summary"]["status"] == "active"


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


def test_sync_backfill_flag_forwards_to_run_sync(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    captured: dict[str, object] = {}

    def _fake_run_sync(conn, days=None, item_id=None, **kwargs):
        del conn
        captured["days"] = days
        captured["item_id"] = item_id
        captured["force_refresh"] = bool(kwargs.get("force_refresh"))
        captured["backfill"] = bool(kwargs.get("backfill"))
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

    code, payload = _run_cli(
        ["plaid", "sync", "--days", "730", "--item", "item_123", "--backfill"],
        capsys,
    )
    assert code == 0
    assert payload["status"] == "success"
    assert captured == {
        "days": 730,
        "item_id": "item_123",
        "force_refresh": False,
        "backfill": True,
    }
    assert payload["summary"]["backfill"] is True


def test_plaid_sync_proxies_when_using_local_sync_db(tmp_path: Path, monkeypatch, capsys) -> None:
    local_root = tmp_path / ".cashnerd"
    local_data = local_root / "data"
    local_db = local_data / "finance.db"
    local_data.mkdir(parents=True)

    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(local_data))
    monkeypatch.delenv("FINANCE_CLI_DB", raising=False)

    import finance_cli.sync.cli_proxy as cli_proxy
    import finance_cli.sync.auth as sync_auth
    import finance_cli.sync.config as sync_config
    import finance_cli.sync.engine as sync_engine

    monkeypatch.setattr(sync_config, "CASHNERD_DIR", local_root)
    monkeypatch.setattr(sync_config, "CASHNERD_CONFIG_PATH", local_root / "config.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AUTH_DIR", local_root / "auth")
    monkeypatch.setattr(sync_config, "CASHNERD_TOKEN_PATH", local_root / "auth" / "token.json")
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", local_data)
    monkeypatch.setattr(sync_config, "CASHNERD_DB_PATH", local_db)
    monkeypatch.setattr(sync_config, "CASHNERD_RULES_PATH", local_data / "rules.yaml")
    monkeypatch.setattr(sync_config, "CASHNERD_UPLOADS_DIR", local_data / "uploads")
    monkeypatch.setattr(sync_config, "CASHNERD_SKILL_STATE_PATH", local_data / "skill_state.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AGENT_MEMORY_PATH", local_data / "agent_memory.md")
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_DIR", local_root / "sync")
    monkeypatch.setattr(sync_config, "CASHNERD_PENDING_CHANGESET_PATH", local_root / "sync" / "pending_changeset.json")
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_LOG_PATH", local_root / "sync" / "sync_log.json")

    monkeypatch.setattr(sync_engine, "CASHNERD_DIR", sync_config.CASHNERD_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DATA_DIR", sync_config.CASHNERD_DATA_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DB_PATH", sync_config.CASHNERD_DB_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_RULES_PATH", sync_config.CASHNERD_RULES_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_SKILL_STATE_PATH", sync_config.CASHNERD_SKILL_STATE_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_AGENT_MEMORY_PATH", sync_config.CASHNERD_AGENT_MEMORY_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_PENDING_CHANGESET_PATH", sync_config.CASHNERD_PENDING_CHANGESET_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_SYNC_LOG_PATH", sync_config.CASHNERD_SYNC_LOG_PATH)
    monkeypatch.setattr(cli_proxy, "CASHNERD_DB_PATH", local_db)
    monkeypatch.setattr(sync_auth, "CASHNERD_TOKEN_PATH", sync_config.CASHNERD_TOKEN_PATH)

    proxy_calls: list[tuple[str, dict[str, object], bool]] = []
    pulls = 0

    async def fake_proxy_tool(self, tool_name, arguments=None, *, wait_for_subscriber=True):
        del self
        proxy_calls.append((tool_name, dict(arguments or {}), wait_for_subscriber))
        return {
            "data": {
                "items_requested": 1,
                "items_synced": 1,
                "items_skipped": 0,
                "items_failed": 0,
                "added": 2,
                "modified": 0,
                "removed": 0,
                "total_elapsed_ms": 12,
                "errors": [],
                "items": [],
            },
            "summary": {"items_synced": 1, "added": 2},
        }

    async def fake_pull(self):
        nonlocal pulls
        del self
        pulls += 1
        return True

    monkeypatch.setattr(sync_engine.SyncEngine, "proxy_tool", fake_proxy_tool)
    monkeypatch.setattr(sync_engine.SyncEngine, "pull", fake_pull)

    def _unexpected_run_sync(*args, **kwargs):
        raise AssertionError("local synced Plaid sync should use the server proxy")

    monkeypatch.setattr("finance_cli.commands.plaid_cmd.run_sync", _unexpected_run_sync)

    code, payload = _run_cli(["plaid", "sync", "--force", "--backfill"], capsys)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "plaid.sync"
    assert proxy_calls == [
        ("plaid_sync", {"days": None, "item": None, "force": True, "backfill": True}, False)
    ]
    assert pulls == 1
    assert payload["data"]["items_synced"] == 1
    assert "items_synced=1" in payload["cli_report"]


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
