from __future__ import annotations

import sqlite3
from pathlib import Path

import finance_cli.cost_tracking as cost_tracking
from finance_cli.db import connect, initialize_database
import finance_cli.error_capture as error_capture
import finance_cli.plaid_client as plaid_client
from finance_cli.plaid_client import PlaidConfigStatus, refresh_balances, revoke_item_access
from finance_cli.storage_client import StorageConnection


class _StorageConnectionProxy(StorageConnection):
    def __init__(self, inner: sqlite3.Connection, *, user_id: str) -> None:
        self._inner = inner
        self._user_id = user_id

    def execute(self, sql, params=None):
        if str(sql).strip().lower().startswith("pragma database_list"):
            raise AssertionError("Plaid guardrails must not resolve StorageConnection paths via PRAGMA database_list")
        if params is None:
            return self._inner.execute(sql)
        return self._inner.execute(sql, params)

    def commit(self) -> None:
        self._inner.commit()

    def rollback(self) -> None:
        self._inner.rollback()


def _seed_plaid_item(conn, *, plaid_item_id: str) -> None:
    conn.execute(
        """
        INSERT INTO plaid_items (
            id,
            plaid_item_id,
            institution_name,
            access_token_ref,
            status,
            consented_products
        ) VALUES (?, ?, 'Test Bank', 'secret/token', 'active', '["transactions"]')
        """,
        (f"row-{plaid_item_id}", plaid_item_id),
    )
    conn.commit()


def _set_last_balance_refresh_at(conn, plaid_item_id: str, modifier: str) -> None:
    conn.execute(
        f"""
        UPDATE plaid_items
           SET last_balance_refresh_at = datetime('now', '{modifier}')
         WHERE plaid_item_id = ?
        """,
        (plaid_item_id,),
    )
    conn.commit()


def _mock_plaid_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        plaid_client,
        "config_status",
        lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
    )


def _mock_access_token(monkeypatch) -> None:
    monkeypatch.setattr(plaid_client, "_get_access_token_for_item", lambda item, region_name=None, **kwargs: "access-token")


def test_coerce_db_path_from_arg_handles_storage_connection(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))
    from finance_cli.storage_client import StorageConnection

    conn = StorageConnection.__new__(StorageConnection)
    conn._user_id = "1"

    result = plaid_client._coerce_db_path_from_arg(conn)

    assert result == tmp_path.resolve() / "1" / "finance.db"


def test_coerce_db_path_from_arg_passthrough_for_path(tmp_path: Path) -> None:
    db_path = str(tmp_path / "finance.db")

    result = plaid_client._coerce_db_path_from_arg(db_path)

    assert result == db_path


def test_coerce_db_path_from_arg_handles_sqlite_connection(tmp_path: Path) -> None:
    db_file = tmp_path / "finance.db"
    conn = sqlite3.connect(str(db_file))
    try:
        result = plaid_client._coerce_db_path_from_arg(conn)

        assert Path(str(result)).resolve() == db_file.resolve()
    finally:
        conn.close()


def test_coerce_db_path_from_arg_storage_conn_no_data_root_returns_none(monkeypatch) -> None:
    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)
    from finance_cli.storage_client import StorageConnection

    conn = StorageConnection.__new__(StorageConnection)
    conn._user_id = "1"

    assert plaid_client._coerce_db_path_from_arg(conn) is None


def test_revoke_item_access_returns_true_on_success(monkeypatch) -> None:
    revoked: list[str] = []
    monkeypatch.setattr("finance_cli.plaid_client._remove_remote_item", lambda access_token: revoked.append(access_token))

    assert revoke_item_access("access-token") is True
    assert revoked == ["access-token"]


def test_revoke_item_access_returns_false_on_failure(monkeypatch) -> None:
    def _raise(access_token: str) -> None:
        raise RuntimeError(f"boom:{access_token}")

    monkeypatch.setattr("finance_cli.plaid_client._remove_remote_item", _raise)

    assert revoke_item_access("access-token") is False


def test_refresh_balances_passes_webhook_source_to_check_cost_limit(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    observed: dict[str, object] = {}

    def _check_cost_limit(db_path_arg, provider, projected_cost_usd6=0, source="api"):
        observed["db_path"] = db_path_arg
        observed["provider"] = provider
        observed["projected_cost_usd6"] = projected_cost_usd6
        observed["source"] = source
        return True, None

    class _Resp:
        def to_dict(self):
            return {"accounts": []}

    class _Client:
        def accounts_balance_get(self, request):
            return _Resp()

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setattr(cost_tracking, "check_cost_limit", _check_cost_limit)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item-webhook")
        result = refresh_balances(conn, item_id="item-webhook", source="webhook")

    assert result["items_refreshed"] == 1
    assert observed == {
        "db_path": db_path.resolve(),
        "provider": "plaid",
        "projected_cost_usd6": 100_000,
        "source": "webhook",
    }


def test_refresh_balances_storage_connection_uses_user_db_path_for_guardrail(tmp_path: Path, monkeypatch) -> None:
    web_root = tmp_path / "web-data"
    db_path = web_root / "42" / "finance.db"
    db_path.parent.mkdir(parents=True)
    initialize_database(db_path)

    observed: dict[str, object] = {}

    def _check_cost_limit(db_path_arg, provider, projected_cost_usd6=0, source="api"):
        observed["db_path"] = db_path_arg
        observed["provider"] = provider
        observed["projected_cost_usd6"] = projected_cost_usd6
        observed["source"] = source
        return True, None

    class _Resp:
        def to_dict(self):
            return {"accounts": []}

    class _Client:
        def accounts_balance_get(self, request):
            return _Resp()

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(web_root))
    monkeypatch.setattr(cost_tracking, "check_cost_limit", _check_cost_limit)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())
    monkeypatch.setattr(plaid_client, "_record_plaid_api_call", lambda *args, **kwargs: None)

    with connect(db_path) as inner:
        _seed_plaid_item(inner, plaid_item_id="item-storage")
        conn = _StorageConnectionProxy(inner, user_id="42")
        result = refresh_balances(conn, item_id="item-storage", source="webhook")

    assert result["items_refreshed"] == 1
    assert observed == {
        "db_path": db_path.resolve(),
        "provider": "plaid",
        "projected_cost_usd6": 100_000,
        "source": "webhook",
    }


def test_refresh_balances_storage_connection_missing_data_root_blocks_before_spend(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    def _check_cost_limit(*args, **kwargs):
        raise AssertionError("check_cost_limit must not run without a resolved cost DB path")

    def _create_client():
        raise AssertionError("Plaid client must not be created when the cost guardrail path is unavailable")

    _mock_plaid_ready(monkeypatch)
    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)
    monkeypatch.setattr(cost_tracking, "check_cost_limit", _check_cost_limit)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", _create_client)

    with connect(db_path) as inner:
        _seed_plaid_item(inner, plaid_item_id="item-storage-no-root")
        conn = _StorageConnectionProxy(inner, user_id="42")
        result = refresh_balances(conn, item_id="item-storage-no-root", source="webhook")

    assert result["items_requested"] == 1
    assert result["items_refreshed"] == 0
    assert result["items"] == [
        {
            "plaid_item_id": "item-storage-no-root",
            "institution_name": "Test Bank",
            "status": "blocked_cost_limit",
            "reason": plaid_client.PLAID_COST_DB_PATH_UNAVAILABLE_REASON,
        }
    ]


def test_refresh_balances_guardrail_failure_fails_open(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    captured_errors: list[tuple[str, dict[str, object]]] = []
    client_calls = {"count": 0}

    def _raise_guardrail(*_args, **_kwargs):
        raise RuntimeError("guardrail down")

    class _Resp:
        def to_dict(self):
            return {"accounts": []}

    class _Client:
        def accounts_balance_get(self, request):
            client_calls["count"] += 1
            return _Resp()

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setattr(cost_tracking, "check_cost_limit", _raise_guardrail)
    monkeypatch.setattr(
        error_capture,
        "capture_error",
        lambda exc, **kwargs: captured_errors.append((str(exc), kwargs)),
    )
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item-guardrail")
        result = refresh_balances(conn, item_id="item-guardrail", source="webhook")

    assert result["items_refreshed"] == 1
    assert client_calls["count"] == 1
    assert captured_errors == [
        (
            "guardrail down",
            {
                "source": "webhook",
                "endpoint": "plaid.balance_refresh",
                "db_path": db_path.resolve(),
            },
        )
    ]


def test_refresh_balances_cooldown_skips_preflight_and_ledger_row(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    guardrail_calls: list[str] = []
    client_calls = {"count": 0}

    def _check_cost_limit(*_args, **_kwargs):
        guardrail_calls.append("called")
        return True, None

    class _Client:
        def accounts_balance_get(self, request):
            client_calls["count"] += 1
            raise AssertionError("accounts_balance_get should not be called during cooldown skip")

    _mock_plaid_ready(monkeypatch)
    _mock_access_token(monkeypatch)
    monkeypatch.setattr(cost_tracking, "check_cost_limit", _check_cost_limit)
    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item-cooldown")
        _set_last_balance_refresh_at(conn, "item-cooldown", "-60 seconds")
        result = refresh_balances(conn, item_id="item-cooldown")

    with connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM cost_ledger").fetchone()

    assert result["items_skipped"] == 1
    assert guardrail_calls == []
    assert client_calls["count"] == 0
    assert row["count"] == 0
