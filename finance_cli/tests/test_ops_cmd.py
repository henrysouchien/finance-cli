from __future__ import annotations

import contextlib
import json
from types import SimpleNamespace

from finance_cli import __main__ as cli_main
from finance_cli.commands import ops_cmd


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.closed = True

    def execute(self, *_args, **_kwargs):
        return None

    def commit(self) -> None:
        return None


def test_ops_commands_skip_single_user_cli_db_bootstrap(monkeypatch, capsys) -> None:
    def fail_default_db(*_args, **_kwargs):
        raise AssertionError("ops command should not open the default CLI database")

    def fake_reseed(args, conn):
        assert conn is None
        assert args.user == "42"
        return {
            "data": {"results": []},
            "summary": {"users": 0, "reseeded": 0},
            "cli_report": "ok",
        }

    monkeypatch.setattr(cli_main, "initialize_database", fail_default_db)
    monkeypatch.setattr(cli_main, "connect", fail_default_db)
    monkeypatch.setattr(cli_main.ops_cmd, "handle_plan_caps_reseed", fake_reseed)

    code = cli_main.main(["ops", "plan-caps-reseed", "--user", "42", "--format", "json"])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["command"] == "ops.plan-caps-reseed"


def test_connect_user_db_uses_storage_aware_expected_user_id(monkeypatch, tmp_path) -> None:
    settings = ops_cmd.OpsSettings(
        data_root=tmp_path / "users",
        database_url="postgres://example/db",
    )
    calls: list[tuple[str, object]] = []

    @contextlib.contextmanager
    def fake_optional_lease_scope(user_id, **kwargs):
        calls.append(("lease", user_id, kwargs))
        yield None

    def fake_connect(db_path, **kwargs):
        calls.append(("connect", db_path, kwargs))
        return _FakeConn()

    monkeypatch.setattr(ops_cmd, "optional_lease_scope", fake_optional_lease_scope)
    monkeypatch.setattr(ops_cmd, "connect", fake_connect)

    with ops_cmd._connect_user_db(settings, "42"):
        pass

    assert calls[0][0] == "lease"
    assert calls[0][1] == "42"
    assert calls[0][2]["operation"] == "ops_user_db"
    assert calls[1][0] == "connect"
    assert calls[1][1] == tmp_path / "users" / "42" / "finance.db"
    assert calls[1][2]["expected_user_id"] == "42"
    assert calls[1][2]["busy_timeout"] == 5000


def test_plan_caps_reseed_routes_remote_users_without_local_file(monkeypatch, tmp_path) -> None:
    settings = ops_cmd.OpsSettings(
        data_root=tmp_path / "users",
        database_url="postgres://example/db",
    )
    args = SimpleNamespace(all_users=False, user="42")
    opened_users: list[str] = []

    @contextlib.contextmanager
    def fake_connect_user_db(_settings, user_id):
        opened_users.append(str(user_id))
        yield _FakeConn()

    monkeypatch.setattr(ops_cmd, "load_ops_settings", lambda: settings)
    monkeypatch.setattr(
        ops_cmd,
        "_fetch_user",
        lambda _settings, user_id: {
            "id": user_id,
            "tier": "paid",
            "lifetime_deal": False,
            "stripe_price_id": None,
            "storage_mode": "remote",
        },
    )
    monkeypatch.setattr(ops_cmd, "_connect_user_db", fake_connect_user_db)
    monkeypatch.setattr(
        ops_cmd,
        "reseed_user_plan_caps",
        lambda _conn, user, _settings: {"user_id": str(user["id"]), "plan_code": "standard"},
    )

    result = ops_cmd.handle_plan_caps_reseed(args, None)

    assert opened_users == ["42"]
    assert result["summary"] == {"users": 1, "reseeded": 1}
    assert result["data"]["results"][0]["status"] == "reseeded"


def test_plan_caps_reseed_reports_missing_local_db(monkeypatch, tmp_path) -> None:
    settings = ops_cmd.OpsSettings(
        data_root=tmp_path / "users",
        database_url="postgres://example/db",
    )
    args = SimpleNamespace(all_users=False, user="42")

    def fail_connect_user_db(*_args, **_kwargs):
        raise AssertionError("local missing DB should be reported before opening")

    monkeypatch.setattr(ops_cmd, "load_ops_settings", lambda: settings)
    monkeypatch.setattr(
        ops_cmd,
        "_fetch_user",
        lambda _settings, user_id: {
            "id": user_id,
            "tier": "paid",
            "lifetime_deal": False,
            "stripe_price_id": None,
            "storage_mode": "local",
        },
    )
    monkeypatch.setattr(ops_cmd, "_connect_user_db", fail_connect_user_db)

    result = ops_cmd.handle_plan_caps_reseed(args, None)

    assert result["summary"] == {"users": 1, "reseeded": 0}
    assert result["data"]["results"][0]["status"] == "missing_db"
