from __future__ import annotations

import hashlib
from datetime import datetime, timezone, timedelta
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.user_provisioning import user_db_path


@pytest.fixture()
def mcp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _hash(user_id: str) -> str:
    return hashlib.sha256(user_id.encode("utf-8")).hexdigest()


def _utc_rollup_date() -> date:
    return datetime.now(timezone.utc).date()


def _init_user_db(data_root: Path, user_id: str) -> Path:
    db_path = user_db_path(data_root, user_id)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(db_path)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tenant_marker (singleton, user_id) VALUES (1, ?)",
            (user_id,),
        )
        conn.commit()
    return db_path


def _insert_topup(data_root: Path, user_id: str, created_at: str) -> None:
    with connect(user_db_path(data_root, user_id), expected_user_id=user_id) as conn:
        conn.execute(
            """
            INSERT INTO credit_ledger (source, amount_usd6, stripe_payment_intent_id, created_at, notes)
            VALUES ('topup', 5000000, ?, ?, 'test pack')
            """,
            (f"pi_{user_id}", created_at),
        )
        conn.commit()


def _install_fake_rollups(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]) -> None:
    import finance_cli.mcp_server as mcp_server

    def fake_fetch(*, database_url: str, start_date: date, end_exclusive: date) -> list[dict[str, Any]]:
        assert database_url == "postgresql://fake/finance"
        return [
            row
            for row in rows
            if start_date <= date.fromisoformat(str(row["date"])[:10]) < end_exclusive
        ]

    monkeypatch.setenv("DATABASE_URL", "postgresql://fake/finance")
    monkeypatch.setattr(mcp_server, "_fetch_ops_cost_rollup_rows", fake_fetch)


class _FakeUserRefCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def execute(self, _sql: str, _params: tuple[Any, ...] = ()) -> None:
        return None

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class _FakeUserRefPg:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def cursor(self) -> _FakeUserRefCursor:
        return _FakeUserRefCursor(self.rows)

    def __enter__(self) -> "_FakeUserRefPg":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False


def test_cost_unit_economics_empty_rollups_returns_no_data(
    mcp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    _install_fake_rollups(monkeypatch, [])

    result = mcp_server.cost_unit_economics(months=3)

    assert result["data"]["available"] is False
    assert result["data"]["reason"] == "no_data"
    assert result["data"]["period_days"] == 90
    assert result["data"]["rollup_plan_quality"] == {
        "included_row_count": 0,
        "excluded_row_count": 0,
        "excluded_total_usd6": 0,
        "excluded_total_usd": 0.0,
        "excluded_request_count": 0,
        "attribution_sources": {},
        "excluded_reasons": {},
    }
    assert result["summary"]["excluded_rollup_row_count"] == 0


def test_cost_unit_economics_allows_missing_ambient_user_db(
    mcp_db: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    today = _utc_rollup_date()
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path / "users"))
    _install_fake_rollups(
        monkeypatch,
        [
            {
                "date": today.isoformat(),
                "user_hash": _hash("ops-user"),
                "provider": "claude",
                "tier": "paid",
                "plan_code": "standard",
                "total_usd6": 478_238,
                "request_count": 9,
            }
        ],
    )

    def missing_ambient_db():
        raise ValueError(
            "DB path does not live under FINANCE_WEB_DATA_ROOT: "
            "/root/.local/share/finance_cli/finance.db"
        )

    monkeypatch.setattr(mcp_server, "_get_conn", missing_ambient_db)

    result = mcp_server.cost_unit_economics(months=3)

    assert result["data"]["available"] is True
    assert result["data"]["plans"]["standard"]["total_users"] == 1
    assert result["data"]["plans"]["standard"]["total_usd6"] == 478_238
    assert result["data"]["plans"]["standard"]["request_count"] == 9
    assert result["data"]["rollup_plan_quality"]["included_row_count"] == 1


def test_cost_unit_economics_reports_unrelated_ambient_db_errors(
    mcp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    today = _utc_rollup_date()
    _install_fake_rollups(
        monkeypatch,
        [
            {
                "date": today.isoformat(),
                "user_hash": _hash("ops-user"),
                "provider": "claude",
                "tier": "paid",
                "plan_code": "standard",
                "total_usd6": 478_238,
                "request_count": 9,
            }
        ],
    )

    def unrelated_db_error():
        raise ValueError("unexpected SQLite bootstrap failure")

    monkeypatch.setattr(mcp_server, "_get_conn", unrelated_db_error)

    result = mcp_server.cost_unit_economics(months=3)

    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert result["message"] == "unexpected SQLite bootstrap failure"


def test_cost_unit_economics_populated_rollups_group_by_plan_and_percentiles(
    mcp_db: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    data_root = tmp_path / "users"
    today = _utc_rollup_date()
    for user_id in ("alice", "bob", "carol"):
        _init_user_db(data_root, user_id)
    _insert_topup(data_root, "alice", f"{today.isoformat()} 12:00:00")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    rows = [
        {
            "date": today.isoformat(),
            "user_hash": _hash("alice"),
            "provider": "claude",
            "tier": "paid",
            "plan_code": "lite",
            "total_usd6": 1_000_000,
            "request_count": 2,
        },
        {
            "date": today.isoformat(),
            "user_hash": _hash("bob"),
            "provider": "claude",
            "tier": "paid",
            "plan_code": "lite",
            "total_usd6": 2_000_000,
            "request_count": 1,
        },
        {
            "date": today.isoformat(),
            "user_hash": _hash("carol"),
            "provider": "openai",
            "tier": "paid",
            "plan_code": "standard",
            "total_usd6": 500_000,
            "request_count": 1,
        },
    ]
    _install_fake_rollups(monkeypatch, rows)

    result = mcp_server.cost_unit_economics(months=3, price_points="5,10,20")

    assert result["data"]["available"] is True
    lite = result["data"]["plans"]["lite"]
    assert {"p50", "p85", "p95", "p99"}.issubset(lite)
    assert lite["total_users"] == 2
    assert lite["total_usd6"] == 3_000_000
    assert lite["p50"] == 1.0
    assert lite["p85"] == 2.0
    assert lite["cap_hit_count"] == 1
    assert lite["credit_purchase_user_count"] == 1
    assert lite["credit_purchase_rate_pct"] == 50.0
    assert lite["by_provider"]["claude"]["total_users"] == 2
    assert result["data"]["plans"]["standard"]["by_provider"]["openai"]["p50"] == 0.5
    assert result["data"]["rollup_plan_quality"]["included_row_count"] == 3
    assert result["data"]["rollup_plan_quality"]["excluded_row_count"] == 0
    assert result["data"]["price_points"] == [5, 10, 20]


def test_cost_unit_economics_counts_remote_credit_topups(
    mcp_db: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    data_root = tmp_path / "users"
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    today = _utc_rollup_date()
    rows = [
        {
            "date": today.isoformat(),
            "user_hash": _hash("remote-user"),
            "provider": "claude",
            "tier": "paid",
            "plan_code": "lite",
            "total_usd6": 1_000_000,
            "request_count": 1,
        },
    ]
    _install_fake_rollups(monkeypatch, rows)
    monkeypatch.setattr(
        mcp_server,
        "_ops_postgres_connect",
        lambda _database_url: _FakeUserRefPg(
            [{"id": "remote-user", "storage_mode": "remote"}]
        ),
    )
    manager = object()
    monkeypatch.setattr(
        mcp_server,
        "_default_cost_economics_session_manager",
        lambda: manager,
    )
    scanned: list[tuple[str, str | None, object | None]] = []

    def fake_user_has_credit_topup(
        *,
        data_root: Path,
        user_id: str,
        start_ts: str,
        end_ts: str,
        storage_mode: str | None = None,
        storage_session_manager=None,
    ) -> bool:
        del data_root, start_ts, end_ts
        scanned.append((user_id, storage_mode, storage_session_manager))
        return True

    monkeypatch.setattr(
        mcp_server,
        "_user_has_credit_topup",
        fake_user_has_credit_topup,
    )

    result = mcp_server.cost_unit_economics(months=3)

    lite = result["data"]["plans"]["lite"]
    assert lite["credit_purchase_user_count"] == 1
    assert lite["credit_purchase_rate_pct"] == 100.0
    assert scanned == [("remote-user", "remote", manager)]


def test_cost_unit_economics_excludes_ambiguous_plan_rows_from_percentiles(
    mcp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    today = _utc_rollup_date()
    _install_fake_rollups(
        monkeypatch,
        [
            {
                "date": today.isoformat(),
                "user_hash": _hash("standard"),
                "provider": "claude",
                "tier": "paid",
                "plan_code": "standard",
                "total_usd6": 1_000_000,
                "request_count": 1,
            },
            {
                "date": today.isoformat(),
                "user_hash": _hash("trial"),
                "provider": "claude",
                "tier": "trial",
                "plan_code": None,
                "total_usd6": 2_000_000,
                "request_count": 1,
            },
            {
                "date": today.isoformat(),
                "user_hash": _hash("lifetime"),
                "provider": "openai",
                "tier": "lifetime",
                "plan_code": None,
                "total_usd6": 3_000_000,
                "request_count": 1,
            },
            {
                "date": today.isoformat(),
                "user_hash": _hash("ambiguous-paid"),
                "provider": "claude",
                "tier": "paid",
                "plan_code": None,
                "total_usd6": 9_000_000,
                "request_count": 4,
            },
            {
                "date": today.isoformat(),
                "user_hash": _hash("unknown-plan"),
                "provider": "claude",
                "tier": "paid",
                "plan_code": "enterprise",
                "total_usd6": 8_000_000,
                "request_count": 3,
            },
        ],
    )

    result = mcp_server.cost_unit_economics(months=3)

    assert result["data"]["available"] is True
    assert result["data"]["plans"]["standard"]["total_users"] == 2
    assert result["data"]["plans"]["standard"]["total_usd6"] == 3_000_000
    assert result["data"]["plans"]["lifetime"]["total_users"] == 1
    assert result["data"]["plans"]["lifetime"]["total_usd6"] == 3_000_000
    quality = result["data"]["rollup_plan_quality"]
    assert quality["included_row_count"] == 3
    assert quality["excluded_row_count"] == 2
    assert quality["excluded_total_usd6"] == 17_000_000
    assert quality["excluded_request_count"] == 7
    assert quality["excluded_reasons"] == {
        "invalid_plan_code": 1,
        "missing_plan_code": 1,
    }
    assert quality["attribution_sources"] == {
        "invalid_plan_code": 1,
        "missing_plan_code": 1,
        "plan_code": 1,
        "tier_lifetime_fallback": 1,
        "tier_trial_fallback": 1,
    }
    assert result["summary"]["excluded_rollup_row_count"] == 2


def test_cost_unit_economics_all_ambiguous_rows_returns_unavailable(
    mcp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    today = _utc_rollup_date()
    _install_fake_rollups(
        monkeypatch,
        [
            {
                "date": today.isoformat(),
                "user_hash": _hash("ambiguous-paid"),
                "provider": "claude",
                "tier": "paid",
                "plan_code": None,
                "total_usd6": 9_000_000,
                "request_count": 4,
            },
        ],
    )

    result = mcp_server.cost_unit_economics(months=3)

    assert result["data"]["available"] is False
    assert result["data"]["reason"] == "no_attributed_plan_data"
    assert result["data"]["rollup_plan_quality"]["excluded_row_count"] == 1
    assert result["summary"]["available"] is False
    assert result["summary"]["reason"] == "no_attributed_plan_data"


def test_cost_unit_economics_summary_only_uses_cache(
    mcp_db: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    fake_module_file = tmp_path / "finance_cli" / "mcp_server.py"
    fake_module_file.parent.mkdir(parents=True, exist_ok=True)
    fake_module_file.write_text("# test path\n", encoding="utf-8")
    monkeypatch.setattr(mcp_server, "__file__", str(fake_module_file))
    today = _utc_rollup_date()
    _install_fake_rollups(
        monkeypatch,
        [
            {
                "date": today.isoformat(),
                "user_hash": _hash("alice"),
                "provider": "claude",
                "tier": "paid",
                "plan_code": "standard",
                "total_usd6": 1_000_000,
                "request_count": 1,
            }
        ],
    )

    result = mcp_server.cost_unit_economics(summary_only=True)

    assert "plans" not in result["data"]
    assert result["data"]["available"] is True
    cache_id = result["data"]["cache_id"]
    assert (tmp_path / "exports" / "mcp_cache" / f"{cache_id}.readthrough.json").exists()


def test_cost_unit_economics_trailing_90_day_window_excludes_older_rows(
    mcp_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_db
    import finance_cli.mcp_server as mcp_server

    today = _utc_rollup_date()
    rows = [
        {
            "date": today.isoformat(),
            "user_hash": _hash("recent"),
            "provider": "claude",
            "tier": "paid",
            "plan_code": "standard",
            "total_usd6": 1_000_000,
            "request_count": 1,
        },
        {
            "date": (today - timedelta(days=100)).isoformat(),
            "user_hash": _hash("old"),
            "provider": "claude",
            "tier": "paid",
            "plan_code": "standard",
            "total_usd6": 9_000_000,
            "request_count": 1,
        },
    ]
    _install_fake_rollups(monkeypatch, rows)

    result = mcp_server.cost_unit_economics(months=3)

    standard = result["data"]["plans"]["standard"]
    assert standard["total_users"] == 1
    assert standard["total_usd6"] == 1_000_000
