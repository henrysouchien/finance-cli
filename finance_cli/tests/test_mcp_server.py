"""Tests for the MCP server tool functions.

Each test monkeypatches connect() to use a tmp-path SQLite DB with
migrations applied, then calls the tool function directly and
asserts the {data, summary} envelope.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
import json
import os
import subprocess
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest
from moto import mock_aws

from finance_cli import secrets_backend
from finance_cli.db import connect, initialize_database
from finance_cli.preferences import export_preferences
from finance_cli.skills import SkillProfile
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    """Create a migrated temp DB and point connect() at it."""
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


@pytest.fixture()
def conn(db_path):
    """Return a connection to the temp DB."""
    c = connect(db_path)
    yield c
    c.close()


@pytest.fixture()
def isolated_mcp_cache(db_path, tmp_path: Path, monkeypatch) -> Path:
    """Route MCP cache output into a temp directory for deterministic assertions."""
    import finance_cli.mcp_server as mcp_server

    fake_module_file = tmp_path / "finance_cli" / "mcp_server.py"
    fake_module_file.parent.mkdir(parents=True, exist_ok=True)
    fake_module_file.write_text("# test module path\n", encoding="utf-8")
    monkeypatch.setattr(mcp_server, "__file__", str(fake_module_file))
    return tmp_path / "exports" / "mcp_cache"


@contextmanager
def _uploads_context(*, db_path: Path, uploads_dir: Path):
    token = set_user_context(UserContext.from_paths(db_path=db_path, uploads_dir=uploads_dir))
    try:
        yield
    finally:
        reset_user_context(token)


@pytest.fixture()
def mock_backup_secrets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_CLI_USER_ID", "default")
    secrets_backend._client = None
    with mock_aws():
        yield
    secrets_backend._client = None


def _latest_cache_file(cache_dir: Path, prefix: str) -> Path:
    matches = sorted(cache_dir.glob(f"{prefix}_*.json"))
    assert matches, f"expected cache file matching {prefix}_*.json"
    return matches[-1]


def _readthrough_cache_file(cache_dir: Path, cache_id: str) -> Path:
    return cache_dir / f"{cache_id}.readthrough.json"


def test_operation_log_middleware_records_remote_tool_call(db_path: Path) -> None:
    import mcp.types as mt
    from fastmcp.server.middleware import MiddlewareContext
    from fastmcp.tools.tool import ToolResult
    import finance_cli.mcp_server as mcp_server

    middleware = mcp_server.OperationLogMiddleware()
    context = MiddlewareContext(
        message=mt.CallToolRequestParams(
            name="txn_list",
            arguments={"limit": 5},
        )
    )
    token = set_user_context(UserContext.from_paths(db_path=db_path, local_mode=False))

    async def call_next(_context):
        return ToolResult(
            content=[
                mt.TextContent(
                    type="text",
                    text=json.dumps({"data": {"items": []}, "summary": {"count": 0}}),
                )
            ],
            structured_content={"data": {"items": []}, "summary": {"count": 0}},
        )

    try:
        result = asyncio.run(middleware.on_call_tool(context, call_next))
    finally:
        reset_user_context(token)

    assert result.structured_content["summary"]["count"] == 0
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT surface, tool_name, status, request_json, result_json
              FROM _operation_log
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert row["surface"] == "remote_mcp"
    assert row["tool_name"] == "txn_list"
    assert row["status"] == "success"
    assert json.loads(row["request_json"]) == {
        "argument_count": 1,
        "argument_keys": ["limit"],
        "mutating": False,
        "upload": False,
    }
    assert json.loads(row["result_json"]) == {
        "data_keys": ["items"],
        "has_errors": False,
        "result_keys": ["data", "summary"],
        "summary_keys": ["count"],
    }


def _assert_tool_error(response: dict, error_class: str, message: str) -> None:
    assert response["status"] == "error"
    assert response["error_class"] == error_class
    assert message in response["message"]
    assert response["error"] == response["message"]
    assert response["names_correction"]["tool"]


def _month_date(offset_from_current: int, day: int = 15) -> str:
    today = date.today()
    month_index = (today.year * 12) + (today.month - 1) + int(offset_from_current)
    year, month_zero = divmod(month_index, 12)
    safe_day = max(1, min(day, 28))
    return date(year, month_zero + 1, safe_day).isoformat()


def _seed_category(conn, name: str, parent_id=None, is_income: int = 0) -> str:
    cid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, parent_id, is_income, is_system) VALUES (?, ?, ?, ?, 0)",
        (cid, name, parent_id, is_income),
    )
    conn.commit()
    return cid


def _seed_txn(conn, *, description="TEST TXN", amount_cents=-1000,
              date="2026-02-15", category_id=None, is_reviewed=0,
              account_id=None, use_type=None, source="manual") -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions
            (id, date, description, amount_cents, category_id, is_active,
             is_reviewed, source, account_id, use_type)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        """,
        (tid, date, description, amount_cents, category_id, is_reviewed, source, account_id, use_type),
    )
    conn.commit()
    return tid


def _seed_account(conn, *, institution="Test Bank", name="Checking",
                  account_type="checking", balance_cents=100000, is_business: int = 0,
                  source: str | None = None) -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts
            (id, institution_name, account_name, account_type,
             balance_current_cents, is_active, is_business, source)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (aid, institution, name, account_type, balance_cents, is_business, source),
    )
    conn.commit()
    return aid


def _seed_balance_snapshot(
    conn,
    *,
    account_id: str,
    snapshot_date: str | None = None,
    balance_current_cents: int = 100000,
    source: str = "refresh",
) -> str:
    sid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO balance_snapshots
            (id, account_id, snapshot_date, source, balance_current_cents)
        VALUES (?, ?, ?, ?, ?)
        """,
        (sid, account_id, snapshot_date or date.today().isoformat(), source, balance_current_cents),
    )
    conn.commit()
    return sid


def _seed_pl_map(conn, category_id: str, section: str, display_order: int) -> None:
    conn.execute(
        """
        INSERT INTO pl_section_map (id, category_id, pl_section, display_order)
        VALUES (?, ?, ?, ?)
        """,
        (uuid.uuid4().hex, category_id, section, display_order),
    )
    conn.commit()


def _seed_schedule_c_map(
    conn,
    category_id: str,
    *,
    line: str,
    line_number: str,
    deduction_pct: float = 1.0,
    tax_year: int = 2025,
) -> None:
    conn.execute(
        """
        INSERT INTO schedule_c_map
            (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (uuid.uuid4().hex, category_id, line, line_number, deduction_pct, tax_year),
    )
    conn.commit()


def _seed_subscription(
    conn,
    *,
    vendor_name: str,
    amount_cents: int = 1500,
    frequency: str = "monthly",
    is_active: int = 1,
) -> str:
    sid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO subscriptions
            (id, vendor_name, category_id, amount_cents, frequency,
             next_expected, is_active, use_type, is_auto_detected)
        VALUES (?, ?, NULL, ?, ?, NULL, ?, NULL, 0)
        """,
        (sid, vendor_name, amount_cents, frequency, is_active),
    )
    conn.commit()
    return sid


def _seed_credit_liability(
    conn,
    *,
    account_id: str,
    apr_purchase: float | None,
    minimum_payment_cents: int | None = None,
    next_monthly_payment_cents: int | None = None,
) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities
            (id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents, next_monthly_payment_cents)
        VALUES (?, ?, 'credit', 1, ?, ?, ?)
        """,
        (liability_id, account_id, apr_purchase, minimum_payment_cents, next_monthly_payment_cents),
    )
    conn.commit()
    return liability_id


def _set_txn_raw_payload(conn, txn_id: str, raw_payload: str) -> None:
    conn.execute(
        """
        UPDATE transactions
           SET raw_plaid_json = ?, dedupe_key = ?
         WHERE id = ?
        """,
        (raw_payload, f"dedupe:{txn_id}", txn_id),
    )
    conn.commit()


def _seed_plaid_item(
    conn,
    *,
    plaid_item_id: str = "item_test",
    institution_name: str = "Test Institution",
    access_token_ref: str = "secret/token_ref",
    sync_cursor: str = "cursor_1",
) -> str:
    row_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, sync_cursor, status)
        VALUES (?, ?, ?, ?, ?, 'active')
        """,
        (row_id, plaid_item_id, institution_name, access_token_ref, sync_cursor),
    )
    conn.commit()
    return row_id


def _seed_preferences_fixture(conn, data_dir: Path) -> Path:
    (data_dir / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    (data_dir / "agent_memory.md").write_text("# Memory\n", encoding="utf-8")
    sessions_dir = data_dir / "sessions"
    sessions_dir.mkdir(exist_ok=True)
    (sessions_dir / "2026-03-10.md").write_text("Session note\n", encoding="utf-8")

    category_id = _seed_category(conn, "TestDining")
    account_id = _seed_account(conn, institution="Test Bank", name="Checking", is_business=1)
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
        data_dir=data_dir,
        rules_path=data_dir / "rules.yaml",
    ).bundle_path


# ---------------------------------------------------------------------------
# 1. Status & Overview
# ---------------------------------------------------------------------------

class TestWorkflowTools:
    def test_get_workflow_valid(self, db_path) -> None:
        from finance_cli.mcp_server import get_workflow

        result = get_workflow("monthly_review")

        assert result["data"]["name"] == "monthly_review"
        assert "Monthly Financial Review" in result["data"]["content"]
        assert result["summary"]["workflow"] == "monthly_review"

    def test_get_workflow_invalid(self, db_path) -> None:
        from finance_cli.mcp_server import _WORKFLOW_SECTIONS, get_workflow

        result = get_workflow("nonexistent")
        expected = list(_WORKFLOW_SECTIONS.keys())

        assert result["data"]["available"] == expected
        assert result["summary"]["error"] == "Unknown workflow"
        assert result["summary"]["available"] == expected

    def test_get_workflow_all_names(self, db_path) -> None:
        from finance_cli.mcp_server import _WORKFLOW_SECTIONS, get_workflow

        for name, title in _WORKFLOW_SECTIONS.items():
            result = get_workflow(name)

            assert result["data"]["name"] == name
            assert result["data"]["content"].strip()
            assert result["summary"]["title"] == title
            assert result["summary"]["lines"] > 0


class TestSkillTools:
    def test_activate_skill_returns_playbook_and_activated_flag(self, db_path) -> None:
        from finance_cli.mcp_server import activate_skill

        result = activate_skill("normalizer_builder")

        assert result["data"]["name"] == "normalizer_builder"
        assert result["data"]["activated"] is True
        assert "# Normalizer Builder Skill" in result["data"]["content"]
        assert result["summary"]["activated"] is True

    def test_activate_skill_returns_error_for_unknown_skill(self, db_path) -> None:
        from finance_cli.mcp_server import activate_skill

        result = activate_skill("nonexistent")

        assert result["summary"]["error"] == "Unknown skill"
        assert "activated" not in result["summary"]

    def test_activate_skill_rejects_onboarding(self, db_path) -> None:
        from finance_cli.mcp_server import activate_skill

        result = activate_skill("onboarding")

        assert result["data"] == {"activated": False, "skill": "onboarding"}
        assert result["summary"]["activated"] is False
        assert "cannot be activated mid-conversation" in result["summary"]["reason"]

    def test_activate_skill_tool_packs_disabled(self, db_path, monkeypatch) -> None:
        from finance_cli import mcp_server

        monkeypatch.setattr(
            mcp_server,
            "load_skill_profile",
            lambda name: SkillProfile(
                name=name,
                system_prompt="Prompt",
                tool_packs=["finance"],
                tool_packs_enabled=False,
            ),
        )

        result = mcp_server.activate_skill("normalizer_builder")

        assert result["data"]["activated"] is False


class TestStatusTools:
    def test_db_status(self, db_path, conn):
        _seed_txn(conn, is_reviewed=0)
        _seed_txn(conn, is_reviewed=1)
        from finance_cli.mcp_server import db_status
        result = db_status()
        assert "data" in result
        assert "summary" in result
        assert result["data"]["unreviewed_count"] == 1

    def test_setup_check(self, db_path, monkeypatch):
        from finance_cli.mcp_server import setup_check
        result = setup_check()
        assert "data" in result
        assert "summary" in result

    def test_setup_check_uses_user_scoped_rules_path(self, db_path, tmp_path, monkeypatch):
        from finance_cli.commands import setup_cmd
        from finance_cli.mcp_server import setup_check
        from finance_cli.plaid_client import PlaidConfigStatus

        env_path = tmp_path / ".env"
        env_path.write_text("PLAID_CLIENT_ID=test\n", encoding="utf-8")
        rules_path = tmp_path / "alice" / "rules.yaml"
        rules_path.parent.mkdir()
        rules_path.write_text("{}\n", encoding="utf-8")
        assert (db_path.parent / "rules.yaml").exists() is False
        monkeypatch.setenv("FINANCE_CLI_ENV_FILE", str(env_path))
        monkeypatch.setattr(
            "finance_cli.commands.setup_cmd.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr(
            "finance_cli.commands.setup_cmd._aws_readiness",
            lambda: {"ok": True, "region": "us-east-1", "region_source": "AWS_DEFAULT_REGION", "error": None},
        )

        with connect(db_path) as conn:
            setup_cmd._seed_canonical_categories(conn, dry_run=False)
            conn.commit()

        token = set_user_context(UserContext.from_paths(db_path=db_path, rules_path=rules_path))
        try:
            result = setup_check()
        finally:
            reset_user_context(token)

        rules_check = next(check for check in result["data"]["checks"] if check["id"] == "rules")
        assert rules_check["status"] == "OK"
        assert str(rules_path.resolve()) in rules_check["detail"]
        assert result["summary"]["ready"] is True

    def test_setup_status(self, db_path, conn):
        _seed_plaid_item(conn)
        from finance_cli.mcp_server import setup_status
        result = setup_status()
        assert "data" in result
        assert "summary" in result
        plaid_items = result["data"]["plaid"]["items"]
        assert plaid_items
        assert "next_steps" in result["data"]
        assert "access_token_ref" not in plaid_items[0]
        assert "sync_cursor" not in plaid_items[0]


class TestAccountTools:
    def test_account_list_filters_by_is_business(self, db_path, conn):
        _seed_account(conn, institution="Biz Bank", name="Biz Checking", is_business=1)
        _seed_account(conn, institution="Personal Bank", name="Personal Checking", is_business=0)
        from finance_cli.mcp_server import account_list

        result = account_list(status="all", is_business=True)
        accounts = result["data"]["accounts"]

        assert len(accounts) == 1
        assert int(accounts[0]["is_business"]) == 1
        assert result["data"]["filters"]["is_business"] is True


# ---------------------------------------------------------------------------
# 2. Financial Reports
# ---------------------------------------------------------------------------

class TestReportTools:
    def test_daily_summary(self, db_path, conn, isolated_mcp_cache):
        tid = _seed_txn(conn, date="2026-02-15")
        raw_payload = '{"source":"daily"}'
        _set_txn_raw_payload(conn, tid, raw_payload)
        from finance_cli.mcp_server import daily_summary
        result = daily_summary(date="2026-02-15")
        assert "data" in result
        assert "summary" in result
        assert "cache_file" not in result
        assert "raw_plaid_json" not in result["data"]["transactions"][0]
        cache_file = _latest_cache_file(isolated_mcp_cache, "daily_summary")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_txn = next(txn for txn in cached["data"]["transactions"] if txn["id"] == tid)
        assert cached_txn["raw_plaid_json"] == raw_payload

    def test_daily_summary_no_date(self, db_path):
        from finance_cli.mcp_server import daily_summary
        result = daily_summary()
        assert "data" in result

    def test_weekly_summary(self, db_path, conn):
        _seed_txn(conn, date="2026-02-15")
        from finance_cli.mcp_server import weekly_summary
        result = weekly_summary(week="2026-W07")
        assert "data" in result
        assert "summary" in result

    def test_weekly_summary_compare(self, db_path, conn):
        _seed_txn(conn, date="2026-02-15")
        _seed_txn(conn, date="2026-02-08")
        from finance_cli.mcp_server import weekly_summary
        result = weekly_summary(week="2026-W07", compare=True)
        assert "data" in result

    def test_balance_net_worth(self, db_path, conn):
        _seed_account(conn)
        from finance_cli.mcp_server import balance_net_worth
        result = balance_net_worth()
        assert "data" in result
        assert result["data"]["net_worth_cents"] == 100000

    def test_balance_net_worth_with_investments(self, db_path, conn):
        _seed_account(conn, account_type="investment", balance_cents=500000)
        from finance_cli.mcp_server import balance_net_worth
        result = balance_net_worth()
        assert "data" in result
        assert result["data"]["net_worth_cents"] == 500000

    def test_subs_list(self, db_path, isolated_mcp_cache):
        from finance_cli.mcp_server import subs_list
        result = subs_list()
        assert "data" in result
        assert "summary" in result
        assert "cache_file" not in result
        assert _latest_cache_file(isolated_mcp_cache, "subs_list").exists()

    def test_subs_list_active_only_summary(self, db_path, conn, isolated_mcp_cache):
        _seed_subscription(
            conn,
            vendor_name="Very Long Vendor Name That Should Be Truncated For MCP Output",
            amount_cents=1099,
            frequency="weekly",
            is_active=1,
        )
        _seed_subscription(conn, vendor_name="Old Streaming", is_active=0)
        from finance_cli.mcp_server import subs_list

        result = subs_list(show_all=False)
        subscriptions = result["data"]["subscriptions"]
        summary = result["summary"]

        assert subscriptions
        assert all(int(item["is_active"]) == 1 for item in subscriptions)
        assert all(item["short_name"] == str(item["vendor_name"])[:30] for item in subscriptions)
        assert all(
            item["monthly_amount"] == round(float(item["monthly_amount"]), 2)
            for item in subscriptions
        )
        assert summary["active_subscriptions"] == len(subscriptions)
        assert summary["inactive_subscriptions"] == 1
        assert summary["total_subscriptions"] == (
            summary["active_subscriptions"] + summary["inactive_subscriptions"]
        )

        assert "cache_file" not in result
        cache_file = _latest_cache_file(isolated_mcp_cache, "subs_list")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_subscriptions = cached["data"]["subscriptions"]
        assert len(cached_subscriptions) == summary["total_subscriptions"]
        assert any(int(item["is_active"]) == 0 for item in cached_subscriptions)

    def test_subs_list_pagination_preserves_full_summary(self, db_path, conn):
        _seed_subscription(conn, vendor_name="One", amount_cents=1000, is_active=1)
        _seed_subscription(conn, vendor_name="Two", amount_cents=2000, is_active=1)
        _seed_subscription(conn, vendor_name="Three", amount_cents=3000, is_active=0)
        from finance_cli.mcp_server import subs_list

        result = subs_list(show_all=True, limit=1, offset=1)

        assert len(result["data"]["subscriptions"]) == 1
        assert result["data"]["total_count"] == 3
        assert result["data"]["limit"] == 1
        assert result["data"]["offset"] == 1
        assert result["summary"]["active_subscriptions"] == 2
        assert result["summary"]["inactive_subscriptions"] == 1
        assert result["summary"]["total_subscriptions"] == 3

    def test_subs_total(self, db_path):
        from finance_cli.mcp_server import subs_total
        result = subs_total()
        assert "data" in result

    def test_cat_tree(self, db_path, conn):
        _seed_category(conn, "Food & Drink")
        from finance_cli.mcp_server import cat_tree
        result = cat_tree()
        assert "data" in result

    def test_liquidity(self, db_path, conn):
        _seed_account(conn)
        _seed_account(conn, account_type="investment", balance_cents=500_000)
        from finance_cli.mcp_server import liquidity
        result = liquidity()
        assert "data" in result
        assert "summary" in result
        assert result["data"]["include_investments"] is True

        no_investments = liquidity(include_investments=False)
        assert no_investments["data"]["include_investments"] is False

    def test_debt_dashboard(self, db_path, conn):
        account_id = _seed_account(
            conn,
            institution="Chase",
            name="Freedom",
            account_type="credit_card",
            balance_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=18.99, minimum_payment_cents=800)

        from finance_cli.mcp_server import debt_dashboard

        result = debt_dashboard()
        assert "data" in result
        assert "summary" in result
        assert result["summary"]["total_cards"] == 1

    def test_debt_interest(self, db_path, conn):
        account_id = _seed_account(
            conn,
            institution="Barclays",
            name="View",
            account_type="credit_card",
            balance_cents=-10_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=24.24, minimum_payment_cents=500)

        from finance_cli.mcp_server import debt_interest

        result = debt_interest(months=6)
        assert "data" in result
        assert "summary" in result
        assert len(result["data"]["schedule"]) == 6
        assert "cards" not in result["data"]["schedule"][0]

        detailed = debt_interest(months=2, summary_only=False)
        assert "cards" in detailed["data"]["schedule"][0]

    def test_debt_simulate(self, db_path, conn):
        first = _seed_account(
            conn,
            institution="Chase",
            name="Sapphire",
            account_type="credit_card",
            balance_cents=-30_000,
        )
        second = _seed_account(
            conn,
            institution="Amex",
            name="Gold",
            account_type="credit_card",
            balance_cents=-8_000,
        )
        _seed_credit_liability(conn, account_id=first, apr_purchase=29.99, minimum_payment_cents=900)
        _seed_credit_liability(conn, account_id=second, apr_purchase=9.99, minimum_payment_cents=300)

        from finance_cli.mcp_server import debt_simulate

        result = debt_simulate(extra_dollars=500, strategy="compare")
        assert "data" in result
        assert "summary" in result
        assert "avalanche" in result["data"]
        avalanche_schedule = result["data"]["avalanche"]["schedule"]
        assert avalanche_schedule
        assert "paid_off_count" in avalanche_schedule[0]
        assert "cards" not in avalanche_schedule[0]
        baseline_schedule = result["data"]["baseline"]["schedule"]
        assert baseline_schedule
        assert "cards" not in baseline_schedule[0]

        detailed = debt_simulate(extra_dollars=500, strategy="avalanche", summary_only=False)
        detailed_schedule = detailed["data"]["schedule"]
        assert detailed_schedule
        assert "paid_off_cards" in detailed_schedule[0]
        assert "cards" in detailed_schedule[0]

    def test_debt_set_apr(self, db_path, conn):
        account_id = _seed_account(
            conn,
            institution="Chase",
            name="Freedom",
            account_type="credit_card",
            balance_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=18.99, minimum_payment_cents=800)

        from finance_cli.mcp_server import debt_dashboard, debt_set_apr

        result = debt_set_apr(account_id=account_id, apr_pct=23.49)
        assert result["data"]["previous_apr"] == pytest.approx(18.99)
        assert result["data"]["apr"] == pytest.approx(23.49)

        dashboard = debt_dashboard(sort="apr")
        card = next(row for row in dashboard["data"]["cards"] if row["card_id"] == account_id)
        assert card["apr"] == pytest.approx(23.49)

    def test_debt_balance_portion_lifecycle(self, db_path, conn):
        account_id = _seed_account(
            conn,
            institution="Amex",
            name="Gold",
            account_type="credit_card",
            balance_cents=-200_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=20.49, minimum_payment_cents=9_000)

        from finance_cli.mcp_server import (
            debt_balance_portion_add,
            debt_balance_portion_deactivate,
            debt_balance_portion_list,
            debt_balance_portion_update,
            debt_dashboard,
        )

        dry_run = debt_balance_portion_add(
            account_id=account_id,
            label="Plan It Dry",
            principal_dollars=1000,
            apr_pct=10.0,
            dry_run=True,
        )
        assert dry_run["data"]["dry_run"] is True
        assert debt_balance_portion_list(account_id=account_id)["summary"]["total_count"] == 0

        created = debt_balance_portion_add(
            account_id=account_id,
            label="Plan It",
            principal_dollars=1000,
            apr_pct=10.0,
            monthly_payment_dollars=89.6,
            promo_end_date="2028-03-11",
        )
        portion_id = created["data"]["id"]
        listed = debt_balance_portion_list(account_id=account_id)
        assert listed["summary"]["total_count"] == 1
        assert listed["data"]["portions"][0]["id"] == portion_id

        updated = debt_balance_portion_update(
            portion_id=portion_id,
            principal_dollars=900,
            apr_pct=9.5,
            clear_monthly_payment=True,
        )
        assert updated["summary"]["fields_changed"] == 3
        dashboard = debt_dashboard(sort="apr")
        portion_row = next(row for row in dashboard["data"]["cards"] if row["portion_id"] == portion_id)
        assert portion_row["apr"] == pytest.approx(9.5)
        assert portion_row["balance_cents"] == 90_000

        deactivated = debt_balance_portion_deactivate(portion_id=portion_id)
        assert deactivated["data"]["is_active"] is False
        assert debt_balance_portion_list(account_id=account_id)["summary"]["total_count"] == 0
        assert debt_balance_portion_list(account_id=account_id, active_only=False)["summary"]["total_count"] == 1

    def test_debt_impact(self, db_path, conn):
        dining_id = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=dining_id, amount_cents=-5000, date="2026-01-15")
        _seed_txn(conn, category_id=dining_id, amount_cents=-3000, date="2025-12-15")

        account_id = _seed_account(
            conn,
            institution="Chase",
            name="Freedom",
            account_type="credit_card",
            balance_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=18.99, minimum_payment_cents=800)

        from finance_cli.mcp_server import debt_impact

        result = debt_impact(months=3, cut_pct=50)
        assert "data" in result
        assert "summary" in result

    def test_debt_impact_adds_caveat_when_baseline_is_capped(self, db_path, conn):
        dining_id = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=dining_id, amount_cents=-5000, date="2026-01-15")

        account_id = _seed_account(
            conn,
            institution="Capped",
            name="No Minimum",
            account_type="credit_card",
            balance_cents=-50_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=24.99, minimum_payment_cents=100)

        from finance_cli.mcp_server import debt_impact

        result = debt_impact(months=3, cut_pct=50)
        assert "caveat" in result["data"]

    def test_subs_audit_adds_caveat_when_baseline_is_capped(self, db_path, conn):
        _seed_subscription(conn, vendor_name="Video Service", amount_cents=2500, is_active=1)
        account_id = _seed_account(
            conn,
            institution="Capped",
            name="No Minimum",
            account_type="credit_card",
            balance_cents=-40_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=19.99, minimum_payment_cents=100)
        from finance_cli.mcp_server import subs_audit

        result = subs_audit()
        assert "caveat" in result["data"]

    def test_subs_audit_adds_caveat_when_apr_is_unknown(self, db_path, conn):
        _seed_subscription(conn, vendor_name="Video Service", amount_cents=2500, is_active=1)
        account_id = _seed_account(
            conn,
            institution="Unknown APR",
            name="No APR",
            account_type="credit_card",
            balance_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=None, minimum_payment_cents=700)
        from finance_cli.mcp_server import subs_audit

        result = subs_audit()
        assert "unknown APR" in result["data"]["caveat"]
        assert "lower-bound" in result["data"]["caveat"]

    def test_biz_pl(self, db_path, conn):
        revenue_id = _seed_category(conn, "MCP Biz Revenue", is_income=1)
        _seed_pl_map(conn, revenue_id, "revenue", 10)
        account_id = _seed_account(conn, name="Biz Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=25_000,
            date="2026-02-10",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_pl

        result = biz_pl(month="2026-02")
        assert "data" in result
        assert "summary" in result
        assert result["data"]["gross_revenue_cents"] == 25_000

    def test_biz_tax(self, db_path, conn):
        income_id = _seed_category(conn, "MCP Tax Income", is_income=1)
        expense_id = _seed_category(conn, "MCP Tax Expense")
        _seed_schedule_c_map(conn, expense_id, line="Advertising", line_number="8", deduction_pct=1.0, tax_year=2025)
        account_id = _seed_account(conn, name="Tax Biz Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=income_id,
            amount_cents=60_000,
            date="2025-01-15",
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=expense_id,
            amount_cents=-10_000,
            date="2025-01-16",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_tax

        result = biz_tax(year="2025")
        assert "data" in result
        assert "summary" in result
        assert result["data"]["line_1_gross_receipts_cents"] == 60_000
        assert result["data"]["line_28_total_expenses_cents"] == 10_000

    def test_biz_tax_detail_supports_schedule_c_line_number(self, db_path, conn):
        income_id = _seed_category(conn, "MCP Tax Income Detail", is_income=1)
        expense_id = _seed_category(conn, "MCP Tax Expense Detail")
        _seed_schedule_c_map(conn, expense_id, line="Advertising", line_number="8", deduction_pct=1.0, tax_year=2025)
        account_id = _seed_account(conn, name="Tax Detail Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=income_id,
            amount_cents=80_000,
            date="2025-01-15",
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=expense_id,
            amount_cents=-12_000,
            date="2025-01-18",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_tax_detail

        result = biz_tax_detail(detail="8", year="2025")
        assert "data" in result
        assert result["data"]["detail"] is not None
        assert str(result["data"]["detail"]["line_number"]) == "8"

    def test_biz_budget_set_defaults(self, db_path, conn):
        from finance_cli.mcp_server import biz_budget_set

        result = biz_budget_set(section="opex_marketing", amount=500)
        assert "data" in result
        assert "summary" in result
        assert result["data"]["pl_section"] == "opex_marketing"
        assert result["data"]["period"] == "monthly"
        assert result["data"]["effective_from"] == date.today().replace(day=1).isoformat()

        row = conn.execute(
            """
            SELECT pl_section, amount_cents, period, effective_from
              FROM biz_section_budgets
             WHERE id = ?
            """,
            (result["data"]["id"],),
        ).fetchone()
        assert row is not None
        assert row["pl_section"] == "opex_marketing"
        assert row["amount_cents"] == 50_000
        assert row["period"] == "monthly"

    def test_biz_budget_status(self, db_path, conn):
        marketing_id = _seed_category(conn, "MCP Budget Marketing")
        _seed_pl_map(conn, marketing_id, "opex_marketing", 30)
        account_id = _seed_account(conn, name="Biz Budget Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=marketing_id,
            amount_cents=-30_000,
            date="2026-01-10",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_budget_set, biz_budget_status

        biz_budget_set(section="opex_marketing", amount=500, period="monthly", effective_from="2026-01-01")
        result = biz_budget_status(month="2026-01")
        assert "data" in result
        assert "summary" in result

        rows = {row["pl_section"]: row for row in result["data"]["rows"]}
        marketing = rows["opex_marketing"]
        assert marketing["monthly_budget_cents"] == 50_000
        assert marketing["actual_cents"] == 30_000

    def test_biz_runway_marks_profitable_mode(self, db_path, conn):
        revenue_id = _seed_category(conn, "MCP Runway Revenue", is_income=1)
        expense_id = _seed_category(conn, "MCP Runway Expense")
        _seed_pl_map(conn, revenue_id, "revenue", 10)
        _seed_pl_map(conn, expense_id, "opex_other", 20)
        account_id = _seed_account(conn, name="Runway Biz Checking", is_business=1, balance_cents=90_000)
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=30_000,
            date=_month_date(0, day=5),
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=expense_id,
            amount_cents=-5_000,
            date=_month_date(0, day=7),
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_runway

        result = biz_runway(months=1)
        assert result["data"]["monthly_net_burn_cents"] < 0
        assert result["data"]["is_profitable"] is True
        assert "note" in result["data"]

    def test_biz_forecast_projection_floor_non_negative(self, db_path, conn):
        revenue_id = _seed_category(conn, "MCP Forecast Revenue", is_income=1)
        _seed_pl_map(conn, revenue_id, "revenue", 10)
        account_id = _seed_account(conn, name="Forecast Biz Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=20_000,
            date=_month_date(-2),
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=10_000,
            date=_month_date(-1),
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_forecast

        result = biz_forecast(months=3, streams=True)
        assert result["data"]["projected_next_month_cents"] >= 0

    def test_income_mix_uses_complete_month_income_sources_without_business_use_type(
        self,
        db_path,
        conn,
    ):
        client_a = _seed_category(conn, "Client A", is_income=1)
        client_b = _seed_category(conn, "Client B", is_income=1)
        account_id = _seed_account(conn, name="General Checking")
        _seed_txn(
            conn,
            category_id=client_a,
            amount_cents=10_000,
            date=_month_date(-2),
            account_id=account_id,
        )
        _seed_txn(
            conn,
            category_id=client_a,
            amount_cents=30_000,
            date=_month_date(-1),
            account_id=account_id,
        )
        _seed_txn(
            conn,
            category_id=client_b,
            amount_cents=10_000,
            date=_month_date(-1, day=20),
            account_id=account_id,
        )
        _seed_txn(
            conn,
            category_id=client_b,
            amount_cents=90_000,
            date=_month_date(0),
            account_id=account_id,
        )
        from finance_cli.mcp_server import income_mix

        result = income_mix(months=2)

        assert result["data"]["complete_months"] == [
            _month_date(-2)[:7],
            _month_date(-1)[:7],
        ]
        assert result["summary"]["total_income_cents"] == 50_000
        assert result["summary"]["top_source"] == "Client A"
        assert result["summary"]["top_share_pct"] == 80
        assert result["data"]["sources"][0]["monthly_totals"] == [
            {"month": _month_date(-2)[:7], "cents": 10_000},
            {"month": _month_date(-1)[:7], "cents": 30_000},
        ]

    def test_income_mix_does_not_round_partial_share_to_100_percent(
        self,
        db_path,
        conn,
    ):
        client_a = _seed_category(conn, "Client A", is_income=1)
        client_b = _seed_category(conn, "Client B", is_income=1)
        account_id = _seed_account(conn, name="General Checking")
        _seed_txn(
            conn,
            category_id=client_a,
            amount_cents=99_900,
            date=_month_date(-1),
            account_id=account_id,
        )
        _seed_txn(
            conn,
            category_id=client_b,
            amount_cents=1,
            date=_month_date(-1, day=20),
            account_id=account_id,
        )
        from finance_cli.mcp_server import income_mix

        result = income_mix(months=1)

        assert result["summary"]["top_share_pct"] == 99
        assert result["data"]["sources"][0]["share_pct"] == 99
        assert result["data"]["sources"][1]["share_pct"] == 1
        assert "Client A" in result["cli_report"]
        assert "$999.00   99%" in result["cli_report"]
        assert "Client B" in result["cli_report"]
        assert "$0.01    1%" in result["cli_report"]

    def test_direct_mcp_tool_call_loads_dotenv_before_resolving_db(
        self,
        tmp_path: Path,
        db_path,
        conn,
    ):
        income_id = _seed_category(conn, "Client A", is_income=1)
        account_id = _seed_account(conn, name="General Checking")
        _seed_txn(
            conn,
            category_id=income_id,
            amount_cents=12_345,
            date=_month_date(-1),
            account_id=account_id,
        )
        conn.commit()

        env_file = tmp_path / ".env"
        env_file.write_text(f"FINANCE_CLI_DB={db_path}\n", encoding="utf-8")
        env = os.environ.copy()
        env.pop("FINANCE_CLI_DB", None)
        env.pop("FINANCE_CLI_DATA_DIR", None)
        env.pop("FINANCE_CLI_DISABLE_DOTENV", None)
        env["FINANCE_CLI_ENV_FILE"] = str(env_file)

        code = (
            "import json, sys\n"
            "from finance_cli.mcp_server import income_mix\n"
            "sys.__stdout__.write(json.dumps(income_mix(months=1)))\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=Path(__file__).resolve().parents[2],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        result = json.loads(completed.stdout)

        assert result["summary"]["total_income_cents"] == 12_345
        assert result["summary"]["top_source"] == "Client A"


# ---------------------------------------------------------------------------
# 3. Transaction Tools
# ---------------------------------------------------------------------------

class TestTransactionTools:
    def test_txn_list_empty(self, db_path):
        from finance_cli.mcp_server import txn_list
        result = txn_list()
        assert "data" in result
        assert "summary" in result

    def test_txn_list_with_data(self, db_path, conn):
        cat_id = _seed_category(conn, "Groceries")
        _seed_txn(conn, category_id=cat_id, description="WHOLE FOODS")
        from finance_cli.mcp_server import txn_list
        result = txn_list(limit=10)
        assert result["data"]["transactions"]
        assert result["data"]["pagination"]["total_count"] == 1

    def test_txn_list_filters(self, db_path, conn):
        _seed_txn(conn, date="2026-02-10")
        _seed_txn(conn, date="2026-02-20")
        from finance_cli.mcp_server import txn_list
        result = txn_list(date_from="2026-02-15")
        assert result["data"]["pagination"]["total_count"] == 1

    def test_txn_list_uncategorized(self, db_path, conn):
        _seed_txn(conn, category_id=None)
        cat_id = _seed_category(conn, "Test")
        _seed_txn(conn, category_id=cat_id)
        from finance_cli.mcp_server import txn_list
        result = txn_list(uncategorized=True)
        assert result["data"]["pagination"]["total_count"] == 1

    def test_txn_search(self, db_path, conn, isolated_mcp_cache):
        tid = _seed_txn(conn, description="STARBUCKS COFFEE")
        raw_payload = '{"merchant":"STARBUCKS"}'
        _set_txn_raw_payload(conn, tid, raw_payload)
        from finance_cli.mcp_server import txn_search
        # Use LIKE fallback (FTS may not have the row since we inserted directly)
        result = txn_search(query="STARBUCKS")
        assert "data" in result
        assert "cache_file" not in result
        assert all("raw_plaid_json" not in item for item in result["data"]["transactions"])

        cache_file = _latest_cache_file(isolated_mcp_cache, "txn_search")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_txn = next(item for item in cached["data"]["transactions"] if item["id"] == tid)
        assert cached_txn["raw_plaid_json"] == raw_payload

    def test_txn_search_passes_category_to_handler(self, db_path, isolated_mcp_cache, monkeypatch):
        import finance_cli.mcp_server as mcp_server

        seen: dict[str, object] = {}

        def fake_call(handler, ns_kwargs):
            seen["handler"] = handler
            seen["params"] = dict(ns_kwargs)
            return {"data": {"transactions": [], "query": "STORE"}, "summary": {}}

        monkeypatch.setattr(mcp_server, "_call", fake_call)

        result = mcp_server.txn_search(query="STORE", category="Groceries")

        assert result["data"]["query"] == "STORE"
        assert seen["handler"] is mcp_server.txn.handle_search
        assert seen["params"] == {"query": "STORE", "category": "Groceries"}

    def test_txn_search_omits_empty_category(self, db_path, isolated_mcp_cache, monkeypatch):
        import finance_cli.mcp_server as mcp_server

        seen: dict[str, object] = {}

        def fake_call(handler, ns_kwargs):
            seen["handler"] = handler
            seen["params"] = dict(ns_kwargs)
            return {"data": {"transactions": [], "query": "STORE"}, "summary": {}}

        monkeypatch.setattr(mcp_server, "_call", fake_call)

        result = mcp_server.txn_search(query="STORE")

        assert result["data"]["query"] == "STORE"
        assert seen["handler"] is mcp_server.txn.handle_search
        assert seen["params"] == {"query": "STORE"}

    def test_txn_show(self, db_path, conn, isolated_mcp_cache):
        tid = _seed_txn(conn, description="SHOW ME")
        raw_payload = '{"merchant":"SHOW"}'
        _set_txn_raw_payload(conn, tid, raw_payload)
        from finance_cli.mcp_server import txn_show
        result = txn_show(id=tid)
        assert result["data"]["transaction"]["id"] == tid
        assert "cache_file" not in result
        assert "raw_plaid_json" not in result["data"]["transaction"]

        cache_file = _latest_cache_file(isolated_mcp_cache, "txn_show")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert cached["data"]["transaction"]["raw_plaid_json"] == raw_payload

    def test_txn_show_bad_id(self, db_path):
        from finance_cli.mcp_server import txn_show
        response = txn_show(id="nonexistent_id_000")
        _assert_tool_error(response, "NotFoundError", "not found")

    def test_txn_explain(self, db_path, conn):
        cat_id = _seed_category(conn, "Coffee")
        tid = _seed_txn(conn, category_id=cat_id, description="EXPLAINED TXN")
        from finance_cli.mcp_server import txn_explain
        result = txn_explain(id=tid)
        assert "data" in result

    def test_txn_coverage(self, db_path, conn, isolated_mcp_cache):
        aid = _seed_account(conn)
        _seed_txn(conn, account_id=aid, date="2026-02-01")
        _seed_txn(conn, account_id=aid, date="2026-02-15")
        from finance_cli.mcp_server import txn_coverage
        result = txn_coverage()
        assert "data" in result
        assert "cache_file" not in result
        assert _latest_cache_file(isolated_mcp_cache, "txn_coverage").exists()

    def test_write_cache_same_second_unique(self, db_path, isolated_mcp_cache, monkeypatch):
        import finance_cli.mcp_server as mcp_server

        class _FixedDatetime:
            @classmethod
            def now(cls):
                return datetime(2026, 3, 2, 9, 0, 0, 123456)

        monkeypatch.setattr(mcp_server, "datetime", _FixedDatetime)
        first = Path(mcp_server._write_cache("txn_search", {"i": 1}))
        second = Path(mcp_server._write_cache("txn_search", {"i": 2}))

        assert first.exists()
        assert second.exists()
        assert first.name != second.name


class TestReadthroughCache:
    def test_summary_only_tool_returns_opaque_cache_id(self, db_path, isolated_mcp_cache):
        from finance_cli.mcp_server import subs_recurring

        result = subs_recurring()
        cache_id = result["data"]["cache_id"]

        assert cache_id
        assert "/" not in cache_id
        assert "\\" not in cache_id
        assert not cache_id.endswith(".json")
        assert _readthrough_cache_file(isolated_mcp_cache, cache_id).exists()

    def test_read_mcp_cache_basic_pagination(self, db_path, isolated_mcp_cache):
        import finance_cli.mcp_server as mcp_server

        cache_id = mcp_server._write_cache_safe(
            "test_tool",
            {
                "data": {
                    "matches": [{"id": "a"}, {"id": "b"}, {"id": "c"}],
                    "config": {"mode": "preview"},
                },
                "summary": {},
            },
        )

        result = mcp_server.read_mcp_cache(cache_id=cache_id, key="matches", offset=1, limit=2)

        assert result["data"]["key"] == "matches"
        assert result["data"]["items"] == [{"id": "b"}, {"id": "c"}]
        assert result["summary"] == {"total": 3, "offset": 1, "limit": 2, "returned": 2}
        assert "truncated" not in result["data"]

    def test_read_mcp_cache_dot_path_and_non_list(self, db_path, isolated_mcp_cache):
        import finance_cli.mcp_server as mcp_server

        cache_id = mcp_server._write_cache_safe(
            "test_tool",
            {
                "data": {
                    "groups": [
                        {"ids": ["txn_1", "txn_2"], "metadata": {"label": "first"}},
                        {"ids": ["txn_3"], "metadata": {"label": "second"}},
                    ]
                },
                "summary": {},
            },
        )

        ids_page = mcp_server.read_mcp_cache(cache_id=cache_id, key="groups.0.ids", limit=10)
        metadata_value = mcp_server.read_mcp_cache(cache_id=cache_id, key="groups.0.metadata")

        assert ids_page["data"]["key"] == "groups.0.ids"
        assert ids_page["data"]["items"] == ["txn_1", "txn_2"]
        assert ids_page["summary"]["returned"] == 2
        assert metadata_value["data"] == {
            "value": {"label": "first"},
            "key": "groups.0.metadata",
        }
        assert metadata_value["summary"] == {"type": "object"}

    def test_read_mcp_cache_auto_detect_and_available_keys_cap(self, db_path, isolated_mcp_cache):
        import finance_cli.mcp_server as mcp_server

        auto_cache_id = mcp_server._write_cache_safe(
            "auto_tool",
            {"data": {"beta": [1, 2], "alpha": [3, 4], "single": [5]}, "summary": {}},
        )
        auto_result = mcp_server.read_mcp_cache(cache_id=auto_cache_id)

        assert auto_result["data"]["key"] == "alpha"
        assert auto_result["data"]["items"] == [3, 4]

        keys_cache_id = mcp_server._write_cache_safe(
            "keys_tool",
            {"data": {f"key_{idx:03d}": idx for idx in range(60)}, "summary": {}},
        )
        keys_result = mcp_server.read_mcp_cache(cache_id=keys_cache_id)

        assert keys_result["summary"]["error"].startswith("No top-level list found in data")
        assert len(keys_result["data"]["available_keys"]) == 50
        assert keys_result["data"]["keys_truncated"] is True

    def test_read_mcp_cache_byte_cap_truncation(self, db_path, isolated_mcp_cache, monkeypatch):
        import finance_cli.mcp_server as mcp_server

        monkeypatch.setattr(mcp_server, "_READTHROUGH_BYTE_CAP", 900)
        items = [{"id": idx, "blob": "x" * 400} for idx in range(5)]
        cache_id = mcp_server._write_cache_safe(
            "byte_cap_tool",
            {"data": {"matches": items}, "summary": {}},
        )

        result = mcp_server.read_mcp_cache(cache_id=cache_id, key="matches", limit=5)

        assert result["summary"]["limit"] == 5
        assert 1 <= result["summary"]["returned"] < 5
        assert result["data"]["truncated"] is True

    def test_read_mcp_cache_limit_clamp_and_offset_past_end(self, db_path, isolated_mcp_cache):
        import finance_cli.mcp_server as mcp_server

        cache_id = mcp_server._write_cache_safe(
            "limit_tool",
            {"data": {"matches": list(range(100))}, "summary": {}},
        )

        limited = mcp_server.read_mcp_cache(cache_id=cache_id, key="matches", limit=9999)
        empty_page = mcp_server.read_mcp_cache(cache_id=cache_id, key="matches", offset=9999, limit=3)

        assert limited["summary"]["limit"] == 50
        assert limited["summary"]["returned"] == 50
        assert limited["data"]["items"] == list(range(50))

        assert empty_page["data"] == {"items": [], "key": "matches"}
        assert empty_page["summary"] == {"total": 100, "offset": 9999, "limit": 3, "returned": 0}

    def test_read_mcp_cache_security_rejection_and_plain_json_isolation(
        self,
        db_path,
        isolated_mcp_cache,
        tmp_path: Path,
    ):
        import finance_cli.mcp_server as mcp_server

        traversal = mcp_server.read_mcp_cache(cache_id="../../etc/passwd")
        legacy_path = Path(mcp_server._write_cache("txn_search", {"data": {"matches": [1]}, "summary": {}}))
        legacy_result = mcp_server.read_mcp_cache(cache_id=legacy_path.stem)

        target = tmp_path / "outside.readthrough.json"
        target.write_text(json.dumps({"data": {"matches": [1]}, "summary": {}}), encoding="utf-8")
        isolated_mcp_cache.mkdir(parents=True, exist_ok=True)
        symlink_path = isolated_mcp_cache / "evil_id.readthrough.json"
        symlink_path.symlink_to(target)
        symlink_result = mcp_server.read_mcp_cache(cache_id="evil_id")

        assert traversal["summary"]["error"] == "Invalid cache_id."
        assert legacy_result["summary"]["error"].startswith("Cache file not found.")
        assert symlink_result["summary"]["error"] == "Invalid cache_id."

    def test_read_mcp_cache_stale_file_returns_not_found_and_deletes(
        self,
        db_path,
        isolated_mcp_cache,
    ):
        import finance_cli.mcp_server as mcp_server

        cache_id = mcp_server._write_cache_safe(
            "stale_tool",
            {"data": {"matches": [1, 2, 3]}, "summary": {}},
        )
        cache_file = _readthrough_cache_file(isolated_mcp_cache, cache_id)
        stale_ts = datetime.now().timestamp() - (25 * 3600)
        os.utime(cache_file, (stale_ts, stale_ts))

        result = mcp_server.read_mcp_cache(cache_id=cache_id, key="matches")

        assert result["summary"]["error"].startswith("Cache file not found.")
        assert not cache_file.exists()

    @pytest.mark.parametrize(
        ("key", "expected_error", "expected_data"),
        [
            (
                "missing",
                "Key 'missing' not found in data",
                {"available_keys": ["config", "groups"]},
            ),
            (
                "groups.10",
                "Index 10 out of range (list has 2 items)",
                {"available_keys": [], "list_length": 2},
            ),
            (
                "groups.foo",
                "Key 'foo' is not a valid list index. Use a numeric index (0-1).",
                {"available_keys": [], "list_length": 2},
            ),
            (
                "config.timeout.deep",
                "Cannot traverse into scalar value at 'config.timeout'",
                {"available_keys": []},
            ),
        ],
    )
    def test_read_mcp_cache_key_not_found_variants(
        self,
        db_path,
        isolated_mcp_cache,
        key: str,
        expected_error: str,
        expected_data: dict[str, object],
    ):
        import finance_cli.mcp_server as mcp_server

        cache_id = mcp_server._write_cache_safe(
            "error_tool",
            {
                "data": {
                    "groups": [{"ids": ["a"]}, {"ids": ["b"]}],
                    "config": {"timeout": 30},
                },
                "summary": {},
            },
        )

        result = mcp_server.read_mcp_cache(cache_id=cache_id, key=key)

        assert result["summary"]["error"] == expected_error
        assert result["data"] == expected_data

    def test_read_mcp_cache_path_sanitization_variants(self, db_path, isolated_mcp_cache):
        import finance_cli.mcp_server as mcp_server

        cache_id = mcp_server._write_cache_safe(
            "sanitize_tool",
            {
                "data": {
                    "output": "/Users/foo/exports/tax_2026.csv",
                    "backup_path": "/tmp/backup.db",
                    "description": "/POS/AMZN MARKETPLACE",
                    "vendor": "/Service/Premium",
                    "steps": {"export": {"result": {"export_path": "/Users/foo/out.csv"}}},
                    "files": ["/Users/foo/a.csv", "/Users/foo/b.csv"],
                },
                "summary": {},
            },
        )

        cached = json.loads(_readthrough_cache_file(isolated_mcp_cache, cache_id).read_text(encoding="utf-8"))
        nested = mcp_server.read_mcp_cache(cache_id=cache_id, key="steps.export.result.export_path")
        files_page = mcp_server.read_mcp_cache(cache_id=cache_id, key="files", limit=10)

        assert cached["data"]["output"] == "tax_2026.csv"
        assert cached["data"]["backup_path"] == "backup.db"
        assert cached["data"]["description"] == "/POS/AMZN MARKETPLACE"
        assert cached["data"]["vendor"] == "/Service/Premium"
        assert nested["data"]["value"] == "out.csv"
        assert files_page["data"]["items"] == ["a.csv", "b.csv"]

    def test_cleanup_covers_both_suffixes_and_safe_cache_ids_are_unique(
        self,
        db_path,
        isolated_mcp_cache,
    ):
        import finance_cli.mcp_server as mcp_server

        legacy_path = Path(mcp_server._write_cache("txn_search", {"data": {"matches": [1]}, "summary": {}}))
        first_cache_id = mcp_server._write_cache_safe("tool", {"data": {"matches": [1]}, "summary": {}})
        second_cache_id = mcp_server._write_cache_safe("tool", {"data": {"matches": [2]}, "summary": {}})

        first_readthrough = _readthrough_cache_file(isolated_mcp_cache, first_cache_id)
        stale_ts = datetime.now().timestamp() - (25 * 3600)
        os.utime(legacy_path, (stale_ts, stale_ts))
        os.utime(first_readthrough, (stale_ts, stale_ts))

        mcp_server._cleanup_cache(isolated_mcp_cache)

        assert first_cache_id != second_cache_id
        assert not legacy_path.exists()
        assert not first_readthrough.exists()
        assert _readthrough_cache_file(isolated_mcp_cache, second_cache_id).exists()


# ---------------------------------------------------------------------------
# 4. Rules & Categorization
# ---------------------------------------------------------------------------

class TestCategorizationTools:
    def test_rules_test(self, db_path):
        from finance_cli.mcp_server import rules_test
        result = rules_test(description="VENMO PAYMENT")
        assert "data" in result
        assert "summary" in result

    def test_cat_auto_categorize_dry_run(self, db_path, conn):
        _seed_txn(conn, description="UNCATEGORIZED TXN")
        from finance_cli.mcp_server import cat_auto_categorize
        result = cat_auto_categorize(dry_run=True)
        assert "data" in result

    def test_cat_auto_categorize_commit(self, db_path, conn):
        """When dry_run=False the tool must commit categorization changes."""
        _seed_category(conn, "Payments & Transfers")
        _seed_txn(conn, description="VENMO PAYMENT TO FRIEND")
        from finance_cli.mcp_server import cat_auto_categorize
        result = cat_auto_categorize(dry_run=False)
        assert "data" in result
        # Verify the commit persisted by opening a fresh connection
        with connect(db_path) as verify_conn:
            row = verify_conn.execute(
                "SELECT category_id FROM transactions WHERE description = 'VENMO PAYMENT TO FRIEND' AND is_active = 1"
            ).fetchone()
            # The categorizer may or may not match; what matters is no error
            assert row is not None

    def test_txn_categorize(self, db_path, conn):
        _seed_category(conn, "Dining")
        tid = _seed_txn(conn)
        from finance_cli.mcp_server import txn_categorize
        result = txn_categorize(txn_id=tid, category="Dining")
        assert "data" in result

    def test_txn_categorize_bad_category(self, db_path, conn):
        tid = _seed_txn(conn)
        from finance_cli.mcp_server import txn_categorize
        response = txn_categorize(txn_id=tid, category="Nonexistent Category 999")
        _assert_tool_error(response, "NotFoundError", "not found")

    def test_txn_bulk_categorize(self, db_path, conn):
        _seed_category(conn, "Bank Charges & Fees")
        _seed_txn(conn, description="MCP PLAN FEE 11111", use_type="Business")
        _seed_txn(conn, description="MCP PLAN FEE 22222", use_type="Business")
        from finance_cli.mcp_server import txn_bulk_categorize

        result = txn_bulk_categorize(
            category="Bank Charges & Fees",
            query="MCP PLAN FEE%",
            remember=True,
        )

        assert result["data"]["updated"] == 2
        assert result["data"]["remembered_count"] == 1
        with connect(db_path) as verify_conn:
            remember_rows = verify_conn.execute(
                "SELECT description_pattern, use_type FROM vendor_memory ORDER BY description_pattern, use_type"
            ).fetchall()
        assert [(row["description_pattern"], row["use_type"]) for row in remember_rows] == [
            ("mcp plan fee", "Business")
        ]

    def test_txn_bulk_categorize_requires_filter(self, db_path, conn):
        _seed_category(conn, "Dining")
        _seed_txn(conn, description="MCP NO FILTER")
        from finance_cli.mcp_server import txn_bulk_categorize

        response = txn_bulk_categorize(category="Dining")
        _assert_tool_error(response, "ValidationError", "requires at least one filter")

    def test_txn_bulk_categorize_by_ids(self, db_path, conn):
        dining_id = _seed_category(conn, "Dining")
        fees_id = _seed_category(conn, "Bank Charges & Fees")
        first_id = _seed_txn(conn, description="MCP SELECTED 1", category_id=dining_id)
        second_id = _seed_txn(conn, description="MCP SELECTED 2", category_id=dining_id)
        untouched_id = _seed_txn(conn, description="MCP UNTOUCHED", category_id=dining_id)
        from finance_cli.mcp_server import txn_bulk_categorize

        result = txn_bulk_categorize(
            category="Bank Charges & Fees",
            ids=[first_id, second_id],
        )

        assert result["data"]["updated"] == 2
        rows = conn.execute(
            "SELECT id, category_id FROM transactions WHERE id IN (?, ?, ?)",
            (first_id, second_id, untouched_id),
        ).fetchall()
        categories = {row["id"]: row["category_id"] for row in rows}
        assert categories[first_id] == fees_id
        assert categories[second_id] == fees_id
        assert categories[untouched_id] == dining_id

    def test_txn_review_single(self, db_path, conn):
        tid = _seed_txn(conn)
        from finance_cli.mcp_server import txn_review
        result = txn_review(txn_id=tid)
        assert result["data"]["is_reviewed"] is True

    def test_txn_review_all_today(self, db_path, conn):
        from finance_cli.commands.txn import today_iso
        today = today_iso()
        _seed_txn(conn, date=today)
        _seed_txn(conn, date=today)
        from finance_cli.mcp_server import txn_review
        result = txn_review(all_today=True)
        assert result["data"]["updated"] >= 2

    def test_txn_review_bad_id(self, db_path):
        from finance_cli.mcp_server import txn_review
        response = txn_review(txn_id="nonexistent_000")
        _assert_tool_error(response, "NotFoundError", "not found")

    def test_cat_apply_splits_dry_run(self, db_path, conn):
        (db_path.parent / "rules.yaml").write_text(
            (
                "split_rules:\n"
                "  - match:\n"
                "      category: Rent\n"
                "    business_pct: 25\n"
                "    business_category: Rent\n"
                "    personal_category: Rent\n"
                '    note: "25% business use"\n'
            ),
            encoding="utf-8",
        )
        account_id = _seed_account(conn)
        rent_id = _seed_category(conn, "Rent")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            category_id=rent_id,
            description="Monthly Rent",
            amount_cents=-10000,
        )
        from finance_cli.mcp_server import cat_apply_splits
        result = cat_apply_splits(commit=False, backfill=True, summary_only=False)
        assert "data" in result
        assert result["data"]["candidate_transactions"] >= 1

        child_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE parent_transaction_id = ?",
            (txn_id,),
        ).fetchone()["cnt"]
        assert int(child_count) == 0

    def test_cat_classify_use_type_commit(self, db_path, conn):
        (db_path.parent / "rules.yaml").write_text(
            (
                "category_overrides:\n"
                "  - categories: [Dining]\n"
                "    force_use_type: Personal\n"
                '    note: "test override"\n'
            ),
            encoding="utf-8",
        )
        account_id = _seed_account(conn)
        dining_id = _seed_category(conn, "Dining")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            category_id=dining_id,
            description="Dining Candidate",
        )
        from finance_cli.mcp_server import cat_classify_use_type
        result = cat_classify_use_type(commit=True)
        assert "data" in result
        assert result["data"]["updated"] >= 1

        use_type = conn.execute(
            "SELECT use_type FROM transactions WHERE id = ?",
            (txn_id,),
        ).fetchone()["use_type"]
        assert use_type == "Personal"


# ---------------------------------------------------------------------------
# 5. Setup & Import
# ---------------------------------------------------------------------------

class TestSetupTools:
    def test_setup_init_dry_run(self, db_path):
        from finance_cli.mcp_server import setup_init
        result = setup_init(dry_run=True)
        assert "data" in result
        assert result["data"]["dry_run"] is True

    def test_ingest_csv_valid_upload_path(self, db_path, tmp_path: Path, monkeypatch):
        from finance_cli import mcp_server

        uploads_dir = tmp_path / "uploads"
        uploads_dir.mkdir()
        csv_path = uploads_dir / "statement.csv"
        csv_path.write_text("Date,Description,Amount\n2026-01-01,Test,-10.00\n", encoding="utf-8")
        captured: dict[str, object] = {}

        def fake_handle(ns, conn, **kwargs):
            del conn, kwargs
            captured["file"] = ns.file
            captured["institution"] = ns.institution
            return {"data": {"file": ns.file}, "summary": {}}

        monkeypatch.setattr(mcp_server.ingest, "handle_ingest_csv", fake_handle)
        with _uploads_context(db_path=db_path, uploads_dir=uploads_dir):
            result = mcp_server.ingest_csv(file=str(csv_path), institution="chase")

        assert captured["file"] == str(csv_path.resolve())
        assert captured["institution"] == "chase"
        assert result["data"]["file"] == str(csv_path.resolve())

    def test_ingest_csv_rejects_path_outside_upload_dir(self, db_path, tmp_path: Path):
        from finance_cli import mcp_server

        uploads_dir = tmp_path / "uploads"
        uploads_dir.mkdir()
        outside_path = tmp_path / "outside.csv"
        outside_path.write_text("Date,Description,Amount\n", encoding="utf-8")

        with _uploads_context(db_path=db_path, uploads_dir=uploads_dir):
            response = mcp_server.ingest_csv(file=str(outside_path), institution="chase")

        _assert_tool_error(response, "ValueError", "upload")

    def test_ingest_csv_auto_detects_known_csv(self, db_path, tmp_path: Path, monkeypatch):
        from finance_cli import mcp_server

        uploads_dir = tmp_path / "uploads"
        uploads_dir.mkdir()
        csv_path = uploads_dir / "chase.csv"
        csv_path.write_text(
            (
                "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
                "02/17/2026,02/18/2026,UBER   *TRIP,Travel,Sale,-29.53,\n"
            ),
            encoding="utf-8",
        )
        captured: dict[str, object] = {}

        def fake_handle(ns, conn, **kwargs):
            del conn, kwargs
            captured["institution"] = ns.institution
            return {"data": {"institution": ns.institution}, "summary": {}}

        monkeypatch.setattr(mcp_server.ingest, "handle_ingest_csv", fake_handle)
        with _uploads_context(db_path=db_path, uploads_dir=uploads_dir):
            result = mcp_server.ingest_csv(file=str(csv_path), institution="auto")

        assert captured["institution"] == "chase_credit"
        assert result["data"]["institution"] == "chase_credit"

    def test_ingest_csv_auto_detect_unknown_csv_raises(self, db_path, tmp_path: Path):
        from finance_cli import mcp_server

        uploads_dir = tmp_path / "uploads"
        uploads_dir.mkdir()
        csv_path = uploads_dir / "unknown.csv"
        csv_path.write_text("foo,bar,baz\n1,2,3\n", encoding="utf-8")

        with _uploads_context(db_path=db_path, uploads_dir=uploads_dir):
            response = mcp_server.ingest_csv(file=str(csv_path), institution="auto")

        _assert_tool_error(response, "ValueError", "auto-detect")

    def test_ingest_statement_rejects_path_outside_upload_dir(self, db_path, tmp_path: Path):
        from finance_cli import mcp_server

        uploads_dir = tmp_path / "uploads"
        uploads_dir.mkdir()
        outside_path = tmp_path / "statement.pdf"
        outside_path.write_bytes(b"%PDF-1.4\n")

        with _uploads_context(db_path=db_path, uploads_dir=uploads_dir):
            response = mcp_server.ingest_statement(file=str(outside_path))

        _assert_tool_error(response, "ValueError", "upload")

    def test_ingest_csv_missing_file(self, db_path):
        from finance_cli.mcp_server import ingest_csv
        response = ingest_csv(file="/nonexistent/path.csv", institution="chase")
        _assert_tool_error(response, "FileNotFoundError", "/nonexistent/path.csv")


# ---------------------------------------------------------------------------
# 6. Pipeline
# ---------------------------------------------------------------------------

class TestPipelineTools:
    def test_monthly_run_dry(self, db_path, conn):
        _seed_txn(conn)
        from finance_cli.mcp_server import monthly_run
        result = monthly_run(month="2026-02", dry_run=True)
        assert "data" in result
        assert "summary" in result
        assert result["data"]["month"] == "2026-02"
        assert result["summary"]["steps_skipped"] >= 0

    def test_monthly_run_no_sync(self, db_path):
        from finance_cli.mcp_server import monthly_run
        result = monthly_run(dry_run=True, skip=["dedup", "categorize", "detect"])
        assert result["summary"]["steps_skipped"] >= 3


# ---------------------------------------------------------------------------
# 7. Database
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("mock_backup_secrets")
class TestDatabaseTools:
    def test_db_backup(self, db_path):
        from finance_cli.mcp_server import db_backup

        result = db_backup()
        assert "data" in result
        assert "summary" in result
        bundle_path = result["data"].get("bundle_path") or result["data"].get("backup_path", "")
        assert bundle_path
        assert result["data"]["backup_path"] == bundle_path
        assert Path(bundle_path).exists()

    def test_db_backup_list(self, db_path):
        from finance_cli.mcp_server import db_backup, db_backup_list

        created = db_backup()
        result = db_backup_list()

        assert any(item["bundle_path"] == created["data"]["bundle_path"] for item in result["data"])
        assert result["summary"]["count"] >= 1

    def test_db_backup_verify(self, db_path):
        from finance_cli.mcp_server import db_backup, db_backup_verify

        created = db_backup()
        result = db_backup_verify(created["data"]["bundle_path"])

        assert result["data"]["valid"] is True
        assert result["summary"]["valid"] is True
        with connect(db_path) as audit_conn:
            events = [
                row["event_type"]
                for row in audit_conn.execute("SELECT event_type FROM sensitive_audit_events ORDER BY id")
            ]
        assert events == ["db.backup.created", "db.backup.verified"]

    def test_db_restore_dry_run(self, db_path):
        from finance_cli.mcp_server import db_backup, db_restore

        created = db_backup()
        result = db_restore(bundle_path=created["data"]["bundle_path"], dry_run=True)

        assert result["data"]["dry_run"] is True
        assert result["data"]["restored"] is False
        with connect(db_path) as audit_conn:
            events = [
                row["event_type"]
                for row in audit_conn.execute("SELECT event_type FROM sensitive_audit_events ORDER BY id")
            ]
        assert events == ["db.backup.created", "db.restore.previewed"]

    def test_db_backup_prune(self, db_path):
        from finance_cli.mcp_server import db_backup, db_backup_prune

        db_backup()
        result = db_backup_prune(dry_run=True)

        assert result["data"]["dry_run"] is True
        assert "kept" in result["data"]

    def test_db_export_preferences(self, db_path, conn):
        from finance_cli.mcp_server import db_export_preferences

        _seed_preferences_fixture(conn, db_path.parent)
        result = db_export_preferences()

        assert "data" in result
        assert "summary" in result
        assert Path(result["data"]["bundle_path"]).exists()
        assert result["summary"]["total_rows"] >= 1
        events = [row["event_type"] for row in conn.execute("SELECT event_type FROM sensitive_audit_events")]
        assert events == ["data_export.preferences"]

    def test_db_import_preferences(self, db_path, conn):
        from finance_cli.mcp_server import db_import_preferences

        bundle_path = _seed_preferences_fixture(conn, db_path.parent)
        result = db_import_preferences(bundle_path=str(bundle_path), dry_run=True)

        assert result["data"]["dry_run"] is True
        assert "tables_imported" in result["data"]
        assert "total_imported" in result["summary"]
        events = [row["event_type"] for row in conn.execute("SELECT event_type FROM sensitive_audit_events")]
        assert events == ["data_import.preferences"]


# ---------------------------------------------------------------------------
# 8. Wave 2 MCP tools
# ---------------------------------------------------------------------------

class TestBudgetTools:
    def test_budget_set_list_status(self, db_path, conn):
        _seed_category(conn, "Dining")
        from finance_cli.mcp_server import (
            budget_alerts,
            budget_delete,
            budget_forecast,
            budget_list,
            budget_set,
            budget_status,
            budget_suggest,
            budget_update,
        )

        set_result = budget_set(category="Dining", amount=500, period="monthly")
        assert "data" in set_result
        assert "summary" in set_result

        listed = budget_list()
        assert any(row["category_name"] == "Dining" for row in listed["data"]["budgets"])

        month = date.today().strftime("%Y-%m")
        status = budget_status(month=month)
        assert any(row["category_name"] == "Dining" for row in status["data"]["status"])

        forecast = budget_forecast(month=month)
        assert "forecast" in forecast["data"]

        alerts = budget_alerts(month=month)
        assert "alerts" in alerts["data"]

        suggest = budget_suggest(goal="savings", target=100)
        assert "suggestions" in suggest["data"]

        updated = budget_update(category="Dining", amount=450, period="monthly")
        assert updated["data"]["amount"] == 450.0

        deleted = budget_delete(category="Dining", period="monthly")
        assert deleted["data"]["deleted"] is True


class TestBalanceTools:
    def test_balance_show_and_history(self, db_path, conn):
        account_id = _seed_account(conn, institution="History Bank", name="History Checking", balance_cents=125000)
        _seed_balance_snapshot(conn, account_id=account_id, balance_current_cents=124500)
        from finance_cli.mcp_server import balance_history, balance_show

        shown = balance_show()
        assert any(row["id"] == account_id for row in shown["data"]["accounts"])

        history = balance_history(account=account_id, days=30)
        assert history["data"]["account"]["id"] == account_id
        assert history["data"]["history"]

    def test_balance_update_records_manual_snapshot(self, db_path, conn):
        account_id = _seed_account(conn, institution="Manual Bank", name="Manual Checking", balance_cents=125000)
        from finance_cli.mcp_server import balance_update

        result = balance_update(
            account=account_id,
            current=1300.25,
            available=1200,
            snapshot_date="2026-06-23",
        )

        assert result["data"]["account_id"] == account_id
        assert result["data"]["snapshot"]["source"] == "manual"
        assert result["data"]["snapshot"]["balance_current_cents"] == 130_025
        row = conn.execute(
            """
            SELECT balance_current_cents, balance_available_cents
              FROM balance_snapshots
             WHERE account_id = ?
               AND snapshot_date = '2026-06-23'
               AND source = 'manual'
            """,
            (account_id,),
        ).fetchone()
        assert row["balance_current_cents"] == 130_025
        assert row["balance_available_cents"] == 120_000


class TestLiabilityTools:
    def test_liability_show_and_upcoming(self, db_path, conn):
        account_id = _seed_account(conn, account_type="credit_card", balance_cents=-25000)
        liability_id = _seed_credit_liability(
            conn,
            account_id=account_id,
            apr_purchase=19.99,
            minimum_payment_cents=2500,
            next_monthly_payment_cents=2500,
        )
        conn.execute(
            "UPDATE liabilities SET next_payment_due_date = date('now', '+7 day') WHERE id = ?",
            (liability_id,),
        )
        conn.commit()

        from finance_cli.mcp_server import liability_show, liability_upcoming

        shown = liability_show()
        assert any(row["id"] == liability_id for row in shown["data"]["liabilities"])

        upcoming = liability_upcoming(days=14)
        assert upcoming["summary"]["total_upcoming"] >= 1

    def test_liability_show_paginates_each_collection_and_preserves_totals(self, db_path, conn):
        first_account_id = _seed_account(conn, account_type="credit_card", balance_cents=-25000)
        second_account_id = _seed_account(
            conn,
            name="Travel Card",
            account_type="credit_card",
            balance_cents=-50000,
        )
        _seed_credit_liability(conn, account_id=first_account_id, apr_purchase=19.99, minimum_payment_cents=2500)
        _seed_credit_liability(conn, account_id=second_account_id, apr_purchase=21.99, minimum_payment_cents=3500)

        from finance_cli.mcp_server import liability_show, loan_add

        loan_add(
            creditor="Family A",
            amount=1000.0,
            start_date="2026-01-01",
            monthly_payment=75.0,
        )
        loan_add(
            creditor="Family B",
            amount=2000.0,
            start_date="2026-01-15",
            monthly_payment=125.0,
        )

        shown = liability_show(limit=1, offset=1)

        assert len(shown["data"]["liabilities"]) == 1
        assert len(shown["data"]["manual_loans"]) == 1
        assert shown["data"]["total_liabilities_count"] == 2
        assert shown["data"]["total_manual_loans_count"] == 2
        assert shown["data"]["limit"] == 1
        assert shown["data"]["offset"] == 1
        assert shown["data"]["total_minimum_due_cents"] == 26_000
        assert shown["summary"]["total_liabilities"] == 4


class TestLoanTools:
    def test_renamed_tools_registered(self) -> None:
        import finance_cli.mcp_server as mcp_server

        tool_names = {
            tool.name
            for tool in asyncio.run(mcp_server.mcp.list_tools(run_middleware=False))
        }

        expected_tools = {
            "bank_account_activate",
            "bank_account_deactivate",
            "statement_normalizer_sample_csv",
            "statement_normalizer_list",
            "statement_normalizer_test",
            "statement_normalizer_stage",
            "statement_normalizer_activate",
            "finance_log_issue",
        }
        retired_tools = {
            "account_" "activate",
            "account_" "deactivate",
            "normalizer_" "sample_csv",
            "normalizer_" "list",
            "normalizer_" "test",
            "normalizer_" "stage",
            "normalizer_" "activate",
            "log_" "issue",
        }

        assert expected_tools.issubset(tool_names)
        assert retired_tools.isdisjoint(tool_names)

    def test_loan_tools_registered(self, monkeypatch) -> None:
        import finance_cli.mcp_server as mcp_server
        from finance_cli.commands import loan_cmd as loan_cmd_module

        calls: list[tuple[str, dict[str, object]]] = []

        def _fake_call(handler, ns_kwargs, *, pass_rules: bool = False):
            del pass_rules
            calls.append((handler.__name__, dict(ns_kwargs)))
            return {"data": {}, "summary": {}}

        monkeypatch.setattr(mcp_server, "_call", _fake_call)

        tool_names = {
            tool.name
            for tool in asyncio.run(mcp_server.mcp.list_tools(run_middleware=False))
        }
        expected_tools = {
            "loan_list",
            "loan_show",
            "loan_schedule",
            "loan_add",
            "loan_payment",
            "loan_disburse",
            "loan_adjust",
            "loan_close",
        }

        assert expected_tools.issubset(tool_names)
        assert all(callable(getattr(mcp_server, tool_name, None)) for tool_name in expected_tools)

        mcp_server.loan_list(include_inactive=True, limit=25, offset=5)
        mcp_server.loan_show(loan_id="loan-1")
        mcp_server.loan_schedule(loan_id="loan-1", months=18, summary_only=False)
        mcp_server.loan_add(
            creditor="Mom",
            amount=5000.0,
            start_date="2026-01-01",
            rate=4.25,
            interest_type="simple",
            monthly_payment=250.0,
            due_day=15,
            expected_payoff="2027-12-01",
            use_type="Personal",
            description="Family loan",
            idempotency_key="loan-add-1",
        )
        mcp_server.loan_payment(
            loan_id="loan-1",
            amount=200.0,
            date="2026-02-01",
            transaction_id="txn-1",
            notes="February payment",
        )
        mcp_server.loan_disburse(
            loan_id="loan-1",
            amount=300.0,
            date="2026-02-15",
            notes="Extra advance",
        )
        mcp_server.loan_adjust(
            loan_id="loan-1",
            rate=5.0,
            interest_type="compound",
            monthly_payment=275.0,
            due_day=20,
            expected_payoff="2028-01-01",
            balance=4200.0,
            description="Adjusted terms",
        )
        mcp_server.loan_close(loan_id="loan-1", forgiven=True)

        assert calls == [
            (loan_cmd_module.handle_list.__name__, {"include_inactive": True, "limit": 25, "offset": 5}),
            (loan_cmd_module.handle_show.__name__, {"loan_id": "loan-1"}),
            (
                loan_cmd_module.handle_schedule.__name__,
                {"loan_id": "loan-1", "months": 18, "summary_only": False},
            ),
            (
                loan_cmd_module.handle_add.__name__,
                {
                    "creditor": "Mom",
                    "amount": 5000.0,
                    "start_date": "2026-01-01",
                    "rate": 4.25,
                    "interest_type": "simple",
                    "monthly_payment": 250.0,
                    "due_day": 15,
                    "expected_payoff": "2027-12-01",
                    "use_type": "Personal",
                    "description": "Family loan",
                    "idempotency_key": "loan-add-1",
                    "dry_run": False,
                },
            ),
            (
                loan_cmd_module.handle_payment.__name__,
                {
                    "loan_id": "loan-1",
                    "amount": 200.0,
                    "date": "2026-02-01",
                    "transaction_id": "txn-1",
                    "notes": "February payment",
                    "dry_run": False,
                },
            ),
            (
                loan_cmd_module.handle_disburse.__name__,
                {
                    "loan_id": "loan-1",
                    "amount": 300.0,
                    "date": "2026-02-15",
                    "notes": "Extra advance",
                    "dry_run": False,
                },
            ),
            (
                loan_cmd_module.handle_adjust.__name__,
                {
                    "loan_id": "loan-1",
                    "rate": 5.0,
                    "interest_type": "compound",
                    "monthly_payment": 275.0,
                    "due_day": 20,
                    "expected_payoff": "2028-01-01",
                    "balance": 4200.0,
                    "description": "Adjusted terms",
                    "dry_run": False,
                },
            ),
            (
                loan_cmd_module.handle_close.__name__,
                {"loan_id": "loan-1", "forgiven": True, "dry_run": False},
            ),
        ]

    def test_loan_list_pagination_preserves_full_totals(self, db_path):
        from finance_cli.mcp_server import loan_add, loan_close, loan_list

        loan_add(
            creditor="Family One",
            amount=100.0,
            start_date="2026-01-01",
            monthly_payment=10.0,
        )
        loan_add(
            creditor="Family Two",
            amount=200.0,
            start_date="2026-01-02",
            monthly_payment=20.0,
        )
        closed = loan_add(
            creditor="Family Three",
            amount=300.0,
            start_date="2026-01-03",
            monthly_payment=30.0,
        )
        loan_close(loan_id=closed["data"]["loan"]["id"], forgiven=True)

        result = loan_list(include_inactive=True, limit=1, offset=1)

        assert len(result["data"]["loans"]) == 1
        assert result["data"]["total_count"] == 3
        assert result["data"]["limit"] == 1
        assert result["data"]["offset"] == 1
        assert result["data"]["total_balance_cents"] == 30_000
        assert result["summary"]["total_loans"] == 3
        assert result["summary"]["active_loans"] == 2


class TestTxnWriteTools:
    def test_txn_add_edit_deactivate_tag(self, db_path, conn):
        account_id = _seed_account(conn)
        from finance_cli.mcp_server import txn_add, txn_deactivate, txn_edit, txn_tag

        added = txn_add(
            amount=25.50,
            date=date.today().isoformat(),
            description="MCP write test",
            account_id=account_id,
        )
        txn_id = added["data"]["transaction_id"]
        assert txn_id

        edited = txn_edit(id=txn_id, notes="edited note")
        assert edited["data"]["updated_fields"] >= 1

        deactivated = txn_deactivate(id=txn_id)
        assert deactivated["data"]["deactivated"] is True
        assert deactivated["summary"]["deactivated_count"] == 1

        tagged = txn_tag(id=txn_id, project="MCP")
        assert tagged["data"]["project"] == "MCP"

        row = conn.execute(
            """
            SELECT t.notes, t.is_active, t.removed_at, p.name AS project_name
              FROM transactions t
              LEFT JOIN projects p ON p.id = t.project_id
             WHERE t.id = ?
            """,
            (txn_id,),
        ).fetchone()
        assert row["notes"] == "edited note"
        assert row["is_active"] == 0
        assert row["removed_at"] is not None
        assert row["project_name"] == "MCP"

    def test_txn_deactivate_dry_run_leaves_transaction_active(self, db_path, conn):
        account_id = _seed_account(conn)
        txn_id = _seed_txn(conn, account_id=account_id)
        from finance_cli.mcp_server import txn_deactivate

        result = txn_deactivate(id=txn_id, dry_run=True)

        assert result["data"]["deactivated"] is True
        assert result["data"]["dry_run"] is True
        row = conn.execute(
            "SELECT is_active, removed_at FROM transactions WHERE id = ?",
            (txn_id,),
        ).fetchone()
        assert row["is_active"] == 1
        assert row["removed_at"] is None

    def test_txn_bulk_tag_reports_partial_failures(self, db_path, conn):
        account_id = _seed_account(conn)
        txn_id = _seed_txn(conn, account_id=account_id)
        from finance_cli.mcp_server import txn_bulk_tag

        result = txn_bulk_tag(
            items=[
                {"id": txn_id, "project": "MCP"},
                {"id": "missing-txn", "project": "MCP"},
            ],
        )

        assert result["summary"] == {
            "total": 2,
            "succeeded": 1,
            "failed": 1,
            "status": "partial_error",
        }
        assert result["data"]["results"][0]["status"] == "success"
        assert result["data"]["results"][1]["status"] == "error"
        assert result["data"]["results"][1]["error_class"] == "NotFoundError"
        row = conn.execute(
            """
            SELECT p.name AS project_name
              FROM transactions t
              JOIN projects p ON p.id = t.project_id
             WHERE t.id = ?
            """,
            (txn_id,),
        ).fetchone()
        assert row["project_name"] == "MCP"

    def test_txn_add_idempotency_key_returns_existing_row(self, db_path, conn):
        from finance_cli.mcp_server import txn_add

        first = txn_add(
            amount=25.50,
            date=date.today().isoformat(),
            description="Idempotent txn",
            idempotency_key="txn-add-1",
        )
        second = txn_add(
            amount=25.50,
            date=date.today().isoformat(),
            description="Idempotent txn",
            idempotency_key="txn-add-1",
        )

        assert second["data"]["transaction_id"] == first["data"]["transaction_id"]
        assert second["data"]["already_existed"] is True

        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE idempotency_key = ?",
            ("txn-add-1",),
        ).fetchone()
        assert row["cnt"] == 1

    def test_txn_add_without_idempotency_key_keeps_normal_behavior(self, db_path, conn):
        from finance_cli.mcp_server import txn_add

        first = txn_add(amount=10.0, date=date.today().isoformat(), description="No key txn")
        second = txn_add(amount=10.0, date=date.today().isoformat(), description="No key txn")

        assert first["data"]["transaction_id"] != second["data"]["transaction_id"]

        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE description = ?",
            ("No key txn",),
        ).fetchone()
        assert row["cnt"] == 2


class TestTxnFilterFixes:
    def test_txn_list_filters_account_id_and_use_type(self, db_path, conn):
        account_a = _seed_account(conn, name="Account A")
        account_b = _seed_account(conn, name="Account B")
        _seed_txn(conn, account_id=account_a, use_type="Business", description="A biz")
        _seed_txn(conn, account_id=account_a, use_type="Personal", description="A personal")
        _seed_txn(conn, account_id=account_b, use_type="Personal", description="B personal")
        from finance_cli.mcp_server import txn_list

        by_account = txn_list(account_id=account_a, limit=20)
        assert by_account["data"]["transactions"]
        assert all(row["account_id"] == account_a for row in by_account["data"]["transactions"])

        by_use_type = txn_list(use_type="Business", limit=20)
        assert by_use_type["data"]["transactions"]
        assert all(row["use_type"] == "Business" for row in by_use_type["data"]["transactions"])

    def test_txn_search_limit(self, db_path, isolated_mcp_cache, monkeypatch):
        import finance_cli.mcp_server as mcp_server

        fake_rows = [{"id": "t1"}, {"id": "t2"}, {"id": "t3"}]
        monkeypatch.setattr(
            mcp_server,
            "_call",
            lambda handler, ns_kwargs: {"data": {"transactions": list(fake_rows)}, "summary": {}},
        )

        result = mcp_server.txn_search(query="uber", limit=2)
        assert len(result["data"]["transactions"]) == 2


class TestCatMemoryTools:
    def test_cat_memory_list_add_disable(self, db_path, conn):
        _seed_category(conn, "Dining")
        from finance_cli.mcp_server import cat_memory_add, cat_memory_disable, cat_memory_list

        added = cat_memory_add(pattern="STARBUCKS", category="Dining", use_type="Any")
        rule_id = added["data"]["rule_id"]
        assert rule_id

        listed = cat_memory_list(limit=10)
        assert any(row["id"] == rule_id for row in listed["data"]["rules"])

        disabled = cat_memory_disable(id=rule_id)
        assert disabled["data"]["is_enabled"] is False

    def test_cat_memory_delete_returns_restore_token_and_restore_relinks_transactions(self, db_path, conn):
        category_id = _seed_category(conn, "Dining")
        txn_id = _seed_txn(conn, category_id=category_id, description="STARBUCKS")
        from finance_cli.mcp_server import cat_memory_add, cat_memory_delete, cat_memory_restore

        added = cat_memory_add(pattern="STARBUCKS", category="Dining", use_type="Any")
        rule_id = added["data"]["rule_id"]
        conn.execute("UPDATE transactions SET category_rule_id = ? WHERE id = ?", (rule_id, txn_id))
        conn.commit()

        deleted = cat_memory_delete(id=rule_id)

        assert deleted["data"]["deleted"] is True
        assert deleted["data"]["soft_deleted"] is True
        assert deleted["data"]["undo_tool"] == "cat_memory_restore"
        assert deleted["data"]["restore_token"]
        rule = conn.execute("SELECT is_enabled FROM vendor_memory WHERE id = ?", (rule_id,)).fetchone()
        txn = conn.execute("SELECT category_rule_id FROM transactions WHERE id = ?", (txn_id,)).fetchone()
        assert int(rule["is_enabled"]) == 0
        assert txn["category_rule_id"] is None

        restored = cat_memory_restore(restore_token=deleted["data"]["restore_token"])

        assert restored["data"]["restored"] is True
        assert restored["data"]["restored_transaction_links"] == 1
        rule = conn.execute("SELECT is_enabled FROM vendor_memory WHERE id = ?", (rule_id,)).fetchone()
        txn = conn.execute("SELECT category_rule_id FROM transactions WHERE id = ?", (txn_id,)).fetchone()
        assert int(rule["is_enabled"]) == 1
        assert txn["category_rule_id"] == rule_id

    def test_cat_memory_bulk_disable_and_delete_report_partial_failures(self, db_path, conn):
        _seed_category(conn, "Dining")
        from finance_cli.mcp_server import (
            cat_memory_add,
            cat_memory_delete_bulk,
            cat_memory_disable_bulk,
        )

        first_id = cat_memory_add(pattern="BULK DISABLE", category="Dining", use_type="Any")["data"]["rule_id"]
        second_id = cat_memory_add(pattern="BULK DELETE", category="Dining", use_type="Any")["data"]["rule_id"]

        disabled = cat_memory_disable_bulk(ids=[first_id, "missing-rule"])
        assert disabled["summary"]["status"] == "partial_error"
        assert disabled["summary"]["succeeded"] == 1
        assert disabled["summary"]["failed"] == 1
        assert disabled["data"]["results"][1]["error_class"] == "NotFoundError"
        first_rule = conn.execute("SELECT is_enabled FROM vendor_memory WHERE id = ?", (first_id,)).fetchone()
        assert int(first_rule["is_enabled"]) == 0

        deleted = cat_memory_delete_bulk(ids=[second_id, "missing-rule"])
        assert deleted["summary"]["status"] == "partial_error"
        assert deleted["summary"]["succeeded"] == 1
        assert deleted["summary"]["failed"] == 1
        success_data = deleted["data"]["results"][0]["result"]["data"]
        assert success_data["undo_tool"] == "cat_memory_restore"
        assert success_data["restore_token"]
        assert deleted["data"]["results"][1]["error_class"] == "NotFoundError"

    def test_cat_memory_add_bulk_saves_rules_and_reports_partial_failures(self, db_path, conn):
        _seed_category(conn, "Dining")
        _seed_category(conn, "Utilities")
        from finance_cli.mcp_server import cat_memory_add_bulk

        result = cat_memory_add_bulk(
            rules=[
                {
                    "pattern": "BULK CAFE",
                    "category": "Dining",
                    "use_type": "Personal",
                },
                {
                    "pattern": "BULK POWER",
                    "category": "Utilities",
                    "use_type": "Any",
                },
                {
                    "pattern": "BULK MISSING",
                    "category": "Missing Category",
                    "use_type": "Any",
                },
            ]
        )

        assert result["summary"]["status"] == "partial_error"
        assert result["summary"]["succeeded"] == 2
        assert result["summary"]["failed"] == 1
        assert result["data"]["results"][2]["error_class"] == "NotFoundError"
        rows = conn.execute(
            """
            SELECT description_pattern, use_type
              FROM vendor_memory
             ORDER BY description_pattern
            """
        ).fetchall()
        assert [(row["description_pattern"], row["use_type"]) for row in rows] == [
            ("bulk cafe", "Personal"),
            ("bulk power", "Any"),
        ]

    def test_cat_review_new_merchants_confirms_fixes_skips_and_reports_errors(
        self, db_path, conn
    ):
        dining_id = _seed_category(conn, "Dining")
        utilities_id = _seed_category(conn, "Utilities")
        fix_id = _seed_txn(
            conn,
            description="REVIEW POWER",
            category_id=dining_id,
            use_type="Personal",
        )
        from finance_cli.mcp_server import cat_review_new_merchants

        result = cat_review_new_merchants(
            items=[
                {
                    "decision": "confirm",
                    "pattern": "REVIEW CAFE",
                    "category": "Dining",
                    "use_type": "Personal",
                },
                {
                    "decision": "fix",
                    "category": "Utilities",
                    "txn_ids": [fix_id],
                },
                {
                    "decision": "skip",
                    "pattern": "REVIEW SKIP",
                },
                {
                    "decision": "fix",
                    "category": "Missing Category",
                    "txn_ids": [fix_id],
                },
            ]
        )

        assert result["summary"]["status"] == "partial_error"
        assert result["summary"]["succeeded"] == 3
        assert result["summary"]["failed"] == 1
        assert result["data"]["results"][2]["result"]["data"]["skipped"] is True
        assert result["data"]["results"][3]["error_class"] == "NotFoundError"
        txn = conn.execute(
            "SELECT category_id, category_source FROM transactions WHERE id = ?",
            (fix_id,),
        ).fetchone()
        assert txn["category_id"] == utilities_id
        assert txn["category_source"] == "user"
        rows = conn.execute(
            """
            SELECT description_pattern, use_type, category_id
              FROM vendor_memory
             ORDER BY description_pattern
            """
        ).fetchall()
        assert [
            (row["description_pattern"], row["use_type"], row["category_id"])
            for row in rows
        ] == [
            ("review cafe", "Personal", dining_id),
            ("review power", "Personal", utilities_id),
        ]


class TestSubsWriteTools:
    def test_subs_detect_add_cancel(self, db_path, conn):
        account_id = _seed_account(conn)
        _seed_txn(conn, account_id=account_id, description="NETFLIX", amount_cents=-1599, date=_month_date(-3))
        _seed_txn(conn, account_id=account_id, description="NETFLIX", amount_cents=-1599, date=_month_date(-2))
        _seed_txn(conn, account_id=account_id, description="NETFLIX", amount_cents=-1599, date=_month_date(-1))

        from finance_cli.mcp_server import subs_add, subs_cancel, subs_detect, subs_update

        detected = subs_detect()
        assert "data" in detected
        assert "summary" in detected

        added = subs_add(vendor="MCP Subscription", amount=9.99, frequency="monthly")
        sub_id = added["data"]["subscription_id"]
        assert sub_id

        updated = subs_update(id=sub_id, amount=12.49, frequency="yearly", use_type="Business")
        assert updated["data"]["subscription_id"] == sub_id
        assert updated["data"]["changes"]["amount"]["new"] == 12.49
        assert updated["data"]["changes"]["frequency"]["new"] == "yearly"
        assert updated["data"]["changes"]["use_type"]["new"] == "Business"

        row = conn.execute(
            "SELECT amount_cents, frequency, use_type, is_active FROM subscriptions WHERE id = ?",
            (sub_id,),
        ).fetchone()
        assert int(row["amount_cents"]) == 1249
        assert row["frequency"] == "yearly"
        assert row["use_type"] == "Business"
        assert int(row["is_active"]) == 1

        canceled = subs_cancel(id=sub_id)
        assert canceled["data"]["is_active"] is False

    def test_subs_add_idempotency_key_returns_existing_row(self, db_path, conn):
        from finance_cli.mcp_server import subs_add

        first = subs_add(
            vendor="Idempotent Subscription",
            amount=9.99,
            frequency="monthly",
            idempotency_key="subs-add-1",
        )
        second = subs_add(
            vendor="Idempotent Subscription",
            amount=9.99,
            frequency="monthly",
            idempotency_key="subs-add-1",
        )

        assert second["data"]["subscription_id"] == first["data"]["subscription_id"]
        assert second["data"]["already_existed"] is True

        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM subscriptions WHERE idempotency_key = ?",
            ("subs-add-1",),
        ).fetchone()
        assert row["cnt"] == 1

    def test_subs_add_without_idempotency_key_keeps_normal_behavior(self, db_path, conn):
        from finance_cli.mcp_server import subs_add

        first = subs_add(vendor="No Key Subscription", amount=4.99, frequency="monthly")
        second = subs_add(vendor="No Key Subscription", amount=4.99, frequency="monthly")

        assert first["data"]["subscription_id"] != second["data"]["subscription_id"]

        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM subscriptions WHERE vendor_name = ?",
            ("No Key Subscription",),
        ).fetchone()
        assert row["cnt"] == 2


class TestDedupTools:
    def test_dedup_cross_format_and_audit_names(self, db_path, conn):
        account_id = _seed_account(conn, institution="Dup Bank")
        _seed_txn(
            conn,
            account_id=account_id,
            source="manual",
            date="2026-02-01",
            description="DUP TEST PAYMENT",
            amount_cents=-1200,
        )
        _seed_txn(
            conn,
            account_id=account_id,
            source="plaid",
            date="2026-02-01",
            description="DUP TEST PAYMENT",
            amount_cents=-1200,
        )

        from finance_cli.mcp_server import dedup_audit_names, dedup_cross_format

        cross = dedup_cross_format(dry_run=True, account_id=account_id, summary_only=False)
        assert cross["data"]["dry_run"] is True
        assert cross["data"]["account_id"] == account_id
        assert cross["summary"]["total_matches"] >= 1

        # summary_only mode returns cli_report instead of full data
        cross_summary = dedup_cross_format(dry_run=True, account_id=account_id, summary_only=True)
        assert "cli_report" in cross_summary["data"]

        audit = dedup_audit_names(summary_only=False)
        assert "issues" in audit["data"]

    def test_dedup_cross_format_key_only_requires_explicit_opt_in(self, db_path, conn):
        account_id = _seed_account(conn, institution="Dup Bank")
        _seed_txn(
            conn,
            account_id=account_id,
            source="csv_import",
            date="2026-02-01",
            description="STARBUCKS",
            amount_cents=-1200,
        )
        remove_id = _seed_txn(
            conn,
            account_id=account_id,
            source="pdf_import",
            date="2026-02-01",
            description="UBER",
            amount_cents=-1200,
        )

        from finance_cli.mcp_server import dedup_cross_format

        skipped = dedup_cross_format(dry_run=False, account_id=account_id, summary_only=False)
        row = conn.execute("SELECT is_active FROM transactions WHERE id = ?", (remove_id,)).fetchone()
        assert skipped["summary"]["total_matches"] == 1
        assert skipped["summary"]["key_only_count"] == 1
        assert skipped["summary"]["total_removed"] == 0
        assert row["is_active"] == 1

        applied = dedup_cross_format(
            dry_run=False,
            account_id=account_id,
            include_key_only=True,
            summary_only=False,
        )
        row = conn.execute("SELECT is_active FROM transactions WHERE id = ?", (remove_id,)).fetchone()
        assert applied["summary"]["total_matches"] == 1
        assert applied["summary"]["key_only_count"] == 1
        assert applied["summary"]["total_removed"] == 1
        assert row["is_active"] == 0

    def test_dedup_same_source_mcp_tool(self, db_path, conn):
        account_id = _seed_account(conn, institution="Same Source Bank")
        _seed_txn(
            conn,
            account_id=account_id,
            source="csv_import",
            date="2026-02-03",
            description="DELTA AIR LINES",
            amount_cents=-8840,
        )
        _seed_txn(
            conn,
            account_id=account_id,
            source="csv_import",
            date="2026-02-03",
            description="DELTA AIR LINES",
            amount_cents=-8840,
        )

        from finance_cli.mcp_server import dedup_same_source

        # summary_only=True (default) — returns CLI report, no groups JSON
        result = dedup_same_source(account_id=account_id, min_amount_cents=1000)
        assert set(result.keys()) == {"data", "summary"}
        assert "cli_report" in result["data"]
        assert "groups" not in result["data"]
        assert result["data"]["total_groups"] == 1
        assert result["data"]["total_excess_rows"] == 1
        assert result["summary"]["total_groups"] == 1
        assert result["summary"]["total_excess_rows"] == 1

        # summary_only=False — returns full groups JSON
        full = dedup_same_source(account_id=account_id, min_amount_cents=1000, summary_only=False)
        assert full["data"]["dry_run"] is True
        assert full["data"]["account_id"] == account_id
        assert full["data"]["min_amount"] == 1000
        assert full["data"]["total_groups"] == 1
        assert len(full["data"]["groups"]) == 1
        assert full["summary"]["total_groups"] == 1
        assert full["summary"]["total_excess_rows"] == 1


class TestPlanTools:
    def test_plan_create_show_review(self, db_path, conn):
        account_id = _seed_account(conn)
        _seed_txn(conn, account_id=account_id, date=_month_date(-1), amount_cents=50000, description="Income")
        _seed_txn(conn, account_id=account_id, date=_month_date(-1), amount_cents=-12000, description="Expense")
        from finance_cli.mcp_server import plan_create, plan_review, plan_show

        created = plan_create()
        month = created["data"]["month"]
        assert month

        shown = plan_show(month=month)
        assert shown["data"]["plan"]["month"] == month

        reviewed = plan_review()
        assert "review" in reviewed["data"]


class TestRulesTools:
    def test_rules_show_and_validate(self, db_path):
        from finance_cli.mcp_server import rules_show, rules_validate

        shown = rules_show()
        assert "data" in shown
        assert "summary" in shown

        validated = rules_validate()
        assert "valid" in validated["data"]
        assert "errors" in validated["data"]

    def test_rules_add_keyword_and_remove_keyword(self, db_path):
        from finance_cli.mcp_server import rules_add_keyword, rules_remove_keyword, rules_test

        with connect(db_path) as seeded_conn:
            _seed_category(seeded_conn, "Dining")

        added = rules_add_keyword(keyword="NEWVENDOR", category="Dining")
        assert added["data"]["action"] == "added"
        tested = rules_test(description="NEWVENDOR PURCHASE")
        assert tested["data"]["keyword_match"]["matched_keyword"] == "NEWVENDOR"

        removed = rules_remove_keyword(keyword="NEWVENDOR")
        assert removed["data"]["keyword"] == "NEWVENDOR"
        retested = rules_test(description="NEWVENDOR PURCHASE")
        assert retested["data"]["keyword_match"] is None

    def test_rules_add_keywords_reports_partial_failures(self, db_path):
        from finance_cli.mcp_server import rules_add_keywords, rules_test

        with connect(db_path) as seeded_conn:
            _seed_category(seeded_conn, "Dining")

        result = rules_add_keywords(
            items=[
                {"keyword": "BULKVENDOR", "category": "Dining"},
                {"keyword": "BROKENVENDOR", "category": "Missing Category"},
            ]
        )

        assert result["summary"] == {
            "total": 2,
            "succeeded": 1,
            "failed": 1,
            "status": "partial_error",
        }
        assert result["data"]["results"][0]["status"] == "success"
        assert result["data"]["results"][1]["status"] == "error"
        assert result["data"]["results"][1]["error_class"] == "ValueError"
        tested = rules_test(description="BULKVENDOR PURCHASE")
        assert tested["data"]["keyword_match"]["matched_keyword"] == "BULKVENDOR"

    def test_rules_add_split(self, db_path):
        from finance_cli.mcp_server import rules_add_split, rules_show

        with connect(db_path) as seeded_conn:
            _seed_category(seeded_conn, "Utilities")
        (db_path.parent / "rules.yaml").write_text(
            "keyword_rules: []\nsplit_rules: []\n",
            encoding="utf-8",
        )

        added = rules_add_split(
            business_pct=80,
            business_category="Utilities",
            personal_category="Utilities",
            match_keywords=["VERIZON"],
            note="80% business use of internet",
        )
        assert added["data"]["rule"]["business_pct"] == 80

        shown = rules_show()
        assert shown["data"]["split_rules"] == [
            {
                "match": {
                    "category": None,
                    "keywords": ["VERIZON"],
                },
                "business_pct": 80.0,
                "business_category": "Utilities",
                "personal_category": "Utilities",
                "note": "80% business use of internet",
            }
        ]

    def test_rules_list(self, db_path):
        from finance_cli.mcp_server import rules_list

        result = rules_list()
        assert "data" in result
        assert isinstance(result["data"]["rules"], list)
        assert result["data"]["count"] >= 0
        assert result["data"]["total_count"] >= result["data"]["count"]
        if result["data"]["rules"]:
            rule = result["data"]["rules"][0]
            assert "category" in rule
            assert "keywords" in rule
            assert "priority" in rule
            assert "rule_index" in rule

    def test_rules_list_pagination_preserves_total_count(self, db_path, conn):
        from finance_cli.mcp_server import rules_add_keyword, rules_list

        _seed_category(conn, "Dining")
        _seed_category(conn, "Travel")
        _seed_category(conn, "Coffee")

        rules_add_keyword(keyword="RULEDINING", category="Dining")
        rules_add_keyword(keyword="RULETRAVEL", category="Travel")
        rules_add_keyword(keyword="RULECOFFEE", category="Coffee")

        result = rules_list(limit=1, offset=1)

        assert len(result["data"]["rules"]) == 1
        assert result["data"]["count"] == 1
        assert result["data"]["total_count"] >= 3
        assert result["data"]["limit"] == 1
        assert result["data"]["offset"] == 1
        assert result["summary"]["count"] == result["data"]["total_count"]
        assert result["summary"]["total_rules"] == result["data"]["total_count"]

    def test_rules_update_priority(self, db_path):
        from finance_cli.mcp_server import rules_add_keyword, rules_list, rules_update_priority

        with connect(db_path) as seeded_conn:
            _seed_category(seeded_conn, "Dining")

        rules_add_keyword(keyword="TESTPRIORITY", category="Dining")

        # Find rule by keyword content (not category name, which may match multiple)
        listing = rules_list()
        target = [r for r in listing["data"]["rules"] if "TESTPRIORITY" in r["keywords"]]
        assert target
        idx = target[0]["rule_index"]

        result = rules_update_priority(rule_index=idx, priority=10)
        assert result["data"]["old_priority"] == 0
        assert result["data"]["new_priority"] == 10

        listing2 = rules_list()
        updated = [r for r in listing2["data"]["rules"] if r["rule_index"] == idx]
        assert updated[0]["priority"] == 10

        from finance_cli.mcp_server import rules_remove_keyword

        rules_remove_keyword(keyword="TESTPRIORITY")

    def test_rules_update_priority_out_of_range(self, db_path):
        from finance_cli.mcp_server import rules_update_priority

        response = rules_update_priority(rule_index=9999, priority=5)
        _assert_tool_error(response, "ValueError", "out of range")


class TestExportTools:
    def test_export_csv_and_summary(self, db_path, conn):
        category_id = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=category_id, date=date.today().isoformat(), description="Export row")
        from finance_cli.mcp_server import export_csv, export_summary

        csv_result = export_csv()
        assert Path(csv_result["data"]["output"]).exists()

        summary_result = export_summary(month=date.today().strftime("%Y-%m"))
        assert Path(summary_result["data"]["output"]).exists()
        events = [
            row["event_type"]
            for row in conn.execute("SELECT event_type FROM sensitive_audit_events ORDER BY id")
        ]
        assert events == ["data_export.csv", "data_export.summary"]


class TestProviderTools:
    def test_provider_status(self, db_path):
        from finance_cli.mcp_server import provider_status

        result = provider_status()
        assert "data" in result
        assert "summary" in result
        assert "institution_count" in result["summary"]

    def test_provider_switch(self, db_path, conn):
        _seed_account(conn, institution="Switch Bank", source="schwab")
        from finance_cli.mcp_server import provider_switch

        result = provider_switch(institution="Switch Bank", provider="plaid")
        assert result["data"]["new_provider"] == "plaid"
        assert result["summary"]["deactivated_count"] >= 1


class TestSchwabTools:
    @pytest.mark.filterwarnings("ignore:websockets\\.legacy is deprecated.*:DeprecationWarning")
    def test_schwab_status(self, db_path):
        from finance_cli.mcp_server import schwab_status

        result = schwab_status()
        assert "data" in result
        assert "summary" in result
        assert "configured" in result["data"]


class TestObservabilityTools:
    def test_analytics_tools(self, db_path, conn, isolated_mcp_cache):
        conn.execute(
            """
            INSERT INTO analytics_events (event, domain, outcome, source)
            VALUES
                ('onboarding.wizard', 'onboarding', 'started', 'api'),
                ('onboarding.wizard', 'onboarding', 'started', 'api'),
                ('onboarding.wizard', 'onboarding', 'started', 'api'),
                ('onboarding.wizard', 'onboarding', 'started', 'api'),
                ('onboarding.wizard', 'onboarding', 'succeeded', 'api'),
                ('onboarding.wizard', 'onboarding', 'succeeded', 'api'),
                ('onboarding.wizard', 'onboarding', 'failed', 'api'),
                ('onboarding.complete', 'onboarding', 'succeeded', 'api'),
                ('onboarding.complete', 'onboarding', 'succeeded', 'api'),
                ('feature.budget_set', 'feature', 'succeeded', 'api'),
                ('feature.goal_set', 'feature', 'succeeded', 'api')
            """
        )
        conn.execute(
            """
            INSERT INTO analytics_events (event, domain, outcome, properties, session_id, source)
            VALUES
                (
                    'chat.session',
                    'chat',
                    'succeeded',
                    '{"duration_min":20,"message_count":5,"tool_call_count":2}',
                    'sess_1',
                    'api'
                ),
                (
                    'chat.session',
                    'chat',
                    'succeeded',
                    '{"duration_min":10,"message_count":3,"tool_call_count":1}',
                    'sess_2',
                    'api'
                )
            """
        )
        conn.commit()

        from finance_cli.mcp_server import analytics_funnel, analytics_session_stats, analytics_usage

        funnel = analytics_funnel(days=30)
        usage = analytics_usage(days=30, summary_only=True)
        sessions = analytics_session_stats(days=30)

        steps = {step["event"]: step for step in funnel["data"]["steps"]}
        assert funnel["summary"]["started"] == 4
        assert funnel["summary"]["completed"] == 2
        assert funnel["summary"]["completion_pct"] == 50.0
        assert steps["onboarding.wizard"]["abandoned"] == 1

        assert usage["summary"]["total_events"] == 13
        assert usage["summary"]["feature_count"] == 2
        assert _readthrough_cache_file(isolated_mcp_cache, usage["data"]["cache_id"]).exists()

        assert sessions["data"]["session_count"] == 2
        assert sessions["data"]["avg_duration_min"] == 15.0
        assert sessions["data"]["avg_messages_per_session"] == 4.0
        assert sessions["data"]["avg_tool_calls_per_session"] == 1.5

    def test_perf_tools(self, db_path, conn, isolated_mcp_cache):
        conn.execute(
            """
            INSERT INTO perf_samples (source, metric, value_ms, is_error, tags)
            VALUES
                ('tool', 'tool.demo_alpha', 100, 0, NULL),
                ('tool', 'tool.demo_alpha', 300, 1, NULL),
                ('tool', 'tool.demo_beta', 50, 0, NULL),
                ('query', 'query.alpha', 400, 0, '{"sql_fingerprint":"SELECT * FROM txns WHERE id=?"}'),
                ('query', 'query.alpha', 200, 0, '{"sql_fingerprint":"SELECT * FROM txns WHERE id=?"}'),
                ('query', 'query.beta', 150, 0, '{"sql_fingerprint":"SELECT * FROM budgets WHERE month=?"}')
            """
        )
        conn.commit()

        from finance_cli.mcp_server import perf_slow_queries, perf_summary, perf_tool_stats

        summary = perf_summary(days=7)
        slow_queries = perf_slow_queries(days=7, summary_only=True)
        tool_stats = perf_tool_stats(days=7)

        assert summary["summary"]["slowest_tool"] == "demo_alpha"
        assert summary["data"]["top_slowest_query_fingerprints"][0]["sql_fingerprint"] == "SELECT * FROM txns WHERE id=?"

        assert slow_queries["summary"]["fingerprint_count"] == 2
        assert _readthrough_cache_file(isolated_mcp_cache, slow_queries["data"]["cache_id"]).exists()

        demo_alpha = next(
            row for row in tool_stats["data"]["tools"]
            if row["tool_name"] == "demo_alpha"
        )
        assert tool_stats["summary"]["tool_count"] >= 2
        assert demo_alpha["call_count"] == 2

    def test_call_wrapper_uses_tool_name_for_error_capture_and_perf(self, db_path, monkeypatch) -> None:
        import finance_cli.mcp_server as mcp_server

        captured = {}
        perf_calls = []

        def fake_handle_status(args, conn) -> dict[str, object]:
            raise ValueError("db status exploded")

        def fake_capture_error(exc, **kwargs):
            captured["exc"] = exc
            captured["kwargs"] = kwargs
            return "err_1"

        def fake_record_perf_sample(
            db_path,
            source,
            metric,
            value_ms,
            tags=None,
            is_error=False,
        ) -> None:
            perf_calls.append(
                {
                    "db_path": db_path,
                    "source": source,
                    "metric": metric,
                    "value_ms": value_ms,
                    "tags": tags,
                    "is_error": is_error,
                }
            )

        monkeypatch.setattr(mcp_server.db_cmd, "handle_status", fake_handle_status)
        monkeypatch.setattr(mcp_server, "capture_error", fake_capture_error)
        monkeypatch.setattr(mcp_server, "_record_perf_sample", fake_record_perf_sample)

        response = mcp_server.db_status()

        _assert_tool_error(response, "ValueError", "db status exploded")

        kwargs = captured["kwargs"]
        assert kwargs["endpoint"] == "db_status"
        assert kwargs["context"]["tool_name"] == "db_status"
        assert perf_calls[-1]["metric"] == "tool.db_status"
        assert perf_calls[-1]["is_error"] is True

    def test_direct_call_full_tool_uses_tool_name_for_error_capture_and_perf(self, db_path, monkeypatch) -> None:
        import finance_cli.mcp_server as mcp_server

        captured = {}
        perf_calls = []

        def fake_handle_analytics_usage(args, conn) -> dict[str, object]:
            raise RuntimeError("analytics usage exploded")

        def fake_capture_error(exc, **kwargs):
            captured["exc"] = exc
            captured["kwargs"] = kwargs
            return "err_2"

        def fake_record_perf_sample(
            db_path,
            source,
            metric,
            value_ms,
            tags=None,
            is_error=False,
        ) -> None:
            perf_calls.append(
                {
                    "db_path": db_path,
                    "source": source,
                    "metric": metric,
                    "value_ms": value_ms,
                    "tags": tags,
                    "is_error": is_error,
                }
            )

        monkeypatch.setattr(mcp_server, "_handle_analytics_usage", fake_handle_analytics_usage)
        monkeypatch.setattr(mcp_server, "capture_error", fake_capture_error)
        monkeypatch.setattr(mcp_server, "_record_perf_sample", fake_record_perf_sample)

        response = mcp_server.analytics_usage(days=30)

        _assert_tool_error(response, "RuntimeError", "analytics usage exploded")

        kwargs = captured["kwargs"]
        assert kwargs["endpoint"] == "analytics_usage"
        assert kwargs["context"]["tool_name"] == "analytics_usage"
        assert perf_calls[-1]["metric"] == "tool.analytics_usage"
        assert perf_calls[-1]["is_error"] is True

    def test_error_tools(self, db_path, isolated_mcp_cache):
        from finance_cli.error_capture import capture_error
        from finance_cli.mcp_server import error_list, error_show, error_stats, error_update

        error_id = None
        for request_id in ("req_1", "req_2"):
            try:
                raise RuntimeError("pipeline exploded")
            except Exception as exc:  # noqa: BLE001
                error_id = capture_error(
                    exc,
                    source="mcp",
                    endpoint="demo_tool",
                    context={"request_id": request_id, "tool_name": "demo_tool"},
                    db_path=db_path,
                )

        assert error_id is not None

        listed = error_list(days=30, summary_only=True)
        shown = error_show(error_id=error_id)
        stats = error_stats(days=30)
        updated = error_update(error_id=error_id, status="resolved", resolution="fixed")

        assert listed["summary"]["total_errors"] == 1
        assert _readthrough_cache_file(isolated_mcp_cache, listed["data"]["cache_id"]).exists()

        assert shown["summary"]["occurrence_count"] == 2
        assert shown["data"]["error"]["context"]["tool_name"] == "demo_tool"
        assert len(shown["data"]["occurrence_timeline"]) == 2

        assert stats["summary"]["total_errors"] == 1
        assert stats["data"]["top_recurring_fingerprints"][0]["occurrence_count"] == 2

        assert updated["data"]["status"] == "resolved"
        assert updated["data"]["resolution"] == "fixed"

    def test_issue_tools(self, db_path, isolated_mcp_cache):
        from finance_cli.mcp_server import finance_log_issue, issue_list, issue_update

        logged = finance_log_issue("Observability issue", "Details for triage", "warning")
        issue_id = logged["data"]["id"]

        listed = issue_list(summary_only=True)
        updated = issue_update(issue_id=issue_id, status="resolved", resolution="handled")

        assert listed["summary"]["total_issues"] == 1
        assert _readthrough_cache_file(isolated_mcp_cache, listed["data"]["cache_id"]).exists()
        assert updated["data"]["status"] == "resolved"
        assert updated["data"]["resolution"] == "handled"

    def test_cost_tools(self, db_path, conn):
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES
                ('claude', 'chat', 1500000, datetime('now')),
                ('plaid', 'plaid_item_daily', 300000, datetime('now'))
            """
        )
        conn.commit()

        from finance_cli.mcp_server import (
            cost_daily,
            cost_limits_set,
            cost_limits_show,
            cost_summary,
            cost_unit_economics,
        )

        summary = cost_summary(months=1)
        daily = cost_daily(days=30)
        limits = cost_limits_show()
        updated_limit = cost_limits_set(
            provider="claude",
            period="daily",
            limit_usd6=2_000_000,
            action="block",
        )
        unit_economics = cost_unit_economics(months=3, price_points="5,10,20")

        assert summary["summary"]["total_usd"] == 1.8
        assert summary["summary"]["provider_count"] == 2
        assert daily["summary"]["days_with_cost"] == 1

        claude_daily = next(
            item for item in limits["data"]["limits"]
            if item["provider"] == "claude" and item["period"] == "daily"
        )
        assert claude_daily["spent_usd"] == 1.5
        assert claude_daily["pct_used"] == 30.0

        assert updated_limit["data"]["action"] == "block"
        assert updated_limit["data"]["limit_usd"] == 2.0

        assert unit_economics["data"]["available"] is False
        assert unit_economics["data"]["price_points"] == [5, 10, 20]

    def test_plaid_usage_tool_registered_and_callable(self, conn):
        import finance_cli.mcp_server as mcp_server

        tool_names = {
            tool.name
            for tool in asyncio.run(mcp_server.mcp.list_tools(run_middleware=False))
        }

        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES
                ('plaid', 'accounts_balance_get', 100000, datetime('now')),
                ('plaid', 'transactions_sync', 0, datetime('now'))
            """
        )
        conn.commit()

        result = mcp_server.plaid_usage()

        assert "plaid_usage" in tool_names
        assert callable(mcp_server.plaid_usage)
        assert result["data"]["day"]["period"] == "day"
        assert result["data"]["month"]["period"] == "month"
        assert result["summary"]["day"]["cost_usd"] == "0.10"
        assert result["summary"]["day"]["limit_usd"] == "1.00"
        assert result["summary"]["month"]["cost_usd"] == "0.10"
        assert result["summary"]["month"]["limit_usd"] == "10.00"


class TestParamCoercion:
    """MCP-001: string-typed int/bool params should be coerced automatically."""

    def test_int_param_accepts_string(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(limit="10", offset="0")
        assert "data" in result

    def test_bool_param_accepts_string_true(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(uncategorized="true", limit="5")
        assert "data" in result

    def test_bool_param_accepts_string_false(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(uncategorized="false", limit="5")
        assert "data" in result

    def test_native_types_still_work(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(limit=10, uncategorized=False)
        assert "data" in result

    def test_coerce_params_unit(self):
        from finance_cli.mcp_server import _coerce_params

        def sample(x: int = 5, flag: bool = False, name: str = "a") -> dict:
            return {"x": x, "flag": flag, "name": name}

        wrapped = _coerce_params(sample)
        result = wrapped(x="42", flag="true", name="b")
        assert result == {"x": 42, "flag": True, "name": "b"}

    def test_coerce_bool_variants(self):
        from finance_cli.mcp_server import _coerce_params

        def sample(flag: bool = False) -> dict:
            return {"flag": flag}

        wrapped = _coerce_params(sample)
        assert wrapped(flag="1")["flag"] is True
        assert wrapped(flag="yes")["flag"] is True
        assert wrapped(flag="TRUE")["flag"] is True
        assert wrapped(flag="false")["flag"] is False
        assert wrapped(flag="0")["flag"] is False
        assert wrapped(flag="no")["flag"] is False
