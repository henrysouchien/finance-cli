from __future__ import annotations

import asyncio
import io
import json
import logging
import sqlite3
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import finance_cli.intervention_engine as intervention_engine
from finance_cli.db import connect
from finance_cli.gateway import server as gateway_server
from finance_cli.gateway.config import GatewaySettings
from finance_cli.tests.test_intervention_engine import (
    NOW,
    _seed_account,
    _seed_credit_liability,
    _seed_transaction,
)
from finance_cli.user_provisioning import provision_user, user_db_path


class FakeMcpClientManager:
    def get_tool_definitions(self) -> list[dict[str, Any]]:
        return [{"name": "goal_list", "description": "List goals"}]


class FakeChatRuntime:
    def __init__(self, **kwargs) -> None:
        self.system_prompt = kwargs["system_prompt"]
        self.excluded_tools = kwargs["excluded_tools"]


@pytest.fixture()
def settings(tmp_path: Path) -> GatewaySettings:
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")
    return GatewaySettings(
        **{
            "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-shared-token",
            "GATEWAY_USER_KEYS": json.dumps(
                [
                    {
                        "key": "gateway-key",
                        "channel": "web",
                        "user_id": 1,
                        "email": "user1@example.test",
                        "role": "owner",
                    }
                ]
            ),
            "FINANCE_GATEWAY_JWT_SECRET": "jwt-secret",
            "FINANCE_GATEWAY_HOST": "127.0.0.1",
            "FINANCE_GATEWAY_PORT": 8002,
            "FINANCE_GATEWAY_DATA_ROOT": tmp_path / "users",
            "FINANCE_GATEWAY_RULES_TEMPLATE": template_rules,
            "FINANCE_GATEWAY_CODE_EXECUTION": False,
        }
    )


@pytest.fixture(autouse=True)
def fake_chat_runtime(monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)


@pytest.fixture(autouse=True)
def fixed_intervention_clock(monkeypatch) -> None:
    original = intervention_engine.evaluate_for_surface

    def evaluate_at_now(conn, surface, *, rules_path=None, log_to_surface=None, now=None):
        return original(
            conn,
            surface,
            rules_path=rules_path,
            log_to_surface=log_to_surface,
            now=NOW,
        )

    monkeypatch.setattr(intervention_engine, "evaluate_for_surface", evaluate_at_now)


def _render(system_prompt) -> str:
    if isinstance(system_prompt, list):
        return "".join(text for text, _ in system_prompt)
    return str(system_prompt)


def _session() -> SimpleNamespace:
    return SimpleNamespace(
        approved_tool_types=set(),
        session_id="sess-test",
        result_queue=asyncio.Queue(),
        auth_config=None,
    )


def _request(user_id: str | None = "alice", *, skill: str | None = None) -> SimpleNamespace:
    context: dict[str, object] = {}
    if user_id is not None:
        context["user_id"] = user_id
    if skill is not None:
        context["skill"] = skill
    return SimpleNamespace(model=None, context=context, user_id=user_id)


def _build_runtime(settings: GatewaySettings, request, channel: str | None = "web") -> FakeChatRuntime:
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())
    return asyncio.run(build_runtime(_session(), request, channel, SimpleNamespace()))


def _capture_gateway_log(monkeypatch) -> io.StringIO:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.ERROR)
    monkeypatch.setattr(gateway_server.log, "handlers", [handler])
    monkeypatch.setattr(gateway_server.log, "propagate", False)
    monkeypatch.setattr(gateway_server.log, "level", logging.ERROR)
    return stream


def _seed_d1_only(db_path: Path) -> None:
    with connect(db_path) as conn:
        high = _seed_account(conn, account_type="credit_card", balance_cents=-9_000_000, institution_name="High")
        mid = _seed_account(conn, account_type="credit_card", balance_cents=-300_000, institution_name="Mid")
        low = _seed_account(conn, account_type="credit_card", balance_cents=-50_000, institution_name="Low")
        _seed_credit_liability(conn, account_id=high, apr_purchase=29.99, minimum_payment_cents=90_000)
        _seed_credit_liability(conn, account_id=mid, apr_purchase=19.99, minimum_payment_cents=5_000)
        _seed_credit_liability(conn, account_id=low, apr_purchase=9.99, minimum_payment_cents=2_000)


def _seed_schedule_c_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, 0, 0, 0)
        """,
        (category_id, name),
    )
    conn.execute(
        """
        INSERT INTO schedule_c_map (
            id, category_id, schedule_c_line, line_number, deduction_pct, tax_year
        ) VALUES (?, ?, 'Other expenses', '27a', 1.0, 2026)
        """,
        (uuid.uuid4().hex, category_id),
    )
    conn.commit()
    return category_id


def _seed_multi_pattern(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking = _seed_account(conn, account_type="checking", balance_cents=20_000, institution_name="Bank")
        high = _seed_account(conn, account_type="credit_card", balance_cents=-9_000_000, institution_name="High")
        mid = _seed_account(conn, account_type="credit_card", balance_cents=-300_000, institution_name="Mid")
        low = _seed_account(conn, account_type="credit_card", balance_cents=-50_000, institution_name="Low")
        _seed_credit_liability(conn, account_id=high, apr_purchase=29.99, minimum_payment_cents=90_000)
        _seed_credit_liability(conn, account_id=mid, apr_purchase=19.99, minimum_payment_cents=5_000)
        _seed_credit_liability(conn, account_id=low, apr_purchase=9.99, minimum_payment_cents=2_000)

        for month in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking, category_name="Income: Salary", amount_cents=120_000, txn_date=month)
            _seed_transaction(conn, account_id=checking, category_name="Rent", amount_cents=-100_000, txn_date=month)
        _seed_transaction(conn, account_id=checking, category_name="Dining", amount_cents=-30_000, txn_date="2026-04-05")

        schedule_c_category = _seed_schedule_c_category(conn, "Software")
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, category_id, use_type,
                is_payment, is_active, is_reviewed, source
            ) VALUES (?, ?, '2026-03-10', 'software', -184000, ?, 'Personal', 0, 1, 1, 'manual')
            """,
            (uuid.uuid4().hex, checking, schedule_c_category),
        )
        conn.commit()


def _provision_and_seed(settings: GatewaySettings, user_id: str, seeder) -> Path:
    provision_user(
        data_root=settings.data_root,
        user_id=user_id,
        template_rules_path=settings.template_rules_path,
    )
    db_path = user_db_path(settings.data_root, user_id)
    seeder(db_path)
    return db_path


def test_gateway_injects_ranked_interventions_and_logs_fires(settings: GatewaySettings) -> None:
    db_path = _provision_and_seed(settings, "alice", _seed_d1_only)

    runtime = _build_runtime(settings, _request("alice"), "web")

    rendered = _render(runtime.system_prompt)
    with connect(db_path) as conn:
        rows = conn.execute("SELECT pattern_id, surface FROM intervention_log").fetchall()
    assert "<interventions>" in rendered
    assert "[D-1]" in rendered
    assert "[D-6]" in rendered
    assert len(rows) == 2
    assert [row["pattern_id"] for row in rows] == ["D-1", "D-6"]
    assert {row["surface"] for row in rows} == {"agent_prompt"}


def test_gateway_ranks_and_caps_agent_prompt_interventions(settings: GatewaySettings) -> None:
    db_path = _provision_and_seed(settings, "alice", _seed_multi_pattern)

    runtime = _build_runtime(settings, _request("alice"), "web")

    rendered = _render(runtime.system_prompt)
    d1_index = rendered.index("[D-1]")
    c1_index = rendered.index("[C-1]")
    d6_index = rendered.index("[D-6]")
    assert d1_index < c1_index < d6_index
    assert "[T-2]" not in rendered
    assert "[C-5]" not in rendered
    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]
    assert int(count) == 3


def test_gateway_dedups_second_request_within_one_hour(settings: GatewaySettings) -> None:
    db_path = _provision_and_seed(settings, "alice", _seed_d1_only)

    _build_runtime(settings, _request("alice"), "web")
    _build_runtime(settings, _request("alice"), "web")

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]
    assert int(count) == 2


def test_gateway_gracefully_skips_interventions_on_engine_error(
    settings: GatewaySettings,
    monkeypatch,
) -> None:
    _provision_and_seed(settings, "alice", _seed_d1_only)

    def fail(*args, **kwargs):
        raise RuntimeError("engine down")

    monkeypatch.setattr(intervention_engine, "evaluate_for_surface", fail)
    log_stream = _capture_gateway_log(monkeypatch)

    runtime = _build_runtime(settings, _request("alice"), "web")

    assert "<interventions>" not in _render(runtime.system_prompt)
    log_text = log_stream.getvalue()
    assert "intervention injection failed for user alice" in log_text
    assert "Traceback" in log_text


def test_gateway_skips_interventions_during_onboarding(settings: GatewaySettings) -> None:
    db_path = _provision_and_seed(settings, "alice", _seed_d1_only)
    with connect(db_path) as conn:
        before = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]

    runtime = _build_runtime(settings, _request("alice", skill="onboarding"), "web")

    with connect(db_path) as conn:
        after = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]
    assert "<interventions>" not in _render(runtime.system_prompt)
    assert int(after) == int(before)


def test_gateway_skips_interventions_for_non_user_scoped_channel(
    settings: GatewaySettings,
    monkeypatch,
) -> None:
    db_path = _provision_and_seed(settings, "alice", _seed_d1_only)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("evaluate_for_surface should not run")

    monkeypatch.setattr(intervention_engine, "evaluate_for_surface", fail_if_called)
    runtime = _build_runtime(settings, _request(None), "local")

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]
    assert "<interventions>" not in _render(runtime.system_prompt)
    assert int(count) == 0


def test_gateway_skips_interventions_when_db_connect_fails(
    settings: GatewaySettings,
    monkeypatch,
) -> None:
    _provision_and_seed(settings, "alice", _seed_d1_only)
    monkeypatch.setattr(gateway_server, "provision_user", lambda **kwargs: {})

    from finance_cli import db as finance_db

    def fail_connect(*args, **kwargs):
        raise sqlite3.OperationalError("cannot open database")

    monkeypatch.setattr(finance_db, "connect", fail_connect)
    log_stream = _capture_gateway_log(monkeypatch)

    runtime = _build_runtime(settings, _request("alice"), "web")

    assert "<interventions>" not in _render(runtime.system_prompt)
    assert "intervention injection failed for user alice" in log_stream.getvalue()
