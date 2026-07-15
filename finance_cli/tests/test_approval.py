from __future__ import annotations

import ast
import asyncio
import json
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest

import finance_cli.telegram_bot.approval as approval_mod
from finance_cli.gateway.tools import (
    APPROVAL_REQUIRED_TOOLS,
    EXCLUDED_TOOLS,
    READ_ONLY_TOOLS,
)
from finance_cli.telegram_bot.approval import (
    _APPROVAL_REQUIRED_TOOLS,
    _READ_ONLY_TOOLS,
    build_approval_keyboard,
    format_approval_message,
    needs_approval,
    parse_callback_data,
)
from finance_cli.telegram_bot.bot import APPROVAL_EXPIRED_TEXT, TelegramBot, _friendly_tool_name
from finance_cli.telegram_bot.config import BotConfig


def _make_config(**kwargs: Any) -> BotConfig:
    base = dict(
        TELEGRAM_BOT_TOKEN="bot-token",
        TELEGRAM_CHAT_ID="12345",
        GATEWAY_USER_KEY="gateway-key",
    )
    base.update(kwargs)
    return BotConfig(**base)


class FakeAPI:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, Any]] = []
        self.keyboard_messages: list[dict[str, Any]] = []
        self.callback_answers: list[dict[str, Any]] = []
        self.edits: list[dict[str, Any]] = []
        self._next_message_id = 100

    async def get_updates(self, offset: int | None = None, timeout: int | None = None) -> list[dict[str, Any]]:
        del offset, timeout
        return []

    async def send_message(
        self,
        chat_id: str | int,
        text: str,
        *,
        parse_mode: str | None = None,
    ) -> dict[str, Any]:
        message = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.sent_messages.append(message)
        return {"message_id": message["message_id"]}

    async def send_message_with_keyboard(
        self,
        chat_id: str | int,
        text: str,
        inline_keyboard: list[list[dict[str, str]]],
    ) -> dict[str, Any]:
        message = {
            "chat_id": chat_id,
            "text": text,
            "inline_keyboard": inline_keyboard,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.keyboard_messages.append(message)
        return {"message_id": message["message_id"]}

    async def send_chat_action(self, chat_id: str | int, action: str) -> dict[str, Any]:
        del chat_id, action
        return {"ok": True}

    async def answer_callback_query(self, callback_query_id: str, text: str = "") -> dict[str, Any]:
        self.callback_answers.append({"callback_query_id": callback_query_id, "text": text})
        return {"ok": True}

    async def edit_message_text(
        self,
        chat_id: str | int,
        message_id: int,
        text: str,
        *,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
    ) -> dict[str, Any]:
        self.edits.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "reply_markup": reply_markup,
                "parse_mode": parse_mode,
            }
        )
        return {"message_id": message_id}


class FakeGatewayClient:
    def __init__(
        self,
        *,
        responses: list[int | tuple[int, dict[str, Any]] | Exception] | None = None,
    ) -> None:
        self.responses = list(responses or [200])
        self.submit_calls: list[tuple[str, str, bool]] = []
        self.invalidate_calls = 0

    async def ensure_session(self, *, force_refresh: bool = False):
        del force_refresh
        return None

    async def close(self) -> None:
        return None

    def invalidate_session(self) -> None:
        self.invalidate_calls += 1

    async def submit_approval(
        self,
        tool_call_id: str,
        nonce: str,
        approved: bool,
    ) -> tuple[int, dict[str, Any]]:
        self.submit_calls.append((tool_call_id, nonce, approved))
        outcome = self.responses.pop(0) if self.responses else 200
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, tuple):
            status_code, body = outcome
            return status_code, body
        return outcome, {"status": "ok"}


class BlockingApprovalGatewayClient(FakeGatewayClient):
    def __init__(
        self,
        *,
        final_response: int | tuple[int, dict[str, Any]] | Exception = 200,
    ) -> None:
        super().__init__(responses=[])
        self.final_response = final_response
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def submit_approval(
        self,
        tool_call_id: str,
        nonce: str,
        approved: bool,
    ) -> tuple[int, dict[str, Any]]:
        self.submit_calls.append((tool_call_id, nonce, approved))
        self.started.set()
        await self.release.wait()
        outcome = self.final_response
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, tuple):
            status_code, body = outcome
            return status_code, body
        return outcome, {"status": "ok"}


def _callback(nonce: str, action: str, *, chat_id: int = 12345, message_id: int = 50) -> dict[str, Any]:
    return {
        "id": "cq-1",
        "data": json.dumps({"n": nonce, "a": action}, separators=(",", ":")),
        "message": {
            "chat": {"id": chat_id},
            "message_id": message_id,
            "text": "Prompt",
        },
    }


def _approval_event(
    nonce: str,
    *,
    tool_call_id: str | None = None,
    tool_name: str = "txn_categorize",
) -> dict[str, Any]:
    return {
        "type": "tool_approval_request",
        "nonce": nonce,
        "tool_call_id": tool_call_id or f"tool-{nonce}",
        "tool_name": tool_name,
        "tool_input": {"category": "Dining"},
    }


async def _drain_loop(turns: int = 3) -> None:
    for _ in range(turns):
        await asyncio.sleep(0)


async def _cancel_task(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


def test_needs_approval_read_tools() -> None:
    assert needs_approval("db_status") is False
    assert needs_approval("db_backup_list") is False
    assert needs_approval("db_backup_verify") is False
    assert needs_approval("txn_list") is False
    assert needs_approval("agent_memory_read") is False
    assert needs_approval("agent_session_search") is False
    assert needs_approval("agent_session_read") is False


def test_needs_approval_write_tools() -> None:
    assert needs_approval("db_backup") is True
    assert needs_approval("db_backup_prune") is True
    assert needs_approval("db_export_preferences") is True
    assert needs_approval("db_import_preferences") is True
    assert needs_approval("dedup_cross_format") is True
    assert needs_approval("plaid_sync") is True
    assert needs_approval("txn_categorize") is True
    assert needs_approval("agent_session_write") is True


def test_loan_read_tools_are_classified_read_only() -> None:
    expected = {"loan_list", "loan_show", "loan_schedule"}
    assert expected.issubset(READ_ONLY_TOOLS)
    assert all(needs_approval(tool_name) is False for tool_name in expected)


def test_loan_write_tools_are_classified_for_approval() -> None:
    expected = {"loan_add", "loan_payment", "loan_disburse", "loan_adjust", "loan_close"}
    assert expected.issubset(APPROVAL_REQUIRED_TOOLS)
    assert all(needs_approval(tool_name) is True for tool_name in expected)


def test_intervention_write_tools_are_classified_for_approval() -> None:
    expected = {
        "interventions_act",
        "interventions_dismiss",
        "interventions_mute",
        "interventions_unmute",
    }
    assert expected.issubset(APPROVAL_REQUIRED_TOOLS)
    assert all(needs_approval(tool_name) is True for tool_name in expected)


def test_needs_approval_unknown_tools() -> None:
    assert needs_approval("unknown_tool_name") is True


def test_needs_approval_dedup_read_only() -> None:
    assert needs_approval("dedup_suggest_aliases") is False
    assert needs_approval("dedup_audit_names") is False


def test_parse_callback_data_valid() -> None:
    assert parse_callback_data('{"n":"abc","a":"y"}') == ("abc", "y")


@pytest.mark.parametrize(
    "payload",
    [
        "",
        "not-json",
        '{"n":"abc"}',
        '{"a":"y"}',
        '{"n":1,"a":"y"}',
        '{"n":"abc","a":"maybe"}',
        "[]",
    ],
)
def test_parse_callback_data_invalid(payload: str) -> None:
    assert parse_callback_data(payload) is None


def test_format_approval_message_uses_event_dict() -> None:
    message = format_approval_message(
        {
            "tool_name": "dedup_cross_format",
            "tool_input": {"dry_run": False, "account_id": "abc123"},
        }
    )

    assert "Tool: dedup_cross_format" in message
    assert "dry_run: false" in message.lower()
    assert "account_id: abc123" in message


def test_format_approval_message_truncates_long_values() -> None:
    message = format_approval_message(
        {
            "tool_name": "txn_categorize",
            "tool_input": {"note": "x" * 120},
        }
    )

    assert "x" * 80 not in message
    assert "..." in message


def test_summary_budget_set() -> None:
    assert approval_mod._summarize_tool(
        "budget_set",
        {"category": "Dining", "amount": 400, "period": "monthly"},
    ) == "Set budget: Dining → $400/mo"


def test_summary_budget_reallocate() -> None:
    assert approval_mod._summarize_tool(
        "budget_reallocate",
        {"from_category": "Travel", "to_category": "Dining", "amount": 100},
    ) == "Move budget: $100 Travel → Dining"


def test_summary_subs_cancel() -> None:
    assert approval_mod._summarize_tool("subs_cancel", {"id": "sub_123"}) == "Cancel subscription: sub_123"


def test_summary_subs_update() -> None:
    assert (
        approval_mod._summarize_tool(
            "subs_update",
            {"id": "sub_1234567890", "amount": 12.49, "use_type": "Business"},
        )
        == "Update subscription: sub_12345678 (amount, use_type)"
    )


def test_summary_remove_preview_tools() -> None:
    assert (
        approval_mod._summarize_tool(
            "notify_channel_remove",
            {"channel": "telegram", "dry_run": True},
        )
        == "Remove telegram notification config (preview)"
    )
    assert (
        approval_mod._summarize_tool(
            "rules_remove_keyword",
            {"keyword": "COFFEE", "dry_run": True},
        )
        == 'Remove rule: "COFFEE" (preview)'
    )


def test_summary_account_set_business_uses_id() -> None:
    assert (
        approval_mod._summarize_tool(
            "account_set_business",
            {"id": "acct_123", "is_business": True, "backfill": True},
        )
        == "Mark acct_123 as business and update past transactions"
    )


def test_summary_db_backup_prune() -> None:
    assert approval_mod._summarize_tool("db_backup_prune", {"dry_run": True}) == "Prune old backups (preview)"
    assert approval_mod._summarize_tool("db_backup_prune", {"dry_run": False}) == "Prune old backups (apply)"


def test_summary_db_export_preferences() -> None:
    assert approval_mod._summarize_tool("db_export_preferences", {}) == "Export preferences bundle"


def test_summary_db_import_preferences() -> None:
    assert (
        approval_mod._summarize_tool(
            "db_import_preferences",
            {"mode": "merge", "dry_run": True},
        )
        == "Import preferences (merge, preview)"
    )
    assert (
        approval_mod._summarize_tool(
            "db_import_preferences",
            {"mode": "overwrite", "dry_run": False},
        )
        == "Import preferences (overwrite, apply)"
    )


def test_summary_debt_set_apr() -> None:
    assert (
        approval_mod._summarize_tool(
            "debt_set_apr",
            {"account_id": "acct_1234567890", "apr_pct": 24.5, "dry_run": True},
        )
        == "Set debt APR: acct_1234567 to 24.5% (preview)"
    )


def test_summary_debt_balance_portion_tools() -> None:
    assert (
        approval_mod._summarize_tool(
            "debt_balance_portion_add",
            {
                "account_id": "acct_1234567890",
                "label": "Amex Plan It",
                "principal_dollars": 34875,
                "apr_pct": 10.0,
                "monthly_payment_dollars": 896,
                "dry_run": True,
            },
        )
        == "Add portion: acct_123 Amex Pla… $34,875@10% $896/mo preview"
    )
    assert (
        approval_mod._summarize_tool(
            "debt_balance_portion_update",
            {
                "portion_id": "portion_1234567890",
                "apr_pct": 9.5,
                "clear_monthly_payment": True,
            },
        )
        == "Update portion: portion_1234 (apr, clear_payment) apply"
    )
    assert (
        approval_mod._summarize_tool(
            "debt_balance_portion_deactivate",
            {"portion_id": "portion_1234567890", "dry_run": True},
        )
        == "Deactivate portion: portion_1234 preview"
    )


@pytest.mark.parametrize(
    ("tool_name", "payload", "expected"),
    [
        ("statement_normalizer_stage", {"key": "demo_bank"}, "Stage normalizer: demo_bank"),
        ("statement_normalizer_activate", {"key": "demo_bank"}, "Activate normalizer: demo_bank"),
        ("normalizer_update", {"key": "demo_bank"}, "Update normalizer: demo_bank"),
        (
            "normalizer_register_institution",
            {"canonical_name": "Demo Bank"},
            "Register institution: Demo Bank",
        ),
    ],
)
def test_summary_normalizer_write_tools(tool_name: str, payload: dict[str, Any], expected: str) -> None:
    assert approval_mod._summarize_tool(tool_name, payload) == expected


@pytest.mark.parametrize(
    ("tool_name", "payload", "expected"),
    [
        ("skill_state_set", {"name": "onboarding"}, "Set skill state: onboarding"),
        ("skill_state_clear", {"name": "onboarding"}, "Clear skill state: onboarding"),
    ],
)
def test_summary_skill_state_tools(tool_name: str, payload: dict[str, Any], expected: str) -> None:
    assert approval_mod._summarize_tool(tool_name, payload) == expected


def test_summary_txn_categorize_remember() -> None:
    assert approval_mod._summarize_tool(
        "txn_categorize",
        {"category": "Dining", "remember": "true"},
    ) == "Categorize → Dining (+ remember)"


@pytest.mark.parametrize(
    ("formatter_name", "payload", "expected"),
    [
        ("_fmt_loan_add", {"creditor": "Mom", "amount": 5000}, "Add loan: Mom $5,000"),
        ("_fmt_loan_payment", {"amount": 250}, "Record loan payment: $250"),
        ("_fmt_loan_disburse", {"amount": 125.50}, "Record loan disbursement: $125.50"),
        ("_fmt_loan_adjust", {"loan_id": "1234567890abcdef"}, "Adjust loan terms: 1234567890ab"),
        ("_fmt_loan_close", {"loan_id": "1234567890abcdef", "forgiven": True}, "Close loan: 1234567890ab (forgiven)"),
    ],
)
def test_loan_write_formatters(formatter_name: str, payload: dict[str, Any], expected: str) -> None:
    formatter = getattr(approval_mod, formatter_name)
    assert formatter(payload) == expected


def test_summary_dedup_dry_run_vs_commit() -> None:
    assert approval_mod._summarize_tool("dedup_cross_format", {"dry_run": True}) == "Remove duplicate transactions (preview)"
    assert approval_mod._summarize_tool("dedup_cross_format", {"dry_run": "false"}) == "Remove duplicate transactions (apply)"
    assert approval_mod._summarize_tool(
        "dedup_cross_format",
        {"dry_run": False, "include_key_only": True},
    ) == "Remove duplicate transactions (apply + key-only)"


def test_summary_dedup_same_source_apply_counts_ids() -> None:
    assert (
        approval_mod._summarize_tool(
            "dedup_same_source_apply",
            {"ids": "txn_a, txn_b,, txn_c"},
        )
        == "Remove 3 duplicate transaction(s)"
    )


def test_summary_cat_auto_with_ai() -> None:
    assert approval_mod._summarize_tool(
        "cat_auto_categorize",
        {"dry_run": True, "ai": "true"},
    ) == "Categorize transactions automatically (preview + AI)"


@pytest.mark.parametrize(
    ("tool_name", "expected"),
    [
        ("coach_homebuying_readiness_artifact_save", "Save homebuying readiness plan"),
        (
            "coach_retirement_contribution_readiness_artifact_save",
            "Save retirement contribution readiness plan",
        ),
        (
            "coach_retirement_income_readiness_artifact_save",
            "Save retirement income readiness plan",
        ),
        (
            "coach_investment_readiness_artifact_save",
            "Save investment readiness plan",
        ),
        (
            "coach_estate_document_readiness_artifact_save",
            "Save estate document readiness checklist",
        ),
        (
            "coach_financial_plan_intake_artifact_save",
            "Save financial planning snapshot",
        ),
        (
            "coach_risk_insurance_readiness_artifact_save",
            "Save risk insurance readiness plan",
        ),
        (
            "coach_advisor_handoff_readiness_artifact_save",
            "Save advisor handoff readiness packet",
        ),
    ],
)
def test_summary_phase2_coaching_artifact_save_tools(
    tool_name: str,
    expected: str,
) -> None:
    assert approval_mod._summarize_tool(tool_name, {}) == expected
    assert approval_mod._summarize_tool(tool_name, {"dry_run": True}) == (
        f"{expected} (preview)"
    )


def test_summary_unknown_tool() -> None:
    assert approval_mod._summarize_tool("mystery_tool", {}) == "Mystery tool"


def test_summary_formatter_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(approval_mod._TOOL_SUMMARIES, "broken_tool", lambda _: 1 / 0)
    assert approval_mod._summarize_tool("broken_tool", {}) == "Broken tool"


def test_all_approval_tools_covered() -> None:
    assert set(approval_mod._TOOL_SUMMARIES) == APPROVAL_REQUIRED_TOOLS


def test_format_approval_message_uses_summary() -> None:
    message = format_approval_message(
        {
            "tool_name": "budget_set",
            "tool_input": {"category": "Dining", "amount": 400, "period": "monthly"},
        }
    )

    assert message.startswith("🔒 Set budget: Dining → $400/mo")
    assert "Tool: budget_set" in message
    assert "category: Dining" in message


def test_approval_reason_displayed() -> None:
    message = format_approval_message(
        {
            "tool_name": "budget_set",
            "tool_input": {
                "category": "Dining",
                "amount": 400,
                "period": "monthly",
                "_approval_reason": "Reducing dining budget as you requested",
            },
        }
    )
    assert "Reducing dining budget as you requested" in message
    assert "_approval_reason" not in message.split("Tool:")[1]  # not in params


def test_approval_reason_missing() -> None:
    message = format_approval_message(
        {"tool_name": "plaid_sync", "tool_input": {"days": 30}}
    )
    # No reason line — just header, blank, tool
    lines = message.split("\n")
    assert lines[0].startswith("🔒")
    assert lines[1] == ""
    assert lines[2].startswith("Tool:")


def test_approval_reason_sanitized() -> None:
    message = format_approval_message(
        {
            "tool_name": "plaid_sync",
            "tool_input": {
                "days": 30,
                "_approval_reason": "line1\nline2\u2028line3",
            },
        }
    )
    # Reason should be single line
    for line in message.split("\n"):
        if line.startswith('"'):
            assert "\n" not in line
            assert "line1 line2 line3" in line


def test_approval_reason_truncated() -> None:
    long_reason = "x" * 200
    message = format_approval_message(
        {
            "tool_name": "plaid_sync",
            "tool_input": {"days": 30, "_approval_reason": long_reason},
        }
    )
    for line in message.split("\n"):
        if line.startswith('"'):
            assert len(line) <= 122  # quotes + 120 chars max
            assert line.endswith('…"')


def test_fmt_amount_zero() -> None:
    assert approval_mod._fmt_amount(0) == "$0"


def test_fmt_amount_negative() -> None:
    assert approval_mod._fmt_amount(-123.45) == "-$123.45"


def test_fmt_amount_large() -> None:
    assert approval_mod._fmt_amount(1234567) == "$1,234,567"


def test_fmt_amount_fractional() -> None:
    assert approval_mod._fmt_amount(9.99) == "$9.99"


def test_fmt_amount_non_numeric() -> None:
    assert approval_mod._fmt_amount("abc") == "abc"



def test_txn_bulk_categorize_uses_query() -> None:
    summary = approval_mod._summarize_tool(
        "txn_bulk_categorize",
        {"category": "Dining", "query": "starbucks"},
    )
    assert "starbucks" in summary


def test_txn_bulk_categorize_date_range() -> None:
    summary = approval_mod._summarize_tool(
        "txn_bulk_categorize",
        {
            "category": "Dining",
            "date_from": "2026-01-01",
            "date_to": "2026-01-31",
        },
    )
    assert "2026-01-01 to 2026-01-31" in summary


def test_txn_bulk_categorize_ids() -> None:
    summary = approval_mod._summarize_tool(
        "txn_bulk_categorize",
        {"category": "Dining", "ids": ["txn-1", "txn-2"]},
    )
    assert "2 selected txn(s)" in summary


@pytest.mark.parametrize(
    ("tool_name", "payload", "expected"),
    [
        ("txn_bulk_tag", {"items": [{"id": "t1"}, {"id": "t2"}], "dry_run": True}, "Tag 2 transaction(s) (preview)"),
        ("rules_add_keywords", {"items": [{"keyword": "A"}, {"keyword": "B"}]}, "Add 2 keyword rule(s)"),
        ("cat_memory_add_bulk", {"rules": [{"pattern": "A"}, {"pattern": "B"}], "dry_run": False}, "Save 2 vendor memory rule(s) (apply)"),
        ("cat_review_new_merchants", {"items": [{"pattern": "A"}], "dry_run": True}, "Review 1 new merchant(s) (preview)"),
        ("cat_memory_disable_bulk", {"ids": ["m1", "m2"], "dry_run": False}, "Disable 2 vendor memory rule(s) (apply)"),
        ("cat_memory_delete_bulk", {"ids": ["m1"], "dry_run": True}, "Delete 1 vendor memory rule(s) (preview)"),
    ],
)
def test_bulk_write_summaries(tool_name: str, payload: dict[str, Any], expected: str) -> None:
    assert approval_mod._summarize_tool(tool_name, payload) == expected


def test_txn_edit_uses_notes() -> None:
    assert "notes" in approval_mod._summarize_tool("txn_edit", {"id": "abc", "notes": "test"})


def test_txn_deactivate_summary_shows_id_and_mode() -> None:
    assert (
        approval_mod._summarize_tool("txn_deactivate", {"id": "abc1234567890"})
        == "Deactivate transaction: abc123456789 (apply)"
    )
    assert (
        approval_mod._summarize_tool(
            "txn_deactivate",
            {"id": "abc1234567890", "dry_run": True},
        )
        == "Deactivate transaction: abc123456789 (preview)"
    )


def test_dedup_create_alias_shows_ids() -> None:
    summary = approval_mod._summarize_tool(
        "dedup_create_alias",
        {"from_id": "aaa", "to_id": "bbb", "commit": True},
    )
    assert "aaa" in summary
    assert "bbb" in summary
    assert "apply" in summary


def test_account_deactivate_shows_id() -> None:
    summary = approval_mod._summarize_tool(
        "bank_account_deactivate",
        {"id": "abc123", "cascade": True},
    )
    assert "abc123" in summary
    assert "and its transactions" in summary


def test_balance_update_summary_shows_account_and_amount() -> None:
    summary = approval_mod._summarize_tool(
        "balance_update",
        {"account": "acct_123456789", "current": 1234.56, "dry_run": False},
    )
    assert "acct_123456" in summary
    assert "current=$1,234.56" in summary
    assert "apply" in summary


def test_plaid_unlink_shows_item() -> None:
    assert "item_abc" in approval_mod._summarize_tool("plaid_unlink", {"item": "item_abc"})



def test_cat_memory_undo_uses_txn_id() -> None:
    assert "abc123" in approval_mod._summarize_tool("cat_memory_undo", {"txn_id": "abc123"})


def test_provider_switch_shows_both() -> None:
    summary = approval_mod._summarize_tool(
        "provider_switch",
        {"institution": "Chase", "provider": "plaid"},
    )
    assert "Chase" in summary
    assert "plaid" in summary


def test_notify_budget_alerts_dry_run() -> None:
    summary = approval_mod._summarize_tool(
        "notify_budget_alerts",
        {"channel": "telegram", "dry_run": True},
    )
    assert "Send budget alerts" in summary
    assert "(preview)" in summary


def test_notify_test_label() -> None:
    summary = approval_mod._summarize_tool("notify_test", {"channel": "telegram"})
    assert "Test notification" in summary
    assert "Send budget alerts" not in summary


def test_notify_test_dry_run() -> None:
    summary = approval_mod._summarize_tool("notify_test", {"dry_run": True})
    assert "Test notification" in summary
    assert "(preview)" in summary


def test_stripe_link_vs_unlink() -> None:
    assert approval_mod._summarize_tool("stripe_link", {}) == "Link Stripe account"
    assert approval_mod._summarize_tool("stripe_unlink", {}) == "Unlink Stripe account"


def test_memory_update_vs_session_write() -> None:
    assert approval_mod._summarize_tool("agent_memory_update", {}) == "Update agent memory"
    assert approval_mod._summarize_tool("agent_session_write", {}) == "Write session notes"



def test_account_activate_vs_deactivate() -> None:
    assert "Activate" in approval_mod._summarize_tool("bank_account_activate", {"id": "abc123"})
    assert "Deactivate" in approval_mod._summarize_tool("bank_account_deactivate", {"id": "abc123"})


def test_summary_newline_stripped() -> None:
    summary = approval_mod._summarize_tool(
        "txn_add",
        {"description": "line1\nline2", "amount": 5},
    )
    assert summary == "Add transaction: line1 line2 $5"
    assert "\n" not in summary


def test_summary_all_line_separators() -> None:
    summary = approval_mod._summarize_tool(
        "finance_log_issue",
        {"title": "a\u2028b\u2029c\x0bd\x0ce\x85f\x1cg"},
    )
    assert summary == "Log issue: a b c d e f g"
    for char in ("\u2028", "\u2029", "\x0b", "\x0c", "\x85", "\x1c"):
        assert char not in summary


def test_summary_truncated_at_60() -> None:
    summary = approval_mod._summarize_tool(
        "subs_add",
        {"vendor": "x" * 100, "amount": 9.99},
    )
    assert len(summary) == 60
    assert summary.endswith("…")


def test_format_value_all_line_breaks() -> None:
    assert approval_mod._format_value("a\u2028b\x0bc\x85d") == "a b c d"


def test_build_approval_keyboard() -> None:
    keyboard = build_approval_keyboard("abc", "txn_categorize")

    assert len(keyboard) == 1
    assert [button["text"] for button in keyboard[0]] == ["✅ Approve", "❌ Deny"]
    assert parse_callback_data(keyboard[0][0]["callback_data"]) == ("abc", "y")
    assert parse_callback_data(keyboard[0][1]["callback_data"]) == ("abc", "n")


def test_stale_callback_shows_expired() -> None:
    async def scenario() -> FakeAPI:
        api = FakeAPI()
        bot = TelegramBot(config=_make_config(), api=api, client=FakeGatewayClient())
        await bot._handle_callback_query(_callback("missing", "y"))
        return api

    api = asyncio.run(scenario())

    assert api.callback_answers[-1]["text"] == "Expired"
    assert api.edits == []


def test_unauthorized_callback_is_ignored() -> None:
    async def scenario() -> tuple[FakeAPI, TelegramBot]:
        api = FakeAPI()
        bot = TelegramBot(config=_make_config(), api=api, client=FakeGatewayClient())
        bot._pending_approvals["nonce-1"] = {
            "tool_call_id": "tool-1",
            "message_id": 50,
            "chat_id": "12345",
            "generation": bot._approval_generation,
        }
        await bot._handle_callback_query(_callback("nonce-1", "y", chat_id=99999))
        return api, bot

    api, bot = asyncio.run(scenario())

    assert "nonce-1" in bot._pending_approvals
    assert api.callback_answers[-1]["text"] == ""
    assert api.edits == []


def test_approval_timeout_auto_denies() -> None:
    async def scenario() -> tuple[FakeAPI, FakeGatewayClient, TelegramBot]:
        api = FakeAPI()
        client = FakeGatewayClient()
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        await bot._approval_timeout("nonce-1", 0)
        await _cancel_task(timeout_task)
        return api, client, bot

    api, client, bot = asyncio.run(scenario())

    assert client.submit_calls == [("tool-1", "nonce-1", False)]
    assert bot._pending_approvals == {}
    assert api.edits[-1]["text"] == APPROVAL_EXPIRED_TEXT
    assert api.edits[-1]["reply_markup"] == {"inline_keyboard": []}


def test_callback_cancels_approval_timeout() -> None:
    async def scenario() -> tuple[FakeAPI, FakeGatewayClient, TelegramBot, asyncio.Task[Any]]:
        api = FakeAPI()
        client = FakeGatewayClient()
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        await bot._handle_callback_query(_callback("nonce-1", "y"))
        await _drain_loop()
        return api, client, bot, timeout_task

    api, client, bot, timeout_task = asyncio.run(scenario())

    assert client.submit_calls == [("tool-1", "nonce-1", True)]
    assert bot._pending_approvals == {}
    assert timeout_task.cancelled()
    assert api.callback_answers[-1]["text"] == "Approved"
    assert api.edits[-1]["text"] == "✅ Approved"


def test_stop_cancels_approval_timeouts() -> None:
    async def scenario() -> tuple[TelegramBot, asyncio.Task[Any]]:
        bot = TelegramBot(
            config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60),
            api=FakeAPI(),
            client=FakeGatewayClient(),
        )
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        bot.stop()
        await _drain_loop()
        return bot, timeout_task

    bot, timeout_task = asyncio.run(scenario())

    assert timeout_task.cancelled()
    assert bot._pending_approvals == {}


def test_approval_timeout_handles_gateway_404() -> None:
    async def scenario() -> tuple[FakeAPI, FakeGatewayClient]:
        api = FakeAPI()
        client = FakeGatewayClient(responses=[404])
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        await bot._approval_timeout("nonce-1", 0)
        await _cancel_task(timeout_task)
        return api, client

    api, client = asyncio.run(scenario())

    assert client.submit_calls == [("tool-1", "nonce-1", False)]
    assert api.edits[-1]["text"] == APPROVAL_EXPIRED_TEXT


def test_multiple_approval_timeouts_independent() -> None:
    async def scenario() -> tuple[FakeAPI, FakeGatewayClient, asyncio.Task[Any], asyncio.Task[Any]]:
        api = FakeAPI()
        client = FakeGatewayClient()
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        await bot._send_approval_prompt(_approval_event("nonce-2", tool_call_id="tool-2"))
        first_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        second_task = bot._pending_approvals["nonce-2"]["timeout_task"]
        await bot._handle_callback_query(_callback("nonce-1", "y"))
        await _drain_loop()
        await bot._approval_timeout("nonce-2", 0)
        await _cancel_task(second_task)
        return api, client, first_task, second_task

    api, client, first_task, second_task = asyncio.run(scenario())

    assert client.submit_calls == [
        ("tool-1", "nonce-1", True),
        ("tool-2", "nonce-2", False),
    ]
    assert first_task.cancelled()
    assert second_task.done()
    assert [edit["text"] for edit in api.edits] == ["✅ Approved", APPROVAL_EXPIRED_TEXT]


def test_callback_transport_error_reinserts_with_fresh_timer() -> None:
    async def scenario() -> tuple[FakeAPI, FakeGatewayClient, TelegramBot, bool, bool, bool]:
        api = FakeAPI()
        client = FakeGatewayClient(responses=[RuntimeError("boom"), 200])
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        original_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        await bot._handle_callback_query(_callback("nonce-1", "y"))
        await _drain_loop()
        pending = bot._pending_approvals["nonce-1"]
        fresh_task = pending["timeout_task"]
        fresh_is_new = fresh_task is not original_task
        fresh_is_alive = not fresh_task.done() and not fresh_task.cancelled()
        original_cancelled = original_task.cancelled()
        await bot._approval_timeout("nonce-1", 0)
        fresh_task.cancel()
        with suppress(asyncio.CancelledError):
            await fresh_task
        return api, client, bot, fresh_is_new, fresh_is_alive, original_cancelled

    api, client, bot, fresh_is_new, fresh_is_alive, original_cancelled = asyncio.run(scenario())

    assert "nonce-1" not in bot._pending_approvals
    assert client.submit_calls == [
        ("tool-1", "nonce-1", True),
        ("tool-1", "nonce-1", False),
    ]
    assert fresh_is_new is True
    assert fresh_is_alive is True
    assert original_cancelled is True
    assert api.callback_answers[-1]["text"] == "Error — try again"
    assert api.edits[-1]["text"] == APPROVAL_EXPIRED_TEXT


def test_callback_error_after_original_timer_fired() -> None:
    async def scenario() -> tuple[FakeAPI, BlockingApprovalGatewayClient, bool, bool]:
        api = FakeAPI()
        client = BlockingApprovalGatewayClient(final_response=RuntimeError("boom"))
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        callback_task = asyncio.create_task(bot._handle_callback_query(_callback("nonce-1", "y")))
        await client.started.wait()
        original_task = asyncio.create_task(bot._approval_timeout("nonce-1", 0))
        await _drain_loop()
        original_fired = original_task.done() and not original_task.cancelled()
        client.release.set()
        await callback_task
        await _drain_loop()
        fresh_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        fresh_is_alive = not fresh_task.done() and not fresh_task.cancelled()
        await bot._approval_timeout("nonce-1", 0)
        await _cancel_task(fresh_task)
        return api, client, original_fired, fresh_is_alive

    api, client, original_fired, fresh_is_alive = asyncio.run(scenario())

    assert original_fired is True
    assert fresh_is_alive is True
    assert client.submit_calls == [
        ("tool-1", "nonce-1", True),
        ("tool-1", "nonce-1", False),
    ]
    assert api.edits[-1]["text"] == APPROVAL_EXPIRED_TEXT


def test_reset_clears_approvals_and_edits_keyboards() -> None:
    async def scenario() -> tuple[FakeAPI, FakeGatewayClient, TelegramBot, asyncio.Task[Any]]:
        api = FakeAPI()
        client = FakeGatewayClient()
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        await bot._handle_command(12345, "/reset")
        await _drain_loop()
        return api, client, bot, timeout_task

    api, client, bot, timeout_task = asyncio.run(scenario())

    assert timeout_task.cancelled()
    assert bot._pending_approvals == {}
    assert bot._approval_generation == 1
    assert client.invalidate_calls == 1
    assert api.edits[-1]["text"] == "🚫 Cancelled"


def test_timeout_and_callback_race() -> None:
    async def scenario() -> tuple[FakeAPI, FakeGatewayClient, asyncio.Task[Any]]:
        api = FakeAPI()
        client = FakeGatewayClient()
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        await bot._handle_callback_query(_callback("nonce-1", "y"))
        await _drain_loop()
        return api, client, timeout_task

    api, client, timeout_task = asyncio.run(scenario())

    assert client.submit_calls == [("tool-1", "nonce-1", True)]
    assert timeout_task.cancelled()
    assert len(api.edits) == 1
    assert api.edits[-1]["text"] == "✅ Approved"


def test_generation_mismatch_prevents_reinsertion() -> None:
    async def scenario() -> TelegramBot:
        api = FakeAPI()
        client = BlockingApprovalGatewayClient(final_response=RuntimeError("boom"))
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        callback_task = asyncio.create_task(bot._handle_callback_query(_callback("nonce-1", "y")))
        await client.started.wait()
        await _drain_loop()
        await bot._clear_pending_approvals(reason="cancelled")
        client.release.set()
        await callback_task
        await _drain_loop()
        await _cancel_task(timeout_task)
        return bot

    bot = asyncio.run(scenario())

    # Key assertion: stale callback did NOT reinsert after generation bump
    assert bot._pending_approvals == {}
    assert bot._approval_generation == 1


def test_callback_error_after_cleanup_edits_keyboard() -> None:
    async def scenario() -> tuple[FakeAPI, TelegramBot]:
        api = FakeAPI()
        client = BlockingApprovalGatewayClient(final_response=RuntimeError("boom"))
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        timeout_task = bot._pending_approvals["nonce-1"]["timeout_task"]
        callback_task = asyncio.create_task(bot._handle_callback_query(_callback("nonce-1", "y")))
        await client.started.wait()
        await _drain_loop()
        await bot._clear_pending_approvals(reason="cancelled")
        client.release.set()
        await callback_task
        await _drain_loop()
        await _cancel_task(timeout_task)
        return api, bot

    api, bot = asyncio.run(scenario())

    assert bot._pending_approvals == {}
    assert api.callback_answers[-1]["text"] == "Error"
    assert api.edits[-1]["text"] == "🚫 Cancelled"
    assert api.edits[-1]["reply_markup"] == {"inline_keyboard": []}


def test_callback_success_after_cleanup_edits_keyboard() -> None:
    async def scenario() -> tuple[FakeAPI, TelegramBot]:
        api = FakeAPI()
        client = BlockingApprovalGatewayClient(final_response=200)
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-1", tool_call_id="tool-1"))
        callback_task = asyncio.create_task(bot._handle_callback_query(_callback("nonce-1", "y")))
        await client.started.wait()
        await _drain_loop()
        await bot._clear_pending_approvals(reason="cancelled")
        client.release.set()
        await callback_task
        await _drain_loop()
        return api, bot

    api, bot = asyncio.run(scenario())

    assert bot._pending_approvals == {}
    assert api.callback_answers[-1]["text"] == "Approved"
    assert api.edits[-1]["text"] == "✅ Approved"


def test_cross_request_callback_cannot_reinsert() -> None:
    async def scenario() -> tuple[FakeAPI, TelegramBot]:
        api = FakeAPI()
        client = BlockingApprovalGatewayClient(final_response=RuntimeError("boom"))
        bot = TelegramBot(config=_make_config(TELEGRAM_BOT_APPROVAL_TIMEOUT=60), api=api, client=client)
        await bot._send_approval_prompt(_approval_event("nonce-old", tool_call_id="tool-old"))
        old_timeout_task = bot._pending_approvals["nonce-old"]["timeout_task"]
        callback_task = asyncio.create_task(bot._handle_callback_query(_callback("nonce-old", "y")))
        await client.started.wait()
        await _drain_loop()
        bot.stop()
        await bot._send_approval_prompt(_approval_event("nonce-new", tool_call_id="tool-new"))
        new_task = bot._pending_approvals["nonce-new"]["timeout_task"]
        new_task.cancel()
        client.release.set()
        await callback_task
        await _drain_loop()
        await _cancel_task(old_timeout_task)
        with suppress(asyncio.CancelledError):
            await new_task
        return api, bot

    api, bot = asyncio.run(scenario())

    assert set(bot._pending_approvals) == {"nonce-new"}
    assert bot._pending_approvals["nonce-new"]["generation"] == 1
    assert api.edits[-1]["text"] == "🚫 Cancelled"


def test_excluded_tools_disjoint_from_read_only() -> None:
    assert EXCLUDED_TOOLS.isdisjoint(READ_ONLY_TOOLS)


def test_tool_partition_covers_all_mcp_tools() -> None:
    all_tools = _all_mcp_tool_names()
    classified = READ_ONLY_TOOLS | APPROVAL_REQUIRED_TOOLS | EXCLUDED_TOOLS

    assert READ_ONLY_TOOLS.isdisjoint(APPROVAL_REQUIRED_TOOLS)
    assert READ_ONLY_TOOLS.isdisjoint(EXCLUDED_TOOLS)
    assert APPROVAL_REQUIRED_TOOLS.isdisjoint(EXCLUDED_TOOLS)
    assert classified == all_tools


def test_excluded_tools_cover_file_and_browser_tools() -> None:
    assert {
        "ingest_statement",
        "ingest_csv",
        "ingest_batch",
        "export_csv",
        "export_summary",
        "export_wave",
        "biz_tax_package",
        "db_restore",
    }.issubset(EXCLUDED_TOOLS)


def test_module_reexports_match_gateway_tool_sets() -> None:
    assert _READ_ONLY_TOOLS == READ_ONLY_TOOLS
    assert _APPROVAL_REQUIRED_TOOLS == APPROVAL_REQUIRED_TOOLS


def test_loan_friendly_tool_names() -> None:
    assert _friendly_tool_name("loan_list") == "Loading loans"
    assert _friendly_tool_name("loan_show") == "Loading loan details"
    assert _friendly_tool_name("loan_schedule") == "Calculating repayment schedule"


def _all_mcp_tool_names() -> set[str]:
    path = Path(__file__).resolve().parents[1] / "mcp_server.py"
    module = ast.parse(path.read_text(encoding="utf-8"))
    tools: set[str] = set()
    for node in module.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            call = decorator if isinstance(decorator, ast.Call) else None
            func = call.func if call is not None else decorator
            if (
                isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Name)
                and func.value.id == "mcp"
                and func.attr == "tool"
            ):
                tools.add(node.name)
                break
    return tools
