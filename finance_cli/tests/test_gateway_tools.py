from __future__ import annotations

import asyncio

from finance_cli.gateway.tools import (
    ALL_NORMALIZER_TOOLS,
    APPROVAL_REQUIRED_TOOLS,
    BRIDGE_TOOLS,
    EXCLUDED_TOOLS,
    NORMALIZER_WRITE_TOOLS,
    NORMALIZER_SKILL_TOOLS,
    ONBOARDING_AUTO_APPROVED,
    READ_ONLY_TOOLS,
    REGULATED_SCOPE_EXCLUDED_TOOLS,
    VALID_SKILLS,
    WEB_EXCLUDED_TOOLS,
    WEB_IMPORT_TOOLS,
    _NON_ACTIVATABLE_SKILLS,
    needs_approval,
    web_excluded_tools,
)
from finance_cli.skills import SKILL_FILES


def test_needs_approval_false_for_read_only_tools() -> None:
    assert all(needs_approval(tool_name) is False for tool_name in READ_ONLY_TOOLS)


def test_needs_approval_true_for_approval_required_tools() -> None:
    assert all(needs_approval(tool_name) is True for tool_name in APPROVAL_REQUIRED_TOOLS)


def test_needs_approval_true_for_unknown_tool() -> None:
    assert needs_approval("unknown_tool_name") is True


def test_get_skill_is_read_only() -> None:
    assert "get_skill" in READ_ONLY_TOOLS


def test_activate_skill_is_read_only() -> None:
    assert "activate_skill" in READ_ONLY_TOOLS


def test_get_skill_does_not_need_approval() -> None:
    assert needs_approval("get_skill") is False


def test_activate_skill_does_not_need_approval() -> None:
    assert needs_approval("activate_skill") is False


def test_activate_skill_is_not_a_bridge_tool() -> None:
    assert "activate_skill" not in BRIDGE_TOOLS


def test_onboarding_is_non_activatable() -> None:
    assert "onboarding" in _NON_ACTIVATABLE_SKILLS


def test_valid_skills_derived_from_registry() -> None:
    assert VALID_SKILLS == frozenset(SKILL_FILES.keys())


def test_session_tools_are_read_only() -> None:
    assert "session_recap" in READ_ONLY_TOOLS
    assert "session_list" in READ_ONLY_TOOLS
    assert needs_approval("session_recap") is False
    assert needs_approval("session_list") is False


def test_excluded_tools_match_expected_set() -> None:
    assert EXCLUDED_TOOLS == {
        "ingest_statement",
        "ingest_csv",
        "ingest_batch",
        "export_csv",
        "export_summary",
        "export_wave",
        "biz_tax_package",
        "db_restore",
    }


def test_no_overlap_between_read_only_and_approval_required_tools() -> None:
    assert READ_ONLY_TOOLS.isdisjoint(APPROVAL_REQUIRED_TOOLS)


def test_web_excluded_tools_match_expected_set() -> None:
    assert WEB_EXCLUDED_TOOLS == {
        "statement_normalizer_list",
        "normalizer_detect",
        "normalizer_validate",
        "statement_normalizer_sample_csv",
        "statement_normalizer_test",
        "statement_normalizer_stage",
        "statement_normalizer_activate",
        "normalizer_update",
        "normalizer_register_institution",
        "setup_check",
        "setup_status",
        "db_backup",
        "db_export_preferences",
        "db_import_preferences",
        "db_backup_verify",
        "session_recap",
        "session_list",
        "advisory_target_allocation",
    }


def test_regulated_scope_tools_are_hidden_from_gateway_chat_surfaces() -> None:
    assert REGULATED_SCOPE_EXCLUDED_TOOLS == {"advisory_target_allocation"}
    assert REGULATED_SCOPE_EXCLUDED_TOOLS <= WEB_EXCLUDED_TOOLS
    assert REGULATED_SCOPE_EXCLUDED_TOOLS.isdisjoint(BRIDGE_TOOLS)


def test_normalizer_tool_sets_stay_in_sync() -> None:
    assert ALL_NORMALIZER_TOOLS <= WEB_EXCLUDED_TOOLS
    assert NORMALIZER_SKILL_TOOLS == ALL_NORMALIZER_TOOLS


def test_web_excluded_tools_default_includes_all_normalizer_tools() -> None:
    excluded = web_excluded_tools(None)

    assert ALL_NORMALIZER_TOOLS <= excluded


def test_web_excluded_tools_skill_unblocks_all_normalizer_tools() -> None:
    excluded = web_excluded_tools("normalizer_builder")

    assert excluded.isdisjoint(ALL_NORMALIZER_TOOLS)


def test_web_excluded_tools_onboarding_skill_unblocks_all_normalizer_tools() -> None:
    excluded = web_excluded_tools("onboarding")

    assert excluded.isdisjoint(ALL_NORMALIZER_TOOLS)


def test_web_excluded_tools_unknown_skill_unchanged() -> None:
    assert web_excluded_tools("some_other_skill") == web_excluded_tools(None)


def test_web_import_tools_match_expected_set() -> None:
    assert WEB_IMPORT_TOOLS == {"ingest_csv", "ingest_statement"}


def test_onboarding_auto_approved_tools_match_expected_set() -> None:
    assert ONBOARDING_AUTO_APPROVED == {
        "setup_init",
        "plaid_sync",
        "plaid_balance_refresh",
        "cat_auto_categorize",
        "cat_normalize",
        "dedup_backfill_aliases",
        "dedup_cross_format",
        "subs_detect",
        "agent_session_write",
        "skill_state_set",
        "skill_state_clear",
        "skip_onboarding",
    }


def test_normalizer_sample_and_test_are_read_only() -> None:
    assert "statement_normalizer_sample_csv" in READ_ONLY_TOOLS
    assert "statement_normalizer_test" in READ_ONLY_TOOLS
    assert "statement_normalizer_sample_csv" in WEB_EXCLUDED_TOOLS
    assert "statement_normalizer_test" in WEB_EXCLUDED_TOOLS
    assert needs_approval("statement_normalizer_sample_csv") is False
    assert needs_approval("statement_normalizer_test") is False


def test_renamed_account_tools_require_approval() -> None:
    assert "bank_account_activate" in APPROVAL_REQUIRED_TOOLS
    assert "bank_account_deactivate" in APPROVAL_REQUIRED_TOOLS
    assert needs_approval("bank_account_activate") is True
    assert needs_approval("bank_account_deactivate") is True


def test_balance_update_requires_approval() -> None:
    assert "balance_update" in APPROVAL_REQUIRED_TOOLS
    assert "balance_update" not in READ_ONLY_TOOLS
    assert needs_approval("balance_update") is True


def test_normalizer_write_tools_require_approval() -> None:
    assert NORMALIZER_WRITE_TOOLS <= APPROVAL_REQUIRED_TOOLS
    assert NORMALIZER_WRITE_TOOLS.isdisjoint(READ_ONLY_TOOLS)
    assert all(needs_approval(tool_name) is True for tool_name in NORMALIZER_WRITE_TOOLS)


def test_normalizer_write_tools_subset_of_all() -> None:
    assert NORMALIZER_WRITE_TOOLS <= ALL_NORMALIZER_TOOLS


def test_agent_memory_update_requires_approval() -> None:
    assert "agent_memory_update" in APPROVAL_REQUIRED_TOOLS
    assert "agent_memory_update" not in ONBOARDING_AUTO_APPROVED
    assert needs_approval("agent_memory_update") is True


def test_skill_state_tools_require_approval() -> None:
    assert "skill_state_set" in APPROVAL_REQUIRED_TOOLS
    assert "skill_state_clear" in APPROVAL_REQUIRED_TOOLS
    assert "skill_state_set" not in READ_ONLY_TOOLS
    assert "skill_state_clear" not in READ_ONLY_TOOLS
    assert needs_approval("skill_state_set") is True
    assert needs_approval("skill_state_clear") is True


def test_skill_state_tools_auto_approved_during_onboarding() -> None:
    assert "skill_state_set" in ONBOARDING_AUTO_APPROVED
    assert "skill_state_clear" in ONBOARDING_AUTO_APPROVED


def _live_mcp_tool_names() -> frozenset[str]:
    import finance_cli.mcp_server as mcp_server

    return frozenset(
        tool.name
        for tool in asyncio.run(mcp_server.mcp.list_tools(run_middleware=False))
    )


def test_every_live_mcp_tool_is_classified() -> None:
    # Every @mcp.tool in mcp_server.py must land in exactly one of the three
    # classification sets. Unclassified tools silently break: system prompt
    # counts go stale, bridge catalog drops the tool, and downstream
    # AI-excel-addin's test_policy_tool_name_snapshot fails in a different repo.
    live = _live_mcp_tool_names()
    classified = READ_ONLY_TOOLS | APPROVAL_REQUIRED_TOOLS | EXCLUDED_TOOLS
    unclassified = live - classified
    assert not unclassified, (
        f"{len(unclassified)} MCP tool(s) not in READ_ONLY_TOOLS, "
        f"APPROVAL_REQUIRED_TOOLS, or EXCLUDED_TOOLS: {sorted(unclassified)}"
    )


def test_classification_sets_have_no_dead_entries() -> None:
    live = _live_mcp_tool_names()
    classified = READ_ONLY_TOOLS | APPROVAL_REQUIRED_TOOLS | EXCLUDED_TOOLS
    dead = classified - live
    assert not dead, (
        f"{len(dead)} classified tool(s) no longer registered via @mcp.tool: {sorted(dead)}"
    )


def test_classification_sets_are_pairwise_disjoint() -> None:
    assert READ_ONLY_TOOLS.isdisjoint(APPROVAL_REQUIRED_TOOLS)
    assert READ_ONLY_TOOLS.isdisjoint(EXCLUDED_TOOLS)
    assert APPROVAL_REQUIRED_TOOLS.isdisjoint(EXCLUDED_TOOLS)
