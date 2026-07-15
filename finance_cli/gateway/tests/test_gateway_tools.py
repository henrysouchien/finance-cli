from __future__ import annotations

from finance_cli.gateway.tools import ALL_NORMALIZER_TOOLS, needs_approval, web_excluded_tools


def test_web_excluded_tools_default_includes_all_normalizer_tools() -> None:
    assert ALL_NORMALIZER_TOOLS <= web_excluded_tools(None)


def test_web_excluded_tools_skill_unblocks_all_normalizer_tools() -> None:
    assert web_excluded_tools("normalizer_builder").isdisjoint(ALL_NORMALIZER_TOOLS)


def test_web_excluded_tools_onboarding_skill_unblocks_all_normalizer_tools() -> None:
    assert web_excluded_tools("onboarding").isdisjoint(ALL_NORMALIZER_TOOLS)


def test_web_excluded_tools_unknown_skill_unchanged() -> None:
    assert web_excluded_tools("some_other_skill") == web_excluded_tools(None)


def test_normalizer_write_tools_require_approval() -> None:
    assert needs_approval("statement_normalizer_stage") is True
    assert needs_approval("statement_normalizer_activate") is True
    assert needs_approval("normalizer_update") is True
    assert needs_approval("normalizer_register_institution") is True


def test_bank_account_tools_require_approval() -> None:
    assert needs_approval("bank_account_activate") is True
    assert needs_approval("bank_account_deactivate") is True
