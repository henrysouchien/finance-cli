from __future__ import annotations

import json

import pytest

from finance_cli.telegram_bot.bot import (
    _TOOL_SUMMARY_MAX,
    _build_tool_summary,
    _format_args,
    _summarize_result,
)


def test_summarize_result_returns_empty_for_none() -> None:
    assert _summarize_result(None) == ""


def test_summarize_result_prefers_non_empty_summary() -> None:
    result = {"data": {"cash": 1234}, "summary": {"net_worth": "$50,234", "total_accounts": 3}}

    assert _summarize_result(result) == json.dumps(result["summary"], default=str, sort_keys=True)


@pytest.mark.parametrize(
    "result",
    [
        {"data": {"cash": 1234}, "summary": {}},
        {"data": {"cash": 1234}, "summary": None},
    ],
)
def test_summarize_result_falls_back_for_empty_summary(result: dict[str, object]) -> None:
    assert _summarize_result(result) == json.dumps(result, default=str, sort_keys=True)


def test_summarize_result_serializes_strings() -> None:
    assert _summarize_result("line\nbreak") == json.dumps("line\nbreak", default=str, sort_keys=True)


def test_summarize_result_truncates_long_output() -> None:
    result = {"summary": {"note": "x" * 200}}
    expected = json.dumps(result["summary"], default=str, sort_keys=True)

    assert _summarize_result(result, max_len=30) == expected[:30] + "..."


def test_format_args_returns_empty_for_empty_input() -> None:
    assert _format_args({}) == ""


def test_format_args_skips_empty_and_preserves_explicit_falsy_values() -> None:
    tool_input = {
        "view": "personal",
        "include_pending": False,
        "limit": 0,
        "verbose": True,
        "optional": None,
        "empty": "",
    }

    assert (
        _format_args(tool_input)
        == 'view="personal", include_pending=false, limit=0, verbose'
    )


def test_format_args_json_encodes_and_escapes_values() -> None:
    tool_input = {"note": 'line1\n"quote"', "ids": [1, 2]}

    assert _format_args(tool_input, max_val_len=100) == 'note="line1\\n\\"quote\\"", ids=[1, 2]'


def test_format_args_truncates_long_values() -> None:
    assert _format_args({"query": "x" * 40}, max_val_len=10) == 'query="xxxxxxxxx...'


def test_build_tool_summary_returns_empty_for_no_calls() -> None:
    assert _build_tool_summary([]) == ""


def test_build_tool_summary_formats_success_error_and_missing_args() -> None:
    tool_calls = [
        {
            "tool_name": "balance_show",
            "tool_input": {"view": "personal"},
            "result_summary": '{"net_worth": "$50,234"}',
            "is_error": False,
        },
        {
            "tool_name": "txn_list",
            "tool_input": {"limit": 5},
            "is_error": True,
            "error_message": "timeout",
        },
        {
            "tool_name": "ping",
            "result_summary": "",
            "is_error": False,
        },
    ]

    assert _build_tool_summary(tool_calls) == (
        '[Tools: balance_show(view="personal") → {"net_worth": "$50,234"}'
        " | txn_list(limit=5) → ERROR: timeout"
        " | ping()]"
    )


def test_build_tool_summary_truncates_total_length() -> None:
    tool_calls = [
        {
            "tool_name": "balance_show",
            "tool_input": {"view": "personal"},
            "result_summary": "x" * 600,
            "is_error": False,
        }
    ]

    summary = _build_tool_summary(tool_calls)

    assert len(summary) == _TOOL_SUMMARY_MAX
    assert summary.endswith("...]")
