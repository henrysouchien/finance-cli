from __future__ import annotations

from finance_cli.telegram_bot import compaction as compaction_module


def _message(role: str, content: str) -> dict[str, str]:
    return {"role": role, "content": content}


def test_estimate_tokens_uses_chars_divided_by_four() -> None:
    messages = [
        _message("user", "abcd"),
        _message("assistant", "abcde"),
        _message("user", ""),
    ]

    assert compaction_module.estimate_tokens(messages) == 3


def test_needs_compaction_is_false_below_thresholds() -> None:
    messages = [_message("user" if index % 2 == 0 else "assistant", f"m{index}") for index in range(10)]

    assert compaction_module.needs_compaction(messages) is False


def test_needs_compaction_triggers_on_token_threshold() -> None:
    messages = [_message("user", "x" * 240001)] + [_message("assistant", "") for _ in range(8)]

    assert compaction_module.needs_compaction(messages) is True


def test_needs_compaction_triggers_on_message_threshold() -> None:
    messages = [_message("user" if index % 2 == 0 else "assistant", "x") for index in range(31)]

    assert compaction_module.needs_compaction(messages) is True


def test_build_flush_messages_uses_only_older_messages() -> None:
    messages = [
        _message("user", "old-1"),
        _message("assistant", "old-2"),
        _message("user", "recent-1"),
        _message("assistant", "recent-2"),
        _message("user", "recent-3"),
        _message("assistant", "recent-4"),
        _message("user", "recent-5"),
        _message("assistant", "recent-6"),
    ]

    prompt = compaction_module.build_flush_messages(messages)

    assert prompt == [
        {
            "role": "user",
            "content": (
                "USER: old-1\n\nASSISTANT: old-2\n\n"
                f"{compaction_module.FLUSH_PROMPT}"
            ),
        }
    ]


def test_build_summary_messages_uses_only_older_messages() -> None:
    messages = [
        _message("user", "old-1"),
        _message("assistant", "old-2"),
        _message("user", "recent-1"),
        _message("assistant", "recent-2"),
        _message("user", "recent-3"),
        _message("assistant", "recent-4"),
        _message("user", "recent-5"),
        _message("assistant", "recent-6"),
    ]

    prompt = compaction_module.build_summary_messages(messages)

    assert prompt == [
        {
            "role": "user",
            "content": (
                "USER: old-1\n\nASSISTANT: old-2\n\n"
                f"{compaction_module.SUMMARY_PROMPT}"
            ),
        }
    ]


def test_apply_compaction_replaces_old_messages_and_keeps_recent() -> None:
    messages = [
        _message("user", "old-1"),
        _message("assistant", "old-2"),
        _message("user", "recent-1"),
        _message("assistant", "recent-2"),
        _message("user", "recent-3"),
        _message("assistant", "recent-4"),
        _message("user", "recent-5"),
        _message("assistant", "recent-6"),
    ]

    compacted = compaction_module.apply_compaction(messages, "Summary text.")

    assert compacted == [
        {"role": "user", "content": "[Previous conversation summary]\nSummary text."},
        {
            "role": "assistant",
            "content": "Understood. I have the context from our previous conversation.",
        },
        _message("user", "recent-1"),
        _message("assistant", "recent-2"),
        _message("user", "recent-3"),
        _message("assistant", "recent-4"),
        _message("user", "recent-5"),
        _message("assistant", "recent-6"),
    ]


def test_build_transcript_truncates_long_content() -> None:
    transcript = compaction_module._build_transcript([_message("user", "x" * 2100)])

    assert transcript == f"USER: {'x' * 2000}..."
