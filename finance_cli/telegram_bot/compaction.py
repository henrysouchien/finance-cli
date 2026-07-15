"""Conversation compaction for Telegram bot."""

from __future__ import annotations

import math

COMPACT_TOKEN_THRESHOLD = 60_000
COMPACT_AFTER_MESSAGES = 30
KEEP_RECENT_MESSAGES = 6

FLUSH_PROMPT = (
    "Your conversation context is about to be compacted. Review the conversation and "
    "write a session note using agent_session_write with any important context: "
    "decisions made, follow-ups needed, what was being worked on, key findings. "
    "If nothing important, reply briefly."
)

SUMMARY_PROMPT = (
    "Summarize this conversation in 2-3 concise paragraphs. Focus on: what was being "
    "worked on, open threads, key decisions or numbers. Write in third person. "
    "Preserve specific amounts, category names, and action items."
)

_SUMMARY_PREFIX = "[Previous conversation summary]\n"
_SUMMARY_ACK = "Understood. I have the context from our previous conversation."


def estimate_tokens(messages: list[dict[str, str]]) -> int:
    """Estimate token count using chars/4 heuristic."""
    return sum(math.ceil(len(str(m.get("content", ""))) / 4) for m in messages)


def needs_compaction(messages: list[dict[str, str]]) -> bool:
    """Check if history exceeds compaction thresholds.
    Only trigger if there are enough messages to actually compact
    (more than KEEP_RECENT_MESSAGES), otherwise there's nothing to summarize.
    """
    # Need enough messages that compaction actually reduces count:
    # summary pair (2) + recent (KEEP_RECENT) must be < current count
    min_for_compaction = KEEP_RECENT_MESSAGES + 2 + 1  # at least 1 message gets summarized
    if len(messages) < min_for_compaction:
        return False
    return estimate_tokens(messages) > COMPACT_TOKEN_THRESHOLD or len(messages) > COMPACT_AFTER_MESSAGES


def build_flush_messages(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    """Build the flush prompt with transcript of older messages."""
    older = messages[:-KEEP_RECENT_MESSAGES] if len(messages) > KEEP_RECENT_MESSAGES else messages
    transcript = _build_transcript(older)
    return [{"role": "user", "content": f"{transcript}\n\n{FLUSH_PROMPT}"}]


def build_summary_messages(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    """Build the summary prompt with transcript of older messages."""
    older = messages[:-KEEP_RECENT_MESSAGES] if len(messages) > KEEP_RECENT_MESSAGES else messages
    transcript = _build_transcript(older)
    return [{"role": "user", "content": f"{transcript}\n\n{SUMMARY_PROMPT}"}]


def apply_compaction(messages: list[dict[str, str]], summary: str) -> list[dict[str, str]]:
    """Replace older messages with summary, keeping recent messages."""
    recent = messages[-KEEP_RECENT_MESSAGES:] if len(messages) > KEEP_RECENT_MESSAGES else []
    return [
        {"role": "user", "content": f"{_SUMMARY_PREFIX}{summary}"},
        {"role": "assistant", "content": _SUMMARY_ACK},
        *recent,
    ]


def _build_transcript(messages: list[dict[str, str]]) -> str:
    """Build a readable transcript from messages, truncating long content."""
    lines = []
    for m in messages:
        role = str(m.get("role", "unknown")).upper()
        content = str(m.get("content", ""))
        if len(content) > 2000:
            content = content[:2000] + "..."
        lines.append(f"{role}: {content}")
    return "\n\n".join(lines)
