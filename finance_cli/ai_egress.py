"""AI data-egress policy helpers."""

from __future__ import annotations

from typing import Literal

AIEgressMode = Literal["full", "redacted", "off"]

AI_EGRESS_FULL: AIEgressMode = "full"
AI_EGRESS_REDACTED: AIEgressMode = "redacted"
AI_EGRESS_OFF: AIEgressMode = "off"
AI_EGRESS_MODES: frozenset[str] = frozenset(
    {AI_EGRESS_FULL, AI_EGRESS_REDACTED, AI_EGRESS_OFF}
)


class AIEgressBlockedError(RuntimeError):
    """Raised when a user privacy setting blocks an external AI request."""

    def __init__(self, *, mode: str, surface: str) -> None:
        self.mode = normalize_ai_egress_mode(mode)
        self.surface = str(surface or "ai")
        super().__init__(ai_egress_blocked_message(self.mode, self.surface))


def normalize_ai_egress_mode(value: object) -> AIEgressMode:
    raw = str(value or "").strip().lower()
    if raw in AI_EGRESS_MODES:
        return raw  # type: ignore[return-value]
    return AI_EGRESS_FULL


def ai_egress_blocked_message(mode: str, surface: str) -> str:
    normalized = normalize_ai_egress_mode(mode)
    label = str(surface or "AI")
    if normalized == AI_EGRESS_OFF:
        return f"{label} is disabled by your AI privacy setting."
    return (
        f"{label} needs raw financial data, but your AI privacy setting is "
        "set to redacted."
    )


def assert_raw_financial_ai_allowed(mode: object, *, surface: str) -> None:
    normalized = normalize_ai_egress_mode(mode)
    if normalized != AI_EGRESS_FULL:
        raise AIEgressBlockedError(mode=normalized, surface=surface)


__all__ = [
    "AIEgressBlockedError",
    "AIEgressMode",
    "AI_EGRESS_FULL",
    "AI_EGRESS_MODES",
    "AI_EGRESS_OFF",
    "AI_EGRESS_REDACTED",
    "ai_egress_blocked_message",
    "assert_raw_financial_ai_allowed",
    "normalize_ai_egress_mode",
]
