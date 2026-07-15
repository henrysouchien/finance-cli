"""Gateway tool interceptors for runtime hardening."""
from __future__ import annotations

import json
import time
from collections import defaultdict
from typing import DefaultDict

from agent_gateway.tool_dispatcher import InterceptContext, InterceptDecision, ToolInterceptor

_RATE_LIMIT_WINDOW_SECONDS = 60.0
_monotonic = time.monotonic


def make_rate_limit_interceptor(max_rpm: int) -> ToolInterceptor:
    """Return a per-session sliding-window tool rate limiter."""
    calls_by_session: DefaultDict[str, list[float]] = defaultdict(list)

    async def _interceptor(ctx: InterceptContext) -> InterceptDecision:
        now = _monotonic()
        window_start = now - _RATE_LIMIT_WINDOW_SECONDS
        recent_calls = [ts for ts in calls_by_session[ctx.session_id] if ts > window_start]
        calls_by_session[ctx.session_id] = recent_calls

        if len(recent_calls) >= max_rpm:
            return InterceptDecision(
                action="deny",
                code="rate_limit_exceeded",
                message=(
                    f"tool rate limit exceeded: more than {max_rpm} tool calls per minute "
                    "are not allowed for this session"
                ),
            )

        recent_calls.append(now)
        return InterceptDecision(action="allow")

    _interceptor.__intercept_critical__ = False  # type: ignore[attr-defined]
    return _interceptor


def make_input_size_interceptor(max_bytes: int) -> ToolInterceptor:
    """Return a tool input size guard that measures serialized UTF-8 bytes."""

    async def _interceptor(ctx: InterceptContext) -> InterceptDecision:
        try:
            payload = json.dumps(
                ctx.tool_input,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        except (TypeError, ValueError, OverflowError):
            return InterceptDecision(
                action="deny",
                code="tool_input_not_serializable",
                message="tool input not serializable",
            )

        payload_size = len(payload)
        if payload_size > max_bytes:
            return InterceptDecision(
                action="deny",
                code="tool_input_too_large",
                message=(
                    f"tool input too large: {payload_size} bytes exceeds limit of "
                    f"{max_bytes} bytes"
                ),
            )

        return InterceptDecision(action="allow")

    _interceptor.__intercept_critical__ = False  # type: ignore[attr-defined]
    return _interceptor
