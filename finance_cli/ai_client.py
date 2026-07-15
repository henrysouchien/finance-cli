"""Shared HTTP client helpers for platform AI providers."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

DEFAULT_MODELS = {
    "openai": "gpt-4o-mini",
    "claude": "claude-sonnet-4-5-20250929",
}
API_KEY_ENV_VARS = {
    "openai": "OPENAI_API_KEY",
    "claude": "ANTHROPIC_API_KEY",
}


def default_model(provider: str) -> str:
    provider_name = str(provider or "").strip().lower()
    model = DEFAULT_MODELS.get(provider_name)
    if model:
        return model
    raise ValueError(f"Unsupported AI provider '{provider}'")


def resolve_api_key(provider: str, *, api_key: str | None = None) -> str:
    provider_name = str(provider or "").strip().lower()
    env_var = API_KEY_ENV_VARS.get(provider_name)
    if not env_var:
        raise ValueError(f"Unsupported AI provider '{provider}'")

    explicit_key = str(api_key or "").strip()
    if explicit_key:
        return explicit_key

    env_key = str(os.getenv(env_var) or "").strip()
    if env_key:
        return env_key

    raise ValueError(f"{env_var} is not set")


def _to_usage_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _parse_openai_response(body: dict[str, Any]) -> tuple[str, dict[str, int]]:
    choices = body.get("choices") or []
    if not choices:
        raise RuntimeError("OpenAI API returned no choices")

    usage = body.get("usage")
    usage_map = usage if isinstance(usage, dict) else {}
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content, {
            "input_tokens": _to_usage_int(usage_map.get("prompt_tokens")),
            "output_tokens": _to_usage_int(usage_map.get("completion_tokens")),
        }
    if isinstance(content, list):
        parts = [str(item.get("text", "")) for item in content if isinstance(item, dict)]
        return "".join(parts), {
            "input_tokens": _to_usage_int(usage_map.get("prompt_tokens")),
            "output_tokens": _to_usage_int(usage_map.get("completion_tokens")),
        }
    raise RuntimeError("OpenAI API returned unexpected content format")


def _parse_claude_response(body: dict[str, Any]) -> tuple[str, dict[str, int]]:
    content = body.get("content") or []
    usage = body.get("usage")
    usage_map = usage if isinstance(usage, dict) else {}
    if isinstance(content, list):
        parts = [str(item.get("text", "")) for item in content if isinstance(item, dict)]
        if parts:
            return "".join(parts), {
                "input_tokens": _to_usage_int(usage_map.get("input_tokens")),
                "output_tokens": _to_usage_int(usage_map.get("output_tokens")),
            }
    raise RuntimeError("Anthropic API returned unexpected content format")


def _send_json_request(
    url: str,
    *,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: int,
    provider_label: str,
) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=headers,
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"{provider_label} API error: {detail or exc.reason}") from exc

    if not isinstance(body, dict):
        raise RuntimeError(f"{provider_label} API returned unexpected response format")
    return body


def send_request(
    provider: str,
    *,
    system_prompt: str,
    user_prompt: str,
    model: str,
    max_tokens: int | None = None,
    temperature: int | float = 0,
    timeout: int = 120,
    api_key: str | None = None,
) -> tuple[str, dict[str, int]]:
    provider_name = str(provider or "").strip().lower()

    if provider_name == "openai":
        payload = {
            "model": model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        body = _send_json_request(
            "https://api.openai.com/v1/chat/completions",
            payload=payload,
            headers={
                "Authorization": f"Bearer {resolve_api_key(provider_name, api_key=api_key)}",
                "Content-Type": "application/json",
            },
            timeout=timeout,
            provider_label="OpenAI",
        )
        return _parse_openai_response(body)

    if provider_name == "claude":
        payload = {
            "model": model,
            "temperature": temperature,
            "system": system_prompt,
            "messages": [
                {"role": "user", "content": user_prompt},
            ],
        }
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        body = _send_json_request(
            "https://api.anthropic.com/v1/messages",
            payload=payload,
            headers={
                "x-api-key": resolve_api_key(provider_name, api_key=api_key),
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=timeout,
            provider_label="Anthropic",
        )
        return _parse_claude_response(body)

    raise ValueError(f"Unsupported AI provider '{provider}'")


__all__ = [
    "API_KEY_ENV_VARS",
    "DEFAULT_MODELS",
    "default_model",
    "resolve_api_key",
    "send_request",
]
