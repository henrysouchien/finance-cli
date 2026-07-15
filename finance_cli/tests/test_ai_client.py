from __future__ import annotations

import io
import json
import urllib.error

import pytest

from finance_cli.ai_client import default_model, resolve_api_key, send_request


class _FakeResponse:
    def __init__(self, body: dict[str, object]) -> None:
        self._body = json.dumps(body).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_default_model_supports_known_providers() -> None:
    assert default_model("openai") == "gpt-4o-mini"
    assert default_model("claude") == "claude-sonnet-4-5-20250929"
    with pytest.raises(ValueError, match="Unsupported AI provider"):
        default_model("unknown")


def test_resolve_api_key_prefers_explicit_then_env(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "env-key")
    assert resolve_api_key("openai", api_key="explicit-key") == "explicit-key"
    assert resolve_api_key("openai") == "env-key"


def test_resolve_api_key_raises_when_missing(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY is not set"):
        resolve_api_key("claude")


def test_send_request_openai_omits_max_tokens_when_none(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_urlopen(req, timeout: int):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        captured["headers"] = dict(req.header_items())
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return _FakeResponse(
            {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": "12", "completion_tokens": 3},
            }
        )

    monkeypatch.setattr("finance_cli.ai_client.urllib.request.urlopen", _fake_urlopen)

    content, usage = send_request(
        "openai",
        system_prompt="sys",
        user_prompt="user",
        model="gpt-test",
        max_tokens=None,
        timeout=45,
        api_key="explicit-openai-key",
    )

    assert content == "ok"
    assert usage == {"input_tokens": 12, "output_tokens": 3}
    assert captured["url"] == "https://api.openai.com/v1/chat/completions"
    assert captured["timeout"] == 45
    assert captured["headers"]["Authorization"] == "Bearer explicit-openai-key"
    assert "max_tokens" not in captured["payload"]


def test_send_request_claude_includes_max_tokens_and_parses_text(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_urlopen(req, timeout: int):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        captured["headers"] = dict(req.header_items())
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return _FakeResponse(
            {
                "content": [{"text": "hello "}, {"text": "world"}],
                "usage": {"input_tokens": 9, "output_tokens": "4"},
            }
        )

    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-claude-key")
    monkeypatch.setattr("finance_cli.ai_client.urllib.request.urlopen", _fake_urlopen)

    content, usage = send_request(
        "claude",
        system_prompt="sys",
        user_prompt="user",
        model="claude-test",
        max_tokens=2048,
        timeout=60,
    )

    assert content == "hello world"
    assert usage == {"input_tokens": 9, "output_tokens": 4}
    assert captured["url"] == "https://api.anthropic.com/v1/messages"
    assert captured["timeout"] == 60
    assert captured["headers"]["X-api-key"] == "env-claude-key"
    assert captured["payload"]["max_tokens"] == 2048


def test_send_request_surfaces_http_errors(monkeypatch) -> None:
    def _fake_urlopen(req, timeout: int):
        raise urllib.error.HTTPError(
            req.full_url,
            401,
            "Unauthorized",
            hdrs=None,
            fp=io.BytesIO(b'{"error":"bad key"}'),
        )

    monkeypatch.setattr("finance_cli.ai_client.urllib.request.urlopen", _fake_urlopen)

    with pytest.raises(RuntimeError, match="OpenAI API error:"):
        send_request(
            "openai",
            system_prompt="sys",
            user_prompt="user",
            model="gpt-test",
            api_key="bad-key",
        )
