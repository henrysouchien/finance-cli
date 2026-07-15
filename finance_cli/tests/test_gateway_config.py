from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest

from finance_cli.gateway.config import GatewaySettings, load_settings


def _gateway_user_keys(key: str = "gateway-key", *, user_id: int = 1, channel: str = "web") -> str:
    return json.dumps(
        [
            {
                "key": key,
                "channel": channel,
                "user_id": user_id,
                "email": f"user{user_id}@example.test",
                "role": "owner",
            }
        ]
    )


_ENV_KEYS = [
    "ANTHROPIC_AUTH_TOKEN",
    "DATABASE_URL",
    "GATEWAY_API_KEY",
    "GATEWAY_USER_KEYS",
    "FINANCE_GATEWAY_API_KEY",
    "FINANCE_GATEWAY_HOST",
    "FINANCE_GATEWAY_PORT",
    "FINANCE_GATEWAY_JWT_SECRET",
    "FINANCE_GATEWAY_ENV",
    "FINANCE_GATEWAY_MODEL",
    "FINANCE_GATEWAY_DATA_ROOT",
    "FINANCE_GATEWAY_RULES_TEMPLATE",
    "FINANCE_GATEWAY_CORS_ORIGINS",
    "FINANCE_GATEWAY_PER_TURN_TIMEOUT",
    "FINANCE_GATEWAY_TELEGRAM_PER_TURN_TIMEOUT",
    "FINANCE_GATEWAY_SESSION_TTL",
    "FINANCE_GATEWAY_MAX_TURNS",
    "FINANCE_GATEWAY_MAX_TOKENS",
    "FINANCE_GATEWAY_THINKING",
    "FINANCE_GATEWAY_ALLOWED_MODELS",
    "FINANCE_GATEWAY_CODE_EXECUTION",
    "CODE_EXECUTE_DOCKER_IMAGE",
    "SESSION_SECRET",
    "RESOLVER_TIMEOUT_SECONDS",
    "FINANCE_GATEWAY_CLIENT_TIMEOUT",
    "FINANCE_GATEWAY_RATE_LIMIT_RPM",
    "FINANCE_GATEWAY_MAX_INPUT_BYTES",
    "FINANCE_GATEWAY_WEB_MAX_BUDGET_USD",
    "FINANCE_GATEWAY_WEB_COMPACTION_TRIGGER",
    "FINANCE_CONFIG_FILE",
    "FINANCE_GATEWAY_CONFIG_FILE",
]


def _clear_gateway_env(monkeypatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_load_settings_raises_for_missing_required_vars(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)

    with pytest.raises(ValueError, match="GATEWAY_USER_KEYS, ANTHROPIC_AUTH_TOKEN or DATABASE_URL"):
        load_settings()


def test_load_settings_maps_all_env_vars(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    data_root = Path("/tmp/finance-gateway-users")
    template_rules = Path("/tmp/gateway-rules.yaml")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("DATABASE_URL", "postgres://gateway:secret@localhost/finance")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_HOST", "127.0.0.1")
    monkeypatch.setenv("FINANCE_GATEWAY_PORT", "9000")
    monkeypatch.setenv("FINANCE_GATEWAY_JWT_SECRET", "jwt-secret")
    monkeypatch.setenv("FINANCE_GATEWAY_ENV", "staging")
    monkeypatch.setenv("FINANCE_GATEWAY_MODEL", "claude-opus-4-6")
    monkeypatch.setenv("FINANCE_GATEWAY_DATA_ROOT", str(data_root))
    monkeypatch.setenv("FINANCE_GATEWAY_RULES_TEMPLATE", str(template_rules))
    monkeypatch.setenv("FINANCE_GATEWAY_CORS_ORIGINS", "http://localhost:3000,http://localhost:5173")
    monkeypatch.setenv("FINANCE_GATEWAY_PER_TURN_TIMEOUT", "45")
    monkeypatch.setenv("FINANCE_GATEWAY_TELEGRAM_PER_TURN_TIMEOUT", "360")
    monkeypatch.setenv("FINANCE_GATEWAY_SESSION_TTL", "7200")
    monkeypatch.setenv("FINANCE_GATEWAY_MAX_TURNS", "9")
    monkeypatch.setenv("FINANCE_GATEWAY_MAX_TOKENS", "32000")
    monkeypatch.setenv("FINANCE_GATEWAY_THINKING", "false")
    monkeypatch.setenv("FINANCE_GATEWAY_ALLOWED_MODELS", "claude-sonnet-4-6,claude-opus-4-6")
    monkeypatch.setenv("FINANCE_GATEWAY_CODE_EXECUTION", "false")
    monkeypatch.setenv("CODE_EXECUTE_DOCKER_IMAGE", "custom-code-exec:dev")
    monkeypatch.setenv("SESSION_SECRET", "session-secret")
    monkeypatch.setenv("RESOLVER_TIMEOUT_SECONDS", "7.25")
    monkeypatch.setenv("FINANCE_GATEWAY_CLIENT_TIMEOUT", "123.5")
    monkeypatch.setenv("FINANCE_GATEWAY_RATE_LIMIT_RPM", "77")
    monkeypatch.setenv("FINANCE_GATEWAY_MAX_INPUT_BYTES", "65432")
    monkeypatch.setenv("FINANCE_GATEWAY_WEB_MAX_BUDGET_USD", "12.25")
    monkeypatch.setenv("FINANCE_GATEWAY_WEB_COMPACTION_TRIGGER", "98765")

    settings = load_settings()

    assert settings.anthropic_auth_token == "anthropic-token"
    assert settings.gateway_user_keys == _gateway_user_keys()
    assert settings.host == "127.0.0.1"
    assert settings.port == 9000
    assert settings.jwt_secret == "jwt-secret"
    assert settings.env == "staging"
    assert settings.model == "claude-opus-4-6"
    assert settings.data_root == data_root.resolve()
    assert settings.template_rules_path == template_rules.resolve()
    assert settings.cors_origins == ["http://localhost:3000", "http://localhost:5173"]
    assert settings.per_turn_timeout == 45
    assert settings.telegram_per_turn_timeout == 360
    assert settings.session_ttl == 7200
    assert settings.max_turns == 9
    assert settings.max_tokens == 32000
    assert settings.thinking is False
    assert settings.allowed_models == ["claude-sonnet-4-6", "claude-opus-4-6"]
    assert settings.code_execution_enabled is False
    assert settings.code_exec_docker_image == "custom-code-exec:dev"
    assert settings.database_url == "postgres://gateway:secret@localhost/finance"
    assert settings.session_secret == "session-secret"
    assert settings.resolver_timeout_seconds == 7.25
    assert settings.client_timeout == 123.5
    assert settings.interceptor_rate_limit_rpm == 77
    assert settings.interceptor_max_input_bytes == 65432
    assert settings.web_max_budget_usd == 12.25
    assert settings.web_compaction_trigger == 98765


def test_load_settings_auto_generates_jwt_secret(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    settings = load_settings()

    assert re.fullmatch(r"[0-9a-f]{64}", settings.jwt_secret)


def test_load_settings_allows_database_url_without_auth_token(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgres://gateway:secret@localhost/finance")
    monkeypatch.setenv("SESSION_SECRET", "session-secret")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    settings = load_settings()

    assert settings.anthropic_auth_token == ""
    assert settings.database_url == "postgres://gateway:secret@localhost/finance"
    assert settings.session_secret == "session-secret"


def test_load_settings_defaults_host_to_loopback(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    settings = load_settings()

    assert settings.host == "127.0.0.1"


def test_load_settings_defaults_code_execution(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    settings = load_settings()

    assert settings.code_execution_enabled is True
    assert settings.code_exec_docker_image == "finance-cli-code-exec:latest"


def test_load_settings_reads_list_values_from_gateway_yaml(monkeypatch, tmp_path: Path) -> None:
    _clear_gateway_env(monkeypatch)
    config_path = tmp_path / "gateway.yaml"
    config_path.write_text(
        "gateway:\n"
        "  cors_origins:\n"
        "    - https://app.example.com\n"
        "    - https://admin.example.com\n"
        "  allowed_models:\n"
        "    - claude-sonnet-4-6\n"
        "    - claude-opus-4-6\n"
        "  telegram_per_turn_timeout: 480\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FINANCE_GATEWAY_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    settings = load_settings()

    assert settings.cors_origins == ["https://app.example.com", "https://admin.example.com"]
    assert settings.allowed_models == ["claude-sonnet-4-6", "claude-opus-4-6"]
    assert settings.telegram_per_turn_timeout == 480


def test_load_settings_env_csv_overrides_gateway_yaml(monkeypatch, tmp_path: Path) -> None:
    _clear_gateway_env(monkeypatch)
    config_path = tmp_path / "gateway.yaml"
    config_path.write_text(
        "cors_origins:\n"
        "  - https://file.example.com\n"
        "allowed_models:\n"
        "  - file-model\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FINANCE_GATEWAY_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_CORS_ORIGINS", "https://env.example.com")
    monkeypatch.setenv("FINANCE_GATEWAY_ALLOWED_MODELS", "env-model-a, env-model-b")

    settings = load_settings()

    assert settings.cors_origins == ["https://env.example.com"]
    assert settings.allowed_models == ["env-model-a", "env-model-b"]


@pytest.mark.parametrize(
    "raw_value",
    [
        "*",
        "https://*.example.com",
        "gateway.example.com",
        "ftp://gateway.example.com",
        "https://user@gateway.example.com",
        "https://gateway.example.com/path",
        "https://gateway.example.com?debug=1",
        "https://gateway.example.com:99999",
    ],
)
def test_load_settings_rejects_invalid_cors_origins(
    monkeypatch,
    raw_value: str,
) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_CORS_ORIGINS", raw_value)

    with pytest.raises(
        ValueError,
        match=r"Missing or invalid config: FINANCE_GATEWAY_CORS_ORIGINS$",
    ):
        load_settings()


def test_load_settings_rejects_wildcard_cors_origin_from_gateway_yaml(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _clear_gateway_env(monkeypatch)
    config_path = tmp_path / "gateway.yaml"
    config_path.write_text("gateway:\n  cors_origins:\n    - '*'\n", encoding="utf-8")
    monkeypatch.setenv("FINANCE_GATEWAY_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    with pytest.raises(
        ValueError,
        match=r"Missing or invalid config: FINANCE_GATEWAY_CORS_ORIGINS$",
    ):
        load_settings()


def test_load_settings_defaults_hardening_values(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    settings = load_settings()

    assert settings.client_timeout == 300.0
    assert settings.interceptor_rate_limit_rpm == 120
    assert settings.interceptor_max_input_bytes == 100_000
    assert settings.web_max_budget_usd == 8.0
    assert settings.web_compaction_trigger == 150_000
    assert settings.telegram_per_turn_timeout == 360


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("true", True),
        ("false", False),
        ("0", False),
        ("1", True),
        ("no", False),
        ("yes", True),
    ],
)
def test_load_settings_parses_thinking_flag(monkeypatch, raw_value: str, expected: bool) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_THINKING", raw_value)

    settings = load_settings()

    assert settings.thinking is expected


def test_load_settings_empty_model_falls_back_to_default(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_MODEL", "   ")

    settings = load_settings()

    assert settings.model == "claude-sonnet-4-6"


def test_load_settings_reports_invalid_port(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_PORT", "not-a-number")

    with pytest.raises(ValueError, match=r"Missing or invalid config: FINANCE_GATEWAY_PORT$"):
        load_settings()


@pytest.mark.parametrize(
    "env_name",
    [
        "FINANCE_GATEWAY_PER_TURN_TIMEOUT",
        "FINANCE_GATEWAY_TELEGRAM_PER_TURN_TIMEOUT",
    ],
)
def test_load_settings_reports_negative_timeout(monkeypatch, env_name: str) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv(env_name, "-5")

    with pytest.raises(
        ValueError,
        match=rf"Missing or invalid config: {env_name}$",
    ):
        load_settings()


def test_load_settings_reports_out_of_range_port(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_PORT", "65536")

    with pytest.raises(ValueError, match=r"Missing or invalid config: FINANCE_GATEWAY_PORT$"):
        load_settings()


def test_load_settings_rejects_unsupported_finance_gateway_api_key(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.delenv("GATEWAY_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("FINANCE_GATEWAY_API_KEY", "gateway-key")
    monkeypatch.setenv("FINANCE_GATEWAY_JWT_SECRET", "jwt-secret")

    with pytest.raises(ValueError, match=r"Missing or invalid config: GATEWAY_API_KEY$"):
        load_settings()


def test_load_settings_rejects_unsupported_gateway_api_key_even_with_user_keys(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_API_KEY", "legacy-key")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())

    with pytest.raises(ValueError, match=r"Missing or invalid config: GATEWAY_API_KEY$"):
        load_settings()


def test_load_settings_summarizes_missing_config_on_one_line(monkeypatch) -> None:
    _clear_gateway_env(monkeypatch)

    with pytest.raises(ValueError) as exc_info:
        load_settings()

    assert (
        str(exc_info.value)
        == "Missing or invalid config: GATEWAY_USER_KEYS, ANTHROPIC_AUTH_TOKEN or DATABASE_URL"
    )
    assert "\n" not in str(exc_info.value)


def test_gateway_settings_requires_jwt_secret_in_production():
    with pytest.raises(ValueError, match="FINANCE_GATEWAY_JWT_SECRET is required"):
        GatewaySettings(
            **{
                "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
                "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
                "FINANCE_GATEWAY_ENV": "production",
            }
        )


@pytest.mark.parametrize("env_val", ["Production", " PRODUCTION ", "  production  "])
def test_gateway_settings_normalizes_env_for_production_check(env_val):
    with pytest.raises(ValueError, match="FINANCE_GATEWAY_JWT_SECRET is required"):
        GatewaySettings(
            **{
                "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
                "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
                "FINANCE_GATEWAY_ENV": env_val,
            }
        )


def test_gateway_settings_autogenerates_jwt_in_dev():
    settings = GatewaySettings(
        **{
            "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
            "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
        }
    )
    assert settings.jwt_secret
    assert len(settings.jwt_secret) == 64


def test_gateway_settings_production_with_jwt_secret():
    settings = GatewaySettings(
        **{
            "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
            "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
            "FINANCE_GATEWAY_JWT_SECRET": "a" * 64,
            "FINANCE_GATEWAY_ENV": "production",
        }
    )
    assert settings.jwt_secret == "a" * 64


def test_gateway_settings_whitespace_jwt_secret_treated_as_empty():
    with pytest.raises(ValueError, match="FINANCE_GATEWAY_JWT_SECRET is required"):
        GatewaySettings(
            **{
                "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
                "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
                "FINANCE_GATEWAY_JWT_SECRET": "   ",
                "FINANCE_GATEWAY_ENV": "production",
            }
        )


def test_gateway_settings_dev_mode_logs_warning():
    # Use a direct handler to avoid caplog propagation issues in full-suite runs
    target_logger = logging.getLogger("finance_cli.gateway.config")
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = lambda r: records.append(r)  # type: ignore[assignment]
    handler.setLevel(logging.WARNING)
    target_logger.addHandler(handler)
    original_level = target_logger.level
    target_logger.setLevel(logging.WARNING)
    try:
        GatewaySettings(
            **{
                "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
                "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
            }
        )
    finally:
        target_logger.removeHandler(handler)
        target_logger.setLevel(original_level)
    assert any("auto-generated ephemeral secret" in r.getMessage() for r in records)


def test_load_settings_raises_in_production_without_jwt_secret(monkeypatch):
    _clear_gateway_env(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token")
    monkeypatch.setenv("GATEWAY_USER_KEYS", _gateway_user_keys())
    monkeypatch.setenv("FINANCE_GATEWAY_ENV", "production")

    with pytest.raises(ValueError, match="FINANCE_GATEWAY_JWT_SECRET is required"):
        load_settings()


@pytest.mark.parametrize(
    ("alias_name", "label"),
    [
        ("RESOLVER_TIMEOUT_SECONDS", "resolver timeout"),
        ("FINANCE_GATEWAY_CLIENT_TIMEOUT", "client timeout"),
        ("FINANCE_GATEWAY_RATE_LIMIT_RPM", "rate limit RPM"),
        ("FINANCE_GATEWAY_MAX_INPUT_BYTES", "max input bytes"),
        ("FINANCE_GATEWAY_WEB_MAX_BUDGET_USD", "web max budget"),
        ("FINANCE_GATEWAY_WEB_COMPACTION_TRIGGER", "web compaction trigger"),
    ],
)
@pytest.mark.parametrize("bad_value", [0, -1])
def test_gateway_settings_requires_positive_hardening_values(
    alias_name: str,
    label: str,
    bad_value: int,
) -> None:
    with pytest.raises(ValueError, match=rf"{re.escape(label)} must be positive, got {bad_value}"):
        GatewaySettings(
            **{
                "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
                "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
                "FINANCE_GATEWAY_JWT_SECRET": "secret",
                alias_name: bad_value,
            }
        )


def test_gateway_settings_requires_session_secret_with_database_url() -> None:
    with pytest.raises(ValueError, match="SESSION_SECRET is required when DATABASE_URL is set"):
        GatewaySettings(
            **{
                "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
                "DATABASE_URL": "postgres://gateway:secret@localhost/finance",
                "FINANCE_GATEWAY_JWT_SECRET": "secret",
            }
        )


def test_gateway_settings_requires_credential_source() -> None:
    with pytest.raises(
        ValueError,
        match="At least one credential source is required: ANTHROPIC_AUTH_TOKEN or DATABASE_URL",
    ):
        GatewaySettings(
            **{
                "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
                "FINANCE_GATEWAY_JWT_SECRET": "secret",
            }
        )


def test_gateway_settings_allows_database_url_without_auth_token() -> None:
    settings = GatewaySettings(
        **{
            "GATEWAY_USER_KEYS": _gateway_user_keys("test-key"),
            "DATABASE_URL": "postgres://gateway:secret@localhost/finance",
            "SESSION_SECRET": "session-secret",
            "FINANCE_GATEWAY_JWT_SECRET": "secret",
        }
    )

    assert settings.anthropic_auth_token == ""
    assert settings.database_url == "postgres://gateway:secret@localhost/finance"
    assert settings.session_secret == "session-secret"
