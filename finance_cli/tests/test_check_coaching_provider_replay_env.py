from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "check_coaching_provider_replay_env.py"
)
SPEC = importlib.util.spec_from_file_location("check_coaching_provider_replay_env", SCRIPT_PATH)
assert SPEC is not None
check_coaching_provider_replay_env = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(check_coaching_provider_replay_env)


def _gateway_user_keys(key: str = "gw-secret-value") -> str:
    return json.dumps(
        [
            {
                "key": key,
                "channel": "web",
                "user_id": "1",
                "email": "user@example.com",
                "role": "owner",
            }
        ]
    )


def test_check_values_accepts_auth_token_source_without_leaking_secret() -> None:
    secret = "sk-ant-oat-do-not-print"
    result = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": _gateway_user_keys("gateway-do-not-print"),
            "ANTHROPIC_AUTH_TOKEN": secret,
        }
    )

    assert result["ok"] is True
    assert result["checks"]["gateway_user_keys_valid"] is True
    assert result["checks"]["gateway_user_keys_count"] == 1
    assert result["checks"]["credential_source"] == "ANTHROPIC_AUTH_TOKEN"
    assert "gateway-do-not-print" not in json.dumps(result)
    assert secret not in json.dumps(result)


def test_check_values_accepts_database_source_with_session_secret() -> None:
    result = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": _gateway_user_keys(),
            "DATABASE_URL": "postgres://gateway:secret@localhost/finance",
            "SESSION_SECRET": "session-secret-do-not-print",
        }
    )

    assert result["ok"] is True
    assert result["checks"]["credential_source"] == "DATABASE_URL+SESSION_SECRET"
    assert "session-secret-do-not-print" not in json.dumps(result)


def test_check_values_rejects_api_key_without_gateway_credential_source() -> None:
    result = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": _gateway_user_keys(),
            "ANTHROPIC_API_KEY": "sk-ant-do-not-print",
        }
    )

    assert result["ok"] is False
    assert result["checks"]["unrecognized_provider_keys_present"] == {
        "ANTHROPIC_API_KEY": True,
        "OPENAI_API_KEY": False,
    }
    assert any(
        "ANTHROPIC_API_KEY/OPENAI_API_KEY alone" in finding
        for finding in result["findings"]
    )
    assert "sk-ant-do-not-print" not in json.dumps(result)


def test_check_values_rejects_unsupported_gateway_key_aliases() -> None:
    result = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": _gateway_user_keys(),
            "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-do-not-print",
            "GATEWAY_API_KEY": "legacy-gateway-secret-do-not-print",
        }
    )
    output = json.dumps(result)

    assert result["ok"] is False
    assert result["checks"]["unsupported_gateway_keys_present"] == {
        "GATEWAY_API_KEY": True,
        "FINANCE_GATEWAY_API_KEY": False,
    }
    assert any("are unsupported" in finding for finding in result["findings"])
    assert "legacy-gateway-secret-do-not-print" not in output
    assert "sk-ant-oat-do-not-print" not in output


def test_check_values_rejects_missing_or_invalid_gateway_user_keys() -> None:
    missing = check_coaching_provider_replay_env.check_values(
        {"ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test"}
    )
    invalid = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": "not-json",
            "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-test",
        }
    )

    assert missing["ok"] is False
    assert missing["checks"]["gateway_user_keys_present"] is False
    assert "GATEWAY_USER_KEYS is required" in missing["findings"]
    assert invalid["ok"] is False
    assert invalid["checks"]["gateway_user_keys_present"] is True
    assert "GATEWAY_USER_KEYS must be valid JSON" in invalid["findings"]


def test_check_values_sanitizes_gateway_user_key_schema_errors() -> None:
    result = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": _gateway_user_keys("gateway-do-not-print").replace(
                '"user_id": "1"',
                '"user_id": "user-secret-do-not-print"',
            ),
            "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-do-not-print",
        }
    )
    output = json.dumps(result)

    assert result["ok"] is False
    assert "user-secret-do-not-print" not in output
    assert "gateway-do-not-print" not in output
    assert "sk-ant-oat-do-not-print" not in output
    assert "(got invalid value)" in output


def test_check_values_rejects_production_without_gateway_jwt_secret() -> None:
    result = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": _gateway_user_keys(),
            "ANTHROPIC_AUTH_TOKEN": "sk-ant-oat-do-not-print",
            "FINANCE_GATEWAY_ENV": "production",
        }
    )
    output = json.dumps(result)

    assert result["ok"] is False
    assert result["checks"]["production_mode"] is True
    assert result["checks"]["finance_gateway_jwt_secret_present"] is False
    assert any(
        "FINANCE_GATEWAY_JWT_SECRET is required" in finding
        for finding in result["findings"]
    )
    assert "sk-ant-oat-do-not-print" not in output


def test_check_values_rejects_database_url_without_session_secret() -> None:
    result = check_coaching_provider_replay_env.check_values(
        {
            "GATEWAY_USER_KEYS": _gateway_user_keys(),
            "DATABASE_URL": "postgres://gateway:secret@localhost/finance",
        }
    )

    assert result["ok"] is False
    assert result["checks"]["database_url_present"] is True
    assert result["checks"]["session_secret_present"] is False
    assert "DATABASE_URL is set but SESSION_SECRET is missing" in result["findings"]


def test_main_reads_env_file_and_returns_failure_without_leaking_secret(
    tmp_path: Path,
    capsys,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"GATEWAY_USER_KEYS={_gateway_user_keys('gateway-do-not-print')}",
                "ANTHROPIC_API_KEY=sk-ant-do-not-print",
            ]
        )
    )

    exit_code = check_coaching_provider_replay_env.main(["--env-file", str(env_file)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "ok=false" in captured.out
    assert "ANTHROPIC_API_KEY: present=True" in captured.out
    assert "gateway-do-not-print" not in captured.out
    assert "sk-ant-do-not-print" not in captured.out


def test_main_json_output_is_non_secret(tmp_path: Path, capsys) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"export GATEWAY_USER_KEYS={_gateway_user_keys('gateway-do-not-print')}",
                "export ANTHROPIC_AUTH_TOKEN=sk-ant-oat-do-not-print",
            ]
        )
    )

    exit_code = check_coaching_provider_replay_env.main(
        ["--env-file", str(env_file), "--json"]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    payload = json.loads(output)
    assert payload["ok"] is True
    assert payload["checks"]["credential_source"] == "ANTHROPIC_AUTH_TOKEN"
    assert "gateway-do-not-print" not in output
    assert "sk-ant-oat-do-not-print" not in output
