from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_aws_local_credential_posture.py"
SPEC = importlib.util.spec_from_file_location("check_aws_local_credential_posture", SCRIPT_PATH)
assert SPEC is not None
check_aws_local_credential_posture = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = check_aws_local_credential_posture
SPEC.loader.exec_module(check_aws_local_credential_posture)


def _write_aws_files(tmp_path: Path, *, config: str, credentials: str = "") -> Path:
    home = tmp_path / "home"
    aws_dir = home / ".aws"
    aws_dir.mkdir(parents=True)
    (aws_dir / "config").write_text(config)
    if credentials:
        (aws_dir / "credentials").write_text(credentials)
    return home


def test_collect_posture_fails_without_mfa_or_sso_and_plaintext_default_key(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[default]
region = us-east-2

[profile finance-web-deploy]
region = us-east-2
""",
        credentials="""
[default]
aws_access_key_id = AKIA_DO_NOT_PRINT
aws_secret_access_key = secret-do-not-print
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["profile_has_mfa_serial"] is False
    assert result["checks"]["profile_has_sso_config"] is False
    assert result["checks"]["plaintext_credential_sections"] == ["default"]
    assert "AKIA_DO_NOT_PRINT" not in json.dumps(result)
    assert "secret-do-not-print" not in json.dumps(result)


def test_collect_posture_accepts_sso_config_without_plaintext_keys(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_session = cashnerd
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
region = us-east-2

[sso-session cashnerd]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is True
    assert result["checks"]["profile_has_sso_config"] is True
    assert result["checks"]["plaintext_credential_sections"] == []


def test_collect_posture_rejects_sso_config_without_aws_cli(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_session = cashnerd
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
region = us-east-2

[sso-session cashnerd]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
""",
    )
    monkeypatch.setattr(
        check_aws_local_credential_posture,
        "_command_version",
        lambda command: None if command == "aws" else "aws-vault 7.test",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert "AWS CLI is not installed/available" in result["findings"]


def test_collect_posture_rejects_incomplete_sso_config(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_session = cashnerd
region = us-east-2
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["profile_has_sso_config"] is False


def test_collect_posture_rejects_sso_session_name_mismatch(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_session = cashnerd
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
region = us-east-2

[sso-session other]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["profile_has_sso_config"] is False


def test_collect_posture_rejects_token_provider_sso_without_account_role(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_session = cashnerd
region = us-east-2

[sso-session cashnerd]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["profile_has_sso_config"] is False


def test_collect_posture_accepts_legacy_sso_shape_without_session_section(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is True
    assert result["checks"]["profile_has_sso_config"] is True


def test_collect_posture_rejects_empty_legacy_sso_values(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_start_url =
sso_region =
sso_account_id =
sso_role_name =
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["profile_has_sso_config"] is False


def test_collect_posture_rejects_empty_token_provider_sso_values(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_session = cashnerd
sso_account_id =
sso_role_name =

[sso-session cashnerd]
sso_start_url =
sso_region =
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["profile_has_sso_config"] is False


def test_collect_posture_detects_case_insensitive_credential_keys(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_session = cashnerd
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
""",
        credentials="""
[default]
AWS_ACCESS_KEY_ID = AKIA_DO_NOT_PRINT
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["plaintext_credential_sections"] == ["default"]
    assert "AKIA_DO_NOT_PRINT" not in json.dumps(result)


def test_collect_posture_fails_on_static_keys_in_config(tmp_path: Path) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[default]
aws_access_key_id = AKIA_DO_NOT_PRINT
aws_secret_access_key = secret-do-not-print

[profile finance-web-deploy]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
""",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["plaintext_config_sections"] == ["default"]
    assert "secret-do-not-print" not in json.dumps(result)


def test_collect_posture_fails_on_static_env_keys(tmp_path: Path, monkeypatch) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
""",
    )
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA_DO_NOT_PRINT")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-do-not-print")

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["env_static_key_present"]["AWS_ACCESS_KEY_ID"] is True
    assert "static AWS access-key environment variables are set" in result["findings"]
    assert "secret-do-not-print" not in json.dumps(result)


def test_collect_posture_honors_aws_config_file_overrides(tmp_path: Path, monkeypatch) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_start_url = https://default.example/start
sso_region = us-east-2
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
""",
    )
    alternate_config = tmp_path / "alt-config"
    alternate_credentials = tmp_path / "alt-credentials"
    alternate_config.write_text("[profile finance-web-deploy]\nregion = us-east-2\n")
    alternate_credentials.write_text("[alt]\nAWS_ACCESS_KEY_ID = AKIA_DO_NOT_PRINT\n")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(alternate_config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(alternate_credentials))

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["files"]["config"]["path"] == str(alternate_config)
    assert result["files"]["credentials"]["path"] == str(alternate_credentials)
    assert result["checks"]["plaintext_credential_sections"] == ["alt"]


def test_main_handles_malformed_credentials_without_leaking_values(tmp_path: Path, capsys) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-2
sso_account_id = 948633118115
sso_role_name = CashNerdAdmin
""",
        credentials="[default]\naws_secret_access_key secret-do-not-print\n",
    )

    exit_code = check_aws_local_credential_posture.main(["--home", str(home), "--json"])
    captured = capsys.readouterr()

    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["files"]["credentials"]["parse_error"] == "ParsingError"
    assert "secret-do-not-print" not in captured.out


def test_main_handles_malformed_config_without_crashing_or_leaking_values(
    tmp_path: Path,
    capsys,
) -> None:
    home = _write_aws_files(
        tmp_path,
        config="[profile finance-web-deploy\nregion = must-not-print\n",
    )

    exit_code = check_aws_local_credential_posture.main(["--home", str(home), "--json"])
    captured = capsys.readouterr()

    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["files"]["config"]["parse_error"] == "MissingSectionHeaderError"
    assert "must-not-print" not in captured.out


def test_collect_posture_accepts_mfa_serial_when_aws_vault_available(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
region = us-east-2
mfa_serial = arn:aws:iam::948633118115:mfa/finance-web-deploy
""",
    )
    monkeypatch.setattr(
        check_aws_local_credential_posture,
        "_command_version",
        lambda command: "aws-vault 7.test" if command == "aws-vault" else "aws-cli/2.test",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is True
    assert result["checks"]["profile_has_mfa_serial"] is True
    assert result["findings"] == []


def test_collect_posture_rejects_blank_mfa_serial_even_when_aws_vault_available(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = _write_aws_files(
        tmp_path,
        config="""
[profile finance-web-deploy]
region = us-east-2
mfa_serial =
""",
    )
    monkeypatch.setattr(
        check_aws_local_credential_posture,
        "_command_version",
        lambda command: "aws-vault 7.test" if command == "aws-vault" else "aws-cli/2.test",
    )

    result = check_aws_local_credential_posture.collect_posture(home=home)

    assert result["ok"] is False
    assert result["checks"]["profile_has_mfa_serial"] is False
    assert "has no mfa_serial or IAM Identity Center/SSO keys" in result["findings"][0]


def test_main_json_output_does_not_leak_credential_values(tmp_path: Path, capsys) -> None:
    home = _write_aws_files(
        tmp_path,
        config="[profile finance-web-deploy]\nregion = us-east-2\n",
        credentials="[default]\naws_secret_access_key = must-not-print\n",
    )

    exit_code = check_aws_local_credential_posture.main(["--home", str(home), "--json"])
    captured = capsys.readouterr()

    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["checks"]["plaintext_credential_sections"] == ["default"]
    assert "must-not-print" not in captured.out
