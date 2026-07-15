from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import Field, ValidationError

from finance_cli.exceptions import ConfigurationError
from finance_cli.settings_base import FinanceBaseSettings


class SampleSettings(FinanceBaseSettings):
    sample_value: str = Field("default", validation_alias="SAMPLE_VALUE")


def test_finance_base_settings_subclass_imports_cleanly() -> None:
    settings = SampleSettings()

    assert settings.log_level == "INFO"
    assert settings.db_encryption_mode == "off"
    assert settings.sample_value == "default"


def test_finance_base_settings_is_frozen() -> None:
    settings = SampleSettings(**{"SAMPLE_VALUE": "initial"})

    try:
        settings.sample_value = "updated"
    except ValidationError as exc:
        assert "Instance is frozen" in str(exc)
    else:
        raise AssertionError("Expected frozen settings reassignment to fail")


def test_finance_base_settings_ignores_unknown_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("SAMPLE_VALUE", "from-env")
    monkeypatch.setenv("UNKNOWN_SAMPLE_SETTING", "ignored")

    settings = SampleSettings()

    assert settings.sample_value == "from-env"


def test_finance_base_settings_validates_db_encryption_mode(monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "  Provision  ")

    settings = SampleSettings()

    assert settings.db_encryption_mode == "provision"


def test_finance_base_settings_rejects_invalid_db_encryption_mode(monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "on")

    with pytest.raises(ConfigurationError) as exc:
        SampleSettings()

    assert "FINANCE_CLI_REQUIRE_DB_ENCRYPTION='on'" in str(exc.value)


def test_finance_base_settings_does_not_auto_load_dotenv(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SAMPLE_VALUE", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("SAMPLE_VALUE=from-dotenv\n", encoding="utf-8")

    settings = SampleSettings()

    assert settings.sample_value == "default"


def test_base_does_not_allow_field_name_env_fallback(monkeypatch) -> None:
    """Lock in: by default, env-var matching uses alias only, not field name."""

    class T(FinanceBaseSettings):
        port: int = Field(8003, validation_alias="MCP_REMOTE_PORT")

    monkeypatch.delenv("MCP_REMOTE_PORT", raising=False)
    monkeypatch.setenv("PORT", "9999")

    settings = T()

    assert settings.port == 8003


def test_config_file_source_reads_yaml_field_names_below_env(monkeypatch, tmp_path: Path) -> None:
    class T(FinanceBaseSettings):
        config_file_env_vars = ("SAMPLE_CONFIG_FILE",)

        sample_items: list[str] = Field(default_factory=list, validation_alias="SAMPLE_ITEMS")

    config_path = tmp_path / "settings.yaml"
    config_path.write_text("sample_items:\n  - file-a\n  - file-b\n", encoding="utf-8")
    monkeypatch.setenv("SAMPLE_CONFIG_FILE", str(config_path))

    settings = T()

    assert settings.sample_items == ["file-a", "file-b"]

    monkeypatch.setenv("SAMPLE_ITEMS", '["env-a"]')

    settings = T()

    assert settings.sample_items == ["env-a"]


def test_config_file_source_reads_toml_sections(monkeypatch, tmp_path: Path) -> None:
    class T(FinanceBaseSettings):
        config_file_env_vars = ("SAMPLE_CONFIG_FILE",)
        config_file_section = "sample"

        sample_items: list[str] = Field(default_factory=list, validation_alias="SAMPLE_ITEMS")

    config_path = tmp_path / "settings.toml"
    config_path.write_text(
        "sample_items = ['root-value']\n[sample]\nsample_items = ['section-value']\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("SAMPLE_CONFIG_FILE", str(config_path))

    settings = T()

    assert settings.sample_items == ["section-value"]
