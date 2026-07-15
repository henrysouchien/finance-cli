from __future__ import annotations

import os
from pathlib import Path
import tomllib
from typing import Any, ClassVar
from urllib.parse import urlsplit

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from finance_cli.exceptions import ConfigurationError


DB_ENCRYPTION_MODE_ENV = "FINANCE_CLI_REQUIRE_DB_ENCRYPTION"
VALID_DB_ENCRYPTION_MODES = ("off", "provision", "require")


def normalize_db_encryption_mode(raw_value: object, *, source: str) -> str:
    normalized = str(raw_value).strip().lower()
    if normalized not in VALID_DB_ENCRYPTION_MODES:
        raise ConfigurationError(
            f"{source}={raw_value!r} (normalized {normalized!r}) "
            f"is not a recognized encryption mode. "
            f"Expected one of {VALID_DB_ENCRYPTION_MODES}. "
            "Use 'require' (prod) or 'provision' (initial setup); 'on'/'true'/'1' are not accepted."
        )
    return normalized


def parse_string_list(value: Any) -> Any:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


def validate_credentialed_cors_origins(value: Any, *, setting_name: str) -> list[str]:
    origins = parse_string_list(value)
    if not isinstance(origins, list):
        raise ValueError(
            f"{setting_name} must be a comma-separated string or list of exact HTTP(S) origins"
        )

    validated: list[str] = []
    for origin in origins:
        if not isinstance(origin, str):
            raise ValueError(
                f"{setting_name} must contain only exact HTTP(S) origin strings"
            )
        raw = origin.strip()
        if not raw:
            continue
        if "*" in raw:
            raise ValueError(
                f"{setting_name} must list exact HTTP(S) origins; wildcard '*' is not allowed "
                "when credentials are enabled"
            )
        if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in raw):
            raise ValueError(
                f"{setting_name} entries must not contain whitespace or control characters"
            )

        parsed = urlsplit(raw)
        try:
            parsed.port
        except ValueError as exc:
            raise ValueError(f"{setting_name} contains an invalid origin port: {raw}") from exc
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc or not parsed.hostname:
            raise ValueError(
                f"{setting_name} must contain exact HTTP(S) origins like https://app.example.com"
            )
        if parsed.username or parsed.password:
            raise ValueError(f"{setting_name} origins must not include credentials")
        if parsed.path or parsed.query or parsed.fragment:
            raise ValueError(
                f"{setting_name} origins must not include path, query, or fragment components"
            )

        validated.append(raw)

    return validated


def _load_config_file(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        parsed = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    elif suffix == ".toml":
        parsed = tomllib.loads(path.read_text(encoding="utf-8")) or {}
    else:
        raise ValueError(f"Unsupported config file extension for {path}; use .yaml, .yml, or .toml")
    if not isinstance(parsed, dict):
        raise ValueError(f"Config file {path} must contain a mapping at the top level")
    return parsed


class FinanceConfigFileSettingsSource(PydanticBaseSettingsSource):
    """Optional YAML/TOML source loaded below env vars.

    Files may use readable field names (``cors_origins``) or validation aliases
    (``CORS_ORIGINS``). Env vars still win because this source is ordered after
    ``env_settings``.
    """

    def get_field_value(self, field, field_name: str):  # type: ignore[no-untyped-def]
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        data = self._load_all_files()
        values: dict[str, Any] = {}
        for field_name, field in self.settings_cls.model_fields.items():
            aliases = self._validation_aliases(field)
            output_key = aliases[0] if aliases else field_name
            for input_key in (field_name, *aliases):
                if input_key in data:
                    values[output_key] = data[input_key]
                    break
        return values

    def _load_all_files(self) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for path in self._configured_paths():
            raw = _load_config_file(path)
            section = getattr(self.settings_cls, "config_file_section", None)
            if section and isinstance(raw.get(section), dict):
                section_data = raw[section]
                root_data = {key: value for key, value in raw.items() if key != section}
                raw = {**root_data, **section_data}
            merged.update(raw)
        return merged

    def _configured_paths(self) -> list[Path]:
        paths: list[Path] = []
        env_vars = getattr(self.settings_cls, "config_file_env_vars", ("FINANCE_CONFIG_FILE",))
        for env_var in env_vars:
            raw = os.environ.get(env_var, "").strip()
            if not raw:
                continue
            for item in raw.split(os.pathsep):
                candidate = item.strip()
                if candidate:
                    paths.append(Path(candidate).expanduser().resolve())
        return paths

    @staticmethod
    def _validation_aliases(field) -> list[str]:  # type: ignore[no-untyped-def]
        alias = field.validation_alias
        if isinstance(alias, str):
            return [alias]
        choices = getattr(alias, "choices", None)
        if choices:
            return [choice for choice in choices if isinstance(choice, str)]
        return []


class FinanceBaseSettings(BaseSettings):
    """Shared config every process respects."""

    config_file_env_vars: ClassVar[tuple[str, ...]] = ("FINANCE_CONFIG_FILE",)
    config_file_section: ClassVar[str | None] = None

    model_config = SettingsConfigDict(
        extra="ignore",
        frozen=True,
        case_sensitive=False,
    )

    log_level: str = "INFO"
    db_encryption_mode: str = Field("off", validation_alias=DB_ENCRYPTION_MODE_ENV)

    @field_validator("db_encryption_mode", mode="before")
    @classmethod
    def _normalize_db_encryption_mode(cls, value):
        return normalize_db_encryption_mode(value, source=DB_ENCRYPTION_MODE_ENV)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            FinanceConfigFileSettingsSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )
