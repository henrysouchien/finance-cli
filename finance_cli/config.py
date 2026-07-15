"""Configuration and path helpers for finance_cli."""

from __future__ import annotations

import logging
import os
import re
import shutil
import sqlite3
import sys
from pathlib import Path

from pydantic import Field, ValidationError, field_validator
from finance_cli.settings_base import FinanceBaseSettings

log = logging.getLogger(__name__)

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
PACKAGE_TEMPLATE_DIR = PACKAGE_DIR / "data"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Legacy alias — some docs/planning files reference this name.
DEFAULT_DATA_DIR = PACKAGE_TEMPLATE_DIR


def _platform_data_dir() -> Path:
    """Return the OS-standard user data directory for finance_cli."""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "finance_cli"
    return Path.home() / ".local" / "share" / "finance_cli"


class CliSettings(FinanceBaseSettings):
    """Typed env surface for the deep CLI path helpers."""

    data_dir: Path | None = Field(None, validation_alias="FINANCE_CLI_DATA_DIR")
    db_path: Path | None = Field(None, validation_alias="FINANCE_CLI_DB")
    user_id: str = Field("default", validation_alias="FINANCE_CLI_USER_ID")
    web_data_root: Path | None = Field(None, validation_alias="FINANCE_WEB_DATA_ROOT")
    log_level: str = Field("INFO", validation_alias="FINANCE_CLI_LOG_LEVEL")

    @field_validator("data_dir", "db_path", "web_data_root", mode="before")
    @classmethod
    def _strip_optional_paths(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value

    @field_validator("user_id", mode="before")
    @classmethod
    def _normalize_user_id(cls, value):
        return str(value or "default").strip() or "default"

    @field_validator("log_level", mode="before")
    @classmethod
    def _strip_log_level(cls, value):
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("data_dir", "db_path", "web_data_root", mode="after")
    @classmethod
    def _normalize_paths(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        return value.expanduser().resolve()

    @classmethod
    def from_env(cls) -> "CliSettings":
        try:
            return cls()
        except ValidationError as exc:
            log.error("CliSettings validation failed: %s", exc)
            fields = cls._summarize_validation_errors(exc)
            raise ValueError(f"Missing or invalid config: {', '.join(fields)}") from exc

    @classmethod
    def _summarize_validation_errors(cls, exc: ValidationError) -> list[str]:
        fields: list[str] = []
        for error in exc.errors():
            loc = error.get("loc", ())
            if not loc:
                continue
            label = str(loc[0])
            if label not in fields:
                fields.append(label)
        return fields or ["unknown"]


_RUNTIME_CLI_SETTINGS: CliSettings | None = None


def set_runtime_cli_settings(settings: CliSettings | None) -> CliSettings | None:
    """Override CLI path settings for the current process.

    Intended for entry points like local MCP that need deterministic paths
    without mutating ``os.environ`` before importing shared tool modules.
    Returns the previous override so tests can restore it.
    """
    global _RUNTIME_CLI_SETTINGS
    previous = _RUNTIME_CLI_SETTINGS
    _RUNTIME_CLI_SETTINGS = settings
    return previous


def runtime_cli_settings() -> CliSettings:
    """Return the active process-local CLI settings."""
    return _RUNTIME_CLI_SETTINGS or CliSettings()


def get_data_dir() -> Path:
    """Return the active data directory.

    Resolution order:
    1. ``FINANCE_CLI_DATA_DIR`` env var (explicit override)
    2. Parent directory of ``FINANCE_CLI_DB`` (co-locate rules/sessions with DB)
    3. Platform-appropriate XDG / Application Support path
    """
    settings = runtime_cli_settings()
    if settings.data_dir:
        return settings.data_dir
    if settings.db_path:
        return settings.db_path.parent
    return _platform_data_dir()


def get_db_path() -> Path:
    """Return the active database path.

    Resolution order:
    1. ``FINANCE_CLI_DB`` env var (DB-only override)
    2. ``get_data_dir() / "finance.db"``
    """
    settings = runtime_cli_settings()
    if settings.db_path:
        return settings.db_path
    return get_data_dir() / "finance.db"


def ensure_data_dir() -> Path:
    """Ensure the data directory exists and return it."""
    data_dir = get_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_default_user_id() -> str:
    """Return the single-user CLI fallback user id."""
    return runtime_cli_settings().user_id


def __getattr__(name: str):
    if name == "default_user_id":
        return get_default_user_id()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


_MIGRATE_MARKER = "_migrated_from.txt"
_MIGRATE_FILES = ["finance.db", "rules.yaml", "agent_memory.md"]
_MIGRATE_DIRS = ["sessions", "backups"]


def auto_migrate_data() -> Path | None:
    """One-time migration from legacy package-dir data to XDG path.

    Copies data files from ``PACKAGE_TEMPLATE_DIR`` (the old default inside the
    package tree) to the platform data directory.  Only runs when:
    - No ``FINANCE_CLI_DATA_DIR`` or ``FINANCE_CLI_DB`` env vars are set
    - The old location has ``finance.db``
    - The new location does NOT have ``finance.db``

    Returns the new data dir on success, or None if migration was skipped.
    """
    runtime_settings = _RUNTIME_CLI_SETTINGS
    if runtime_settings is not None and (runtime_settings.data_dir or runtime_settings.db_path):
        return None
    if os.getenv("FINANCE_CLI_DATA_DIR") or os.getenv("FINANCE_CLI_DB"):
        return None

    target = _platform_data_dir()
    legacy = PACKAGE_TEMPLATE_DIR

    if not (legacy / "finance.db").exists():
        return None
    if (target / "finance.db").exists():
        return None
    if (target / _MIGRATE_MARKER).exists():
        return None

    # WAL checkpoint — ensure all data is in the main DB file before copy
    try:
        conn = sqlite3.connect(str(legacy / "finance.db"))
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except Exception:
        pass

    target.mkdir(parents=True, exist_ok=True)

    copied: list[str] = []
    for name in _MIGRATE_FILES:
        src = legacy / name
        dst = target / name
        if src.exists() and not dst.exists():
            shutil.copy2(str(src), str(dst))
            copied.append(name)

    for name in _MIGRATE_DIRS:
        src = legacy / name
        dst = target / name
        if src.is_dir() and not dst.exists():
            shutil.copytree(str(src), str(dst))
            copied.append(f"{name}/")

    from datetime import datetime, timezone

    (target / _MIGRATE_MARKER).write_text(
        f"Migrated from {legacy}\nat {datetime.now(timezone.utc).isoformat()}\n"
        f"Copied: {', '.join(copied)}\n",
        encoding="utf-8",
    )

    log.info("Migrated data from %s to %s: %s", legacy, target, ", ".join(copied))
    return target


def _parse_env_line(line: str) -> tuple[str, str] | None:
    candidate = line.strip()
    if not candidate or candidate.startswith("#"):
        return None

    if candidate.startswith("export "):
        candidate = candidate[7:].strip()

    if "=" not in candidate:
        return None

    key, value = candidate.split("=", 1)
    key = key.strip()
    if not ENV_KEY_RE.match(key):
        return None

    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    elif " #" in value:
        value = value.split(" #", 1)[0].rstrip()

    return key, value


def load_dotenv(path: Path | None = None, override: bool = False) -> Path | None:
    """Load key=value pairs from a .env file into process env vars."""
    disable = str(os.getenv("FINANCE_CLI_DISABLE_DOTENV") or "").strip().lower()
    if disable in {"1", "true", "yes"}:
        return None

    env_path = path
    if env_path is None:
        env_override = os.getenv("FINANCE_CLI_ENV_FILE")
        env_path = Path(env_override).expanduser() if env_override else DEFAULT_ENV_PATH
    env_path = env_path.resolve()

    if not env_path.exists() or not env_path.is_file():
        return None

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_env_line(raw_line)
        if not parsed:
            continue
        key, value = parsed
        if not override and key in os.environ:
            continue
        os.environ[key] = value

    return env_path
