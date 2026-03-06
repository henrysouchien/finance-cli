"""Configuration and path helpers for finance_cli."""

from __future__ import annotations

import os
import re
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
# TODO(CQ-009): Before packaging, move default data dir to
# ~/.local/share/finance_cli/ and add a one-time migration from this location.
DEFAULT_DATA_DIR = PACKAGE_DIR / "data"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "finance.db"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def get_db_path() -> Path:
    override = os.getenv("FINANCE_CLI_DB")
    if override:
        return Path(override).expanduser().resolve()
    return DEFAULT_DB_PATH


def ensure_data_dir() -> Path:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path.parent


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
