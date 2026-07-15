"""Shared response sanitization helpers for MCP and sync proxy responses."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_PATH_SANITIZE_EXACT_KEYS = {
    "backup_path",
    "cache_file",
    "export_path",
    "file",
    "file_path",
    "files",
    "output",
    "path",
}
_PATH_SANITIZE_SUFFIXES = (
    "_dir",
    "_file",
    "_files",
    "_path",
    "_paths",
)
_PATH_TOKEN_RE = re.compile(
    r"""(?P<prefix>^|[\s'"([{=,;])(?P<path>/(?!/)[^\s'")\]}]+)"""
)
_SERVER_PATH_SEGMENT_RE = re.compile(r"""(?:^|/)(?:data|var)(?:/|$)""")
_SCRUB_STRING_KEYS = {"error", "errors", "warnings", "message", "traceback", "cli_report"}

SECRET_KEYS = frozenset(
    {
        "access_token_ref",
        "api_key_ref",
        "bot_token_ref",
        "webhook_secret",
        "sync_cursor",
        "next_cursor",
    }
)


def _is_path_field(key: str) -> bool:
    lowered = key.lower()
    if lowered in _PATH_SANITIZE_EXACT_KEYS:
        return True
    return any(lowered.endswith(suffix) for suffix in _PATH_SANITIZE_SUFFIXES)


def _sanitize_cache_payload(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[Any, Any] = {}
        for key, item in value.items():
            if isinstance(key, str) and _is_path_field(key):
                if isinstance(item, str):
                    sanitized[key] = Path(item).name if item.startswith("/") else item
                    continue
                if isinstance(item, list):
                    sanitized[key] = [
                        Path(entry).name if isinstance(entry, str) and entry.startswith("/") else entry
                        for entry in item
                    ]
                    continue
            if isinstance(key, str) and key.lower() in _SCRUB_STRING_KEYS:
                sanitized[key] = _scrub_server_paths(item)
                continue
            sanitized[key] = _sanitize_cache_payload(item)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_cache_payload(item) for item in value]
    return value


def _scrub_server_paths_text(text: str) -> str:
    """Replace absolute filesystem path tokens containing server roots with basenames."""

    def _replace_path(match: re.Match[str]) -> str:
        path = match.group("path")
        if not _SERVER_PATH_SEGMENT_RE.search(path):
            return match.group(0)
        return f"{match.group('prefix')}{Path(path).name}"

    return _PATH_TOKEN_RE.sub(_replace_path, text)


def _scrub_server_paths(item: Any) -> Any:
    """Replace server paths (/data/... or /var/...) with basenames in strings/lists."""
    if isinstance(item, str):
        return _scrub_server_paths_text(item)
    if isinstance(item, list):
        return [
            _scrub_server_paths_text(entry)
            if isinstance(entry, str)
            else _sanitize_cache_payload(entry)
            for entry in item
        ]
    return _sanitize_cache_payload(item)


def _strip_secret_keys(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[Any, Any] = {}
        for key, item in value.items():
            # `link_token` is intentionally preserved. The local agent needs the
            # short-lived capability token to open Plaid Hosted Link flows.
            if isinstance(key, str) and key.lower() in SECRET_KEYS:
                continue
            sanitized[key] = _strip_secret_keys(item)
        return sanitized
    if isinstance(value, list):
        return [_strip_secret_keys(item) for item in value]
    return value


def sanitize_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    """Return a deep-copied envelope with paths scrubbed and secrets removed."""
    return _strip_secret_keys(_sanitize_cache_payload(envelope))
