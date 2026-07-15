"""Local JSON-backed skill state storage.

This mirrors the small SkillStateStore interface used by agent-gateway without
requiring the agent-gateway package at import time.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Failed to parse state file %s: %s", path, exc)
        return {}

    if isinstance(payload, dict):
        return payload

    log.warning("State file %s is not a JSON object", path)
    return {}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


class SkillStateStore:
    """Persist per-skill JSON state in a single file."""

    def __init__(self, state_file: str | Path):
        self.state_file = Path(state_file)

    def _read_all(self) -> dict[str, Any]:
        return _read_json_object(self.state_file)

    def get(self, skill_name: str) -> dict[str, Any]:
        payload = self._read_all()
        state = payload.get(skill_name)
        if isinstance(state, dict):
            return dict(state)
        return {}

    def set(self, skill_name: str, state: dict[str, Any]) -> None:
        if not isinstance(state, dict):
            raise TypeError("state must be a dict")
        payload = self._read_all()
        payload[str(skill_name)] = dict(state)
        _atomic_write_json(self.state_file, payload)

    def clear(self, skill_name: str) -> None:
        if not self.state_file.exists():
            return
        payload = self._read_all()
        if skill_name not in payload:
            return
        payload.pop(skill_name, None)
        _atomic_write_json(self.state_file, payload)
