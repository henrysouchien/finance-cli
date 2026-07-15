"""Blocking advisory lock for single-flight bootstrap operations."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path


class InstallBootstrapLock:
    def __init__(self, lock_path: Path) -> None:
        self._lock_path = Path(lock_path)
        self._fd: int | None = None

    def __enter__(self) -> InstallBootstrapLock:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(str(self._lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *_exc_info: object) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None
