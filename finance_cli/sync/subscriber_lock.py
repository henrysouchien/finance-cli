"""Advisory file lock for single-subscriber-per-install coordination."""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class InstallSubscriberLock:
    """Coordinates at most one active subscriber for a local install."""

    def __init__(self, lock_path: Path) -> None:
        self._lock_path = Path(lock_path)
        self._fd: int | None = None

    @property
    def is_held(self) -> bool:
        return self._fd is not None

    def try_acquire(self) -> bool:
        if self._fd is not None:
            return True
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self._lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None


@contextmanager
def acquire_install_lock_for_destructive_op(lock_path: Path, operation: str) -> Iterator[None]:
    """Hold the install subscriber lock for a whole-DB destructive operation."""

    lock = InstallSubscriberLock(lock_path)
    if not lock.try_acquire():
        from .exceptions import SubscriberActiveError

        operation_label = str(operation or "operate").strip() or "operate"
        raise SubscriberActiveError(
            f"Cannot {operation_label}: another CashNerd local MCP process is running. "
            "Stop it (e.g., close Claude Code or kill mcp_local) and retry."
        )
    try:
        yield
    finally:
        lock.release()


@contextmanager
def acquire_install_lock_for_restore(lock_path: Path) -> Iterator[None]:
    """Hold the install subscriber lock for the duration of a DB restore."""

    with acquire_install_lock_for_destructive_op(lock_path, "restore"):
        yield
