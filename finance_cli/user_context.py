from __future__ import annotations

import contextvars
from dataclasses import dataclass
from pathlib import Path


def _normalize_path(path: str | Path | None) -> str | None:
    if path is None:
        return None
    return str(Path(path).expanduser().resolve())


@dataclass(frozen=True)
class UserContext:
    """Per-request user identity and resource scoping for MCP tools."""

    db_path: str
    expected_user_id: str | None
    workspace_name: str
    rules_path: str | None = None
    uploads_dir: str | None = None
    local_mode: bool = False
    storage_mode: str | None = None

    @classmethod
    def from_paths(
        cls,
        db_path: str | Path,
        *,
        expected_user_id: str | None = None,
        rules_path: str | Path | None = None,
        uploads_dir: str | Path | None = None,
        local_mode: bool = False,
        storage_mode: str | None = None,
    ) -> "UserContext":
        resolved_db_path = Path(db_path).expanduser().resolve()
        normalized_storage_mode = (
            str(storage_mode).strip().lower() if storage_mode is not None else None
        )
        return cls(
            expected_user_id=str(expected_user_id) if expected_user_id else None,
            workspace_name=resolved_db_path.parent.name,
            db_path=str(resolved_db_path),
            rules_path=_normalize_path(rules_path),
            uploads_dir=_normalize_path(uploads_dir),
            local_mode=local_mode,
            storage_mode=normalized_storage_mode or None,
        )

    def __repr__(self) -> str:
        return (
            "UserContext("
            f"expected_user_id={self.expected_user_id!r}, "
            f"workspace_name={self.workspace_name!r}, "
            f"local_mode={self.local_mode!r}, "
            f"storage_mode={self.storage_mode!r}"
            ")"
        )


_user_context_var: contextvars.ContextVar[UserContext | None] = contextvars.ContextVar(
    "user_context",
    default=None,
)


def get_user_context() -> UserContext | None:
    return _user_context_var.get()


def set_user_context(ctx: UserContext) -> contextvars.Token:
    return _user_context_var.set(ctx)


def reset_user_context(token: contextvars.Token) -> None:
    _user_context_var.reset(token)


def current_expected_user_id() -> str | None:
    ctx = get_user_context()
    return ctx.expected_user_id if ctx else None


def current_db_path() -> str | None:
    ctx = get_user_context()
    return ctx.db_path if ctx else None


def current_rules_path() -> str | None:
    ctx = get_user_context()
    return ctx.rules_path if ctx else None


def current_uploads_dir() -> str | None:
    ctx = get_user_context()
    return ctx.uploads_dir if ctx else None


def current_local_mode() -> bool:
    ctx = get_user_context()
    return ctx.local_mode if ctx else False


def current_storage_mode() -> str | None:
    ctx = get_user_context()
    return ctx.storage_mode if ctx else None
