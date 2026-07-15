"""Flat-file agent memory handlers."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from finance_cli import storage_files
from finance_cli.config import ensure_data_dir
from finance_cli.storage_client import _dispatch as storage_dispatch
from finance_cli.storage_lease import optional_lease_scope

MEMORY_FILENAME = "agent_memory.md"
SESSIONS_DIR = "sessions"
MAX_BYTES = 12288
MAX_LINES = 120


def _memory_path(data_dir: Path | None = None) -> Path:
    return (data_dir or ensure_data_dir()) / MEMORY_FILENAME


def _sessions_dir(data_dir: Path | None = None) -> Path:
    return (data_dir or ensure_data_dir()) / SESSIONS_DIR


def _remote_target_for_data_dir(data_dir: Path | None) -> tuple[str | None, str | None]:
    resolved_data_dir = data_dir or ensure_data_dir()
    user_id = storage_dispatch.user_id_from_data_dir(resolved_data_dir)
    if user_id is None:
        return None, None
    return storage_dispatch.remote_file_target_for_user(user_id), user_id


def _lease_user_for_data_dir(data_dir: Path | None) -> str | None:
    return storage_dispatch.user_id_from_data_dir(data_dir or ensure_data_dir())


def _remote_file_exists(target: str, *, user_id: str, relative_path: str) -> bool:
    parent = str(Path(relative_path).parent)
    prefix = "" if parent == "." else parent
    return relative_path in storage_files.list_files(
        target,
        user_id=user_id,
        product="finance_cli",
        prefix=prefix,
    )


def handle_read(args, conn, data_dir: Path | None = None) -> dict[str, Any]:
    """Read agent memory file."""
    with optional_lease_scope(
        _lease_user_for_data_dir(data_dir),
        operation="mcp",
        metadata={"source": "memory.handle_read"},
    ):
        path = _memory_path(data_dir=data_dir)
        if not path.exists():
            return {
                "data": {"content": "", "exists": False},
                "summary": {"message": "No memory stored yet."},
            }

        content = path.read_text(encoding="utf-8")
    return {
        "data": {"content": content, "exists": True},
        "summary": {"message": f"Memory loaded ({len(content)} bytes)."},
    }


def handle_update(args, conn, data_dir: Path | None = None) -> dict[str, Any]:
    """Overwrite agent memory file (full replace)."""
    content = args.content
    byte_count = len(content.encode("utf-8"))
    if byte_count > MAX_BYTES:
        raise ValueError(f"Content exceeds {MAX_BYTES} byte limit.")
    if content.count("\n") + 1 > MAX_LINES:
        raise ValueError(f"Content exceeds {MAX_LINES} line limit.")

    with optional_lease_scope(
        _lease_user_for_data_dir(data_dir),
        operation="mcp",
        metadata={"source": "memory.handle_update"},
    ):
        path = _memory_path(data_dir=data_dir)
        remote_target, remote_user_id = _remote_target_for_data_dir(data_dir)
        if remote_target and remote_user_id:
            storage_files.write_file(
                remote_target,
                user_id=remote_user_id,
                product="finance_cli",
                relative_path=MEMORY_FILENAME,
                content=content.encode("utf-8"),
            )
            return {
                "data": {"ok": True, "bytes": byte_count},
                "summary": {"message": f"Memory updated ({byte_count} bytes)."},
            }

        tmp = path.with_suffix(".md.tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
    return {
        "data": {"ok": True, "bytes": byte_count},
        "summary": {"message": f"Memory updated ({byte_count} bytes)."},
    }


def handle_session_write(args, conn, data_dir: Path | None = None) -> dict[str, Any]:
    """Append a session note to today's file."""
    del conn
    content = args.content
    today = date.today().isoformat()
    with optional_lease_scope(
        _lease_user_for_data_dir(data_dir),
        operation="mcp",
        metadata={"source": "memory.handle_session_write"},
    ):
        path = _sessions_dir(data_dir=data_dir) / f"{today}.md"
        path.parent.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%H:%M")
        entry = f"\n## {timestamp}\n{content}\n"

        remote_target, remote_user_id = _remote_target_for_data_dir(data_dir)
        if remote_target and remote_user_id:
            relative_path = f"{SESSIONS_DIR}/{path.name}"
            existing = b""
            if _remote_file_exists(remote_target, user_id=remote_user_id, relative_path=relative_path):
                existing = storage_files.read_file(
                    remote_target,
                    user_id=remote_user_id,
                    product="finance_cli",
                    relative_path=relative_path,
                )
            storage_files.write_file(
                remote_target,
                user_id=remote_user_id,
                product="finance_cli",
                relative_path=relative_path,
                content=existing + entry.encode("utf-8"),
            )
            return {
                "data": {"ok": True, "date": today, "bytes": len(entry.encode("utf-8"))},
                "summary": {"message": f"Session note saved ({today})."},
            }

        with open(path, "a", encoding="utf-8") as f:
            f.write(entry)

    return {
        "data": {"ok": True, "date": today, "bytes": len(entry.encode("utf-8"))},
        "summary": {"message": f"Session note saved ({today})."},
    }


def _parse_date_stem(stem: str) -> date | None:
    """Parse YYYY-MM-DD filename stem, return None for malformed."""
    try:
        return date.fromisoformat(stem)
    except (ValueError, TypeError):
        return None


def _extract_matching_sections(content: str, query: str, max_chars: int = 500) -> str:
    """Extract sections (## delimited) that contain the query, up to max_chars."""
    sections = re.split(r"(?=^## )", content, flags=re.MULTILINE)
    matches = [section.strip() for section in sections if query in section.lower()]
    if not matches:
        return content[:max_chars]
    result = "\n\n".join(matches)
    return result[:max_chars]


def handle_session_search(args, conn, data_dir: Path | None = None) -> dict[str, Any]:
    """Search session notes by keyword. Returns matching section excerpts."""
    del conn
    query = args.query.lower()
    days = getattr(args, "days", 30)
    results = []
    with optional_lease_scope(
        _lease_user_for_data_dir(data_dir),
        operation="mcp",
        metadata={"source": "memory.handle_session_search"},
    ):
        sessions_dir = _sessions_dir(data_dir=data_dir)
        if not sessions_dir.exists():
            return {
                "data": {"results": [], "count": 0},
                "summary": {"message": "No session notes yet."},
            }

        cutoff = date.today() - timedelta(days=days)
        for path in sorted(sessions_dir.glob("*.md"), reverse=True):
            file_date = _parse_date_stem(path.stem)
            if file_date is None:
                continue
            if file_date < cutoff:
                break
            content = path.read_text(encoding="utf-8")
            if query in content.lower():
                excerpt = _extract_matching_sections(content, query, max_chars=500)
                results.append({"date": file_date.isoformat(), "excerpt": excerpt})

    return {
        "data": {"results": results[:10], "count": len(results)},
        "summary": {"message": f"Found {len(results)} session notes matching '{args.query}'."},
    }


def handle_session_read(args, conn, data_dir: Path | None = None) -> dict[str, Any]:
    """Read a specific day's session notes."""
    del conn
    raw = getattr(args, "date", "") or ""
    if raw:
        try:
            target_date = date.fromisoformat(raw).isoformat()
        except (ValueError, TypeError):
            return {
                "data": {"content": "", "exists": False},
                "summary": {"message": f"Invalid date format: {raw!r}. Use YYYY-MM-DD."},
            }
    else:
        target_date = date.today().isoformat()
    with optional_lease_scope(
        _lease_user_for_data_dir(data_dir),
        operation="mcp",
        metadata={"source": "memory.handle_session_read"},
    ):
        path = _sessions_dir(data_dir=data_dir) / f"{target_date}.md"
        if not path.exists():
            return {
                "data": {"content": "", "exists": False},
                "summary": {"message": f"No session notes for {target_date}."},
            }
        content = path.read_text(encoding="utf-8")
    return {
        "data": {"content": content, "exists": True},
        "summary": {"message": f"Session notes for {target_date} ({len(content)} bytes)."},
    }
