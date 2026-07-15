from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

import finance_cli.mcp_server as mcp_server
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@contextmanager
def _upload_context(*, db_path: Path, uploads_dir: Path, local_mode: bool = False):
    token = set_user_context(
        UserContext.from_paths(db_path=db_path, uploads_dir=uploads_dir, local_mode=local_mode)
    )
    try:
        yield
    finally:
        reset_user_context(token)


def test_validate_upload_path_allows_files_within_uploads_dir(tmp_path: Path) -> None:
    uploads_dir = tmp_path / "uploads"
    uploads_dir.mkdir()
    file_path = uploads_dir / "statement.csv"
    file_path.write_text("Date,Amount\n2026-01-01,-10.00\n", encoding="utf-8")
    with _upload_context(db_path=tmp_path / "finance.db", uploads_dir=uploads_dir):
        assert mcp_server._validate_upload_path(str(file_path)) == file_path.resolve()


def test_validate_upload_path_rejects_files_outside_uploads_dir(tmp_path: Path) -> None:
    uploads_dir = tmp_path / "uploads"
    uploads_dir.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("Date,Amount\n2026-01-01,-10.00\n", encoding="utf-8")
    with _upload_context(db_path=tmp_path / "finance.db", uploads_dir=uploads_dir):
        with pytest.raises(ValueError, match="within the user uploads directory"):
            mcp_server._validate_upload_path(str(outside))


def test_validate_upload_path_allows_files_outside_uploads_dir_in_local_mode(
    tmp_path: Path,
) -> None:
    uploads_dir = tmp_path / "uploads"
    uploads_dir.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("Date,Amount\n2026-01-01,-10.00\n", encoding="utf-8")
    with _upload_context(
        db_path=tmp_path / "finance.db",
        uploads_dir=uploads_dir,
        local_mode=True,
    ):
        assert mcp_server._validate_upload_path(str(outside)) == outside.resolve()


def test_validate_upload_path_rejects_path_traversal(tmp_path: Path) -> None:
    uploads_dir = tmp_path / "uploads"
    uploads_dir.mkdir()
    secret = tmp_path / "etc" / "passwd"
    secret.parent.mkdir()
    secret.write_text("secret\n", encoding="utf-8")
    raw_path = uploads_dir / ".." / "etc" / "passwd"
    with _upload_context(db_path=tmp_path / "finance.db", uploads_dir=uploads_dir):
        with pytest.raises(ValueError, match="within the user uploads directory"):
            mcp_server._validate_upload_path(str(raw_path))


def test_validate_upload_path_allows_any_path_without_upload_context(tmp_path: Path) -> None:
    file_path = tmp_path / "anywhere.csv"
    file_path.write_text("Date,Amount\n2026-01-01,-10.00\n", encoding="utf-8")

    assert mcp_server._validate_upload_path(str(file_path)) == file_path.resolve()
