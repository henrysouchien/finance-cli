from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from finance_cli.user_context import (
    UserContext,
    current_db_path,
    current_expected_user_id,
    current_local_mode,
    current_storage_mode,
    current_rules_path,
    current_uploads_dir,
    get_user_context,
    reset_user_context,
    set_user_context,
)


def test_user_context_is_frozen() -> None:
    ctx = UserContext(
        db_path="/tmp/alice/finance.db",
        expected_user_id="alice",
        workspace_name="alice",
    )

    with pytest.raises(FrozenInstanceError):
        ctx.db_path = "/tmp/bob/finance.db"


def test_from_paths_normalizes_and_derives_workspace_name(
    tmp_path: Path, monkeypatch
) -> None:
    home = tmp_path / "home"
    workspace = home / "workspace"
    uploads = workspace / "uploads"
    home.mkdir()
    workspace.mkdir()
    uploads.mkdir()
    monkeypatch.setenv("HOME", str(home))

    ctx = UserContext.from_paths(
        db_path="~/workspace/finance.db",
        expected_user_id="alice",
        rules_path="~/workspace/rules.yaml",
        uploads_dir="~/workspace/uploads",
        local_mode=True,
    )

    assert ctx.db_path == str((workspace / "finance.db").resolve())
    assert ctx.rules_path == str((workspace / "rules.yaml").resolve())
    assert ctx.uploads_dir == str(uploads.resolve())
    assert ctx.workspace_name == Path(ctx.db_path).parent.name == "workspace"
    assert ctx.local_mode is True


def test_user_context_roundtrip() -> None:
    assert get_user_context() is None

    ctx = UserContext.from_paths(
        db_path="/tmp/alice/finance.db",
        expected_user_id="alice",
        rules_path="/tmp/alice/rules.yaml",
        uploads_dir="/tmp/alice/uploads",
    )

    token = set_user_context(ctx)
    try:
        assert get_user_context() == ctx
    finally:
        reset_user_context(token)

    assert get_user_context() is None


def test_repr_redacts_db_path_and_keeps_identity_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "alice" / "finance.db"
    ctx = UserContext.from_paths(
        db_path=db_path,
        expected_user_id="alice-42",
    )

    rendered = repr(ctx)

    assert str(db_path.resolve()) not in rendered
    assert "expected_user_id" in rendered
    assert "alice-42" in rendered
    assert "workspace_name" in rendered
    assert ctx.workspace_name in rendered


def test_current_accessors_follow_active_context_and_default_values() -> None:
    assert current_db_path() is None
    assert current_expected_user_id() is None
    assert current_rules_path() is None
    assert current_uploads_dir() is None
    assert current_local_mode() is False
    assert current_storage_mode() is None

    ctx = UserContext.from_paths(
        db_path="/tmp/alice/finance.db",
        expected_user_id="alice",
        rules_path="/tmp/alice/rules.yaml",
        uploads_dir="/tmp/alice/uploads",
        local_mode=True,
        storage_mode="REMOTE",
    )

    token = set_user_context(ctx)
    try:
        assert current_db_path() == ctx.db_path
        assert current_expected_user_id() == ctx.expected_user_id
        assert current_rules_path() == ctx.rules_path
        assert current_uploads_dir() == ctx.uploads_dir
        assert current_local_mode() is True
        assert current_storage_mode() == "remote"
    finally:
        reset_user_context(token)
