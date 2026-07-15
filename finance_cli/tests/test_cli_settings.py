from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError

from finance_cli.config import (
    CliSettings,
    get_data_dir,
    get_db_path,
    get_default_user_id,
    set_runtime_cli_settings,
)


def _runtime_settings(
    *,
    data_dir: Path | None,
    db_path: Path | None = None,
    user_id: str = "default",
    db_encryption_mode: str = "off",
) -> CliSettings:
    return CliSettings.model_construct(
        data_dir=data_dir.resolve() if data_dir is not None else None,
        db_path=db_path.resolve() if db_path is not None else None,
        user_id=user_id,
        web_data_root=None,
        db_encryption_mode=db_encryption_mode,
        log_level="INFO",
    )


def test_cli_settings_defaults(monkeypatch) -> None:
    for key in (
        "FINANCE_CLI_DATA_DIR",
        "FINANCE_CLI_DB",
        "FINANCE_CLI_USER_ID",
        "FINANCE_WEB_DATA_ROOT",
        "FINANCE_CLI_LOG_LEVEL",
        "FINANCE_CLI_REQUIRE_DB_ENCRYPTION",
    ):
        monkeypatch.delenv(key, raising=False)

    settings = CliSettings()

    assert settings.data_dir is None
    assert settings.db_path is None
    assert settings.user_id == "default"
    assert settings.db_encryption_mode == "off"


def test_cli_settings_reads_data_dir_from_env(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "data"
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(target))

    settings = CliSettings()

    assert settings.data_dir == target.resolve()


def test_cli_settings_reads_db_path_from_env(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(target))

    settings = CliSettings()

    assert settings.db_path == target.resolve()


def test_cli_settings_reads_user_id_from_env(monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_USER_ID", "alice")

    settings = CliSettings()

    assert settings.user_id == "alice"


def test_cli_settings_reads_db_encryption_mode_from_env(monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "  Require  ")

    settings = CliSettings()

    assert settings.db_encryption_mode == "require"


def test_cli_settings_defaults_empty_user_id(monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_USER_ID", "")

    settings = CliSettings()

    assert settings.user_id == "default"


def test_cli_settings_defaults_whitespace_user_id(monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_USER_ID", "   ")

    settings = CliSettings()

    assert settings.user_id == "default"


def test_cli_settings_expands_tilde_paths(monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", "~/foo")

    settings = CliSettings()

    assert settings.data_dir == (Path.home() / "foo").resolve()


def test_cli_settings_is_frozen(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("FINANCE_CLI_DATA_DIR", raising=False)
    settings = CliSettings(**{"FINANCE_CLI_DATA_DIR": tmp_path})

    try:
        settings.data_dir = Path("/x")
    except ValidationError as exc:
        assert "Instance is frozen" in str(exc)
    else:
        raise AssertionError("Expected frozen settings reassignment to fail")


def test_cli_settings_re_reads_late_env_writes(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("FINANCE_CLI_DATA_DIR", raising=False)
    first = CliSettings()
    target = tmp_path / "late-write"

    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(target))
    second = CliSettings()

    assert first.data_dir is None
    assert second.data_dir == target.resolve()


def test_runtime_cli_settings_override_path_helpers(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(tmp_path / "env-data"))
    monkeypatch.setenv("FINANCE_CLI_DB", str(tmp_path / "env.db"))
    runtime_data = tmp_path / "runtime-data"
    previous = set_runtime_cli_settings(
        _runtime_settings(data_dir=runtime_data, user_id="runtime-user")
    )
    try:
        assert get_data_dir() == runtime_data.resolve()
        assert get_db_path() == runtime_data.resolve() / "finance.db"
        assert get_default_user_id() == "runtime-user"
    finally:
        set_runtime_cli_settings(previous)


def test_runtime_cli_settings_can_be_restored(monkeypatch, tmp_path: Path) -> None:
    env_data = tmp_path / "env-data"
    runtime_data = tmp_path / "runtime-data"
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(env_data))
    previous = set_runtime_cli_settings(_runtime_settings(data_dir=runtime_data))
    try:
        assert get_data_dir() == runtime_data.resolve()
    finally:
        set_runtime_cli_settings(previous)

    assert get_data_dir() == env_data.resolve()
