"""Tests for XDG data directory resolution and auto-migration (CQ-009)."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from unittest import mock



# ---------------------------------------------------------------------------
# get_data_dir / get_db_path resolution
# ---------------------------------------------------------------------------

class TestGetDataDir:
    """Verify env-var override and platform fallback."""

    def test_env_override(self, tmp_path: Path):
        override = str(tmp_path / "custom")
        with mock.patch.dict(os.environ, {"FINANCE_CLI_DATA_DIR": override}):
            from finance_cli.config import get_data_dir
            assert get_data_dir() == Path(override).resolve()

    def test_db_override_parent_used_when_data_dir_missing(self, tmp_path: Path):
        db = str(tmp_path / "nested" / "finance.db")
        with mock.patch.dict(os.environ, {"FINANCE_CLI_DB": db}, clear=False):
            os.environ.pop("FINANCE_CLI_DATA_DIR", None)
            from finance_cli.config import get_data_dir
            assert get_data_dir() == Path(db).resolve().parent

    def test_re_reads_env_override_each_call(self, monkeypatch, tmp_path: Path):
        from finance_cli.config import get_data_dir

        first = tmp_path / "x"
        second = tmp_path / "y"

        monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(first))
        assert get_data_dir() == first.resolve()

        monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(second))
        assert get_data_dir() == second.resolve()

    def test_platform_fallback_darwin(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FINANCE_CLI_DATA_DIR", None)
            with mock.patch("finance_cli.config.sys") as mock_sys:
                mock_sys.platform = "darwin"
                from finance_cli.config import _platform_data_dir
                result = _platform_data_dir()
                assert result == Path.home() / "Library" / "Application Support" / "finance_cli"

    def test_platform_fallback_linux(self):
        with mock.patch("finance_cli.config.sys") as mock_sys:
            mock_sys.platform = "linux"
            from finance_cli.config import _platform_data_dir
            result = _platform_data_dir()
            assert result == Path.home() / ".local" / "share" / "finance_cli"


class TestGetDbPath:
    """Verify DB path resolution."""

    def test_env_override(self, tmp_path: Path):
        db = str(tmp_path / "my.db")
        with mock.patch.dict(os.environ, {"FINANCE_CLI_DB": db}):
            from finance_cli.config import get_db_path
            assert get_db_path() == Path(db).resolve()

    def test_chains_through_data_dir(self, tmp_path: Path):
        with mock.patch.dict(os.environ, {"FINANCE_CLI_DATA_DIR": str(tmp_path)}):
            os.environ.pop("FINANCE_CLI_DB", None)
            from finance_cli.config import get_db_path
            assert get_db_path() == tmp_path.resolve() / "finance.db"


class TestEnsureDataDir:
    """Verify directory creation."""

    def test_creates_directory(self, tmp_path: Path):
        target = tmp_path / "sub" / "dir"
        with mock.patch.dict(os.environ, {"FINANCE_CLI_DATA_DIR": str(target)}):
            from finance_cli.config import ensure_data_dir
            result = ensure_data_dir()
            assert result == target.resolve()
            assert target.exists()


# ---------------------------------------------------------------------------
# auto_migrate_data
# ---------------------------------------------------------------------------

class TestAutoMigrateData:
    """Verify one-time migration from package dir to XDG path."""

    def test_skips_when_env_override_set(self, tmp_path: Path):
        with mock.patch.dict(os.environ, {"FINANCE_CLI_DATA_DIR": str(tmp_path)}):
            from finance_cli.config import auto_migrate_data
            assert auto_migrate_data() is None

    def test_skips_when_db_env_set(self, tmp_path: Path):
        with mock.patch.dict(os.environ, {"FINANCE_CLI_DB": str(tmp_path / "x.db")}):
            from finance_cli.config import auto_migrate_data
            assert auto_migrate_data() is None

    def test_skips_when_runtime_override_set(self, tmp_path: Path):
        from finance_cli.config import CliSettings, auto_migrate_data, set_runtime_cli_settings

        previous = set_runtime_cli_settings(
            CliSettings.model_construct(
                data_dir=tmp_path / "runtime",
                db_path=None,
                user_id="default",
                web_data_root=None,
                log_level="INFO",
            )
        )
        try:
            assert auto_migrate_data() is None
        finally:
            set_runtime_cli_settings(previous)

    def test_skips_when_no_legacy_db(self, tmp_path: Path):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FINANCE_CLI_DATA_DIR", None)
            os.environ.pop("FINANCE_CLI_DB", None)
            with mock.patch("finance_cli.config.PACKAGE_TEMPLATE_DIR", tmp_path / "empty"):
                from finance_cli.config import auto_migrate_data
                assert auto_migrate_data() is None

    def test_skips_when_target_already_has_db(self, tmp_path: Path):
        legacy = tmp_path / "legacy"
        legacy.mkdir()
        (legacy / "finance.db").write_text("legacy")
        target = tmp_path / "target"
        target.mkdir()
        (target / "finance.db").write_text("already there")

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FINANCE_CLI_DATA_DIR", None)
            os.environ.pop("FINANCE_CLI_DB", None)
            with mock.patch("finance_cli.config.PACKAGE_TEMPLATE_DIR", legacy), \
                 mock.patch("finance_cli.config._platform_data_dir", return_value=target):
                from finance_cli.config import auto_migrate_data
                assert auto_migrate_data() is None

    def test_migrates_files(self, tmp_path: Path):
        legacy = tmp_path / "legacy"
        legacy.mkdir()
        # Create a real SQLite DB (WAL checkpoint needs it)
        conn = sqlite3.connect(str(legacy / "finance.db"))
        conn.execute("CREATE TABLE t(x)")
        conn.close()
        (legacy / "rules.yaml").write_text("keyword_rules: []")
        (legacy / "agent_memory.md").write_text("# Memory")
        sessions = legacy / "sessions"
        sessions.mkdir()
        (sessions / "2026-01-01.md").write_text("session")

        target = tmp_path / "target"

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FINANCE_CLI_DATA_DIR", None)
            os.environ.pop("FINANCE_CLI_DB", None)
            with mock.patch("finance_cli.config.PACKAGE_TEMPLATE_DIR", legacy), \
                 mock.patch("finance_cli.config._platform_data_dir", return_value=target):
                from finance_cli.config import auto_migrate_data
                result = auto_migrate_data()

        assert result == target
        assert (target / "finance.db").exists()
        assert (target / "rules.yaml").exists()
        assert (target / "agent_memory.md").exists()
        assert (target / "sessions" / "2026-01-01.md").exists()
        assert (target / "_migrated_from.txt").exists()
        marker = (target / "_migrated_from.txt").read_text()
        assert str(legacy) in marker

    def test_skips_when_marker_exists(self, tmp_path: Path):
        legacy = tmp_path / "legacy"
        legacy.mkdir()
        conn = sqlite3.connect(str(legacy / "finance.db"))
        conn.execute("CREATE TABLE t(x)")
        conn.close()

        target = tmp_path / "target"
        target.mkdir()
        (target / "_migrated_from.txt").write_text("already migrated")

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FINANCE_CLI_DATA_DIR", None)
            os.environ.pop("FINANCE_CLI_DB", None)
            with mock.patch("finance_cli.config.PACKAGE_TEMPLATE_DIR", legacy), \
                 mock.patch("finance_cli.config._platform_data_dir", return_value=target):
                from finance_cli.config import auto_migrate_data
                assert auto_migrate_data() is None


# ---------------------------------------------------------------------------
# PACKAGE_TEMPLATE_DIR references
# ---------------------------------------------------------------------------

class TestPackageTemplateDir:
    """Verify PACKAGE_TEMPLATE_DIR points to the right place."""

    def test_points_to_data_subdir(self):
        from finance_cli.config import PACKAGE_TEMPLATE_DIR, PACKAGE_DIR
        assert PACKAGE_TEMPLATE_DIR == PACKAGE_DIR / "data"

    def test_legacy_alias_matches(self):
        from finance_cli.config import DEFAULT_DATA_DIR, PACKAGE_TEMPLATE_DIR
        assert DEFAULT_DATA_DIR == PACKAGE_TEMPLATE_DIR


class TestRulesPathUsesTemplate:
    """Verify rules path helpers reference PACKAGE_TEMPLATE_DIR, not DEFAULT_DATA_DIR."""

    def test_package_default_rules_path(self):
        from finance_cli.user_rules import _package_default_rules_path
        from finance_cli.config import PACKAGE_TEMPLATE_DIR
        assert _package_default_rules_path() == PACKAGE_TEMPLATE_DIR / "rules_template.yaml"
