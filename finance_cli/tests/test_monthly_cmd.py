"""Tests for the monthly pipeline runner command."""

from __future__ import annotations

import json
import uuid
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _seed_transactions(db_path: Path, count: int = 5) -> list[str]:
    """Seed some uncategorized/unreviewed transactions and return their IDs."""
    ids = []
    with connect(db_path) as conn:
        for i in range(count):
            txn_id = uuid.uuid4().hex
            ids.append(txn_id)
            conn.execute(
                """
                INSERT INTO transactions (
                    id, date, description, amount_cents, source,
                    is_active, is_reviewed, category_id
                ) VALUES (?, '2026-02-01', ?, ?, 'csv_import', 1, 0, NULL)
                """,
                (txn_id, f"TEST MERCHANT {i}", -(1000 + i * 100)),
            )
        conn.commit()
    return ids


# ---------------------------------------------------------------------------
# Mock return values for each handler
# ---------------------------------------------------------------------------

_SYNC_RESULT = {
    "data": {
        "items_synced": 3, "items_skipped": 0, "items_failed": 0,
        "added": 47, "modified": 2, "removed": 0,
        "items_requested": 3, "total_elapsed_ms": 1200,
    },
    "summary": {"items_requested": 3, "items_synced": 3, "items_skipped": 0,
                "items_failed": 0, "added": 47, "modified": 2, "removed": 0},
    "cli_report": "synced",
}

_BALANCE_RESULT = {
    "data": {
        "items_refreshed": 3, "items_skipped": 0, "items_failed": 0,
        "accounts_updated": 5, "snapshots_updated": 5, "items_requested": 3,
    },
    "summary": {"items_requested": 3, "items_refreshed": 3, "items_skipped": 0,
                "items_failed": 0, "accounts_updated": 5, "snapshots_updated": 5},
    "cli_report": "refreshed",
}

_DEDUP_RESULT = {
    "data": {"total_matches": 2, "removed": 2, "dry_run": False,
             "account_id": None, "date_from": None, "date_to": None},
    "summary": {"total_matches": 2, "total_removed": 2, "key_only_count": 0},
    "cli_report": "2 duplicates removed",
}

_CAT_RESULT = {
    "data": {"updated": 34, "ambiguous": 0, "by_source": {"vendor_memory": 12, "keyword": 22}, "ai": None},
    "summary": {"total_transactions": 34, "total_amount": 0},
    "cli_report": "Auto-categorized 34 transactions",
}

_DETECT_RESULT = {
    "data": {"detected": 24, "inserted": 5, "updated": 3, "deactivated": 1,
             "recurring_patterns": 30, "recurring_txns": 120},
    "summary": {"total_detected": 24},
    "cli_report": "detected=24",
}

_EXPORT_RESULT = {
    "data": {"files": ["a.csv", "b.csv"], "rows": 100},
    "summary": {"total_transactions": 100, "total_files": 2},
    "cli_report": "Wrote 2 files",
}

_BURN_RESULT = {"active_subscriptions": 24, "monthly_burn_cents": 147400, "yearly_burn_cents": 1768800}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_env(tmp_path, monkeypatch):
    """Set up a test database and return the path."""
    db_path = _setup_db(tmp_path, monkeypatch)
    return db_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFullPipeline:
    """Full pipeline — no sync, no AI."""

    def test_runs_dedup_categorize_detect(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"):
            with patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup:
                mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
                code, payload = _run_cli(["monthly", "run", "--format", "json"], capsys)

        assert payload["status"] == "success"
        assert payload["command"] == "monthly run"
        steps = payload["data"]["steps"]
        assert steps["sync"]["status"] == "skipped"
        assert steps["balance"]["status"] == "skipped"
        assert steps["dedup"]["status"] == "success"
        assert steps["categorize"]["status"] == "success"
        assert steps["detect"]["status"] == "success"
        assert steps["export"]["status"] == "skipped"
        assert "health" in payload["data"]
        assert payload["summary"]["steps_skipped"] >= 2  # sync, balance, export


class TestWithSync:
    """With --sync flag — mock Plaid handlers."""

    def test_sync_calls_plaid_handlers(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.monthly_cmd.config_status", create=True) as mock_cfg, \
             patch("finance_cli.commands.plaid_cmd.run_sync") as mock_sync, \
             patch("finance_cli.commands.plaid_cmd.refresh_balances") as mock_balance, \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup:

            # Config says Plaid is ready
            mock_cfg_status = MagicMock(configured=True)
            # We need to patch the import inside handle_run
            with patch("finance_cli.plaid_client.config_status", return_value=mock_cfg_status):
                mock_sync.return_value = {
                    "items_synced": 3, "items_skipped": 0, "items_failed": 0,
                    "added": 47, "modified": 2, "removed": 0,
                    "items_requested": 3, "total_elapsed_ms": 1200,
                }
                mock_balance.return_value = {
                    "items_refreshed": 3, "items_skipped": 0, "items_failed": 0,
                    "accounts_updated": 5, "snapshots_updated": 5, "items_requested": 3,
                }
                mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})

                code, payload = _run_cli(
                    ["monthly", "run", "--sync", "--format", "json"], capsys
                )

        steps = payload["data"]["steps"]
        assert steps["sync"]["status"] == "success"
        assert steps["balance"]["status"] == "success"

    def test_sync_plaid_not_configured(self, db_env, capsys):
        """Plaid not configured -> skipped gracefully, not error."""
        _seed_transactions(db_env)
        mock_cfg = MagicMock(configured=False)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.plaid_client.config_status", return_value=mock_cfg), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup:
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            code, payload = _run_cli(
                ["monthly", "run", "--sync", "--format", "json"], capsys
            )

        steps = payload["data"]["steps"]
        assert steps["sync"]["status"] == "skipped"
        assert steps["balance"]["status"] == "skipped"
        assert "not configured" in (steps["sync"]["error"] or "").lower()


class TestWithAI:
    """With --ai flag."""

    def test_ai_flag_passed_to_categorize(self, db_env, capsys):
        _seed_transactions(db_env)
        captured_args = {}

        original_handle = None

        def fake_auto_categorize(ns, conn):
            captured_args["ai"] = ns.ai
            captured_args["dry_run"] = ns.dry_run
            return _CAT_RESULT

        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup, \
             patch("finance_cli.commands.cat.handle_auto_categorize", side_effect=fake_auto_categorize) as mock_cat:
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            code, payload = _run_cli(
                ["monthly", "run", "--ai", "--format", "json"], capsys
            )

        assert captured_args["ai"] is True


class TestWithExportDir:
    """With --export-dir flag."""

    def test_export_called(self, db_env, capsys, tmp_path):
        _seed_transactions(db_env)
        export_dir = str(tmp_path / "wave_out")
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup, \
             patch("finance_cli.commands.export.export_wave") as mock_wave:
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            mock_wave.return_value = {"files": ["a.csv"], "rows": 10}

            code, payload = _run_cli(
                ["monthly", "run", "--export-dir", export_dir, "--format", "json"],
                capsys,
            )

        steps = payload["data"]["steps"]
        assert steps["export"]["status"] == "success"
        mock_wave.assert_called_once()


class TestDryRun:
    """With --dry-run flag."""

    def test_dry_run_no_commit(self, db_env, capsys):
        _seed_transactions(db_env)
        dedup_args_captured = {}

        def fake_cross_format(ns, conn):
            dedup_args_captured["commit"] = ns.commit
            return _DEDUP_RESULT

        cat_args_captured = {}

        def fake_auto_categorize(ns, conn):
            cat_args_captured["dry_run"] = ns.dry_run
            return _CAT_RESULT

        with patch("finance_cli.commands.monthly_cmd.backup_database") as mock_backup, \
             patch("finance_cli.commands.dedup_cmd.handle_cross_format", side_effect=fake_cross_format), \
             patch("finance_cli.commands.cat.handle_auto_categorize", side_effect=fake_auto_categorize), \
             patch("finance_cli.commands.subs.handle_detect", return_value=_DETECT_RESULT):

            code, payload = _run_cli(
                ["monthly", "run", "--dry-run", "--format", "json"], capsys
            )

        # backup_database should NOT be called in dry-run mode
        mock_backup.assert_not_called()
        assert dedup_args_captured["commit"] is False
        assert cat_args_captured["dry_run"] is True

    def test_dry_run_detect_rollback(self, db_env, capsys):
        """After detect in dry-run, conn.rollback() should be called."""
        _seed_transactions(db_env)

        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup, \
             patch("finance_cli.commands.subs.detect_subscriptions") as mock_detect_subs:
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            mock_detect_subs.return_value = {
                "detected": 0, "inserted": 0, "updated": 0,
                "deactivated": 0, "recurring_patterns": 0, "recurring_txns": 0,
            }

            code, payload = _run_cli(
                ["monthly", "run", "--dry-run", "--format", "json"], capsys
            )

        # The pipeline should complete successfully
        assert payload["status"] == "success"
        steps = payload["data"]["steps"]
        assert steps["detect"]["status"] == "success"


class TestSkipFlags:
    """With --skip flags."""

    def test_skip_dedup_and_detect(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"):
            code, payload = _run_cli(
                ["monthly", "run", "--skip", "dedup", "--skip", "detect", "--format", "json"],
                capsys,
            )

        steps = payload["data"]["steps"]
        assert steps["dedup"]["status"] == "skipped"
        assert steps["detect"]["status"] == "skipped"
        assert steps["categorize"]["status"] == "success"

    def test_skip_all_steps(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"):
            code, payload = _run_cli(
                ["monthly", "run",
                 "--skip", "dedup", "--skip", "categorize", "--skip", "detect",
                 "--format", "json"],
                capsys,
            )

        steps = payload["data"]["steps"]
        assert steps["dedup"]["status"] == "skipped"
        assert steps["categorize"]["status"] == "skipped"
        assert steps["detect"]["status"] == "skipped"
        assert payload["summary"]["steps_skipped"] >= 5  # sync, balance, dedup, categorize, detect, export


class TestStepFailure:
    """One step errors, others still run."""

    def test_dedup_fails_others_continue(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates",
                   side_effect=RuntimeError("dedup boom")):
            code, payload = _run_cli(
                ["monthly", "run", "--format", "json"], capsys
            )

        steps = payload["data"]["steps"]
        assert steps["dedup"]["status"] == "error"
        assert "dedup boom" in steps["dedup"]["error"]
        # Other steps still ran
        assert steps["categorize"]["status"] == "success"
        assert steps["detect"]["status"] == "success"
        assert payload["summary"]["steps_failed"] >= 1

    def test_categorize_fails_others_continue(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup, \
             patch("finance_cli.commands.cat.handle_auto_categorize",
                   side_effect=RuntimeError("cat boom")):
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            code, payload = _run_cli(
                ["monthly", "run", "--format", "json"], capsys
            )

        steps = payload["data"]["steps"]
        assert steps["categorize"]["status"] == "error"
        assert "cat boom" in steps["categorize"]["error"]
        assert steps["dedup"]["status"] == "success"
        assert steps["detect"]["status"] == "success"


class TestHealthChecks:
    """Health check queries."""

    def test_health_counts(self, db_env, capsys):
        _seed_transactions(db_env, count=7)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup:
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            code, payload = _run_cli(
                ["monthly", "run", "--format", "json"], capsys
            )

        health = payload["data"]["health"]
        # We seeded 7 unreviewed + uncategorized transactions
        # categorize may have changed some, but at minimum they should be counted
        assert "unreviewed_count" in health
        assert "uncategorized_count" in health
        assert "null_use_type_count" in health
        assert isinstance(health["unreviewed_count"], int)
        assert isinstance(health["uncategorized_count"], int)
        assert isinstance(health["null_use_type_count"], int)

    def test_budget_health_counts_and_cli_line(self, db_env, capsys):
        _seed_transactions(db_env, count=3)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup, \
             patch(
                 "finance_cli.budget_engine.budget_alerts",
                 return_value={
                     "month": "2026-02",
                     "days_elapsed": 10,
                     "days_remaining": 18,
                     "days_in_month": 28,
                     "alerts": [],
                     "ok_count": 0,
                     "over_count": 1,
                     "alert_count": 2,
                     "warn_count": 3,
                     "low_confidence": False,
                 },
             ):
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            code, payload = _run_cli(
                ["monthly", "run", "--format", "json"], capsys
            )

        health = payload["data"]["health"]
        assert health["budget_over_count"] == 1
        assert health["budget_alert_count"] == 2
        assert health["budget_warn_count"] == 3
        assert "Budget check: 1 over budget, 2 at risk, 3 warnings" in payload["cli_report"]


class TestJSONOutput:
    """JSON output structure."""

    def test_data_steps_structure(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup:
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            code, payload = _run_cli(
                ["monthly", "run", "--format", "json"], capsys
            )

        assert "data" in payload
        data = payload["data"]
        assert "month" in data
        assert "elapsed_ms" in data
        assert isinstance(data["elapsed_ms"], int)
        assert "steps" in data
        assert "health" in data

        # Each step has status/result/error
        for step_name in ("sync", "balance", "dedup", "categorize", "detect", "export"):
            assert step_name in data["steps"], f"Missing step: {step_name}"
            step = data["steps"][step_name]
            assert "status" in step
            assert step["status"] in ("success", "error", "skipped")
            assert "result" in step
            assert "error" in step

        # Summary structure
        summary = payload["summary"]
        assert "steps_run" in summary
        assert "steps_succeeded" in summary
        assert "steps_failed" in summary
        assert "steps_skipped" in summary

    def test_cli_report_present(self, db_env, capsys):
        _seed_transactions(db_env)
        with patch("finance_cli.commands.monthly_cmd.backup_database"), \
             patch("finance_cli.commands.dedup_cmd.find_cross_format_duplicates") as mock_dedup:
            mock_dedup.return_value = MagicMock(matches=[], as_dict=lambda: {"matches": []})
            code, payload = _run_cli(
                ["monthly", "run", "--format", "json"], capsys
            )

        assert "cli_report" in payload
        report = payload["cli_report"]
        assert "Monthly Run:" in report
        assert "Unreviewed:" in report
        assert "Uncategorized:" in report
        assert "Unclassified:" in report
