from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from finance_cli import __main__ as cli_main
from finance_cli.db import initialize_database
from finance_cli.error_capture import capture_error


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _read_json(capsys):
    return json.loads(capsys.readouterr().out)


def test_importing_cli_entrypoint_does_not_import_mcp_server() -> None:
    env = dict(os.environ)
    env["FINANCE_CLI_DISABLE_DOTENV"] = "1"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import finance_cli.__main__; "
                "print('finance_cli.mcp_server' in sys.modules)"
            ),
        ],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "False"


def test_error_cli_lists_shows_stats_and_updates(db_path, capsys) -> None:
    error_id = None
    for request_id in ("req-one", "req-two"):
        try:
            raise RuntimeError("triage cli exploded")
        except RuntimeError as exc:
            error_id = capture_error(
                exc,
                source="cli",
                endpoint="demo.triage",
                context={"request_id": request_id},
                db_path=db_path,
            )
    assert error_id is not None

    code = cli_main.main(["error", "list", "--days", "30", "--format", "json"])
    assert code == 0
    payload = _read_json(capsys)
    assert payload["command"] == "error.list"
    assert payload["summary"]["total_errors"] == 1
    assert payload["data"]["errors"][0]["id"] == error_id

    code = cli_main.main(["error", "show", error_id, "--format", "json"])
    assert code == 0
    payload = _read_json(capsys)
    assert payload["command"] == "error.show"
    assert payload["summary"]["occurrence_count"] == 2
    assert len(payload["data"]["occurrence_timeline"]) == 2

    code = cli_main.main(["error", "stats", "--days", "30", "--format", "json"])
    assert code == 0
    payload = _read_json(capsys)
    assert payload["command"] == "error.stats"
    assert payload["summary"]["total_errors"] == 1

    code = cli_main.main(
        [
            "error",
            "update",
            error_id,
            "--status",
            "resolved",
            "--resolution",
            "fixed by cli triage command",
            "--format",
            "json",
        ]
    )
    assert code == 0
    payload = _read_json(capsys)
    assert payload["command"] == "error.update"
    assert payload["summary"]["status"] == "resolved"
    assert payload["data"]["resolution"] == "fixed by cli triage command"


def test_issue_cli_lists_and_updates(db_path, capsys) -> None:
    from finance_cli.mcp_server import finance_log_issue

    logged = finance_log_issue("CLI issue", "Triage command coverage", "warning")
    issue_id = logged["data"]["id"]

    code = cli_main.main(["issue", "list", "--format", "json"])
    assert code == 0
    payload = _read_json(capsys)
    assert payload["command"] == "issue.list"
    assert payload["summary"]["total_issues"] == 1
    assert payload["data"]["issues"][0]["id"] == issue_id

    code = cli_main.main(
        [
            "issue",
            "update",
            issue_id,
            "--status",
            "resolved",
            "--resolution",
            "handled by cli triage command",
            "--format",
            "json",
        ]
    )
    assert code == 0
    payload = _read_json(capsys)
    assert payload["command"] == "issue.update"
    assert payload["summary"]["status"] == "resolved"
    assert payload["data"]["resolution"] == "handled by cli triage command"


def test_cli_4xx_finance_errors_are_not_captured_as_runtime_errors(
    db_path,
    monkeypatch,
    capsys,
) -> None:
    captured: list[Exception] = []

    def fake_capture_error(exc, **_kwargs):
        captured.append(exc)
        return "unexpected"

    monkeypatch.setattr(cli_main, "capture_error", fake_capture_error)

    code = cli_main.main(["txn", "show", "missing-transaction", "--format", "json"])

    assert code == 1
    payload = _read_json(capsys)
    assert payload["command"] == "txn.show"
    assert payload["status"] == "error"
    assert "[not_found]" in payload["error"]
    assert captured == []


@pytest.mark.parametrize(
    "argv",
    [
        ["error", "show", "missing-error", "--format", "json"],
        [
            "error",
            "update",
            "missing-error",
            "--status",
            "resolved",
            "--format",
            "json",
        ],
        [
            "issue",
            "update",
            "missing-issue",
            "--status",
            "resolved",
            "--format",
            "json",
        ],
    ],
)
def test_triage_not_found_errors_are_not_captured_as_runtime_errors(
    db_path,
    monkeypatch,
    capsys,
    argv,
) -> None:
    captured: list[Exception] = []

    def fake_capture_error(exc, **_kwargs):
        captured.append(exc)
        return "unexpected"

    monkeypatch.setattr(cli_main, "capture_error", fake_capture_error)

    code = cli_main.main(argv)

    assert code == 1
    payload = _read_json(capsys)
    assert payload["status"] == "error"
    assert "[not_found]" in payload["error"]
    assert captured == []


def test_cli_internal_errors_still_capture_runtime_errors(
    db_path,
    monkeypatch,
    capsys,
) -> None:
    captured: list[tuple[Exception, dict]] = []

    def fake_handle_issue_list(_args, _conn):
        raise RuntimeError("issue list exploded")

    def fake_capture_error(exc, **kwargs):
        captured.append((exc, kwargs))
        return "captured"

    monkeypatch.setattr(cli_main.triage_cmd, "handle_issue_list", fake_handle_issue_list)
    monkeypatch.setattr(cli_main, "capture_error", fake_capture_error)

    code = cli_main.main(["issue", "list", "--format", "json"])

    assert code == 1
    payload = _read_json(capsys)
    assert payload["command"] == "issue.list"
    assert payload["status"] == "error"
    assert payload["error"] == "issue list exploded"
    assert len(captured) == 1
    assert isinstance(captured[0][0], RuntimeError)
    assert captured[0][1]["source"] == "cli"
    assert captured[0][1]["endpoint"] == "issue.list"
