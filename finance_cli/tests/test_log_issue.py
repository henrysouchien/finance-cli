"""Tests for the finance_log_issue MCP tool."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.mcp_server import _insert_issue, finance_log_issue


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def test_insert_issue_writes_issue_report_row(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = _insert_issue(conn, "Test bug", "Something broke.", "bug")
        row = conn.execute(
            """
            SELECT id, title, description, severity, status
            FROM issue_reports
            WHERE id = ?
            """,
            (result["id"],),
        ).fetchone()

    assert row is not None
    assert result["severity"] == "Bug"
    assert result["status"] == "open"
    assert row["title"] == "Test bug"
    assert row["description"] == "Something broke."
    assert row["severity"] == "bug"
    assert row["status"] == "open"


@pytest.mark.parametrize(
    ("severity", "expected_label"),
    [
        ("warning", "Warning"),
        ("suggestion", "Suggestion"),
        ("BUG", "Bug"),
    ],
)
def test_insert_issue_normalizes_supported_severities(
    db_path: Path,
    severity: str,
    expected_label: str,
) -> None:
    with connect(db_path) as conn:
        result = _insert_issue(conn, f"{expected_label} title", "Details.", severity)

    assert result["severity"] == expected_label


def test_insert_issue_rejects_invalid_severity(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = _insert_issue(conn, "Title", "Desc", "critical")

    assert "error" in result
    assert "Invalid severity" in result["error"]


def test_insert_issue_rejects_empty_title_and_description(db_path: Path) -> None:
    with connect(db_path) as conn:
        title_result = _insert_issue(conn, "  ", "Desc", "bug")
        description_result = _insert_issue(conn, "Title", "   ", "bug")

    assert "Title" in title_result["error"]
    assert "Description" in description_result["error"]


def test_multiple_inserts_create_multiple_rows(db_path: Path) -> None:
    with connect(db_path) as conn:
        first = _insert_issue(conn, "First", "One.", "bug")
        second = _insert_issue(conn, "Second", "Two.", "warning")
        rows = conn.execute(
            "SELECT id, title, severity FROM issue_reports ORDER BY title"
        ).fetchall()

    assert first["id"] != second["id"]
    assert [(row["title"], row["severity"]) for row in rows] == [
        ("First", "bug"),
        ("Second", "warning"),
    ]


def test_log_issue_tool_writes_to_issue_reports_table(db_path: Path) -> None:
    result = finance_log_issue("Tool issue", "Tool path details", "suggestion")

    assert result["summary"] == "Logged suggestion: Tool issue"
    issue_id = result["data"]["id"]

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT title, description, severity, status
            FROM issue_reports
            WHERE id = ?
            """,
            (issue_id,),
        ).fetchone()

    assert row is not None
    assert row["title"] == "Tool issue"
    assert row["description"] == "Tool path details"
    assert row["severity"] == "suggestion"
    assert row["status"] == "open"
