from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> dict:
    code = main(args)
    assert code == 0
    return json.loads(capsys.readouterr().out)


def _seed_single_transaction(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES (?, '2026-01-10', 'Only Transaction', -1200, 'manual', 1)
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()


def test_daily_empty_includes_data_range_context(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    _seed_single_transaction(db_path)

    payload = _run_cli(["daily", "--date", "2026-01-11"], capsys)
    assert payload["status"] == "success"
    assert payload["data"]["transactions"] == []
    assert payload["data"]["data_range"] == {"earliest": "2026-01-10", "latest": "2026-01-10"}
    assert payload["cli_report"] == "No transactions on 2026-01-11 (data range: 2026-01-10 to 2026-01-10)"


def test_weekly_empty_includes_data_range_context(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    _seed_single_transaction(db_path)

    payload = _run_cli(["weekly", "--week", "2026-W05"], capsys)
    assert payload["status"] == "success"
    assert payload["data"]["categories"] == []
    assert payload["data"]["data_range"] == {"earliest": "2026-01-10", "latest": "2026-01-10"}
    assert payload["cli_report"] == (
        "No transactions for week 2026-01-26 to 2026-02-01 "
        "(data range: 2026-01-10 to 2026-01-10)"
    )
