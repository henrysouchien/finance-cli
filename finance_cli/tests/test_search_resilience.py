from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def test_fts_search_handles_special_characters(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source)
            VALUES (?, '2025-02-01', 'UBER EATS ORDER', -2500, 'manual')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    code = main(["txn", "search", "--query", '"uber'])
    assert code == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["command"] == "txn.search"
    assert payload["summary"]["total_transactions"] == 1


def test_fts_partial_case_insensitive_and_multi_result(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source)
            VALUES (?, '2025-02-01', 'Uber Trip Downtown', -1800, 'manual')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source)
            VALUES (?, '2025-02-02', 'UBER EATS ORDER', -2500, 'manual')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source)
            VALUES (?, '2025-02-03', 'Lyft Ride', -1200, 'manual')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    code = main(["txn", "search", "--query", "ube*"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["summary"]["total_transactions"] == 2

    code = main(["txn", "search", "--query", "UBER"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["summary"]["total_transactions"] == 2
