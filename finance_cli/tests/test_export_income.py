from __future__ import annotations

import csv
import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database
from finance_cli.exporters import export_wave
from finance_cli.importers import import_income_csv
from finance_cli.user_rules import UserRules


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def test_export_wave_outputs_per_category_files(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        travel_id = _seed_category(conn, "Travel")
        dining_id = _seed_category(conn, "Dining")

        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, category_id, source) VALUES (?, '2025-02-01', 'Flight', -50000, ?, 'manual')",
            (uuid.uuid4().hex, travel_id),
        )
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, category_id, source) VALUES (?, '2025-02-03', 'Dinner', -12000, ?, 'manual')",
            (uuid.uuid4().hex, dining_id),
        )
        conn.execute(
            "INSERT INTO transactions (id, date, description, amount_cents, category_id, source) VALUES (?, '2025-02-05', 'Refund', 1500, ?, 'manual')",
            (uuid.uuid4().hex, dining_id),
        )
        conn.commit()

        report = export_wave(conn, month="2025-02", output_dir=tmp_path / "wave")

    assert report["rows"] == 2
    assert len(report["files"]) == 2

    first_file = Path(report["files"][0])
    rows = list(csv.DictReader(first_file.open("r", encoding="utf-8")))
    assert rows
    assert float(rows[0]["Amount"]) > 0
    assert rows[0]["Category"] in {"Travel", "Dining"}


def test_import_income_csv_uses_rules_and_logs_batch(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    income_csv = tmp_path / "kartra.csv"
    income_csv.write_text(
        "Date,Revenue (USD),Product\n"
        "2025-01-01,100.00,Course A\n"
        "2025-01-15,50.00,Course B\n",
        encoding="utf-8",
    )

    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={
            "kartra": {
                "platform": "Kartra",
                "category": "Income: Business",
                "use_type": "Business",
                "csv_columns": {
                    "date": "Date",
                    "amount": "Revenue (USD)",
                    "description": "Product",
                },
            }
        },
        ai_categorizer={},
        raw={},
    )

    with connect(db_path) as conn:
        first = import_income_csv(conn, file_path=income_csv, source_name="kartra", rules=rules, dry_run=False)
        second = import_income_csv(conn, file_path=income_csv, source_name="kartra", rules=rules, dry_run=False)

        txns = conn.execute(
            "SELECT amount_cents, use_type, category_source FROM transactions ORDER BY date"
        ).fetchall()
        batch = conn.execute(
            "SELECT source_type FROM import_batches ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

    assert first.inserted == 2
    assert first.errors == 0
    assert second.inserted == 0
    assert second.skipped_duplicates == 1

    assert len(txns) == 2
    assert [row["amount_cents"] for row in txns] == [10000, 5000]
    assert all(row["use_type"] == "Business" for row in txns)
    assert all(row["category_source"] == "keyword_rule" for row in txns)
    assert batch["source_type"] == "income_csv"


def test_export_wave_command_and_income_source_command(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        (
            """
income_sources:
  kartra:
    platform: Kartra
    category: "Income: Business"
    use_type: Business
    csv_columns:
      date: Date
      amount: "Revenue (USD)"
      description: Product
"""
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    income_csv = tmp_path / "income.csv"
    income_csv.write_text(
        "Date,Revenue (USD),Product\n"
        "2025-02-01,125.00,Program\n",
        encoding="utf-8",
    )

    code = main(["txn", "import", "--file", str(income_csv), "--income-source", "kartra"])
    assert code == 0
    import_payload = json.loads(capsys.readouterr().out)
    assert import_payload["status"] == "success"
    assert import_payload["data"]["inserted"] == 1

    with connect(db_path) as conn:
        category_row = conn.execute("SELECT id FROM categories WHERE name = 'Travel'").fetchone()
        if category_row:
            travel_id = category_row["id"]
        else:
            travel_id = uuid.uuid4().hex
            conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Travel', 0)", (travel_id,))
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, source)
            VALUES (?, '2025-02-10', 'Airline', -45000, ?, 'manual')
            """,
            (uuid.uuid4().hex, travel_id),
        )
        conn.commit()

    out_dir = tmp_path / "wave_out"
    code = main(["export", "wave", "--month", "2025-02", "--output", str(out_dir)])
    assert code == 0
    wave_payload = json.loads(capsys.readouterr().out)
    assert wave_payload["status"] == "success"
    assert wave_payload["data"]["files"]
