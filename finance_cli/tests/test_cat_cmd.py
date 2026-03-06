from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _write_rules(path: Path) -> None:
    path.write_text(
        (
            "category_aliases:\n"
            '  "Restaurant-Restaurant": "Dining"\n'
            '  "Miscellaneous": null\n'
        ),
        encoding="utf-8",
    )


def _seed_legacy_categories_and_transactions(db_path: Path) -> dict[str, str]:
    ids = {
        "legacy_restaurant": uuid.uuid4().hex,
        "legacy_other": uuid.uuid4().hex,
        "txn_csv": uuid.uuid4().hex,
        "txn_other": uuid.uuid4().hex,
        "txn_plaid": uuid.uuid4().hex,
    }
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Restaurant-Restaurant', 0)",
            (ids["legacy_restaurant"],),
        )
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Miscellaneous', 0)",
            (ids["legacy_other"],),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id,
                date,
                description,
                amount_cents,
                category_id,
                source,
                is_active
            ) VALUES (?, '2026-01-01', 'CSV Legacy Restaurant', -1500, ?, 'csv_import', 1)
            """,
            (ids["txn_csv"], ids["legacy_restaurant"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id,
                date,
                description,
                amount_cents,
                category_id,
                source,
                is_active
            ) VALUES (?, '2026-01-02', 'CSV Misc', -900, ?, 'csv_import', 1)
            """,
            (ids["txn_other"], ids["legacy_other"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id,
                date,
                description,
                amount_cents,
                source,
                raw_plaid_json,
                is_active
            ) VALUES (?, '2026-01-03', 'Plaid Txn', -700, 'plaid', ?, 1)
            """,
            (
                ids["txn_plaid"],
                json.dumps({"personal_finance_category": {"detailed": "FOOD_AND_DRINK_RESTAURANT"}}),
            ),
        )
        conn.commit()
    return ids


def test_cat_normalize_dry_run_reports_counts(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    _write_rules(tmp_path / "rules.yaml")
    ids = _seed_legacy_categories_and_transactions(db_path)

    code = main(["cat", "normalize", "--dry-run"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["source_category_backfilled"]["plaid"] == 1
    assert payload["data"]["source_category_backfilled"]["csv_pdf"] == 2
    assert payload["data"]["mappings_seeded"] == 2
    assert payload["data"]["categories_remapped"] == 2
    assert payload["data"]["transactions_moved"] == 1
    assert payload["data"]["transactions_nulled"] == 1

    with connect(db_path) as conn:
        legacy = conn.execute(
            "SELECT id FROM categories WHERE id IN (?, ?)",
            (ids["legacy_restaurant"], ids["legacy_other"]),
        ).fetchall()
        assert len(legacy) == 2

        source_row = conn.execute(
            "SELECT source_category FROM transactions WHERE id = ?",
            (ids["txn_plaid"],),
        ).fetchone()
        assert source_row["source_category"] is None


def test_cat_normalize_remaps_and_deletes_legacy_categories(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    _write_rules(tmp_path / "rules.yaml")
    ids = _seed_legacy_categories_and_transactions(db_path)

    code = main(["cat", "normalize"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is False
    assert payload["data"]["transactions_moved"] == 1
    assert payload["data"]["transactions_nulled"] == 1

    with connect(db_path) as conn:
        legacy = conn.execute(
            "SELECT id FROM categories WHERE id IN (?, ?)",
            (ids["legacy_restaurant"], ids["legacy_other"]),
        ).fetchall()
        assert legacy == []

        dining = conn.execute("SELECT id, is_system FROM categories WHERE name = 'Dining'").fetchone()
        assert dining is not None
        assert dining["is_system"] == 1

        csv_txn = conn.execute(
            "SELECT category_id, source_category FROM transactions WHERE id = ?",
            (ids["txn_csv"],),
        ).fetchone()
        assert csv_txn["category_id"] == dining["id"]
        assert csv_txn["source_category"] == "Restaurant-Restaurant"

        other_txn = conn.execute(
            "SELECT category_id, source_category FROM transactions WHERE id = ?",
            (ids["txn_other"],),
        ).fetchone()
        assert other_txn["category_id"] is None
        assert other_txn["source_category"] == "Miscellaneous"

        plaid_txn = conn.execute(
            "SELECT source_category FROM transactions WHERE id = ?",
            (ids["txn_plaid"],),
        ).fetchone()
        assert plaid_txn["source_category"] == "FOOD_AND_DRINK_RESTAURANT"


def test_cat_auto_categorize_reprocesses_mapping_sources_and_skips_reviewed(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    category_id = uuid.uuid4().hex
    txn_ids = {
        "null": uuid.uuid4().hex,
        "ambiguous": uuid.uuid4().hex,
        "institution": uuid.uuid4().hex,
        "plaid": uuid.uuid4().hex,
        "category_mapping": uuid.uuid4().hex,
        "user": uuid.uuid4().hex,
        "keyword_rule": uuid.uuid4().hex,
        "vendor_memory": uuid.uuid4().hex,
        "ai": uuid.uuid4().hex,
        "inactive_institution": uuid.uuid4().hex,
        "reviewed_null": uuid.uuid4().hex,
        "reviewed_category_mapping": uuid.uuid4().hex,
    }

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Dining', 1)",
            (category_id,),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW NULL', -100, NULL, NULL, 'manual', 1)
            """,
            (txn_ids["null"],),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW AMBIGUOUS', -100, ?, 'ambiguous', 'manual', 1)
            """,
            (txn_ids["ambiguous"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW INSTITUTION', -100, ?, 'institution', 'csv_import', 1)
            """,
            (txn_ids["institution"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW PLAID', -100, ?, 'plaid', 'plaid', 1)
            """,
            (txn_ids["plaid"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW CATEGORY_MAPPING', -100, ?, 'category_mapping', 'csv_import', 1)
            """,
            (txn_ids["category_mapping"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW USER', -100, ?, 'user', 'manual', 1)
            """,
            (txn_ids["user"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW KEYWORD', -100, ?, 'keyword_rule', 'manual', 1)
            """,
            (txn_ids["keyword_rule"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW MEMORY', -100, ?, 'vendor_memory', 'manual', 1)
            """,
            (txn_ids["vendor_memory"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW AI', -100, ?, 'ai', 'manual', 1)
            """,
            (txn_ids["ai"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active)
            VALUES (?, '2026-01-01', 'ROW INACTIVE INSTITUTION', -100, ?, 'institution', 'csv_import', 0)
            """,
            (txn_ids["inactive_institution"], category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active, is_reviewed)
            VALUES (?, '2026-01-01', 'ROW REVIEWED NULL', -100, NULL, NULL, 'manual', 1, 1)
            """,
            (txn_ids["reviewed_null"],),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_id, category_source, source, is_active, is_reviewed)
            VALUES (?, '2026-01-01', 'ROW REVIEWED CATEGORY_MAPPING', -100, ?, 'category_mapping', 'manual', 1, 1)
            """,
            (txn_ids["reviewed_category_mapping"], category_id),
        )
        conn.commit()

    seen: list[str] = []

    def _fake_match(conn, description, use_type, source_category=None, is_payment=False):
        seen.append(str(description))
        return None

    monkeypatch.setattr("finance_cli.commands.cat.match_transaction", _fake_match)

    code = main(["cat", "auto-categorize", "--dry-run"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"

    assert set(seen) == {
        "ROW NULL",
        "ROW AMBIGUOUS",
        "ROW INSTITUTION",
        "ROW PLAID",
        "ROW CATEGORY_MAPPING",
    }
