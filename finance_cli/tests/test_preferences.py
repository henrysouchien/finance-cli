from __future__ import annotations

import hashlib
import json
import tarfile
import uuid
from pathlib import Path
from types import SimpleNamespace

from finance_cli.db import connect, initialize_database
from finance_cli.preferences import export_preferences, import_preferences, validate_bundle


def _make_workspace(tmp_path: Path, name: str, *, with_files: bool = True) -> tuple[Path, Path]:
    data_dir = tmp_path / name
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "finance.db"
    initialize_database(db_path)

    if with_files:
        (data_dir / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
        (data_dir / "agent_memory.md").write_text("# Test memory\n", encoding="utf-8")
        sessions_dir = data_dir / "sessions"
        sessions_dir.mkdir()
        (sessions_dir / "2026-03-10.md").write_text("Session note\n", encoding="utf-8")

    return db_path, data_dir


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, level) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_account(
    conn,
    *,
    institution: str = "Test Bank",
    account_name: str = "Checking",
    card_ending: str = "1234",
    is_business: int = 0,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts
            (id, institution_name, account_name, card_ending, account_type, is_active, is_business)
        VALUES (?, ?, ?, ?, 'checking', 1, ?)
        """,
        (account_id, institution, account_name, card_ending, is_business),
    )
    conn.commit()
    return account_id


def _seed_preferences(conn) -> tuple[str, str]:
    category_id = _seed_category(conn, "TestDining")
    account_id = _seed_account(conn, is_business=1)

    conn.execute(
        """
        INSERT INTO vendor_memory
            (id, description_pattern, canonical_name, category_id, use_type, confidence, priority,
             is_enabled, is_confirmed, match_count)
        VALUES (?, 'STARBUCKS', 'Starbucks', ?, 'Any', 0.95, 0, 1, 1, 3)
        """,
        (uuid.uuid4().hex, category_id),
    )
    conn.execute(
        """
        INSERT INTO budgets
            (id, category_id, period, amount_cents, effective_from, effective_to, use_type)
        VALUES (?, ?, 'monthly', 40000, '2026-01-01', NULL, 'Personal')
        """,
        (uuid.uuid4().hex, category_id),
    )
    conn.execute(
        """
        INSERT INTO biz_section_budgets
            (id, pl_section, amount_cents, period, effective_from, effective_to)
        VALUES (?, 'opex_technology', 50000, 'monthly', '2026-01-01', NULL)
        """,
        (uuid.uuid4().hex,),
    )
    conn.execute(
        """
        INSERT INTO goals
            (id, name, metric, target_cents, direction, deadline, is_active)
        VALUES (?, 'Save 10k', 'net_worth', 1000000, 'up', '2026-12-31', 1)
        """,
        (uuid.uuid4().hex,),
    )
    conn.execute(
        """
        INSERT INTO subscriptions
            (id, vendor_name, category_id, amount_cents, frequency, next_expected, account_id,
             is_active, use_type, sub_type, is_auto_detected)
        VALUES (?, 'Netflix', ?, 1599, 'monthly', '2026-03-01', ?, 1, 'Personal', 'fixed', 1)
        """,
        (uuid.uuid4().hex, category_id, account_id),
    )
    conn.execute(
        """
        INSERT INTO category_mappings
            (id, source_category, source, category_id, created_by, confidence, match_count, is_enabled)
        VALUES (?, 'Food & Drink', 'plaid', ?, 'system', 1.0, 2, 1)
        """,
        (uuid.uuid4().hex, category_id),
    )
    conn.execute(
        "INSERT INTO pl_section_map (id, category_id, pl_section, display_order) VALUES (?, ?, 'revenue', 1)",
        (uuid.uuid4().hex, category_id),
    )
    conn.execute(
        """
        INSERT INTO schedule_c_map
            (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes)
        VALUES (?, ?, 'Other expenses', '27a', 1.0, 2025, 'Test note')
        """,
        (uuid.uuid4().hex, category_id),
    )
    conn.execute("INSERT INTO mileage_rates (tax_year, rate_cents) VALUES (2099, 70)")
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('theme', 'dark')")
    conn.execute(
        "INSERT OR REPLACE INTO provider_routing (institution_name, provider) VALUES ('Test Bank', 'plaid')"
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO tax_config (tax_year, config_key, config_value)
        VALUES (2025, 'filing_status', 'single')
        """
    )
    conn.commit()
    return category_id, account_id


def _count_rows(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _read_bundle_member(bundle_path: Path, member_name: str) -> bytes:
    with tarfile.open(bundle_path, "r:gz") as tar:
        return tar.extractfile(member_name).read()


def _read_bundle_manifest(bundle_path: Path) -> dict:
    return json.loads(_read_bundle_member(bundle_path, "manifest.json").decode("utf-8"))


def _read_bundle_jsonl(bundle_path: Path, member_name: str) -> list[dict]:
    data = _read_bundle_member(bundle_path, member_name).decode("utf-8")
    rows: list[dict] = []
    for line in data.splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _export_source_bundle(source_db: Path, source_data_dir: Path, *, destination: Path | None = None):
    with connect(source_db) as conn:
        return export_preferences(
            conn,
            destination=destination,
            data_dir=source_data_dir,
            rules_path=source_data_dir / "rules.yaml",
        )


def test_export_creates_valid_bundle(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
        result = export_preferences(conn, data_dir=source_data_dir, rules_path=source_data_dir / "rules.yaml")

    assert result.bundle_path.exists()
    assert result.bundle_path.suffixes == [".tar", ".gz"]
    manifest = _read_bundle_manifest(result.bundle_path)
    with tarfile.open(result.bundle_path, "r:gz") as tar:
        names = set(tar.getnames())

    assert "manifest.json" in names
    expected_jsonl = {
        "vendor_memory.jsonl",
        "budgets.jsonl",
        "biz_section_budgets.jsonl",
        "goals.jsonl",
        "subscriptions.jsonl",
        "category_mappings.jsonl",
        "pl_section_map.jsonl",
        "schedule_c_map.jsonl",
        "mileage_rates.jsonl",
        "settings.jsonl",
        "provider_routing.jsonl",
        "tax_config.jsonl",
        "account_business_flags.jsonl",
    }
    assert expected_jsonl.issubset(names)
    assert manifest["version"] == 1
    assert all(not entry["path"].endswith(".jsonl") for entry in manifest["files"])


def test_export_manifest_checksums(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
        result = export_preferences(conn, data_dir=source_data_dir, rules_path=source_data_dir / "rules.yaml")

    manifest = _read_bundle_manifest(result.bundle_path)
    for file_entry in manifest["files"]:
        actual_sha = hashlib.sha256(_read_bundle_member(result.bundle_path, file_entry["path"])).hexdigest()
        assert actual_sha == file_entry["sha256"]

    for table_name, table_meta in manifest["tables"].items():
        actual_sha = hashlib.sha256(_read_bundle_member(result.bundle_path, f"{table_name}.jsonl")).hexdigest()
        assert actual_sha == table_meta["sha256"]


def test_export_category_denormalization(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
        result = export_preferences(conn, data_dir=source_data_dir, rules_path=source_data_dir / "rules.yaml")

    vendor_rows = _read_bundle_jsonl(result.bundle_path, "vendor_memory.jsonl")
    subscription_rows = _read_bundle_jsonl(result.bundle_path, "subscriptions.jsonl")

    assert vendor_rows[0]["category_name"] == "TestDining"
    assert "category_id" not in vendor_rows[0]
    assert subscription_rows[0]["category_name"] == "TestDining"
    assert subscription_rows[0]["account_institution"] == "Test Bank"
    assert subscription_rows[0]["account_card_ending"] == "1234"
    assert "account_id" not in subscription_rows[0]


def test_import_merge_inserts_new(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        _seed_category(conn, "TestDining")
        _seed_account(conn)
        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="merge",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        assert result.tables_imported["vendor_memory"] == 1
        assert result.tables_imported["budgets"] == 1
        assert result.tables_imported["account_business_flags"] == 1
        assert _count_rows(conn, "vendor_memory") == 1
        assert _count_rows(conn, "budgets") == 1
        assert _count_rows(conn, "goals") == 1

    assert (target_data_dir / "rules.yaml").read_text(encoding="utf-8") == "keyword_rules: []\n"
    assert (target_data_dir / "agent_memory.md").read_text(encoding="utf-8") == "# Test memory\n"
    assert (target_data_dir / "sessions" / "2026-03-10.md").read_text(encoding="utf-8") == "Session note\n"


def test_import_merge_skips_existing(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        category_id = _seed_category(conn, "TestDining")
        _seed_account(conn)
        conn.execute(
            """
            INSERT INTO vendor_memory
                (id, description_pattern, canonical_name, category_id, use_type, confidence, priority,
                 is_enabled, is_confirmed, match_count)
            VALUES (?, 'STARBUCKS', 'Starbucks', ?, 'Any', 0.99, 0, 1, 1, 1)
            """,
            (uuid.uuid4().hex, category_id),
        )
        conn.execute(
            """
            INSERT INTO budgets
                (id, category_id, period, amount_cents, effective_from, effective_to, use_type)
            VALUES (?, ?, 'monthly', 35000, '2026-02-01', NULL, 'Personal')
            """,
            (uuid.uuid4().hex, category_id),
        )
        conn.commit()

        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="merge",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        assert result.tables_skipped["vendor_memory"] == 1
        assert result.tables_skipped["budgets"] == 1
        assert _count_rows(conn, "vendor_memory") == 1
        assert _count_rows(conn, "budgets") == 1


def test_import_overwrite_replaces_all(tmp_path: Path, monkeypatch) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    import finance_cli.backup as backup_mod

    monkeypatch.setattr(
        backup_mod,
        "create_backup",
        lambda *args, **kwargs: SimpleNamespace(bundle_path=target_data_dir / "noop.tar.gz"),
    )

    with connect(target_db) as conn:
        category_id = _seed_category(conn, "TestDining")
        _seed_account(conn)
        conn.execute(
            """
            INSERT INTO vendor_memory
                (id, description_pattern, canonical_name, category_id, use_type, confidence, priority,
                 is_enabled, is_confirmed, match_count)
            VALUES (?, 'OLDPATTERN', 'Old', ?, 'Any', 1.0, 0, 1, 1, 0)
            """,
            (uuid.uuid4().hex, category_id),
        )
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('theme', 'light')")
        conn.commit()

        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="overwrite",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        patterns = {row[0] for row in conn.execute("SELECT description_pattern FROM vendor_memory").fetchall()}
        theme = conn.execute("SELECT value FROM settings WHERE key = 'theme'").fetchone()[0]

        assert result.mode == "overwrite"
        assert patterns == {"STARBUCKS"}
        assert theme == "dark"


def test_import_overwrite_auto_backups(tmp_path: Path, monkeypatch) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    import finance_cli.backup as backup_mod

    calls: list[dict] = []

    def _fake_backup(conn, **kwargs):
        del conn
        calls.append(kwargs)
        return SimpleNamespace(bundle_path=target_data_dir / "noop.tar.gz")

    monkeypatch.setattr(backup_mod, "create_backup", _fake_backup)

    with connect(target_db) as conn:
        _seed_category(conn, "TestDining")
        _seed_account(conn)
        import_preferences(
            bundle.bundle_path,
            conn,
            mode="overwrite",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

    assert len(calls) == 1
    assert calls[0]["backup_type"] == "pre_restore"
    assert calls[0]["data_dir"] == target_data_dir
    assert calls[0]["rules_path"] == target_data_dir / "rules.yaml"


def test_import_dry_run_no_changes(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        _seed_category(conn, "TestDining")
        _seed_account(conn)
        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="merge",
            dry_run=True,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        assert result.dry_run is True
        assert result.tables_imported["vendor_memory"] == 1
        assert _count_rows(conn, "vendor_memory") == 0
        assert not (target_data_dir / "rules.yaml").exists()


def test_import_missing_category_skip(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        _seed_account(conn)
        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="merge",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        assert result.categories_missing == ["TestDining"]
        assert _count_rows(conn, "vendor_memory") == 0
        assert _count_rows(conn, "budgets") == 0
        assert _count_rows(conn, "goals") == 1


def test_import_missing_category_create(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        _seed_account(conn)
        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="merge",
            create_missing_categories=True,
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        created = conn.execute(
            """
            SELECT c.name, p.name
              FROM categories c
              LEFT JOIN categories p ON p.id = c.parent_id
             WHERE c.name = 'TestDining'
            """
        ).fetchone()
        assert result.categories_created == ["TestDining"]
        assert created is not None
        assert created[0] == "TestDining"
        assert created[1] == "Other"
        assert _count_rows(conn, "vendor_memory") == 1


def test_import_account_flag_resolution(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        _seed_category(conn, "TestDining")
        account_id = _seed_account(conn, is_business=0)
        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="merge",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        is_business = conn.execute("SELECT is_business FROM accounts WHERE id = ?", (account_id,)).fetchone()[0]
        assert result.accounts_resolved >= 1
        assert is_business == 1


def test_import_account_flag_unresolved(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
    bundle = _export_source_bundle(source_db, source_data_dir)

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        _seed_category(conn, "TestDining")
        result = import_preferences(
            bundle.bundle_path,
            conn,
            mode="merge",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        assert result.accounts_unresolved >= 1
        assert any("Account not found in target DB" in warning for warning in result.warnings)


def test_roundtrip_export_import(tmp_path: Path) -> None:
    source_db, source_data_dir = _make_workspace(tmp_path, "source")
    with connect(source_db) as conn:
        _seed_preferences(conn)
        export_result = export_preferences(
            conn,
            data_dir=source_data_dir,
            rules_path=source_data_dir / "rules.yaml",
        )

    target_db, target_data_dir = _make_workspace(tmp_path, "target", with_files=False)
    with connect(target_db) as conn:
        _seed_category(conn, "TestDining")
        _seed_account(conn)
        result = import_preferences(
            export_result.bundle_path,
            conn,
            mode="merge",
            dry_run=False,
            data_dir=target_data_dir,
            rules_path=target_data_dir / "rules.yaml",
        )

        for table_name, expected_count in export_result.table_counts.items():
            if table_name == "account_business_flags":
                business_count = int(
                    conn.execute("SELECT COUNT(*) FROM accounts WHERE is_business = 1").fetchone()[0]
                )
                assert business_count == expected_count
            else:
                assert _count_rows(conn, table_name) == expected_count

        subscription_account_id = conn.execute("SELECT account_id FROM subscriptions").fetchone()[0]
        assert result.tables_imported["subscriptions"] == 1
        assert subscription_account_id is not None

    assert (target_data_dir / "rules.yaml").read_text(encoding="utf-8") == "keyword_rules: []\n"
    assert (target_data_dir / "agent_memory.md").read_text(encoding="utf-8") == "# Test memory\n"
    assert (target_data_dir / "sessions" / "2026-03-10.md").read_text(encoding="utf-8") == "Session note\n"


def test_validate_corrupt_bundle(tmp_path: Path) -> None:
    db_path, _ = _make_workspace(tmp_path, "workspace")
    corrupt_bundle = tmp_path / "corrupt.tar.gz"
    corrupt_bundle.write_bytes(b"not a tarball")

    with connect(db_path) as conn:
        result = validate_bundle(corrupt_bundle, conn)

    assert result.valid is False
    assert any("Invalid tar.gz" in error for error in result.errors)
