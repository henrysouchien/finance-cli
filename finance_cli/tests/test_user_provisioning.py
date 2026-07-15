from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.db import connect
from finance_cli.user_provisioning import provision_user, user_db_path, user_dir, user_rules_path
from finance_cli.user_rules import CANONICAL_CATEGORIES


def test_user_db_path_returns_expected_location(tmp_path: Path) -> None:
    data_root = tmp_path / "users"

    assert user_db_path(data_root, "alice") == data_root / "alice" / "finance.db"


def test_user_rules_path_returns_expected_location(tmp_path: Path) -> None:
    data_root = tmp_path / "users"

    assert user_rules_path(data_root, "alice") == data_root / "alice" / "rules.yaml"


def test_provision_user_creates_db_and_copies_rules_template(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")

    report = provision_user(
        data_root=data_root,
        user_id="alice",
        template_rules_path=template_rules,
    )

    db_path = Path(report["db_path"])
    rules_path = Path(report["rules_path"])

    assert db_path == data_root / "alice" / "finance.db"
    assert db_path.exists()
    assert rules_path.read_text(encoding="utf-8") == "keyword_rules: []\n"

    with connect(db_path=db_path) as conn:
        schema_version = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'"
        ).fetchone()

    assert schema_version is not None


def test_provision_user_is_idempotent(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")

    provision_user(
        data_root=data_root,
        user_id="alice",
        template_rules_path=template_rules,
    )

    rules_path = user_rules_path(data_root, "alice")
    rules_path.write_text("keyword_rules:\n  - keywords: [coffee]\n", encoding="utf-8")

    second = provision_user(
        data_root=data_root,
        user_id="alice",
        template_rules_path=template_rules,
    )

    assert Path(second["db_path"]).exists()
    assert rules_path.read_text(encoding="utf-8") == "keyword_rules:\n  - keywords: [coffee]\n"


def test_provision_user_can_seed_canonical_categories(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")

    report = provision_user(
        data_root=data_root,
        user_id="alice",
        template_rules_path=template_rules,
        ensure_canonical_categories=True,
    )

    with connect(Path(report["db_path"])) as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM categories WHERE is_system = 1"
        ).fetchone()["n"]

    assert count == len(CANONICAL_CATEGORIES)


def test_provision_user_reconciles_missing_canonical_categories(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")

    provision_user(
        data_root=data_root,
        user_id="alice",
        template_rules_path=template_rules,
        ensure_canonical_categories=True,
    )
    db_path = user_db_path(data_root, "alice")

    with connect(db_path) as conn:
        conn.execute("DELETE FROM categories WHERE name = 'Dining'")
        conn.commit()

    provision_user(
        data_root=data_root,
        user_id="alice",
        template_rules_path=template_rules,
        ensure_canonical_categories=True,
    )

    with connect(db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM categories WHERE is_system = 1"
        ).fetchone()["n"]
        dining = conn.execute("SELECT name FROM categories WHERE name = 'Dining'").fetchone()

    assert count == len(CANONICAL_CATEGORIES)
    assert dining is not None


def test_existing_user_db_picks_up_pending_migration(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")

    provision_user(
        data_root=data_root,
        user_id="u1",
        template_rules_path=template_rules,
    )

    db_path = data_root / "u1" / "finance.db"

    with connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "telegram_config" in tables

    with connect(db_path) as conn:
        conn.execute("DROP TABLE telegram_config")
        conn.execute("DELETE FROM schema_version WHERE version = 46")

    provision_user(
        data_root=data_root,
        user_id="u1",
        template_rules_path=template_rules,
    )

    with connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "telegram_config" in tables


@pytest.mark.parametrize(
    "bad_user_id",
    [
        "../../etc",
        "alice/../bob",
        "foo/bar",
        r"foo\bar",
        ".",
        "..",
    ],
)
def test_user_dir_rejects_invalid_user_ids(tmp_path: Path, bad_user_id: str) -> None:
    with pytest.raises(ValueError):
        user_dir(tmp_path / "users", bad_user_id)
