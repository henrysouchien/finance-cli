from __future__ import annotations

import json
import textwrap
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from finance_cli.__main__ import main
from finance_cli.commands import rules as rules_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.user_rules import resolve_rules_path


def _ensure_category(conn, name: str) -> None:
    row = conn.execute("SELECT id FROM categories WHERE lower(name) = lower(?)", (name,)).fetchone()
    if row:
        return
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (uuid.uuid4().hex, name),
    )
    conn.commit()


def test_resolve_rules_path_prefers_workspace_when_db_overridden(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    path = resolve_rules_path()
    assert path == (tmp_path / "rules.yaml").resolve()


def test_rules_show_uses_workspace_rules_file(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    code = main(["rules", "show"])
    assert code == 0

    payload = json.loads(capsys.readouterr().out)
    expected_path = (tmp_path / "rules.yaml").resolve()
    assert payload["status"] == "success"
    assert payload["data"]["path"] == str(expected_path)
    assert expected_path.exists()


def test_rules_edit_supports_editor_commands_with_flags(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("EDITOR", "code -w")
    initialize_database(db_path)

    captured: dict[str, object] = {}

    def fake_run(cmd, check):
        captured["cmd"] = cmd
        captured["check"] = check
        return 0

    monkeypatch.setattr("finance_cli.commands.rules.subprocess.run", fake_run)

    with connect(db_path) as conn:
        out = rules_cmd.handle_edit(SimpleNamespace(), conn)

    expected_path = (tmp_path / "rules.yaml").resolve()
    assert captured["cmd"] == ["code", "-w", str(expected_path)]
    assert captured["check"] is True
    assert out["data"]["path"] == str(expected_path)


def test_rules_test_does_not_apply_override_to_keyword_source(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["UBER"]
                category: "Travel"
                use_type: Personal
                priority: 0
            category_overrides:
              - categories: ["Travel"]
                force_use_type: Personal
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    code = main(["rules", "test", "--description", "UBER TRIP", "--source", "plaid"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "success"
    assert payload["data"]["keyword_match"]["category"] == "Travel"
    assert payload["data"]["category_override"] is None


def test_rules_show_returns_structured_sections(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["UBER", "LYFT"]
                category: "Travel"
                use_type: Personal
                priority: 0
            payment_keywords: ["payment", "autopay"]
            category_aliases:
              "Restaurant-Restaurant": "Dining"
            split_rules:
              - match:
                  category: "Dining"
                business_pct: 40
                business_category: "Professional Fees"
                personal_category: "Dining"
                note: "Work meals"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    code = main(["rules", "show"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "success"
    assert payload["command"] == "rules.show"
    assert payload["data"]["keyword_rules"] == [
        {
            "keywords": ["UBER", "LYFT"],
            "category": "Travel",
            "use_type": "Personal",
        }
    ]
    assert payload["data"]["payment_keywords"] == ["payment", "autopay"]
    assert payload["data"]["category_aliases"] == {"Restaurant-Restaurant": "Dining"}
    assert payload["data"]["split_rules"] == [
        {
            "match": {"category": "Dining", "keywords": []},
            "business_pct": 40.0,
            "business_category": "Professional Fees",
            "personal_category": "Dining",
            "note": "Work meals",
        }
    ]
    assert payload["data"]["counts"] == {
        "keyword_rules": 1,
        "payment_keywords": 2,
        "category_aliases": 1,
        "split_rules": 1,
    }
    assert payload["data"]["raw"]["keyword_rules"][0]["category"] == "Travel"


def test_rules_test_reports_payment_match_with_priority(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["LOOM"]
                category: "Software & Subscriptions"
                use_type: Business
                priority: 0
            payment_keywords:
              - "AUTO PYMT"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    code = main(["rules", "test", "--description", "BLOOMINGDALES DES:AUTO PYMT"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "success"
    assert payload["data"]["payment_match"] is True
    assert payload["data"]["keyword_match"]["matched_keyword"] == "LOOM"
    assert "takes priority" in payload["cli_report"]


def test_rules_add_keyword_creates_new_rule(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    (tmp_path / "rules.yaml").write_text("{}\n", encoding="utf-8")

    with connect(db_path) as conn:
        _ensure_category(conn, "Dining")
        result = rules_cmd.handle_add_keyword(
            SimpleNamespace(keyword="NEWVENDOR", category="Dining", use_type=None, priority=2),
            conn,
        )

    rules_path = tmp_path / "rules.yaml"
    payload = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert result["data"]["action"] == "added"
    assert payload["keyword_rules"] == [
        {
            "keywords": ["NEWVENDOR"],
            "category": "Dining",
            "priority": 2,
        }
    ]


def test_rules_add_keyword_appends_existing_category_and_use_type(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["COFFEE"]
                category: "Dining"
                use_type: Business
                priority: 9
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    with connect(db_path) as conn:
        _ensure_category(conn, "Dining")
        result = rules_cmd.handle_add_keyword(
            SimpleNamespace(keyword="LATTE", category="Dining", use_type="Business", priority=0),
            conn,
        )

    payload = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert result["data"]["action"] == "appended"
    assert payload["keyword_rules"] == [
        {
            "keywords": ["COFFEE", "LATTE"],
            "category": "Dining",
            "use_type": "Business",
            "priority": 9,
        }
    ]


def test_rules_add_keyword_rejects_duplicate_case_insensitive(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["NEWVENDOR"]
                category: "Dining"
                priority: 0
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    with connect(db_path) as conn:
        _ensure_category(conn, "Dining")
        with pytest.raises(ValueError, match="already exists"):
            rules_cmd.handle_add_keyword(
                SimpleNamespace(keyword="newvendor", category="Dining", use_type=None, priority=0),
                conn,
            )


def test_rules_remove_keyword_keeps_rule_when_other_keywords_remain(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["A", "B"]
                category: "Dining"
                priority: 0
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    with connect(db_path) as conn:
        result = rules_cmd.handle_remove_keyword(SimpleNamespace(keyword="a"), conn)

    payload = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert result["data"]["removed_rule"] is False
    assert result["data"]["category"] == "Dining"
    assert payload["keyword_rules"] == [
        {
            "keywords": ["B"],
            "category": "Dining",
            "priority": 0,
        }
    ]


def test_rules_remove_keyword_removes_rule_when_last_keyword_removed(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["ONLYONE"]
                category: "Dining"
                priority: 0
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    with connect(db_path) as conn:
        result = rules_cmd.handle_remove_keyword(SimpleNamespace(keyword="ONLYONE"), conn)

    payload = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert result["data"]["removed_rule"] is True
    assert payload["keyword_rules"] == []


def test_rules_remove_keyword_raises_when_not_found(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="not found"):
            rules_cmd.handle_remove_keyword(SimpleNamespace(keyword="MISSING"), conn)


def test_handle_list_returns_structured_rules(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    (tmp_path / "rules.yaml").write_text(
        'keyword_rules:\n  - keywords: ["UBER"]\n    category: "Transportation"\n    priority: 0\n',
        encoding="utf-8",
    )
    with connect(db_path) as conn:
        result = rules_cmd.handle_list(SimpleNamespace(), conn)
    assert "rules" in result["data"]
    assert isinstance(result["data"]["count"], int)
    assert result["data"]["count"] >= 1
    rule = result["data"]["rules"][0]
    assert "category" in rule
    assert "keywords" in rule
    assert "rule_index" in rule
    assert "priority" in rule


def test_handle_update_priority_by_index(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    (tmp_path / "rules.yaml").write_text(
        'keyword_rules:\n  - keywords: ["HANDLETEST"]\n    category: "Dining"\n    priority: 0\n',
        encoding="utf-8",
    )
    with connect(db_path) as conn:
        _ensure_category(conn, "Dining")
        result = rules_cmd.handle_update_priority(
            SimpleNamespace(rule_index=0, priority=7), conn
        )
    assert result["data"]["old_priority"] == 0
    assert result["data"]["new_priority"] == 7
    assert result["data"]["rule_index"] == 0


def test_handle_update_priority_invalid_index(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    (tmp_path / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="out of range"):
            rules_cmd.handle_update_priority(SimpleNamespace(rule_index=0, priority=5), conn)


def test_handle_update_priority_negative_index(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    (tmp_path / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="out of range"):
            rules_cmd.handle_update_priority(SimpleNamespace(rule_index=-1, priority=5), conn)


def test_handle_update_priority_targets_correct_rule(tmp_path: Path, monkeypatch) -> None:
    """With two rules for the same category (different use_types), rule_index targets only the right one."""
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    # Pre-seed two rules for Dining - different use_types create separate rule entries
    (tmp_path / "rules.yaml").write_text(textwrap.dedent("""\
        keyword_rules:
          - keywords: ["RULE_A"]
            category: "Dining"
            use_type: Personal
            priority: 0
          - keywords: ["RULE_B"]
            category: "Dining"
            use_type: Business
            priority: 3
    """), encoding="utf-8")
    with connect(db_path) as conn:
        _ensure_category(conn, "Dining")
        # Update only rule at index 0
        rules_cmd.handle_update_priority(SimpleNamespace(rule_index=0, priority=99), conn)
        listing = rules_cmd.handle_list(SimpleNamespace(), conn)

    rules_data = listing["data"]["rules"]
    rule_a = [r for r in rules_data if "RULE_A" in r["keywords"]][0]
    rule_b = [r for r in rules_data if "RULE_B" in r["keywords"]][0]
    assert rule_a["priority"] == 99  # changed
    assert rule_b["priority"] == 3   # unchanged


def test_write_rules_invalidates_cache(tmp_path: Path, monkeypatch) -> None:
    """After _write_raw_rules_yaml, load_rules() returns fresh data (not stale cache)."""
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    (tmp_path / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")

    from finance_cli import user_rules
    from finance_cli.user_rules import load_rules

    # Populate cache
    rules_before = load_rules()
    assert len(rules_before.keyword_rules) == 0
    assert user_rules._rules_cache is not None  # cache populated

    # Add a keyword via handler (calls _write_raw_rules_yaml internally)
    with connect(db_path) as conn:
        _ensure_category(conn, "Dining")
        rules_cmd.handle_add_keyword(
            SimpleNamespace(keyword="CACHEINVALIDTEST", category="Dining", use_type=None, priority=0), conn
        )

    # Cache should have been cleared by the write
    assert user_rules._rules_cache is None

    # load_rules() should see the new keyword immediately
    rules_after = load_rules()
    keyword_lists = [r.keywords for r in rules_after.keyword_rules]
    assert any("CACHEINVALIDTEST" in kws for kws in keyword_lists)


def test_rules_add_and_remove_keyword_cli(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    with connect(db_path) as conn:
        _ensure_category(conn, "Dining")

    add_code = main(["rules", "add-keyword", "--keyword", "CLIVENDOR", "--category", "Dining"])
    add_payload = json.loads(capsys.readouterr().out)

    assert add_code == 0
    assert add_payload["status"] == "success"
    assert add_payload["data"]["keyword"] == "CLIVENDOR"

    remove_code = main(["rules", "remove-keyword", "--keyword", "CLIVENDOR"])
    remove_payload = json.loads(capsys.readouterr().out)

    assert remove_code == 0
    assert remove_payload["status"] == "success"
    assert remove_payload["data"]["keyword"] == "CLIVENDOR"
