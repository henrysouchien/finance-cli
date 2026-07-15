from __future__ import annotations

import json
from pathlib import Path

from finance_cli.__main__ import build_parser, main


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _patch_local_sync_paths(monkeypatch, tmp_path: Path) -> tuple[Path, Path]:
    local_root = tmp_path / ".cashnerd"
    local_data = local_root / "data"
    local_db = local_data / "finance.db"
    local_data.mkdir(parents=True)

    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(local_data))
    monkeypatch.delenv("FINANCE_CLI_DB", raising=False)

    import finance_cli.sync.auth as sync_auth
    import finance_cli.sync.cli_proxy as cli_proxy
    import finance_cli.sync.config as sync_config
    import finance_cli.sync.engine as sync_engine

    monkeypatch.setattr(sync_config, "CASHNERD_DIR", local_root)
    monkeypatch.setattr(sync_config, "CASHNERD_CONFIG_PATH", local_root / "config.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AUTH_DIR", local_root / "auth")
    monkeypatch.setattr(sync_config, "CASHNERD_TOKEN_PATH", local_root / "auth" / "token.json")
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", local_data)
    monkeypatch.setattr(sync_config, "CASHNERD_DB_PATH", local_db)
    monkeypatch.setattr(sync_config, "CASHNERD_RULES_PATH", local_data / "rules.yaml")
    monkeypatch.setattr(sync_config, "CASHNERD_UPLOADS_DIR", local_data / "uploads")
    monkeypatch.setattr(sync_config, "CASHNERD_SKILL_STATE_PATH", local_data / "skill_state.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AGENT_MEMORY_PATH", local_data / "agent_memory.md")
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_DIR", local_root / "sync")
    monkeypatch.setattr(sync_config, "CASHNERD_PENDING_CHANGESET_PATH", local_root / "sync" / "pending_changeset.json")
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_LOG_PATH", local_root / "sync" / "sync_log.json")

    monkeypatch.setattr(sync_engine, "CASHNERD_DIR", sync_config.CASHNERD_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DATA_DIR", sync_config.CASHNERD_DATA_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DB_PATH", sync_config.CASHNERD_DB_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_RULES_PATH", sync_config.CASHNERD_RULES_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_SKILL_STATE_PATH", sync_config.CASHNERD_SKILL_STATE_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_AGENT_MEMORY_PATH", sync_config.CASHNERD_AGENT_MEMORY_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_PENDING_CHANGESET_PATH", sync_config.CASHNERD_PENDING_CHANGESET_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_SYNC_LOG_PATH", sync_config.CASHNERD_SYNC_LOG_PATH)
    monkeypatch.setattr(cli_proxy, "CASHNERD_DB_PATH", local_db)
    monkeypatch.setattr(sync_auth, "CASHNERD_TOKEN_PATH", sync_config.CASHNERD_TOKEN_PATH)

    return local_root, local_db


def _parser_command_names() -> set[str]:
    parser = build_parser()
    command_names: set[str] = set()

    def walk(subparser) -> None:
        for action in subparser._actions:
            choices = getattr(action, "choices", None)
            if not isinstance(choices, dict):
                continue
            for child in choices.values():
                command_name = getattr(child, "_defaults", {}).get("command_name")
                if command_name:
                    command_names.add(str(command_name))
                walk(child)

    walk(parser)
    return command_names


def _normalized_tool_name(command_name: str) -> str:
    return command_name.replace(".", "_").replace("-", "_").replace(" ", "_")


def test_direct_cli_server_proxied_commands_have_local_sync_proxy_specs() -> None:
    import finance_cli.sync.cli_proxy as cli_proxy
    from finance_cli.sync.tool_classification import SERVER_PROXIED_TOOLS

    expected = {
        command_name
        for command_name in _parser_command_names()
        if _normalized_tool_name(command_name) in SERVER_PROXIED_TOOLS
    }

    assert expected <= set(cli_proxy._LOCAL_SYNC_PROXY_COMMANDS)
    for command_name in expected:
        assert (
            cli_proxy._LOCAL_SYNC_PROXY_COMMANDS[command_name].tool_name
            == _normalized_tool_name(command_name)
        )


def test_server_proxied_cli_commands_have_local_sync_proxy_specs() -> None:
    import finance_cli.sync.cli_proxy as cli_proxy

    parser = build_parser()
    cases = [
        (
            ["interventions", "act", "42"],
            "interventions_act",
            {"log_id": 42},
            True,
            {"data": {"id": 42}, "summary": {"log_id": 42, "action": "acted"}},
            "action=acted",
        ),
        (
            ["interventions", "dismiss", "43"],
            "interventions_dismiss",
            {"log_id": 43},
            True,
            {"data": {"id": 43}, "summary": {"log_id": 43, "action": "dismissed"}},
            "action=dismissed",
        ),
        (
            ["interventions", "mute", "I-2", "--reason", "testing"],
            "interventions_mute",
            {"pattern_id": "I-2", "reason": "testing"},
            True,
            {
                "data": {"pattern_id": "I-2", "reason": "testing", "created": True},
                "summary": {"pattern_id": "I-2", "created": True},
            },
            "created=True",
        ),
        (
            ["interventions", "unmute", "I-2"],
            "interventions_unmute",
            {"pattern_id": "I-2"},
            True,
            {
                "data": {"pattern_id": "I-2", "deleted": True},
                "summary": {"deleted": True},
            },
            "deleted=True",
        ),
        (
            ["plaid", "status"],
            "plaid_status",
            {},
            False,
            {
                "data": {
                    "configured": True,
                    "has_sdk": True,
                    "webhook_url_configured": True,
                    "items": [],
                }
            },
            "webhook_url=True",
        ),
        (
            ["setup", "status"],
            "setup_status",
            {},
            False,
            {
                "data": {
                    "ready": True,
                    "env": {"counts": {"ok": 2, "warn": 0, "fail": 0}},
                    "db": {"transaction_counts": {"active": 5}},
                    "plaid": {"items": [], "active_count": 0, "token_missing_count": 0},
                    "categories": {"present_count": 4, "expected_total": 4},
                    "vendor_memory": {"enabled_count": 1},
                    "rules": {"exists": True},
                    "next_steps": [],
                }
            },
            "System Status: Ready",
        ),
        (
            [
                "setup",
                "connect",
                "--user-id",
                "local-user",
                "--include-liabilities",
                "--timeout",
                "30",
                "--skip-sync",
                "--open-browser",
            ],
            "setup_connect",
            {
                "user_id": "local-user",
                "include_liabilities": True,
                "timeout": 30,
                "skip_sync": True,
                "open_browser": True,
            },
            True,
            {
                "data": {
                    "session": {"hosted_link_url": "https://plaid.example/link"},
                    "hosted_link_url": "https://plaid.example/link",
                    "error": "timed out",
                },
                "summary": {"linked": False, "error": "timed out"},
            },
            "URL: https://plaid.example/link",
        ),
        (
            ["stripe", "link"],
            "stripe_link",
            {},
            True,
            {
                "data": {
                    "ready": True,
                    "account_name": "Acme LLC",
                    "stripe_account_id": "acct_123",
                    "api_key_ref": "vault://secret",
                },
                "summary": {"ready": True},
            },
            "Connected to Acme LLC",
        ),
        (
            ["stripe", "sync", "--days", "14", "--force", "--backfill"],
            "stripe_sync",
            {"days": 14, "force": True, "backfill": True},
            True,
            {
                "data": {
                    "charges_added": 2,
                    "fees_added": 1,
                    "refunds_added": 0,
                    "adjustments_added": 0,
                    "payouts_matched": 1,
                    "payouts_ambiguous": 0,
                    "payouts_unmatched": 0,
                    "skipped_existing": 3,
                    "skipped_non_usd": 0,
                    "skipped_unknown_type": 0,
                    "skipped_cooldown": False,
                }
            },
            "charges_added=2",
        ),
        (
            ["stripe", "status"],
            "stripe_status",
            {},
            False,
            {
                "data": {
                    "configured": True,
                    "has_sdk": True,
                    "connection_count": 1,
                    "transaction_count": 7,
                    "connection_status": "active",
                }
            },
            "connections=1",
        ),
        (
            ["stripe", "revenue", "--month", "2026-06"],
            "stripe_revenue",
            {"month": "2026-06", "quarter": None, "year": None},
            False,
            {
                "data": {
                    "period": "2026-06",
                    "rows": [
                        {
                            "month": "2026-06",
                            "gross_cents": 12345,
                            "fees_cents": 345,
                            "refunds_cents": 0,
                            "net_cents": 12000,
                        }
                    ],
                    "totals": {
                        "gross_cents": 12345,
                        "fees_cents": 345,
                        "refunds_cents": 0,
                        "net_cents": 12000,
                    },
                }
            },
            "period=2026-06",
        ),
        (
            ["stripe", "unlink"],
            "stripe_unlink",
            {},
            True,
            {"data": {"updated": 1}, "summary": {"updated": 1}},
            "Stripe disconnected",
        ),
        (
            ["schwab", "sync"],
            "schwab_sync",
            {},
            True,
            {
                "data": {
                    "accounts_synced": 1,
                    "snapshots_upserted": 1,
                    "accounts_failed": 0,
                    "accounts": [],
                    "errors": [],
                }
            },
            "accounts_synced=1",
        ),
        (
            ["schwab", "status"],
            "schwab_status",
            {},
            False,
            {
                "data": {
                    "configured": True,
                    "has_sdk": True,
                    "token_exists": True,
                    "token_health": {"refresh_token_days_remaining": 3.0, "warnings": []},
                    "accounts": [],
                }
            },
            "token_exists=True",
        ),
        (
            [
                "rules",
                "add-keyword",
                "--keyword",
                "ACME",
                "--category",
                "Office Expense",
                "--use-type",
                "Business",
                "--priority",
                "5",
            ],
            "rules_add_keyword",
            {
                "keyword": "ACME",
                "category": "Office Expense",
                "use_type": "Business",
                "priority": 5,
            },
            True,
            {
                "data": {
                    "keyword": "ACME",
                    "category": "Office Expense",
                    "use_type": "Business",
                    "action": "added",
                },
                "summary": {"updated": 1},
            },
            "Added keyword 'ACME'",
        ),
        (
            [
                "rules",
                "add-split",
                "--business-pct",
                "80",
                "--business-category",
                "Office Expense",
                "--personal-category",
                "Rent",
                "--match-keywords",
                "COWORKING",
                "STUDIO",
                "--note",
                "shared workspace",
            ],
            "rules_add_split",
            {
                "business_pct": 80.0,
                "business_category": "Office Expense",
                "personal_category": "Rent",
                "match_category": None,
                "match_keywords": ["COWORKING", "STUDIO"],
                "note": "shared workspace",
            },
            True,
            {
                "data": {
                    "rule": {
                        "business_pct": 80,
                        "business_category": "Office Expense",
                        "personal_category": "Rent",
                        "match_keywords": ["COWORKING", "STUDIO"],
                    }
                },
                "summary": {"updated": 1, "split_rule_count": 1},
            },
            "Added split rule (80% business)",
        ),
        (
            ["rules", "remove-keyword", "--keyword", "ACME"],
            "rules_remove_keyword",
            {"keyword": "ACME", "dry_run": False},
            True,
            {
                "data": {"keyword": "ACME", "category": "Office Expense"},
                "summary": {"updated": 1},
            },
            "Removed keyword 'ACME'",
        ),
        (
            ["monthly", "run", "--month", "2026-06", "--sync", "--ai", "--dry-run", "--skip", "dedup"],
            "monthly_run",
            {
                "month": "2026-06",
                "sync": True,
                "ai": True,
                "dry_run": True,
                "skip": ["dedup"],
                "summary_only": True,
            },
            True,
            {
                "data": {
                    "month": "2026-06",
                    "steps": {
                        "sync": {"status": "success", "summary": {"added": 2}, "error": None},
                        "dedup": {"status": "skipped", "summary": None, "error": None},
                    },
                    "health": {
                        "unreviewed_count": 1,
                        "uncategorized_count": 2,
                        "budget_over_count": 0,
                    },
                },
                "summary": {
                    "steps_run": 1,
                    "steps_succeeded": 1,
                    "steps_failed": 0,
                    "steps_skipped": 1,
                },
            },
            "steps_run=1",
        ),
        (
            ["db", "restore", "--file", "/tmp/backup.tar.gz", "--yes"],
            "db_restore",
            {"bundle_path": "/tmp/backup.tar.gz", "dry_run": False},
            False,
            {
                "data": {
                    "restored": True,
                    "dry_run": False,
                    "bundle_path": "backup.tar.gz",
                    "warnings": [],
                },
                "summary": {"restored": True, "dry_run": False, "warning_count": 0},
            },
            "Restored from backup.tar.gz",
        ),
        (
            ["db", "import-preferences", "--file", "/tmp/preferences.tar.gz", "--mode", "merge", "--yes"],
            "db_import_preferences",
            {
                "bundle_path": "/tmp/preferences.tar.gz",
                "mode": "merge",
                "create_missing_categories": False,
                "dry_run": False,
            },
            True,
            {
                "data": {
                    "dry_run": False,
                    "categories_missing": [],
                    "accounts_unresolved": 0,
                },
                "summary": {"total_imported": 2, "total_skipped": 1},
            },
            "Imported 2 rows",
        ),
    ]

    for argv, tool_name, expected_args, expected_pull_after, envelope, report_fragment in cases:
        args = parser.parse_args(argv)
        spec = cli_proxy._LOCAL_SYNC_PROXY_COMMANDS[args.command_name]
        assert spec.tool_name == tool_name
        assert spec.pull_after is expected_pull_after
        assert spec.build_arguments(args) == expected_args
        assert spec.build_cli_report is not None
        assert report_fragment in str(spec.build_cli_report(envelope))


def test_setup_status_cli_uses_server_proxy_in_local_sync_mode(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_local_sync_paths(monkeypatch, tmp_path)

    import finance_cli.commands.setup_cmd as setup_cmd
    import finance_cli.sync.engine as sync_engine

    proxy_calls: list[tuple[str, dict[str, object], bool]] = []
    pulls = 0

    async def fake_proxy_tool(self, tool_name, arguments=None, *, wait_for_subscriber=True):
        del self
        proxy_calls.append((tool_name, dict(arguments or {}), wait_for_subscriber))
        return {
            "data": {
                "ready": False,
                "env": {
                    "counts": {"ok": 2, "warn": 0, "fail": 1},
                    "checks": [
                        {
                            "id": "rules",
                            "label": "Rules File",
                            "status": "fail",
                            "detail": "rules.yaml missing at /server/rules.yaml",
                        }
                    ],
                },
                "db": {"transaction_counts": {"active": 5}},
                "plaid": {
                    "items": [
                        {
                            "plaid_item_id": "server_item",
                            "status": "active",
                            "has_token_ref": True,
                            "token_missing": False,
                        }
                    ],
                    "active_count": 1,
                    "token_missing_count": 0,
                },
                "categories": {"present_count": 4, "expected_total": 4},
                "vendor_memory": {"enabled_count": 1},
                "rules": {"exists": True},
                "next_steps": ["Run finance_cli setup init."],
            },
            "summary": {"ready": False, "plaid_token_missing_count": 0},
        }

    async def fake_pull(self):
        nonlocal pulls
        del self
        pulls += 1

    def unexpected_local_status(*_args, **_kwargs):
        raise AssertionError("setup status should use the server proxy in local sync mode")

    monkeypatch.setattr(sync_engine.SyncEngine, "proxy_tool", fake_proxy_tool)
    monkeypatch.setattr(sync_engine.SyncEngine, "pull", fake_pull)
    monkeypatch.setattr(setup_cmd, "handle_status", unexpected_local_status)

    code, payload = _run_cli(["setup", "status"], capsys)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "setup.status"
    assert proxy_calls == [("setup_status", {}, False)]
    assert pulls == 0
    assert payload["data"]["plaid"]["items"][0]["has_token_ref"] is True
    assert payload["summary"]["plaid_token_missing_count"] == 0
    assert "System Status: Not Ready" in payload["cli_report"]
    assert "Plaid Items:   1 total, 1 active" in payload["cli_report"]
    assert "Rules File:    rules.yaml found" in payload["cli_report"]
    assert "Failed Checks:" in payload["cli_report"]
    assert "rules.yaml missing at /server/rules.yaml" in payload["cli_report"]


def test_stripe_sync_cli_uses_server_proxy_and_pulls_in_local_sync_mode(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _patch_local_sync_paths(monkeypatch, tmp_path)

    import finance_cli.commands.stripe_cmd as stripe_cmd
    import finance_cli.sync.engine as sync_engine

    proxy_calls: list[tuple[str, dict[str, object], bool]] = []
    pulls = 0

    async def fake_proxy_tool(self, tool_name, arguments=None, *, wait_for_subscriber=True):
        del self
        proxy_calls.append((tool_name, dict(arguments or {}), wait_for_subscriber))
        return {
            "data": {
                "charges_added": 2,
                "fees_added": 1,
                "refunds_added": 0,
                "adjustments_added": 0,
                "payouts_matched": 1,
                "payouts_ambiguous": 0,
                "payouts_unmatched": 0,
                "skipped_existing": 3,
                "skipped_non_usd": 0,
                "skipped_unknown_type": 0,
                "skipped_cooldown": False,
            },
            "summary": {"charges_added": 2},
        }

    async def fake_pull(self):
        nonlocal pulls
        del self
        pulls += 1

    def unexpected_local_sync(*_args, **_kwargs):
        raise AssertionError("stripe sync should use the server proxy in local sync mode")

    monkeypatch.setattr(sync_engine.SyncEngine, "proxy_tool", fake_proxy_tool)
    monkeypatch.setattr(sync_engine.SyncEngine, "pull", fake_pull)
    monkeypatch.setattr(stripe_cmd, "handle_sync", unexpected_local_sync)

    code, payload = _run_cli(
        ["stripe", "sync", "--days", "14", "--force", "--backfill"], capsys
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "stripe.sync"
    assert proxy_calls == [
        ("stripe_sync", {"days": 14, "force": True, "backfill": True}, False)
    ]
    assert pulls == 1
    assert payload["summary"]["charges_added"] == 2
    assert "charges_added=2" in payload["cli_report"]


def test_rules_add_keyword_cli_uses_server_proxy_and_pulls_in_local_sync_mode(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _patch_local_sync_paths(monkeypatch, tmp_path)

    import finance_cli.commands.rules as rules_cmd
    import finance_cli.sync.engine as sync_engine

    proxy_calls: list[tuple[str, dict[str, object], bool]] = []
    pulls = 0

    async def fake_proxy_tool(self, tool_name, arguments=None, *, wait_for_subscriber=True):
        del self
        proxy_calls.append((tool_name, dict(arguments or {}), wait_for_subscriber))
        return {
            "data": {
                "keyword": "ACME",
                "category": "Office Expense",
                "use_type": "Business",
                "action": "added",
            },
            "summary": {"updated": 1},
        }

    async def fake_pull(self):
        nonlocal pulls
        del self
        pulls += 1

    def unexpected_local_write(*_args, **_kwargs):
        raise AssertionError("rules add-keyword should use the server proxy in local sync mode")

    monkeypatch.setattr(sync_engine.SyncEngine, "proxy_tool", fake_proxy_tool)
    monkeypatch.setattr(sync_engine.SyncEngine, "pull", fake_pull)
    monkeypatch.setattr(rules_cmd, "handle_add_keyword", unexpected_local_write)

    code, payload = _run_cli(
        [
            "rules",
            "add-keyword",
            "--keyword",
            "ACME",
            "--category",
            "Office Expense",
            "--use-type",
            "Business",
            "--priority",
            "5",
        ],
        capsys,
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "rules.add-keyword"
    assert proxy_calls == [
        (
            "rules_add_keyword",
            {
                "keyword": "ACME",
                "category": "Office Expense",
                "use_type": "Business",
                "priority": 5,
            },
            False,
        )
    ]
    assert pulls == 1
    assert payload["summary"]["updated"] == 1
    assert "Added keyword 'ACME'" in payload["cli_report"]


def test_interventions_mute_cli_uses_server_proxy_and_pulls_in_local_sync_mode(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _patch_local_sync_paths(monkeypatch, tmp_path)

    import finance_cli.commands.intervention_cmd as intervention_cmd
    import finance_cli.sync.engine as sync_engine

    proxy_calls: list[tuple[str, dict[str, object], bool]] = []
    pulls = 0

    async def fake_proxy_tool(self, tool_name, arguments=None, *, wait_for_subscriber=True):
        del self
        proxy_calls.append((tool_name, dict(arguments or {}), wait_for_subscriber))
        return {
            "data": {"pattern_id": "I-2", "reason": "reviewed", "created": True},
            "summary": {"pattern_id": "I-2", "created": True},
        }

    async def fake_pull(self):
        nonlocal pulls
        del self
        pulls += 1

    def unexpected_local_mute(*_args, **_kwargs):
        raise AssertionError("interventions mute should use the server proxy in local sync mode")

    monkeypatch.setattr(sync_engine.SyncEngine, "proxy_tool", fake_proxy_tool)
    monkeypatch.setattr(sync_engine.SyncEngine, "pull", fake_pull)
    monkeypatch.setattr(intervention_cmd, "handle_mute", unexpected_local_mute)

    code, payload = _run_cli(
        ["interventions", "mute", "I-2", "--reason", "reviewed"],
        capsys,
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "interventions.mute"
    assert proxy_calls == [
        ("interventions_mute", {"pattern_id": "I-2", "reason": "reviewed"}, False)
    ]
    assert pulls == 1
    assert payload["summary"]["pattern_id"] == "I-2"
    assert "created=True" in payload["cli_report"]
