# finance_cli Package Guide

Last audited: 2026-06-27

`finance_cli/` is the shared engine for the CLI, MCP tools, gateway/chat surfaces, web routes, Telegram, local sync, and storage dispatch. Most product behavior belongs here; the web app should usually call into this package instead of reimplementing finance logic.

## Entry Points

```text
__main__.py                 CLI parser, DB bootstrap, command dispatch, envelopes
mcp_server.py               FastMCP tool library, registered through tool_registry.py
mcp_local.py                Local stdio MCP entrypoint
mcp_remote.py               Streamable HTTP MCP entrypoint
mcp_gateway.py              Gateway subprocess entrypoint
gateway/__main__.py         Chat/gateway service entrypoint
telegram_bot/__main__.py    Local Telegram bot entrypoint
```

The CLI default output is JSON. Use `--format cli` only when a human-readable report is needed.

## Runtime Shape

```text
user/agent command
    |
    v
__main__.py parses args and loads env
    |
    v
initialize_database() and db.connect()
    |
    +--> local sqlite/sqlcipher connection
    |
    +--> StorageConnection for remote-storage users
    |
    v
commands/<group>.py handler
    |
    v
success/error envelope from commands/common.py
```

`db.connect()` is intentionally a choke point. In web/remote-storage mode it resolves the user, checks the active storage lease, reads `users.storage_mode`, and returns `StorageConnection` when `FINANCE_CLI_STORAGE_CLIENT_ENABLED=true`, `STORAGE_SERVER_URL` is set, and the user is `remote`.

## Major Modules

| Area | Main files |
|---|---|
| CLI commands | `commands/*.py` |
| SQLite/SQLCipher schema | `db.py`, `migrations/*.sql` |
| Categorization | `categorizer.py`, `user_rules.py`, `ai_categorizer.py`, `data/rules.yaml` |
| Ingest | `commands/ingest.py`, `extractors/`, `importers/`, `ai_statement_parser.py`, `ingest_validation.py` |
| Providers | `plaid_client.py`, `schwab_client.py`, `commands/plaid_cmd.py`, `commands/stripe_cmd.py` |
| Dedup/account identity | `dedup.py`, `institution_names.py`, `importers/__init__.py` |
| Planning/reporting | `budget_engine.py`, `liquidity.py`, `spending_analysis.py`, `forecasting.py`, `debt_calculator.py` |
| Coaching/interventions | `intervention_engine.py`, `interventions/`, `skills.py`, `skill_state.py` |
| Billing/cost | `billing.py`, `cost_tracking.py`, `commands/ops_cmd.py` |
| Gateway/chat | `gateway/`, `telegram_bot/`, `onboarding.py`, `onboarding_contract.py` |
| MCP/tool metadata | `mcp_server.py`, `tool_registry.py`, `gateway/tools.py`, `sync/tool_classification.py` |
| Remote storage client | `storage_client/`, `storage_lease.py`, `sync/`, `sync_protocol.py` |
| Security/privacy | `crypto_envelope.py`, `db_keys.py`, `backup_crypto.py`, `redaction.py`, `ai_egress.py`, `sensitive_audit.py` |
| Observability | `error_capture.py`, `frontend_logs.py`, `analytics.py`, `perf.py`, `operation_log.py` |

## Data And Schema Notes

- Monetary values are integer cents.
- Schema version is defined in `db.py` (`SCHEMA_VERSION`) and implemented by numbered SQL files in `migrations/`.
- Current migration files run through `079_debt_balance_portions.sql` with historical gaps.
- The local default DB is `finance_cli/data/finance.db`; set `FINANCE_CLI_DB` to override it.
- SQLCipher modes are `off`, `provision`, and `require` through `FINANCE_CLI_REQUIRE_DB_ENCRYPTION`.
- Per-user web paths are documented in `docs/developer/PER_USER_DATA.md`.

## Agent-Facing Contracts

- CLI and MCP results should stay structured and bounded.
- New MCP tools must be registered/classified through `tool_registry.py`; startup validation should fail for unclassified tools.
- Write paths should support dry-run/idempotency when practical.
- Chat/web/Telegram exposure is derived in `gateway/tools.py`; do not hard-code ad hoc tool allowlists in route code.
- Provider tokens should be stored as refs, not plaintext. New Plaid token writes use KMS4 `vault://...` refs.

## Tests

Focused examples:

```bash
python3 -m pytest finance_cli/tests/test_categorizer.py -q
python3 -m pytest finance_cli/tests/test_json_contract.py -q
python3 -m pytest -k "ingest or dedup" -q
python3 -m pytest -k "gateway or mcp or tool_registry" -q
.venv/bin/python -m pytest --collect-only -q finance_cli/tests
```

The collect-only audit on 2026-06-27 found 4,245 CLI tests across 298 top-level `test_*.py` modules. Re-run the collect command instead of copying that count into new docs.
