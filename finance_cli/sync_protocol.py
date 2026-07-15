"""Shared sync protocol constants for server and local client."""

from __future__ import annotations

REPLICATED_TABLES: frozenset[str] = frozenset(
    {
        "transactions",
        "categories",
        "vendor_memory",
        "budgets",
        "subscriptions",
        "goals",
        "manual_loans",
        "debt_balance_portions",
        "accounts",
        "balance_snapshots",
        "liabilities",
        "import_batches",
        "category_mappings",
        "notification_channels",
        "mileage_log",
        "contractors",
        "contractor_payments",
        "contractor_tax_prep_flags",
        "biz_section_budgets",
        "tax_config",
        "loan_disbursements",
        "loan_payments",
        "loan_events",
        "monthly_plans",
        "projects",
        "account_aliases",
        "provider_routing",
        "cost_limits",
        "plaid_items",
        "stripe_connections",
        "telegram_config",
        "telegram_pending_approvals",
        "intervention_log",
        "intervention_mutes",
        "settings",
        "reminders",
        "user_strategy_preferences",
        "account_alert_rules",
        "spending_freeze_flags",
        "card_paydown_flags",
        "retirement_contribution_targets",
        "hysa_transfer_flags",
        "savings_automations",
        "transaction_dispute_workflows",
        "_meta_state",
    }
)

DOWNSTREAM_ONLY_TABLES: frozenset[str] = frozenset(
    {
        "cost_ledger",
    }
)

CHANGELOG_TABLES: frozenset[str] = REPLICATED_TABLES | DOWNSTREAM_ONLY_TABLES

NON_REPLICATED_WRITABLE_TABLES: frozenset[str] = frozenset(
    {
        "analytics_events",
        "perf_samples",
        "errors",
        "error_occurrences",
        "error_alerts",
        "issue_reports",
        "cost_alert_log",
        "credit_balance",
        "credit_ledger",
        "frontend_logs",
        "bot_chat_messages",
        "bot_requests",
        "bot_tool_calls",
        "bot_sessions",
        "backup_log",
        "recurring_flows",
        "ai_categorization_log",
        "telegram_processed_updates",
        "telegram_link_attempts",
        "telegram_pending_links",
        "sensitive_audit_events",
        "_operation_log",
        "sync_reset_state",
    }
)

# Deprecated alias retained for older callers.
SYNCABLE_TABLES: frozenset[str] = REPLICATED_TABLES

# Deprecated alias retained for older callers.
SERVER_ONLY_TABLES: frozenset[str] = frozenset()

READ_ONLY_TABLES: frozenset[str] = frozenset(
    {
        "schema_version",
        "tenant_marker",
        "pl_section_map",
        "schedule_c_map",
        "mileage_rates",
    }
)

SECRET_COLUMNS: dict[str, list[str]] = {
    "plaid_items": ["access_token_ref"],
    "stripe_connections": ["api_key_ref"],
    "telegram_config": ["bot_token_ref", "webhook_secret"],
}

SYNCED_SIDECAR_FILES: tuple[str, ...] = (
    "rules.yaml",
    "skill_state.json",
    "agent_memory.md",
    "db-dek.enc",
)

MAX_CHANGESET_OPS: int = 100_000
