ALTER TABLE _sync_changelog ADD COLUMN origin_session_id TEXT NOT NULL DEFAULT '';

CREATE TABLE _meta_state (
    key TEXT PRIMARY KEY,
    sha256 TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    last_applied_op_id INTEGER NOT NULL DEFAULT 0,
    install_id TEXT NOT NULL DEFAULT '',
    subscriber_status TEXT NOT NULL DEFAULT 'healthy' CHECK (subscriber_status IN ('healthy','degraded','bootstrapping'))
);

INSERT OR IGNORE INTO sync_state (id, last_applied_op_id, install_id) VALUES (0, 0, '');

DROP TRIGGER IF EXISTS _sync_log_transactions_insert;
DROP TRIGGER IF EXISTS _sync_log_transactions_update;
DROP TRIGGER IF EXISTS _sync_log_transactions_delete;
DROP TRIGGER IF EXISTS _sync_log_categories_insert;
DROP TRIGGER IF EXISTS _sync_log_categories_update;
DROP TRIGGER IF EXISTS _sync_log_categories_delete;
DROP TRIGGER IF EXISTS _sync_log_vendor_memory_insert;
DROP TRIGGER IF EXISTS _sync_log_vendor_memory_update;
DROP TRIGGER IF EXISTS _sync_log_vendor_memory_delete;
DROP TRIGGER IF EXISTS _sync_log_budgets_insert;
DROP TRIGGER IF EXISTS _sync_log_budgets_update;
DROP TRIGGER IF EXISTS _sync_log_budgets_delete;
DROP TRIGGER IF EXISTS _sync_log_subscriptions_insert;
DROP TRIGGER IF EXISTS _sync_log_subscriptions_update;
DROP TRIGGER IF EXISTS _sync_log_subscriptions_delete;
DROP TRIGGER IF EXISTS _sync_log_goals_insert;
DROP TRIGGER IF EXISTS _sync_log_goals_update;
DROP TRIGGER IF EXISTS _sync_log_goals_delete;
DROP TRIGGER IF EXISTS _sync_log_manual_loans_insert;
DROP TRIGGER IF EXISTS _sync_log_manual_loans_update;
DROP TRIGGER IF EXISTS _sync_log_manual_loans_delete;
DROP TRIGGER IF EXISTS _sync_log_accounts_insert;
DROP TRIGGER IF EXISTS _sync_log_accounts_update;
DROP TRIGGER IF EXISTS _sync_log_accounts_delete;
DROP TRIGGER IF EXISTS _sync_log_balance_snapshots_insert;
DROP TRIGGER IF EXISTS _sync_log_balance_snapshots_update;
DROP TRIGGER IF EXISTS _sync_log_balance_snapshots_delete;
DROP TRIGGER IF EXISTS _sync_log_liabilities_insert;
DROP TRIGGER IF EXISTS _sync_log_liabilities_update;
DROP TRIGGER IF EXISTS _sync_log_liabilities_delete;
DROP TRIGGER IF EXISTS _sync_log_import_batches_insert;
DROP TRIGGER IF EXISTS _sync_log_import_batches_update;
DROP TRIGGER IF EXISTS _sync_log_import_batches_delete;
DROP TRIGGER IF EXISTS _sync_log_category_mappings_insert;
DROP TRIGGER IF EXISTS _sync_log_category_mappings_update;
DROP TRIGGER IF EXISTS _sync_log_category_mappings_delete;
DROP TRIGGER IF EXISTS _sync_log_notification_channels_insert;
DROP TRIGGER IF EXISTS _sync_log_notification_channels_update;
DROP TRIGGER IF EXISTS _sync_log_notification_channels_delete;
DROP TRIGGER IF EXISTS _sync_log_mileage_log_insert;
DROP TRIGGER IF EXISTS _sync_log_mileage_log_update;
DROP TRIGGER IF EXISTS _sync_log_mileage_log_delete;
DROP TRIGGER IF EXISTS _sync_log_contractors_insert;
DROP TRIGGER IF EXISTS _sync_log_contractors_update;
DROP TRIGGER IF EXISTS _sync_log_contractors_delete;
DROP TRIGGER IF EXISTS _sync_log_contractor_payments_insert;
DROP TRIGGER IF EXISTS _sync_log_contractor_payments_update;
DROP TRIGGER IF EXISTS _sync_log_contractor_payments_delete;
DROP TRIGGER IF EXISTS _sync_log_biz_section_budgets_insert;
DROP TRIGGER IF EXISTS _sync_log_biz_section_budgets_update;
DROP TRIGGER IF EXISTS _sync_log_biz_section_budgets_delete;
DROP TRIGGER IF EXISTS _sync_log_tax_config_insert;
DROP TRIGGER IF EXISTS _sync_log_tax_config_update;
DROP TRIGGER IF EXISTS _sync_log_tax_config_delete;
DROP TRIGGER IF EXISTS _sync_log_loan_disbursements_insert;
DROP TRIGGER IF EXISTS _sync_log_loan_disbursements_update;
DROP TRIGGER IF EXISTS _sync_log_loan_disbursements_delete;
DROP TRIGGER IF EXISTS _sync_log_loan_payments_insert;
DROP TRIGGER IF EXISTS _sync_log_loan_payments_update;
DROP TRIGGER IF EXISTS _sync_log_loan_payments_delete;
DROP TRIGGER IF EXISTS _sync_log_loan_events_insert;
DROP TRIGGER IF EXISTS _sync_log_loan_events_update;
DROP TRIGGER IF EXISTS _sync_log_loan_events_delete;
DROP TRIGGER IF EXISTS _sync_log_monthly_plans_insert;
DROP TRIGGER IF EXISTS _sync_log_monthly_plans_update;
DROP TRIGGER IF EXISTS _sync_log_monthly_plans_delete;
DROP TRIGGER IF EXISTS _sync_log_projects_insert;
DROP TRIGGER IF EXISTS _sync_log_projects_update;
DROP TRIGGER IF EXISTS _sync_log_projects_delete;
DROP TRIGGER IF EXISTS _sync_log_account_aliases_insert;
DROP TRIGGER IF EXISTS _sync_log_account_aliases_update;
DROP TRIGGER IF EXISTS _sync_log_account_aliases_delete;
DROP TRIGGER IF EXISTS _sync_log_provider_routing_insert;
DROP TRIGGER IF EXISTS _sync_log_provider_routing_update;
DROP TRIGGER IF EXISTS _sync_log_provider_routing_delete;
DROP TRIGGER IF EXISTS _sync_log_cost_limits_insert;
DROP TRIGGER IF EXISTS _sync_log_cost_limits_update;
DROP TRIGGER IF EXISTS _sync_log_cost_limits_delete;
DROP TRIGGER IF EXISTS _sync_log_plaid_items_insert;
DROP TRIGGER IF EXISTS _sync_log_plaid_items_update;
DROP TRIGGER IF EXISTS _sync_log_plaid_items_delete;
DROP TRIGGER IF EXISTS _sync_log_stripe_connections_insert;
DROP TRIGGER IF EXISTS _sync_log_stripe_connections_update;
DROP TRIGGER IF EXISTS _sync_log_stripe_connections_delete;
DROP TRIGGER IF EXISTS _sync_log_telegram_config_insert;
DROP TRIGGER IF EXISTS _sync_log_telegram_config_update;
DROP TRIGGER IF EXISTS _sync_log_telegram_config_delete;
DROP TRIGGER IF EXISTS _sync_log_telegram_pending_approvals_insert;
DROP TRIGGER IF EXISTS _sync_log_telegram_pending_approvals_update;
DROP TRIGGER IF EXISTS _sync_log_telegram_pending_approvals_delete;
DROP TRIGGER IF EXISTS _sync_log_intervention_log_insert;
DROP TRIGGER IF EXISTS _sync_log_intervention_log_update;
DROP TRIGGER IF EXISTS _sync_log_intervention_log_delete;
DROP TRIGGER IF EXISTS _sync_log_intervention_mutes_insert;
DROP TRIGGER IF EXISTS _sync_log_intervention_mutes_update;
DROP TRIGGER IF EXISTS _sync_log_intervention_mutes_delete;
DROP TRIGGER IF EXISTS _sync_log_settings_insert;
DROP TRIGGER IF EXISTS _sync_log_settings_update;
DROP TRIGGER IF EXISTS _sync_log_settings_delete;
DROP TRIGGER IF EXISTS _sync_log__meta_state_insert;
DROP TRIGGER IF EXISTS _sync_log__meta_state_update;
DROP TRIGGER IF EXISTS _sync_log__meta_state_delete;

CREATE TRIGGER _sync_log_transactions_insert
AFTER INSERT ON transactions
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('transactions', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'plaid_txn_id', NEW.plaid_txn_id, 'stripe_txn_id', NEW.stripe_txn_id, 'dedupe_key', NEW.dedupe_key, 'date', NEW.date, 'description', NEW.description, 'amount_cents', NEW.amount_cents, 'category_id', NEW.category_id, 'source_category', NEW.source_category, 'category_source', NEW.category_source, 'category_confidence', NEW.category_confidence, 'category_rule_id', NEW.category_rule_id, 'use_type', NEW.use_type, 'is_payment', NEW.is_payment, 'is_recurring', NEW.is_recurring, 'is_reviewed', NEW.is_reviewed, 'is_active', NEW.is_active, 'removed_at', NEW.removed_at, 'project_id', NEW.project_id, 'notes', NEW.notes, 'source', NEW.source, 'raw_plaid_json', NEW.raw_plaid_json, 'split_group_id', NEW.split_group_id, 'parent_transaction_id', NEW.parent_transaction_id, 'split_pct', NEW.split_pct, 'split_note', NEW.split_note, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key)), current_session_id());
END;

CREATE TRIGGER _sync_log_transactions_update
AFTER UPDATE ON transactions
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('transactions', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'plaid_txn_id', OLD.plaid_txn_id, 'stripe_txn_id', OLD.stripe_txn_id, 'dedupe_key', OLD.dedupe_key, 'date', OLD.date, 'description', OLD.description, 'amount_cents', OLD.amount_cents, 'category_id', OLD.category_id, 'source_category', OLD.source_category, 'category_source', OLD.category_source, 'category_confidence', OLD.category_confidence, 'category_rule_id', OLD.category_rule_id, 'use_type', OLD.use_type, 'is_payment', OLD.is_payment, 'is_recurring', OLD.is_recurring, 'is_reviewed', OLD.is_reviewed, 'is_active', OLD.is_active, 'removed_at', OLD.removed_at, 'project_id', OLD.project_id, 'notes', OLD.notes, 'source', OLD.source, 'raw_plaid_json', OLD.raw_plaid_json, 'split_group_id', OLD.split_group_id, 'parent_transaction_id', OLD.parent_transaction_id, 'split_pct', OLD.split_pct, 'split_note', OLD.split_note, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key)), json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'plaid_txn_id', NEW.plaid_txn_id, 'stripe_txn_id', NEW.stripe_txn_id, 'dedupe_key', NEW.dedupe_key, 'date', NEW.date, 'description', NEW.description, 'amount_cents', NEW.amount_cents, 'category_id', NEW.category_id, 'source_category', NEW.source_category, 'category_source', NEW.category_source, 'category_confidence', NEW.category_confidence, 'category_rule_id', NEW.category_rule_id, 'use_type', NEW.use_type, 'is_payment', NEW.is_payment, 'is_recurring', NEW.is_recurring, 'is_reviewed', NEW.is_reviewed, 'is_active', NEW.is_active, 'removed_at', NEW.removed_at, 'project_id', NEW.project_id, 'notes', NEW.notes, 'source', NEW.source, 'raw_plaid_json', NEW.raw_plaid_json, 'split_group_id', NEW.split_group_id, 'parent_transaction_id', NEW.parent_transaction_id, 'split_pct', NEW.split_pct, 'split_note', NEW.split_note, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key)), current_session_id());
END;

CREATE TRIGGER _sync_log_transactions_delete
AFTER DELETE ON transactions
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('transactions', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'plaid_txn_id', OLD.plaid_txn_id, 'stripe_txn_id', OLD.stripe_txn_id, 'dedupe_key', OLD.dedupe_key, 'date', OLD.date, 'description', OLD.description, 'amount_cents', OLD.amount_cents, 'category_id', OLD.category_id, 'source_category', OLD.source_category, 'category_source', OLD.category_source, 'category_confidence', OLD.category_confidence, 'category_rule_id', OLD.category_rule_id, 'use_type', OLD.use_type, 'is_payment', OLD.is_payment, 'is_recurring', OLD.is_recurring, 'is_reviewed', OLD.is_reviewed, 'is_active', OLD.is_active, 'removed_at', OLD.removed_at, 'project_id', OLD.project_id, 'notes', OLD.notes, 'source', OLD.source, 'raw_plaid_json', OLD.raw_plaid_json, 'split_group_id', OLD.split_group_id, 'parent_transaction_id', OLD.parent_transaction_id, 'split_pct', OLD.split_pct, 'split_note', OLD.split_note, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_categories_insert
AFTER INSERT ON categories
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('categories', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id, 'is_income', NEW.is_income, 'is_system', NEW.is_system, 'sort_order', NEW.sort_order, 'level', NEW.level)), current_session_id());
END;

CREATE TRIGGER _sync_log_categories_update
AFTER UPDATE ON categories
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('categories', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'parent_id', OLD.parent_id, 'is_income', OLD.is_income, 'is_system', OLD.is_system, 'sort_order', OLD.sort_order, 'level', OLD.level)), json(json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id, 'is_income', NEW.is_income, 'is_system', NEW.is_system, 'sort_order', NEW.sort_order, 'level', NEW.level)), current_session_id());
END;

CREATE TRIGGER _sync_log_categories_delete
AFTER DELETE ON categories
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('categories', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'parent_id', OLD.parent_id, 'is_income', OLD.is_income, 'is_system', OLD.is_system, 'sort_order', OLD.sort_order, 'level', OLD.level)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_vendor_memory_insert
AFTER INSERT ON vendor_memory
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('vendor_memory', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'description_pattern', NEW.description_pattern, 'canonical_name', NEW.canonical_name, 'category_id', NEW.category_id, 'use_type', NEW.use_type, 'confidence', NEW.confidence, 'priority', NEW.priority, 'is_enabled', NEW.is_enabled, 'is_confirmed', NEW.is_confirmed, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched)), current_session_id());
END;

CREATE TRIGGER _sync_log_vendor_memory_update
AFTER UPDATE ON vendor_memory
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('vendor_memory', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'description_pattern', OLD.description_pattern, 'canonical_name', OLD.canonical_name, 'category_id', OLD.category_id, 'use_type', OLD.use_type, 'confidence', OLD.confidence, 'priority', OLD.priority, 'is_enabled', OLD.is_enabled, 'is_confirmed', OLD.is_confirmed, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched)), json(json_object('id', NEW.id, 'description_pattern', NEW.description_pattern, 'canonical_name', NEW.canonical_name, 'category_id', NEW.category_id, 'use_type', NEW.use_type, 'confidence', NEW.confidence, 'priority', NEW.priority, 'is_enabled', NEW.is_enabled, 'is_confirmed', NEW.is_confirmed, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched)), current_session_id());
END;

CREATE TRIGGER _sync_log_vendor_memory_delete
AFTER DELETE ON vendor_memory
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('vendor_memory', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'description_pattern', OLD.description_pattern, 'canonical_name', OLD.canonical_name, 'category_id', OLD.category_id, 'use_type', OLD.use_type, 'confidence', OLD.confidence, 'priority', OLD.priority, 'is_enabled', OLD.is_enabled, 'is_confirmed', OLD.is_confirmed, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_budgets_insert
AFTER INSERT ON budgets
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('budgets', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'category_id', NEW.category_id, 'period', NEW.period, 'amount_cents', NEW.amount_cents, 'effective_from', NEW.effective_from, 'effective_to', NEW.effective_to, 'use_type', NEW.use_type)), current_session_id());
END;

CREATE TRIGGER _sync_log_budgets_update
AFTER UPDATE ON budgets
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('budgets', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'category_id', OLD.category_id, 'period', OLD.period, 'amount_cents', OLD.amount_cents, 'effective_from', OLD.effective_from, 'effective_to', OLD.effective_to, 'use_type', OLD.use_type)), json(json_object('id', NEW.id, 'category_id', NEW.category_id, 'period', NEW.period, 'amount_cents', NEW.amount_cents, 'effective_from', NEW.effective_from, 'effective_to', NEW.effective_to, 'use_type', NEW.use_type)), current_session_id());
END;

CREATE TRIGGER _sync_log_budgets_delete
AFTER DELETE ON budgets
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('budgets', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'category_id', OLD.category_id, 'period', OLD.period, 'amount_cents', OLD.amount_cents, 'effective_from', OLD.effective_from, 'effective_to', OLD.effective_to, 'use_type', OLD.use_type)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_subscriptions_insert
AFTER INSERT ON subscriptions
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('subscriptions', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'vendor_name', NEW.vendor_name, 'category_id', NEW.category_id, 'amount_cents', NEW.amount_cents, 'frequency', NEW.frequency, 'next_expected', NEW.next_expected, 'account_id', NEW.account_id, 'is_active', NEW.is_active, 'use_type', NEW.use_type, 'is_auto_detected', NEW.is_auto_detected, 'sub_type', NEW.sub_type, 'idempotency_key', NEW.idempotency_key)), current_session_id());
END;

CREATE TRIGGER _sync_log_subscriptions_update
AFTER UPDATE ON subscriptions
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('subscriptions', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'vendor_name', OLD.vendor_name, 'category_id', OLD.category_id, 'amount_cents', OLD.amount_cents, 'frequency', OLD.frequency, 'next_expected', OLD.next_expected, 'account_id', OLD.account_id, 'is_active', OLD.is_active, 'use_type', OLD.use_type, 'is_auto_detected', OLD.is_auto_detected, 'sub_type', OLD.sub_type, 'idempotency_key', OLD.idempotency_key)), json(json_object('id', NEW.id, 'vendor_name', NEW.vendor_name, 'category_id', NEW.category_id, 'amount_cents', NEW.amount_cents, 'frequency', NEW.frequency, 'next_expected', NEW.next_expected, 'account_id', NEW.account_id, 'is_active', NEW.is_active, 'use_type', NEW.use_type, 'is_auto_detected', NEW.is_auto_detected, 'sub_type', NEW.sub_type, 'idempotency_key', NEW.idempotency_key)), current_session_id());
END;

CREATE TRIGGER _sync_log_subscriptions_delete
AFTER DELETE ON subscriptions
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('subscriptions', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'vendor_name', OLD.vendor_name, 'category_id', OLD.category_id, 'amount_cents', OLD.amount_cents, 'frequency', OLD.frequency, 'next_expected', OLD.next_expected, 'account_id', OLD.account_id, 'is_active', OLD.is_active, 'use_type', OLD.use_type, 'is_auto_detected', OLD.is_auto_detected, 'sub_type', OLD.sub_type, 'idempotency_key', OLD.idempotency_key)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_goals_insert
AFTER INSERT ON goals
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('goals', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'name', NEW.name, 'metric', NEW.metric, 'target_cents', NEW.target_cents, 'target_pct', NEW.target_pct, 'starting_cents', NEW.starting_cents, 'starting_pct', NEW.starting_pct, 'direction', NEW.direction, 'deadline', NEW.deadline, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_goals_update
AFTER UPDATE ON goals
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('goals', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'metric', OLD.metric, 'target_cents', OLD.target_cents, 'target_pct', OLD.target_pct, 'starting_cents', OLD.starting_cents, 'starting_pct', OLD.starting_pct, 'direction', OLD.direction, 'deadline', OLD.deadline, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), json(json_object('id', NEW.id, 'name', NEW.name, 'metric', NEW.metric, 'target_cents', NEW.target_cents, 'target_pct', NEW.target_pct, 'starting_cents', NEW.starting_cents, 'starting_pct', NEW.starting_pct, 'direction', NEW.direction, 'deadline', NEW.deadline, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_goals_delete
AFTER DELETE ON goals
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('goals', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'metric', OLD.metric, 'target_cents', OLD.target_cents, 'target_pct', OLD.target_pct, 'starting_cents', OLD.starting_cents, 'starting_pct', OLD.starting_pct, 'direction', OLD.direction, 'deadline', OLD.deadline, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_manual_loans_insert
AFTER INSERT ON manual_loans
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('manual_loans', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'creditor_name', NEW.creditor_name, 'description', NEW.description, 'total_disbursed_cents', NEW.total_disbursed_cents, 'current_balance_cents', NEW.current_balance_cents, 'interest_rate_pct', NEW.interest_rate_pct, 'interest_type', NEW.interest_type, 'monthly_payment_cents', NEW.monthly_payment_cents, 'payment_due_day', NEW.payment_due_day, 'start_date', NEW.start_date, 'expected_payoff_date', NEW.expected_payoff_date, 'use_type', NEW.use_type, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key)), current_session_id());
END;

CREATE TRIGGER _sync_log_manual_loans_update
AFTER UPDATE ON manual_loans
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('manual_loans', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'creditor_name', OLD.creditor_name, 'description', OLD.description, 'total_disbursed_cents', OLD.total_disbursed_cents, 'current_balance_cents', OLD.current_balance_cents, 'interest_rate_pct', OLD.interest_rate_pct, 'interest_type', OLD.interest_type, 'monthly_payment_cents', OLD.monthly_payment_cents, 'payment_due_day', OLD.payment_due_day, 'start_date', OLD.start_date, 'expected_payoff_date', OLD.expected_payoff_date, 'use_type', OLD.use_type, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key)), json(json_object('id', NEW.id, 'creditor_name', NEW.creditor_name, 'description', NEW.description, 'total_disbursed_cents', NEW.total_disbursed_cents, 'current_balance_cents', NEW.current_balance_cents, 'interest_rate_pct', NEW.interest_rate_pct, 'interest_type', NEW.interest_type, 'monthly_payment_cents', NEW.monthly_payment_cents, 'payment_due_day', NEW.payment_due_day, 'start_date', NEW.start_date, 'expected_payoff_date', NEW.expected_payoff_date, 'use_type', NEW.use_type, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key)), current_session_id());
END;

CREATE TRIGGER _sync_log_manual_loans_delete
AFTER DELETE ON manual_loans
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('manual_loans', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'creditor_name', OLD.creditor_name, 'description', OLD.description, 'total_disbursed_cents', OLD.total_disbursed_cents, 'current_balance_cents', OLD.current_balance_cents, 'interest_rate_pct', OLD.interest_rate_pct, 'interest_type', OLD.interest_type, 'monthly_payment_cents', OLD.monthly_payment_cents, 'payment_due_day', OLD.payment_due_day, 'start_date', OLD.start_date, 'expected_payoff_date', OLD.expected_payoff_date, 'use_type', OLD.use_type, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_accounts_insert
AFTER INSERT ON accounts
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('accounts', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'plaid_account_id', NEW.plaid_account_id, 'plaid_item_id', NEW.plaid_item_id, 'institution_name', NEW.institution_name, 'account_name', NEW.account_name, 'account_type', NEW.account_type, 'card_ending', NEW.card_ending, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'iso_currency_code', NEW.iso_currency_code, 'unofficial_currency_code', NEW.unofficial_currency_code, 'balance_updated_at', NEW.balance_updated_at, 'source', NEW.source, 'account_type_override', NEW.account_type_override, 'is_business', NEW.is_business)), current_session_id());
END;

CREATE TRIGGER _sync_log_accounts_update
AFTER UPDATE ON accounts
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('accounts', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'plaid_account_id', OLD.plaid_account_id, 'plaid_item_id', OLD.plaid_item_id, 'institution_name', OLD.institution_name, 'account_name', OLD.account_name, 'account_type', OLD.account_type, 'card_ending', OLD.card_ending, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'iso_currency_code', OLD.iso_currency_code, 'unofficial_currency_code', OLD.unofficial_currency_code, 'balance_updated_at', OLD.balance_updated_at, 'source', OLD.source, 'account_type_override', OLD.account_type_override, 'is_business', OLD.is_business)), json(json_object('id', NEW.id, 'plaid_account_id', NEW.plaid_account_id, 'plaid_item_id', NEW.plaid_item_id, 'institution_name', NEW.institution_name, 'account_name', NEW.account_name, 'account_type', NEW.account_type, 'card_ending', NEW.card_ending, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'iso_currency_code', NEW.iso_currency_code, 'unofficial_currency_code', NEW.unofficial_currency_code, 'balance_updated_at', NEW.balance_updated_at, 'source', NEW.source, 'account_type_override', NEW.account_type_override, 'is_business', NEW.is_business)), current_session_id());
END;

CREATE TRIGGER _sync_log_accounts_delete
AFTER DELETE ON accounts
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('accounts', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'plaid_account_id', OLD.plaid_account_id, 'plaid_item_id', OLD.plaid_item_id, 'institution_name', OLD.institution_name, 'account_name', OLD.account_name, 'account_type', OLD.account_type, 'card_ending', OLD.card_ending, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'iso_currency_code', OLD.iso_currency_code, 'unofficial_currency_code', OLD.unofficial_currency_code, 'balance_updated_at', OLD.balance_updated_at, 'source', OLD.source, 'account_type_override', OLD.account_type_override, 'is_business', OLD.is_business)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_balance_snapshots_insert
AFTER INSERT ON balance_snapshots
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('balance_snapshots', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'source', NEW.source, 'snapshot_date', NEW.snapshot_date, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_balance_snapshots_update
AFTER UPDATE ON balance_snapshots
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('balance_snapshots', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'source', OLD.source, 'snapshot_date', OLD.snapshot_date, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'source', NEW.source, 'snapshot_date', NEW.snapshot_date, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_balance_snapshots_delete
AFTER DELETE ON balance_snapshots
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('balance_snapshots', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'source', OLD.source, 'snapshot_date', OLD.snapshot_date, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_liabilities_insert
AFTER INSERT ON liabilities
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('liabilities', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'liability_type', NEW.liability_type, 'is_active', NEW.is_active, 'last_seen_at', NEW.last_seen_at, 'is_overdue', NEW.is_overdue, 'last_payment_amount_cents', NEW.last_payment_amount_cents, 'last_payment_date', NEW.last_payment_date, 'last_statement_balance_cents', NEW.last_statement_balance_cents, 'last_statement_issue_date', NEW.last_statement_issue_date, 'minimum_payment_cents', NEW.minimum_payment_cents, 'next_payment_due_date', NEW.next_payment_due_date, 'apr_purchase', NEW.apr_purchase, 'apr_balance_transfer', NEW.apr_balance_transfer, 'apr_cash_advance', NEW.apr_cash_advance, 'interest_rate_pct', NEW.interest_rate_pct, 'origination_principal_cents', NEW.origination_principal_cents, 'outstanding_interest_cents', NEW.outstanding_interest_cents, 'expected_payoff_date', NEW.expected_payoff_date, 'loan_name', NEW.loan_name, 'loan_status_type', NEW.loan_status_type, 'loan_status_end_date', NEW.loan_status_end_date, 'repayment_plan_type', NEW.repayment_plan_type, 'repayment_plan_description', NEW.repayment_plan_description, 'servicer_name', NEW.servicer_name, 'ytd_interest_paid_cents', NEW.ytd_interest_paid_cents, 'ytd_principal_paid_cents', NEW.ytd_principal_paid_cents, 'mortgage_rate_pct', NEW.mortgage_rate_pct, 'mortgage_rate_type', NEW.mortgage_rate_type, 'loan_term', NEW.loan_term, 'maturity_date', NEW.maturity_date, 'origination_date', NEW.origination_date, 'escrow_balance_cents', NEW.escrow_balance_cents, 'has_pmi', NEW.has_pmi, 'has_prepayment_penalty', NEW.has_prepayment_penalty, 'next_monthly_payment_cents', NEW.next_monthly_payment_cents, 'past_due_amount_cents', NEW.past_due_amount_cents, 'current_late_fee_cents', NEW.current_late_fee_cents, 'property_address_json', NEW.property_address_json, 'raw_plaid_json', NEW.raw_plaid_json, 'fetched_at', NEW.fetched_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_liabilities_update
AFTER UPDATE ON liabilities
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('liabilities', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'liability_type', OLD.liability_type, 'is_active', OLD.is_active, 'last_seen_at', OLD.last_seen_at, 'is_overdue', OLD.is_overdue, 'last_payment_amount_cents', OLD.last_payment_amount_cents, 'last_payment_date', OLD.last_payment_date, 'last_statement_balance_cents', OLD.last_statement_balance_cents, 'last_statement_issue_date', OLD.last_statement_issue_date, 'minimum_payment_cents', OLD.minimum_payment_cents, 'next_payment_due_date', OLD.next_payment_due_date, 'apr_purchase', OLD.apr_purchase, 'apr_balance_transfer', OLD.apr_balance_transfer, 'apr_cash_advance', OLD.apr_cash_advance, 'interest_rate_pct', OLD.interest_rate_pct, 'origination_principal_cents', OLD.origination_principal_cents, 'outstanding_interest_cents', OLD.outstanding_interest_cents, 'expected_payoff_date', OLD.expected_payoff_date, 'loan_name', OLD.loan_name, 'loan_status_type', OLD.loan_status_type, 'loan_status_end_date', OLD.loan_status_end_date, 'repayment_plan_type', OLD.repayment_plan_type, 'repayment_plan_description', OLD.repayment_plan_description, 'servicer_name', OLD.servicer_name, 'ytd_interest_paid_cents', OLD.ytd_interest_paid_cents, 'ytd_principal_paid_cents', OLD.ytd_principal_paid_cents, 'mortgage_rate_pct', OLD.mortgage_rate_pct, 'mortgage_rate_type', OLD.mortgage_rate_type, 'loan_term', OLD.loan_term, 'maturity_date', OLD.maturity_date, 'origination_date', OLD.origination_date, 'escrow_balance_cents', OLD.escrow_balance_cents, 'has_pmi', OLD.has_pmi, 'has_prepayment_penalty', OLD.has_prepayment_penalty, 'next_monthly_payment_cents', OLD.next_monthly_payment_cents, 'past_due_amount_cents', OLD.past_due_amount_cents, 'current_late_fee_cents', OLD.current_late_fee_cents, 'property_address_json', OLD.property_address_json, 'raw_plaid_json', OLD.raw_plaid_json, 'fetched_at', OLD.fetched_at, 'updated_at', OLD.updated_at)), json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'liability_type', NEW.liability_type, 'is_active', NEW.is_active, 'last_seen_at', NEW.last_seen_at, 'is_overdue', NEW.is_overdue, 'last_payment_amount_cents', NEW.last_payment_amount_cents, 'last_payment_date', NEW.last_payment_date, 'last_statement_balance_cents', NEW.last_statement_balance_cents, 'last_statement_issue_date', NEW.last_statement_issue_date, 'minimum_payment_cents', NEW.minimum_payment_cents, 'next_payment_due_date', NEW.next_payment_due_date, 'apr_purchase', NEW.apr_purchase, 'apr_balance_transfer', NEW.apr_balance_transfer, 'apr_cash_advance', NEW.apr_cash_advance, 'interest_rate_pct', NEW.interest_rate_pct, 'origination_principal_cents', NEW.origination_principal_cents, 'outstanding_interest_cents', NEW.outstanding_interest_cents, 'expected_payoff_date', NEW.expected_payoff_date, 'loan_name', NEW.loan_name, 'loan_status_type', NEW.loan_status_type, 'loan_status_end_date', NEW.loan_status_end_date, 'repayment_plan_type', NEW.repayment_plan_type, 'repayment_plan_description', NEW.repayment_plan_description, 'servicer_name', NEW.servicer_name, 'ytd_interest_paid_cents', NEW.ytd_interest_paid_cents, 'ytd_principal_paid_cents', NEW.ytd_principal_paid_cents, 'mortgage_rate_pct', NEW.mortgage_rate_pct, 'mortgage_rate_type', NEW.mortgage_rate_type, 'loan_term', NEW.loan_term, 'maturity_date', NEW.maturity_date, 'origination_date', NEW.origination_date, 'escrow_balance_cents', NEW.escrow_balance_cents, 'has_pmi', NEW.has_pmi, 'has_prepayment_penalty', NEW.has_prepayment_penalty, 'next_monthly_payment_cents', NEW.next_monthly_payment_cents, 'past_due_amount_cents', NEW.past_due_amount_cents, 'current_late_fee_cents', NEW.current_late_fee_cents, 'property_address_json', NEW.property_address_json, 'raw_plaid_json', NEW.raw_plaid_json, 'fetched_at', NEW.fetched_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_liabilities_delete
AFTER DELETE ON liabilities
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('liabilities', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'liability_type', OLD.liability_type, 'is_active', OLD.is_active, 'last_seen_at', OLD.last_seen_at, 'is_overdue', OLD.is_overdue, 'last_payment_amount_cents', OLD.last_payment_amount_cents, 'last_payment_date', OLD.last_payment_date, 'last_statement_balance_cents', OLD.last_statement_balance_cents, 'last_statement_issue_date', OLD.last_statement_issue_date, 'minimum_payment_cents', OLD.minimum_payment_cents, 'next_payment_due_date', OLD.next_payment_due_date, 'apr_purchase', OLD.apr_purchase, 'apr_balance_transfer', OLD.apr_balance_transfer, 'apr_cash_advance', OLD.apr_cash_advance, 'interest_rate_pct', OLD.interest_rate_pct, 'origination_principal_cents', OLD.origination_principal_cents, 'outstanding_interest_cents', OLD.outstanding_interest_cents, 'expected_payoff_date', OLD.expected_payoff_date, 'loan_name', OLD.loan_name, 'loan_status_type', OLD.loan_status_type, 'loan_status_end_date', OLD.loan_status_end_date, 'repayment_plan_type', OLD.repayment_plan_type, 'repayment_plan_description', OLD.repayment_plan_description, 'servicer_name', OLD.servicer_name, 'ytd_interest_paid_cents', OLD.ytd_interest_paid_cents, 'ytd_principal_paid_cents', OLD.ytd_principal_paid_cents, 'mortgage_rate_pct', OLD.mortgage_rate_pct, 'mortgage_rate_type', OLD.mortgage_rate_type, 'loan_term', OLD.loan_term, 'maturity_date', OLD.maturity_date, 'origination_date', OLD.origination_date, 'escrow_balance_cents', OLD.escrow_balance_cents, 'has_pmi', OLD.has_pmi, 'has_prepayment_penalty', OLD.has_prepayment_penalty, 'next_monthly_payment_cents', OLD.next_monthly_payment_cents, 'past_due_amount_cents', OLD.past_due_amount_cents, 'current_late_fee_cents', OLD.current_late_fee_cents, 'property_address_json', OLD.property_address_json, 'raw_plaid_json', OLD.raw_plaid_json, 'fetched_at', OLD.fetched_at, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_import_batches_insert
AFTER INSERT ON import_batches
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('import_batches', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'source_type', NEW.source_type, 'file_path', NEW.file_path, 'file_hash_sha256', NEW.file_hash_sha256, 'bank_parser', NEW.bank_parser, 'statement_period', NEW.statement_period, 'extracted_count', NEW.extracted_count, 'imported_count', NEW.imported_count, 'skipped_count', NEW.skipped_count, 'reconcile_status', NEW.reconcile_status, 'statement_total_cents', NEW.statement_total_cents, 'extracted_total_cents', NEW.extracted_total_cents, 'created_at', NEW.created_at, 'ai_raw_output_json', NEW.ai_raw_output_json, 'ai_validation_json', NEW.ai_validation_json, 'ai_model', NEW.ai_model, 'ai_prompt_version', NEW.ai_prompt_version, 'ai_prompt_hash', NEW.ai_prompt_hash, 'content_hash_sha256', NEW.content_hash_sha256, 'total_charges_cents', NEW.total_charges_cents, 'total_payments_cents', NEW.total_payments_cents, 'new_balance_cents', NEW.new_balance_cents, 'expected_transaction_count', NEW.expected_transaction_count)), current_session_id());
END;

CREATE TRIGGER _sync_log_import_batches_update
AFTER UPDATE ON import_batches
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('import_batches', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'source_type', OLD.source_type, 'file_path', OLD.file_path, 'file_hash_sha256', OLD.file_hash_sha256, 'bank_parser', OLD.bank_parser, 'statement_period', OLD.statement_period, 'extracted_count', OLD.extracted_count, 'imported_count', OLD.imported_count, 'skipped_count', OLD.skipped_count, 'reconcile_status', OLD.reconcile_status, 'statement_total_cents', OLD.statement_total_cents, 'extracted_total_cents', OLD.extracted_total_cents, 'created_at', OLD.created_at, 'ai_raw_output_json', OLD.ai_raw_output_json, 'ai_validation_json', OLD.ai_validation_json, 'ai_model', OLD.ai_model, 'ai_prompt_version', OLD.ai_prompt_version, 'ai_prompt_hash', OLD.ai_prompt_hash, 'content_hash_sha256', OLD.content_hash_sha256, 'total_charges_cents', OLD.total_charges_cents, 'total_payments_cents', OLD.total_payments_cents, 'new_balance_cents', OLD.new_balance_cents, 'expected_transaction_count', OLD.expected_transaction_count)), json(json_object('id', NEW.id, 'source_type', NEW.source_type, 'file_path', NEW.file_path, 'file_hash_sha256', NEW.file_hash_sha256, 'bank_parser', NEW.bank_parser, 'statement_period', NEW.statement_period, 'extracted_count', NEW.extracted_count, 'imported_count', NEW.imported_count, 'skipped_count', NEW.skipped_count, 'reconcile_status', NEW.reconcile_status, 'statement_total_cents', NEW.statement_total_cents, 'extracted_total_cents', NEW.extracted_total_cents, 'created_at', NEW.created_at, 'ai_raw_output_json', NEW.ai_raw_output_json, 'ai_validation_json', NEW.ai_validation_json, 'ai_model', NEW.ai_model, 'ai_prompt_version', NEW.ai_prompt_version, 'ai_prompt_hash', NEW.ai_prompt_hash, 'content_hash_sha256', NEW.content_hash_sha256, 'total_charges_cents', NEW.total_charges_cents, 'total_payments_cents', NEW.total_payments_cents, 'new_balance_cents', NEW.new_balance_cents, 'expected_transaction_count', NEW.expected_transaction_count)), current_session_id());
END;

CREATE TRIGGER _sync_log_import_batches_delete
AFTER DELETE ON import_batches
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('import_batches', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'source_type', OLD.source_type, 'file_path', OLD.file_path, 'file_hash_sha256', OLD.file_hash_sha256, 'bank_parser', OLD.bank_parser, 'statement_period', OLD.statement_period, 'extracted_count', OLD.extracted_count, 'imported_count', OLD.imported_count, 'skipped_count', OLD.skipped_count, 'reconcile_status', OLD.reconcile_status, 'statement_total_cents', OLD.statement_total_cents, 'extracted_total_cents', OLD.extracted_total_cents, 'created_at', OLD.created_at, 'ai_raw_output_json', OLD.ai_raw_output_json, 'ai_validation_json', OLD.ai_validation_json, 'ai_model', OLD.ai_model, 'ai_prompt_version', OLD.ai_prompt_version, 'ai_prompt_hash', OLD.ai_prompt_hash, 'content_hash_sha256', OLD.content_hash_sha256, 'total_charges_cents', OLD.total_charges_cents, 'total_payments_cents', OLD.total_payments_cents, 'new_balance_cents', OLD.new_balance_cents, 'expected_transaction_count', OLD.expected_transaction_count)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_category_mappings_insert
AFTER INSERT ON category_mappings
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('category_mappings', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'source_category', NEW.source_category, 'source', NEW.source, 'category_id', NEW.category_id, 'created_by', NEW.created_by, 'confidence', NEW.confidence, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched, 'is_enabled', NEW.is_enabled, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_category_mappings_update
AFTER UPDATE ON category_mappings
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('category_mappings', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'source_category', OLD.source_category, 'source', OLD.source, 'category_id', OLD.category_id, 'created_by', OLD.created_by, 'confidence', OLD.confidence, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched, 'is_enabled', OLD.is_enabled, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), json(json_object('id', NEW.id, 'source_category', NEW.source_category, 'source', NEW.source, 'category_id', NEW.category_id, 'created_by', NEW.created_by, 'confidence', NEW.confidence, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched, 'is_enabled', NEW.is_enabled, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_category_mappings_delete
AFTER DELETE ON category_mappings
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('category_mappings', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'source_category', OLD.source_category, 'source', OLD.source, 'category_id', OLD.category_id, 'created_by', OLD.created_by, 'confidence', OLD.confidence, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched, 'is_enabled', OLD.is_enabled, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_notification_channels_insert
AFTER INSERT ON notification_channels
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('notification_channels', 'INSERT', json(json_object('channel', NEW.channel)), NULL, json(json_object('channel', NEW.channel, 'config', NEW.config, 'label', NEW.label, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_notification_channels_update
AFTER UPDATE OF channel, config, label ON notification_channels
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('notification_channels', 'UPDATE', json(json_object('channel', NEW.channel)), json(json_object('channel', OLD.channel, 'config', OLD.config, 'label', OLD.label, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), json(json_object('channel', NEW.channel, 'config', NEW.config, 'label', NEW.label, 'created_at', NEW.created_at, 'updated_at', CASE WHEN NEW.updated_at = OLD.updated_at THEN datetime('now') ELSE NEW.updated_at END)), current_session_id());
END;

CREATE TRIGGER _sync_log_notification_channels_delete
AFTER DELETE ON notification_channels
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('notification_channels', 'DELETE', json(json_object('channel', OLD.channel)), json(json_object('channel', OLD.channel, 'config', OLD.config, 'label', OLD.label, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_mileage_log_insert
AFTER INSERT ON mileage_log
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('mileage_log', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'trip_date', NEW.trip_date, 'miles', NEW.miles, 'destination', NEW.destination, 'business_purpose', NEW.business_purpose, 'vehicle_name', NEW.vehicle_name, 'tax_year', NEW.tax_year, 'round_trip', NEW.round_trip, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_mileage_log_update
AFTER UPDATE ON mileage_log
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('mileage_log', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'trip_date', OLD.trip_date, 'miles', OLD.miles, 'destination', OLD.destination, 'business_purpose', OLD.business_purpose, 'vehicle_name', OLD.vehicle_name, 'tax_year', OLD.tax_year, 'round_trip', OLD.round_trip, 'notes', OLD.notes, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'trip_date', NEW.trip_date, 'miles', NEW.miles, 'destination', NEW.destination, 'business_purpose', NEW.business_purpose, 'vehicle_name', NEW.vehicle_name, 'tax_year', NEW.tax_year, 'round_trip', NEW.round_trip, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_mileage_log_delete
AFTER DELETE ON mileage_log
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('mileage_log', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'trip_date', OLD.trip_date, 'miles', OLD.miles, 'destination', OLD.destination, 'business_purpose', OLD.business_purpose, 'vehicle_name', OLD.vehicle_name, 'tax_year', OLD.tax_year, 'round_trip', OLD.round_trip, 'notes', OLD.notes, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_contractors_insert
AFTER INSERT ON contractors
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('contractors', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'name', NEW.name, 'tin_last4', NEW.tin_last4, 'entity_type', NEW.entity_type, 'is_active', NEW.is_active, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_contractors_update
AFTER UPDATE ON contractors
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('contractors', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'tin_last4', OLD.tin_last4, 'entity_type', OLD.entity_type, 'is_active', OLD.is_active, 'notes', OLD.notes, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'name', NEW.name, 'tin_last4', NEW.tin_last4, 'entity_type', NEW.entity_type, 'is_active', NEW.is_active, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_contractors_delete
AFTER DELETE ON contractors
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('contractors', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'tin_last4', OLD.tin_last4, 'entity_type', OLD.entity_type, 'is_active', OLD.is_active, 'notes', OLD.notes, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_contractor_payments_insert
AFTER INSERT ON contractor_payments
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('contractor_payments', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'contractor_id', NEW.contractor_id, 'transaction_id', NEW.transaction_id, 'tax_year', NEW.tax_year, 'created_at', NEW.created_at, 'paid_via_card', NEW.paid_via_card)), current_session_id());
END;

CREATE TRIGGER _sync_log_contractor_payments_update
AFTER UPDATE ON contractor_payments
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('contractor_payments', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'contractor_id', OLD.contractor_id, 'transaction_id', OLD.transaction_id, 'tax_year', OLD.tax_year, 'created_at', OLD.created_at, 'paid_via_card', OLD.paid_via_card)), json(json_object('id', NEW.id, 'contractor_id', NEW.contractor_id, 'transaction_id', NEW.transaction_id, 'tax_year', NEW.tax_year, 'created_at', NEW.created_at, 'paid_via_card', NEW.paid_via_card)), current_session_id());
END;

CREATE TRIGGER _sync_log_contractor_payments_delete
AFTER DELETE ON contractor_payments
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('contractor_payments', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'contractor_id', OLD.contractor_id, 'transaction_id', OLD.transaction_id, 'tax_year', OLD.tax_year, 'created_at', OLD.created_at, 'paid_via_card', OLD.paid_via_card)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_biz_section_budgets_insert
AFTER INSERT ON biz_section_budgets
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('biz_section_budgets', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'pl_section', NEW.pl_section, 'amount_cents', NEW.amount_cents, 'period', NEW.period, 'effective_from', NEW.effective_from, 'effective_to', NEW.effective_to, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_biz_section_budgets_update
AFTER UPDATE ON biz_section_budgets
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('biz_section_budgets', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'pl_section', OLD.pl_section, 'amount_cents', OLD.amount_cents, 'period', OLD.period, 'effective_from', OLD.effective_from, 'effective_to', OLD.effective_to, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'pl_section', NEW.pl_section, 'amount_cents', NEW.amount_cents, 'period', NEW.period, 'effective_from', NEW.effective_from, 'effective_to', NEW.effective_to, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_biz_section_budgets_delete
AFTER DELETE ON biz_section_budgets
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('biz_section_budgets', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'pl_section', OLD.pl_section, 'amount_cents', OLD.amount_cents, 'period', OLD.period, 'effective_from', OLD.effective_from, 'effective_to', OLD.effective_to, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_tax_config_insert
AFTER INSERT ON tax_config
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('tax_config', 'INSERT', json(json_object('tax_year', NEW.tax_year, 'config_key', NEW.config_key)), NULL, json(json_object('tax_year', NEW.tax_year, 'config_key', NEW.config_key, 'config_value', NEW.config_value, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_tax_config_update
AFTER UPDATE ON tax_config
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('tax_config', 'UPDATE', json(json_object('tax_year', NEW.tax_year, 'config_key', NEW.config_key)), json(json_object('tax_year', OLD.tax_year, 'config_key', OLD.config_key, 'config_value', OLD.config_value, 'updated_at', OLD.updated_at)), json(json_object('tax_year', NEW.tax_year, 'config_key', NEW.config_key, 'config_value', NEW.config_value, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_tax_config_delete
AFTER DELETE ON tax_config
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('tax_config', 'DELETE', json(json_object('tax_year', OLD.tax_year, 'config_key', OLD.config_key)), json(json_object('tax_year', OLD.tax_year, 'config_key', OLD.config_key, 'config_value', OLD.config_value, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_loan_disbursements_insert
AFTER INSERT ON loan_disbursements
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_disbursements', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'loan_id', NEW.loan_id, 'amount_cents', NEW.amount_cents, 'disbursement_date', NEW.disbursement_date, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_loan_disbursements_update
AFTER UPDATE ON loan_disbursements
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_disbursements', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'loan_id', OLD.loan_id, 'amount_cents', OLD.amount_cents, 'disbursement_date', OLD.disbursement_date, 'notes', OLD.notes, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'loan_id', NEW.loan_id, 'amount_cents', NEW.amount_cents, 'disbursement_date', NEW.disbursement_date, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_loan_disbursements_delete
AFTER DELETE ON loan_disbursements
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_disbursements', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'loan_id', OLD.loan_id, 'amount_cents', OLD.amount_cents, 'disbursement_date', OLD.disbursement_date, 'notes', OLD.notes, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_loan_payments_insert
AFTER INSERT ON loan_payments
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_payments', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'loan_id', NEW.loan_id, 'amount_cents', NEW.amount_cents, 'payment_date', NEW.payment_date, 'transaction_id', NEW.transaction_id, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_loan_payments_update
AFTER UPDATE ON loan_payments
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_payments', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'loan_id', OLD.loan_id, 'amount_cents', OLD.amount_cents, 'payment_date', OLD.payment_date, 'transaction_id', OLD.transaction_id, 'notes', OLD.notes, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'loan_id', NEW.loan_id, 'amount_cents', NEW.amount_cents, 'payment_date', NEW.payment_date, 'transaction_id', NEW.transaction_id, 'notes', NEW.notes, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_loan_payments_delete
AFTER DELETE ON loan_payments
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_payments', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'loan_id', OLD.loan_id, 'amount_cents', OLD.amount_cents, 'payment_date', OLD.payment_date, 'transaction_id', OLD.transaction_id, 'notes', OLD.notes, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_loan_events_insert
AFTER INSERT ON loan_events
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_events', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'loan_id', NEW.loan_id, 'event_type', NEW.event_type, 'details', NEW.details, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_loan_events_update
AFTER UPDATE ON loan_events
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_events', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'loan_id', OLD.loan_id, 'event_type', OLD.event_type, 'details', OLD.details, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'loan_id', NEW.loan_id, 'event_type', NEW.event_type, 'details', NEW.details, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_loan_events_delete
AFTER DELETE ON loan_events
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('loan_events', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'loan_id', OLD.loan_id, 'event_type', OLD.event_type, 'details', OLD.details, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_monthly_plans_insert
AFTER INSERT ON monthly_plans
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('monthly_plans', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'month', NEW.month, 'expected_income_cents', NEW.expected_income_cents, 'expected_expenses_cents', NEW.expected_expenses_cents, 'savings_target_cents', NEW.savings_target_cents, 'investment_target_cents', NEW.investment_target_cents, 'notes', NEW.notes)), current_session_id());
END;

CREATE TRIGGER _sync_log_monthly_plans_update
AFTER UPDATE ON monthly_plans
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('monthly_plans', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'month', OLD.month, 'expected_income_cents', OLD.expected_income_cents, 'expected_expenses_cents', OLD.expected_expenses_cents, 'savings_target_cents', OLD.savings_target_cents, 'investment_target_cents', OLD.investment_target_cents, 'notes', OLD.notes)), json(json_object('id', NEW.id, 'month', NEW.month, 'expected_income_cents', NEW.expected_income_cents, 'expected_expenses_cents', NEW.expected_expenses_cents, 'savings_target_cents', NEW.savings_target_cents, 'investment_target_cents', NEW.investment_target_cents, 'notes', NEW.notes)), current_session_id());
END;

CREATE TRIGGER _sync_log_monthly_plans_delete
AFTER DELETE ON monthly_plans
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('monthly_plans', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'month', OLD.month, 'expected_income_cents', OLD.expected_income_cents, 'expected_expenses_cents', OLD.expected_expenses_cents, 'savings_target_cents', OLD.savings_target_cents, 'investment_target_cents', OLD.investment_target_cents, 'notes', OLD.notes)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_projects_insert
AFTER INSERT ON projects
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('projects', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'name', NEW.name, 'description', NEW.description, 'target_amount_cents', NEW.target_amount_cents, 'is_active', NEW.is_active)), current_session_id());
END;

CREATE TRIGGER _sync_log_projects_update
AFTER UPDATE ON projects
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('projects', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'description', OLD.description, 'target_amount_cents', OLD.target_amount_cents, 'is_active', OLD.is_active)), json(json_object('id', NEW.id, 'name', NEW.name, 'description', NEW.description, 'target_amount_cents', NEW.target_amount_cents, 'is_active', NEW.is_active)), current_session_id());
END;

CREATE TRIGGER _sync_log_projects_delete
AFTER DELETE ON projects
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('projects', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'name', OLD.name, 'description', OLD.description, 'target_amount_cents', OLD.target_amount_cents, 'is_active', OLD.is_active)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_account_aliases_insert
AFTER INSERT ON account_aliases
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('account_aliases', 'INSERT', json(json_object('hash_account_id', NEW.hash_account_id)), NULL, json(json_object('hash_account_id', NEW.hash_account_id, 'canonical_id', NEW.canonical_id, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_account_aliases_update
AFTER UPDATE ON account_aliases
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('account_aliases', 'UPDATE', json(json_object('hash_account_id', NEW.hash_account_id)), json(json_object('hash_account_id', OLD.hash_account_id, 'canonical_id', OLD.canonical_id, 'created_at', OLD.created_at)), json(json_object('hash_account_id', NEW.hash_account_id, 'canonical_id', NEW.canonical_id, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_account_aliases_delete
AFTER DELETE ON account_aliases
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('account_aliases', 'DELETE', json(json_object('hash_account_id', OLD.hash_account_id)), json(json_object('hash_account_id', OLD.hash_account_id, 'canonical_id', OLD.canonical_id, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_provider_routing_insert
AFTER INSERT ON provider_routing
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('provider_routing', 'INSERT', json(json_object('institution_name', NEW.institution_name)), NULL, json(json_object('institution_name', NEW.institution_name, 'provider', NEW.provider, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_provider_routing_update
AFTER UPDATE ON provider_routing
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('provider_routing', 'UPDATE', json(json_object('institution_name', NEW.institution_name)), json(json_object('institution_name', OLD.institution_name, 'provider', OLD.provider, 'updated_at', OLD.updated_at)), json(json_object('institution_name', NEW.institution_name, 'provider', NEW.provider, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_provider_routing_delete
AFTER DELETE ON provider_routing
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('provider_routing', 'DELETE', json(json_object('institution_name', OLD.institution_name)), json(json_object('institution_name', OLD.institution_name, 'provider', OLD.provider, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_cost_limits_insert
AFTER INSERT ON cost_limits
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('cost_limits', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'provider', NEW.provider, 'period', NEW.period, 'limit_usd6', NEW.limit_usd6, 'action', NEW.action, 'is_active', NEW.is_active, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_cost_limits_update
AFTER UPDATE ON cost_limits
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('cost_limits', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'provider', OLD.provider, 'period', OLD.period, 'limit_usd6', OLD.limit_usd6, 'action', OLD.action, 'is_active', OLD.is_active, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'provider', NEW.provider, 'period', NEW.period, 'limit_usd6', NEW.limit_usd6, 'action', NEW.action, 'is_active', NEW.is_active, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_cost_limits_delete
AFTER DELETE ON cost_limits
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('cost_limits', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'provider', OLD.provider, 'period', OLD.period, 'limit_usd6', OLD.limit_usd6, 'action', OLD.action, 'is_active', OLD.is_active, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_plaid_items_insert
AFTER INSERT ON plaid_items
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('plaid_items', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'plaid_item_id', NEW.plaid_item_id, 'institution_name', NEW.institution_name, 'access_token_ref', NEW.access_token_ref, 'status', NEW.status, 'error_code', NEW.error_code, 'consented_products', NEW.consented_products, 'sync_cursor', NEW.sync_cursor, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'last_sync_at', NEW.last_sync_at, 'last_balance_refresh_at', NEW.last_balance_refresh_at, 'last_liabilities_fetch_at', NEW.last_liabilities_fetch_at, 'institution_id', NEW.institution_id, 'last_investment_sync_at', NEW.last_investment_sync_at, 'last_webhook_at', NEW.last_webhook_at, 'needs_reauth', NEW.needs_reauth)), current_session_id());
END;

CREATE TRIGGER _sync_log_plaid_items_update
AFTER UPDATE ON plaid_items
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('plaid_items', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'plaid_item_id', OLD.plaid_item_id, 'institution_name', OLD.institution_name, 'access_token_ref', OLD.access_token_ref, 'status', OLD.status, 'error_code', OLD.error_code, 'consented_products', OLD.consented_products, 'sync_cursor', OLD.sync_cursor, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'last_sync_at', OLD.last_sync_at, 'last_balance_refresh_at', OLD.last_balance_refresh_at, 'last_liabilities_fetch_at', OLD.last_liabilities_fetch_at, 'institution_id', OLD.institution_id, 'last_investment_sync_at', OLD.last_investment_sync_at, 'last_webhook_at', OLD.last_webhook_at, 'needs_reauth', OLD.needs_reauth)), json(json_object('id', NEW.id, 'plaid_item_id', NEW.plaid_item_id, 'institution_name', NEW.institution_name, 'access_token_ref', NEW.access_token_ref, 'status', NEW.status, 'error_code', NEW.error_code, 'consented_products', NEW.consented_products, 'sync_cursor', NEW.sync_cursor, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'last_sync_at', NEW.last_sync_at, 'last_balance_refresh_at', NEW.last_balance_refresh_at, 'last_liabilities_fetch_at', NEW.last_liabilities_fetch_at, 'institution_id', NEW.institution_id, 'last_investment_sync_at', NEW.last_investment_sync_at, 'last_webhook_at', NEW.last_webhook_at, 'needs_reauth', NEW.needs_reauth)), current_session_id());
END;

CREATE TRIGGER _sync_log_plaid_items_delete
AFTER DELETE ON plaid_items
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('plaid_items', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'plaid_item_id', OLD.plaid_item_id, 'institution_name', OLD.institution_name, 'access_token_ref', OLD.access_token_ref, 'status', OLD.status, 'error_code', OLD.error_code, 'consented_products', OLD.consented_products, 'sync_cursor', OLD.sync_cursor, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'last_sync_at', OLD.last_sync_at, 'last_balance_refresh_at', OLD.last_balance_refresh_at, 'last_liabilities_fetch_at', OLD.last_liabilities_fetch_at, 'institution_id', OLD.institution_id, 'last_investment_sync_at', OLD.last_investment_sync_at, 'last_webhook_at', OLD.last_webhook_at, 'needs_reauth', OLD.needs_reauth)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_stripe_connections_insert
AFTER INSERT ON stripe_connections
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('stripe_connections', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'account_name', NEW.account_name, 'api_key_ref', NEW.api_key_ref, 'sync_cursor', NEW.sync_cursor, 'last_sync_at', NEW.last_sync_at, 'status', NEW.status, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_stripe_connections_update
AFTER UPDATE ON stripe_connections
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('stripe_connections', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'account_name', OLD.account_name, 'api_key_ref', OLD.api_key_ref, 'sync_cursor', OLD.sync_cursor, 'last_sync_at', OLD.last_sync_at, 'status', OLD.status, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), json(json_object('id', NEW.id, 'account_id', NEW.account_id, 'account_name', NEW.account_name, 'api_key_ref', NEW.api_key_ref, 'sync_cursor', NEW.sync_cursor, 'last_sync_at', NEW.last_sync_at, 'status', NEW.status, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_stripe_connections_delete
AFTER DELETE ON stripe_connections
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('stripe_connections', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'account_id', OLD.account_id, 'account_name', OLD.account_name, 'api_key_ref', OLD.api_key_ref, 'sync_cursor', OLD.sync_cursor, 'last_sync_at', OLD.last_sync_at, 'status', OLD.status, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_telegram_config_insert
AFTER INSERT ON telegram_config
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('telegram_config', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'bot_token_ref', NEW.bot_token_ref, 'bot_username', NEW.bot_username, 'bot_first_name', NEW.bot_first_name, 'bot_id', NEW.bot_id, 'chat_id', NEW.chat_id, 'connected_at', NEW.connected_at, 'updated_at', NEW.updated_at, 'webhook_secret', NEW.webhook_secret, 'webhook_url', NEW.webhook_url, 'telegram_user_id', NEW.telegram_user_id, 'link_code', NEW.link_code, 'link_code_expires_at', NEW.link_code_expires_at, 'model_override', NEW.model_override, 'active_skill', NEW.active_skill, 'current_session_id', NEW.current_session_id, 'last_message_time', NEW.last_message_time, 'onboarding_flags', NEW.onboarding_flags, 'processing_since', NEW.processing_since, 'processing_id', NEW.processing_id, 'cancel_requested', NEW.cancel_requested)), current_session_id());
END;

CREATE TRIGGER _sync_log_telegram_config_update
AFTER UPDATE ON telegram_config
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('telegram_config', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'bot_token_ref', OLD.bot_token_ref, 'bot_username', OLD.bot_username, 'bot_first_name', OLD.bot_first_name, 'bot_id', OLD.bot_id, 'chat_id', OLD.chat_id, 'connected_at', OLD.connected_at, 'updated_at', OLD.updated_at, 'webhook_secret', OLD.webhook_secret, 'webhook_url', OLD.webhook_url, 'telegram_user_id', OLD.telegram_user_id, 'link_code', OLD.link_code, 'link_code_expires_at', OLD.link_code_expires_at, 'model_override', OLD.model_override, 'active_skill', OLD.active_skill, 'current_session_id', OLD.current_session_id, 'last_message_time', OLD.last_message_time, 'onboarding_flags', OLD.onboarding_flags, 'processing_since', OLD.processing_since, 'processing_id', OLD.processing_id, 'cancel_requested', OLD.cancel_requested)), json(json_object('id', NEW.id, 'bot_token_ref', NEW.bot_token_ref, 'bot_username', NEW.bot_username, 'bot_first_name', NEW.bot_first_name, 'bot_id', NEW.bot_id, 'chat_id', NEW.chat_id, 'connected_at', NEW.connected_at, 'updated_at', NEW.updated_at, 'webhook_secret', NEW.webhook_secret, 'webhook_url', NEW.webhook_url, 'telegram_user_id', NEW.telegram_user_id, 'link_code', NEW.link_code, 'link_code_expires_at', NEW.link_code_expires_at, 'model_override', NEW.model_override, 'active_skill', NEW.active_skill, 'current_session_id', NEW.current_session_id, 'last_message_time', NEW.last_message_time, 'onboarding_flags', NEW.onboarding_flags, 'processing_since', NEW.processing_since, 'processing_id', NEW.processing_id, 'cancel_requested', NEW.cancel_requested)), current_session_id());
END;

CREATE TRIGGER _sync_log_telegram_config_delete
AFTER DELETE ON telegram_config
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('telegram_config', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'bot_token_ref', OLD.bot_token_ref, 'bot_username', OLD.bot_username, 'bot_first_name', OLD.bot_first_name, 'bot_id', OLD.bot_id, 'chat_id', OLD.chat_id, 'connected_at', OLD.connected_at, 'updated_at', OLD.updated_at, 'webhook_secret', OLD.webhook_secret, 'webhook_url', OLD.webhook_url, 'telegram_user_id', OLD.telegram_user_id, 'link_code', OLD.link_code, 'link_code_expires_at', OLD.link_code_expires_at, 'model_override', OLD.model_override, 'active_skill', OLD.active_skill, 'current_session_id', OLD.current_session_id, 'last_message_time', OLD.last_message_time, 'onboarding_flags', OLD.onboarding_flags, 'processing_since', OLD.processing_since, 'processing_id', OLD.processing_id, 'cancel_requested', OLD.cancel_requested)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_telegram_pending_approvals_insert
AFTER INSERT ON telegram_pending_approvals
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('telegram_pending_approvals', 'INSERT', json(json_object('nonce', NEW.nonce)), NULL, json(json_object('nonce', NEW.nonce, 'tool_call_id', NEW.tool_call_id, 'tool_name', NEW.tool_name, 'message_id', NEW.message_id, 'chat_id', NEW.chat_id, 'gateway_session_token', NEW.gateway_session_token, 'gateway_session_id', NEW.gateway_session_id, 'gateway_session_expires_at', NEW.gateway_session_expires_at, 'created_at', NEW.created_at, 'expires_at', NEW.expires_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_telegram_pending_approvals_update
AFTER UPDATE ON telegram_pending_approvals
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('telegram_pending_approvals', 'UPDATE', json(json_object('nonce', NEW.nonce)), json(json_object('nonce', OLD.nonce, 'tool_call_id', OLD.tool_call_id, 'tool_name', OLD.tool_name, 'message_id', OLD.message_id, 'chat_id', OLD.chat_id, 'gateway_session_token', OLD.gateway_session_token, 'gateway_session_id', OLD.gateway_session_id, 'gateway_session_expires_at', OLD.gateway_session_expires_at, 'created_at', OLD.created_at, 'expires_at', OLD.expires_at)), json(json_object('nonce', NEW.nonce, 'tool_call_id', NEW.tool_call_id, 'tool_name', NEW.tool_name, 'message_id', NEW.message_id, 'chat_id', NEW.chat_id, 'gateway_session_token', NEW.gateway_session_token, 'gateway_session_id', NEW.gateway_session_id, 'gateway_session_expires_at', NEW.gateway_session_expires_at, 'created_at', NEW.created_at, 'expires_at', NEW.expires_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_telegram_pending_approvals_delete
AFTER DELETE ON telegram_pending_approvals
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('telegram_pending_approvals', 'DELETE', json(json_object('nonce', OLD.nonce)), json(json_object('nonce', OLD.nonce, 'tool_call_id', OLD.tool_call_id, 'tool_name', OLD.tool_name, 'message_id', OLD.message_id, 'chat_id', OLD.chat_id, 'gateway_session_token', OLD.gateway_session_token, 'gateway_session_id', OLD.gateway_session_id, 'gateway_session_expires_at', OLD.gateway_session_expires_at, 'created_at', OLD.created_at, 'expires_at', OLD.expires_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_intervention_log_insert
AFTER INSERT ON intervention_log
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('intervention_log', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'pattern_id', NEW.pattern_id, 'fired_at', NEW.fired_at, 'surface', NEW.surface, 'user_action', NEW.user_action, 'acted_at', NEW.acted_at, 'dollar_impact_cents', NEW.dollar_impact_cents, 'goal_link', NEW.goal_link, 'headline', NEW.headline, 'payload', NEW.payload, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_intervention_log_update
AFTER UPDATE ON intervention_log
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('intervention_log', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'pattern_id', OLD.pattern_id, 'fired_at', OLD.fired_at, 'surface', OLD.surface, 'user_action', OLD.user_action, 'acted_at', OLD.acted_at, 'dollar_impact_cents', OLD.dollar_impact_cents, 'goal_link', OLD.goal_link, 'headline', OLD.headline, 'payload', OLD.payload, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), json(json_object('id', NEW.id, 'pattern_id', NEW.pattern_id, 'fired_at', NEW.fired_at, 'surface', NEW.surface, 'user_action', NEW.user_action, 'acted_at', NEW.acted_at, 'dollar_impact_cents', NEW.dollar_impact_cents, 'goal_link', NEW.goal_link, 'headline', NEW.headline, 'payload', NEW.payload, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_intervention_log_delete
AFTER DELETE ON intervention_log
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('intervention_log', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'pattern_id', OLD.pattern_id, 'fired_at', OLD.fired_at, 'surface', OLD.surface, 'user_action', OLD.user_action, 'acted_at', OLD.acted_at, 'dollar_impact_cents', OLD.dollar_impact_cents, 'goal_link', OLD.goal_link, 'headline', OLD.headline, 'payload', OLD.payload, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_intervention_mutes_insert
AFTER INSERT ON intervention_mutes
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('intervention_mutes', 'INSERT', json(json_object('id', NEW.id)), NULL, json(json_object('id', NEW.id, 'pattern_id', NEW.pattern_id, 'muted_at', NEW.muted_at, 'reason', NEW.reason, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_intervention_mutes_update
AFTER UPDATE ON intervention_mutes
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('intervention_mutes', 'UPDATE', json(json_object('id', NEW.id)), json(json_object('id', OLD.id, 'pattern_id', OLD.pattern_id, 'muted_at', OLD.muted_at, 'reason', OLD.reason, 'created_at', OLD.created_at)), json(json_object('id', NEW.id, 'pattern_id', NEW.pattern_id, 'muted_at', NEW.muted_at, 'reason', NEW.reason, 'created_at', NEW.created_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_intervention_mutes_delete
AFTER DELETE ON intervention_mutes
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('intervention_mutes', 'DELETE', json(json_object('id', OLD.id)), json(json_object('id', OLD.id, 'pattern_id', OLD.pattern_id, 'muted_at', OLD.muted_at, 'reason', OLD.reason, 'created_at', OLD.created_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log_settings_insert
AFTER INSERT ON settings
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('settings', 'INSERT', json(json_object('key', NEW.key)), NULL, json(json_object('key', NEW.key, 'value', NEW.value, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_settings_update
AFTER UPDATE ON settings
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('settings', 'UPDATE', json(json_object('key', NEW.key)), json(json_object('key', OLD.key, 'value', OLD.value, 'updated_at', OLD.updated_at)), json(json_object('key', NEW.key, 'value', NEW.value, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log_settings_delete
AFTER DELETE ON settings
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('settings', 'DELETE', json(json_object('key', OLD.key)), json(json_object('key', OLD.key, 'value', OLD.value, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;

CREATE TRIGGER _sync_log__meta_state_insert
AFTER INSERT ON _meta_state
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('_meta_state', 'INSERT', json(json_object('key', NEW.key)), NULL, json(json_object('key', NEW.key, 'sha256', NEW.sha256, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log__meta_state_update
AFTER UPDATE ON _meta_state
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('_meta_state', 'UPDATE', json(json_object('key', NEW.key)), json(json_object('key', OLD.key, 'sha256', OLD.sha256, 'updated_at', OLD.updated_at)), json(json_object('key', NEW.key, 'sha256', NEW.sha256, 'updated_at', NEW.updated_at)), current_session_id());
END;

CREATE TRIGGER _sync_log__meta_state_delete
AFTER DELETE ON _meta_state
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('_meta_state', 'DELETE', json(json_object('key', OLD.key)), json(json_object('key', OLD.key, 'sha256', OLD.sha256, 'updated_at', OLD.updated_at)), NULL, current_session_id());
END;
