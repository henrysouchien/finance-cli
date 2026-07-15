CREATE TABLE IF NOT EXISTS _sync_changelog (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    op         TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')),
    pk_json    TEXT NOT NULL CHECK (json_valid(pk_json)),
    old_json   TEXT CHECK (old_json IS NULL OR json_valid(old_json)),
    new_json   TEXT CHECK (new_json IS NULL OR json_valid(new_json)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TRIGGER IF NOT EXISTS _sync_log_transactions_insert
AFTER INSERT ON transactions
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('transactions', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'account_id', NEW.account_id, 'plaid_txn_id', NEW.plaid_txn_id, 'stripe_txn_id', NEW.stripe_txn_id, 'dedupe_key', NEW.dedupe_key, 'date', NEW.date, 'description', NEW.description, 'amount_cents', NEW.amount_cents, 'category_id', NEW.category_id, 'source_category', NEW.source_category, 'category_source', NEW.category_source, 'category_confidence', NEW.category_confidence, 'category_rule_id', NEW.category_rule_id, 'use_type', NEW.use_type, 'is_payment', NEW.is_payment, 'is_recurring', NEW.is_recurring, 'is_reviewed', NEW.is_reviewed, 'is_active', NEW.is_active, 'removed_at', NEW.removed_at, 'project_id', NEW.project_id, 'notes', NEW.notes, 'source', NEW.source, 'raw_plaid_json', NEW.raw_plaid_json, 'split_group_id', NEW.split_group_id, 'parent_transaction_id', NEW.parent_transaction_id, 'split_pct', NEW.split_pct, 'split_note', NEW.split_note, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_transactions_update
AFTER UPDATE ON transactions
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('transactions', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'account_id', OLD.account_id, 'plaid_txn_id', OLD.plaid_txn_id, 'stripe_txn_id', OLD.stripe_txn_id, 'dedupe_key', OLD.dedupe_key, 'date', OLD.date, 'description', OLD.description, 'amount_cents', OLD.amount_cents, 'category_id', OLD.category_id, 'source_category', OLD.source_category, 'category_source', OLD.category_source, 'category_confidence', OLD.category_confidence, 'category_rule_id', OLD.category_rule_id, 'use_type', OLD.use_type, 'is_payment', OLD.is_payment, 'is_recurring', OLD.is_recurring, 'is_reviewed', OLD.is_reviewed, 'is_active', OLD.is_active, 'removed_at', OLD.removed_at, 'project_id', OLD.project_id, 'notes', OLD.notes, 'source', OLD.source, 'raw_plaid_json', OLD.raw_plaid_json, 'split_group_id', OLD.split_group_id, 'parent_transaction_id', OLD.parent_transaction_id, 'split_pct', OLD.split_pct, 'split_note', OLD.split_note, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key), json_object('id', NEW.id, 'account_id', NEW.account_id, 'plaid_txn_id', NEW.plaid_txn_id, 'stripe_txn_id', NEW.stripe_txn_id, 'dedupe_key', NEW.dedupe_key, 'date', NEW.date, 'description', NEW.description, 'amount_cents', NEW.amount_cents, 'category_id', NEW.category_id, 'source_category', NEW.source_category, 'category_source', NEW.category_source, 'category_confidence', NEW.category_confidence, 'category_rule_id', NEW.category_rule_id, 'use_type', NEW.use_type, 'is_payment', NEW.is_payment, 'is_recurring', NEW.is_recurring, 'is_reviewed', NEW.is_reviewed, 'is_active', NEW.is_active, 'removed_at', NEW.removed_at, 'project_id', NEW.project_id, 'notes', NEW.notes, 'source', NEW.source, 'raw_plaid_json', NEW.raw_plaid_json, 'split_group_id', NEW.split_group_id, 'parent_transaction_id', NEW.parent_transaction_id, 'split_pct', NEW.split_pct, 'split_note', NEW.split_note, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_transactions_delete
AFTER DELETE ON transactions
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('transactions', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'account_id', OLD.account_id, 'plaid_txn_id', OLD.plaid_txn_id, 'stripe_txn_id', OLD.stripe_txn_id, 'dedupe_key', OLD.dedupe_key, 'date', OLD.date, 'description', OLD.description, 'amount_cents', OLD.amount_cents, 'category_id', OLD.category_id, 'source_category', OLD.source_category, 'category_source', OLD.category_source, 'category_confidence', OLD.category_confidence, 'category_rule_id', OLD.category_rule_id, 'use_type', OLD.use_type, 'is_payment', OLD.is_payment, 'is_recurring', OLD.is_recurring, 'is_reviewed', OLD.is_reviewed, 'is_active', OLD.is_active, 'removed_at', OLD.removed_at, 'project_id', OLD.project_id, 'notes', OLD.notes, 'source', OLD.source, 'raw_plaid_json', OLD.raw_plaid_json, 'split_group_id', OLD.split_group_id, 'parent_transaction_id', OLD.parent_transaction_id, 'split_pct', OLD.split_pct, 'split_note', OLD.split_note, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_categories_insert
AFTER INSERT ON categories
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('categories', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id, 'is_income', NEW.is_income, 'is_system', NEW.is_system, 'sort_order', NEW.sort_order, 'level', NEW.level));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_categories_update
AFTER UPDATE ON categories
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('categories', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'name', OLD.name, 'parent_id', OLD.parent_id, 'is_income', OLD.is_income, 'is_system', OLD.is_system, 'sort_order', OLD.sort_order, 'level', OLD.level), json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id, 'is_income', NEW.is_income, 'is_system', NEW.is_system, 'sort_order', NEW.sort_order, 'level', NEW.level));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_categories_delete
AFTER DELETE ON categories
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('categories', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'name', OLD.name, 'parent_id', OLD.parent_id, 'is_income', OLD.is_income, 'is_system', OLD.is_system, 'sort_order', OLD.sort_order, 'level', OLD.level), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_vendor_memory_insert
AFTER INSERT ON vendor_memory
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('vendor_memory', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'description_pattern', NEW.description_pattern, 'canonical_name', NEW.canonical_name, 'category_id', NEW.category_id, 'use_type', NEW.use_type, 'confidence', NEW.confidence, 'priority', NEW.priority, 'is_enabled', NEW.is_enabled, 'is_confirmed', NEW.is_confirmed, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_vendor_memory_update
AFTER UPDATE ON vendor_memory
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('vendor_memory', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'description_pattern', OLD.description_pattern, 'canonical_name', OLD.canonical_name, 'category_id', OLD.category_id, 'use_type', OLD.use_type, 'confidence', OLD.confidence, 'priority', OLD.priority, 'is_enabled', OLD.is_enabled, 'is_confirmed', OLD.is_confirmed, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched), json_object('id', NEW.id, 'description_pattern', NEW.description_pattern, 'canonical_name', NEW.canonical_name, 'category_id', NEW.category_id, 'use_type', NEW.use_type, 'confidence', NEW.confidence, 'priority', NEW.priority, 'is_enabled', NEW.is_enabled, 'is_confirmed', NEW.is_confirmed, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_vendor_memory_delete
AFTER DELETE ON vendor_memory
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('vendor_memory', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'description_pattern', OLD.description_pattern, 'canonical_name', OLD.canonical_name, 'category_id', OLD.category_id, 'use_type', OLD.use_type, 'confidence', OLD.confidence, 'priority', OLD.priority, 'is_enabled', OLD.is_enabled, 'is_confirmed', OLD.is_confirmed, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_budgets_insert
AFTER INSERT ON budgets
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('budgets', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'category_id', NEW.category_id, 'period', NEW.period, 'amount_cents', NEW.amount_cents, 'effective_from', NEW.effective_from, 'effective_to', NEW.effective_to, 'use_type', NEW.use_type));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_budgets_update
AFTER UPDATE ON budgets
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('budgets', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'category_id', OLD.category_id, 'period', OLD.period, 'amount_cents', OLD.amount_cents, 'effective_from', OLD.effective_from, 'effective_to', OLD.effective_to, 'use_type', OLD.use_type), json_object('id', NEW.id, 'category_id', NEW.category_id, 'period', NEW.period, 'amount_cents', NEW.amount_cents, 'effective_from', NEW.effective_from, 'effective_to', NEW.effective_to, 'use_type', NEW.use_type));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_budgets_delete
AFTER DELETE ON budgets
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('budgets', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'category_id', OLD.category_id, 'period', OLD.period, 'amount_cents', OLD.amount_cents, 'effective_from', OLD.effective_from, 'effective_to', OLD.effective_to, 'use_type', OLD.use_type), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_subscriptions_insert
AFTER INSERT ON subscriptions
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('subscriptions', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'vendor_name', NEW.vendor_name, 'category_id', NEW.category_id, 'amount_cents', NEW.amount_cents, 'frequency', NEW.frequency, 'next_expected', NEW.next_expected, 'account_id', NEW.account_id, 'is_active', NEW.is_active, 'use_type', NEW.use_type, 'is_auto_detected', NEW.is_auto_detected, 'sub_type', NEW.sub_type, 'idempotency_key', NEW.idempotency_key));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_subscriptions_update
AFTER UPDATE ON subscriptions
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('subscriptions', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'vendor_name', OLD.vendor_name, 'category_id', OLD.category_id, 'amount_cents', OLD.amount_cents, 'frequency', OLD.frequency, 'next_expected', OLD.next_expected, 'account_id', OLD.account_id, 'is_active', OLD.is_active, 'use_type', OLD.use_type, 'is_auto_detected', OLD.is_auto_detected, 'sub_type', OLD.sub_type, 'idempotency_key', OLD.idempotency_key), json_object('id', NEW.id, 'vendor_name', NEW.vendor_name, 'category_id', NEW.category_id, 'amount_cents', NEW.amount_cents, 'frequency', NEW.frequency, 'next_expected', NEW.next_expected, 'account_id', NEW.account_id, 'is_active', NEW.is_active, 'use_type', NEW.use_type, 'is_auto_detected', NEW.is_auto_detected, 'sub_type', NEW.sub_type, 'idempotency_key', NEW.idempotency_key));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_subscriptions_delete
AFTER DELETE ON subscriptions
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('subscriptions', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'vendor_name', OLD.vendor_name, 'category_id', OLD.category_id, 'amount_cents', OLD.amount_cents, 'frequency', OLD.frequency, 'next_expected', OLD.next_expected, 'account_id', OLD.account_id, 'is_active', OLD.is_active, 'use_type', OLD.use_type, 'is_auto_detected', OLD.is_auto_detected, 'sub_type', OLD.sub_type, 'idempotency_key', OLD.idempotency_key), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_goals_insert
AFTER INSERT ON goals
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('goals', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'name', NEW.name, 'metric', NEW.metric, 'target_cents', NEW.target_cents, 'target_pct', NEW.target_pct, 'starting_cents', NEW.starting_cents, 'starting_pct', NEW.starting_pct, 'direction', NEW.direction, 'deadline', NEW.deadline, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_goals_update
AFTER UPDATE ON goals
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('goals', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'name', OLD.name, 'metric', OLD.metric, 'target_cents', OLD.target_cents, 'target_pct', OLD.target_pct, 'starting_cents', OLD.starting_cents, 'starting_pct', OLD.starting_pct, 'direction', OLD.direction, 'deadline', OLD.deadline, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at), json_object('id', NEW.id, 'name', NEW.name, 'metric', NEW.metric, 'target_cents', NEW.target_cents, 'target_pct', NEW.target_pct, 'starting_cents', NEW.starting_cents, 'starting_pct', NEW.starting_pct, 'direction', NEW.direction, 'deadline', NEW.deadline, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_goals_delete
AFTER DELETE ON goals
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('goals', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'name', OLD.name, 'metric', OLD.metric, 'target_cents', OLD.target_cents, 'target_pct', OLD.target_pct, 'starting_cents', OLD.starting_cents, 'starting_pct', OLD.starting_pct, 'direction', OLD.direction, 'deadline', OLD.deadline, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_manual_loans_insert
AFTER INSERT ON manual_loans
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('manual_loans', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'creditor_name', NEW.creditor_name, 'description', NEW.description, 'total_disbursed_cents', NEW.total_disbursed_cents, 'current_balance_cents', NEW.current_balance_cents, 'interest_rate_pct', NEW.interest_rate_pct, 'interest_type', NEW.interest_type, 'monthly_payment_cents', NEW.monthly_payment_cents, 'payment_due_day', NEW.payment_due_day, 'start_date', NEW.start_date, 'expected_payoff_date', NEW.expected_payoff_date, 'use_type', NEW.use_type, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_manual_loans_update
AFTER UPDATE ON manual_loans
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('manual_loans', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'creditor_name', OLD.creditor_name, 'description', OLD.description, 'total_disbursed_cents', OLD.total_disbursed_cents, 'current_balance_cents', OLD.current_balance_cents, 'interest_rate_pct', OLD.interest_rate_pct, 'interest_type', OLD.interest_type, 'monthly_payment_cents', OLD.monthly_payment_cents, 'payment_due_day', OLD.payment_due_day, 'start_date', OLD.start_date, 'expected_payoff_date', OLD.expected_payoff_date, 'use_type', OLD.use_type, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key), json_object('id', NEW.id, 'creditor_name', NEW.creditor_name, 'description', NEW.description, 'total_disbursed_cents', NEW.total_disbursed_cents, 'current_balance_cents', NEW.current_balance_cents, 'interest_rate_pct', NEW.interest_rate_pct, 'interest_type', NEW.interest_type, 'monthly_payment_cents', NEW.monthly_payment_cents, 'payment_due_day', NEW.payment_due_day, 'start_date', NEW.start_date, 'expected_payoff_date', NEW.expected_payoff_date, 'use_type', NEW.use_type, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'idempotency_key', NEW.idempotency_key));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_manual_loans_delete
AFTER DELETE ON manual_loans
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('manual_loans', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'creditor_name', OLD.creditor_name, 'description', OLD.description, 'total_disbursed_cents', OLD.total_disbursed_cents, 'current_balance_cents', OLD.current_balance_cents, 'interest_rate_pct', OLD.interest_rate_pct, 'interest_type', OLD.interest_type, 'monthly_payment_cents', OLD.monthly_payment_cents, 'payment_due_day', OLD.payment_due_day, 'start_date', OLD.start_date, 'expected_payoff_date', OLD.expected_payoff_date, 'use_type', OLD.use_type, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'idempotency_key', OLD.idempotency_key), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_accounts_insert
AFTER INSERT ON accounts
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('accounts', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'plaid_account_id', NEW.plaid_account_id, 'plaid_item_id', NEW.plaid_item_id, 'institution_name', NEW.institution_name, 'account_name', NEW.account_name, 'account_type', NEW.account_type, 'card_ending', NEW.card_ending, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'iso_currency_code', NEW.iso_currency_code, 'unofficial_currency_code', NEW.unofficial_currency_code, 'balance_updated_at', NEW.balance_updated_at, 'source', NEW.source, 'account_type_override', NEW.account_type_override, 'is_business', NEW.is_business));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_accounts_update
AFTER UPDATE ON accounts
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('accounts', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'plaid_account_id', OLD.plaid_account_id, 'plaid_item_id', OLD.plaid_item_id, 'institution_name', OLD.institution_name, 'account_name', OLD.account_name, 'account_type', OLD.account_type, 'card_ending', OLD.card_ending, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'iso_currency_code', OLD.iso_currency_code, 'unofficial_currency_code', OLD.unofficial_currency_code, 'balance_updated_at', OLD.balance_updated_at, 'source', OLD.source, 'account_type_override', OLD.account_type_override, 'is_business', OLD.is_business), json_object('id', NEW.id, 'plaid_account_id', NEW.plaid_account_id, 'plaid_item_id', NEW.plaid_item_id, 'institution_name', NEW.institution_name, 'account_name', NEW.account_name, 'account_type', NEW.account_type, 'card_ending', NEW.card_ending, 'is_active', NEW.is_active, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'iso_currency_code', NEW.iso_currency_code, 'unofficial_currency_code', NEW.unofficial_currency_code, 'balance_updated_at', NEW.balance_updated_at, 'source', NEW.source, 'account_type_override', NEW.account_type_override, 'is_business', NEW.is_business));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_accounts_delete
AFTER DELETE ON accounts
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('accounts', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'plaid_account_id', OLD.plaid_account_id, 'plaid_item_id', OLD.plaid_item_id, 'institution_name', OLD.institution_name, 'account_name', OLD.account_name, 'account_type', OLD.account_type, 'card_ending', OLD.card_ending, 'is_active', OLD.is_active, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'iso_currency_code', OLD.iso_currency_code, 'unofficial_currency_code', OLD.unofficial_currency_code, 'balance_updated_at', OLD.balance_updated_at, 'source', OLD.source, 'account_type_override', OLD.account_type_override, 'is_business', OLD.is_business), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_balance_snapshots_insert
AFTER INSERT ON balance_snapshots
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('balance_snapshots', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'account_id', NEW.account_id, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'source', NEW.source, 'snapshot_date', NEW.snapshot_date, 'created_at', NEW.created_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_balance_snapshots_update
AFTER UPDATE ON balance_snapshots
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('balance_snapshots', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'account_id', OLD.account_id, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'source', OLD.source, 'snapshot_date', OLD.snapshot_date, 'created_at', OLD.created_at), json_object('id', NEW.id, 'account_id', NEW.account_id, 'balance_current_cents', NEW.balance_current_cents, 'balance_available_cents', NEW.balance_available_cents, 'balance_limit_cents', NEW.balance_limit_cents, 'source', NEW.source, 'snapshot_date', NEW.snapshot_date, 'created_at', NEW.created_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_balance_snapshots_delete
AFTER DELETE ON balance_snapshots
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('balance_snapshots', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'account_id', OLD.account_id, 'balance_current_cents', OLD.balance_current_cents, 'balance_available_cents', OLD.balance_available_cents, 'balance_limit_cents', OLD.balance_limit_cents, 'source', OLD.source, 'snapshot_date', OLD.snapshot_date, 'created_at', OLD.created_at), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_liabilities_insert
AFTER INSERT ON liabilities
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('liabilities', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'account_id', NEW.account_id, 'liability_type', NEW.liability_type, 'is_active', NEW.is_active, 'last_seen_at', NEW.last_seen_at, 'is_overdue', NEW.is_overdue, 'last_payment_amount_cents', NEW.last_payment_amount_cents, 'last_payment_date', NEW.last_payment_date, 'last_statement_balance_cents', NEW.last_statement_balance_cents, 'last_statement_issue_date', NEW.last_statement_issue_date, 'minimum_payment_cents', NEW.minimum_payment_cents, 'next_payment_due_date', NEW.next_payment_due_date, 'apr_purchase', NEW.apr_purchase, 'apr_balance_transfer', NEW.apr_balance_transfer, 'apr_cash_advance', NEW.apr_cash_advance, 'interest_rate_pct', NEW.interest_rate_pct, 'origination_principal_cents', NEW.origination_principal_cents, 'outstanding_interest_cents', NEW.outstanding_interest_cents, 'expected_payoff_date', NEW.expected_payoff_date, 'loan_name', NEW.loan_name, 'loan_status_type', NEW.loan_status_type, 'loan_status_end_date', NEW.loan_status_end_date, 'repayment_plan_type', NEW.repayment_plan_type, 'repayment_plan_description', NEW.repayment_plan_description, 'servicer_name', NEW.servicer_name, 'ytd_interest_paid_cents', NEW.ytd_interest_paid_cents, 'ytd_principal_paid_cents', NEW.ytd_principal_paid_cents, 'mortgage_rate_pct', NEW.mortgage_rate_pct, 'mortgage_rate_type', NEW.mortgage_rate_type, 'loan_term', NEW.loan_term, 'maturity_date', NEW.maturity_date, 'origination_date', NEW.origination_date, 'escrow_balance_cents', NEW.escrow_balance_cents, 'has_pmi', NEW.has_pmi, 'has_prepayment_penalty', NEW.has_prepayment_penalty, 'next_monthly_payment_cents', NEW.next_monthly_payment_cents, 'past_due_amount_cents', NEW.past_due_amount_cents, 'current_late_fee_cents', NEW.current_late_fee_cents, 'property_address_json', NEW.property_address_json, 'raw_plaid_json', NEW.raw_plaid_json, 'fetched_at', NEW.fetched_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_liabilities_update
AFTER UPDATE ON liabilities
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('liabilities', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'account_id', OLD.account_id, 'liability_type', OLD.liability_type, 'is_active', OLD.is_active, 'last_seen_at', OLD.last_seen_at, 'is_overdue', OLD.is_overdue, 'last_payment_amount_cents', OLD.last_payment_amount_cents, 'last_payment_date', OLD.last_payment_date, 'last_statement_balance_cents', OLD.last_statement_balance_cents, 'last_statement_issue_date', OLD.last_statement_issue_date, 'minimum_payment_cents', OLD.minimum_payment_cents, 'next_payment_due_date', OLD.next_payment_due_date, 'apr_purchase', OLD.apr_purchase, 'apr_balance_transfer', OLD.apr_balance_transfer, 'apr_cash_advance', OLD.apr_cash_advance, 'interest_rate_pct', OLD.interest_rate_pct, 'origination_principal_cents', OLD.origination_principal_cents, 'outstanding_interest_cents', OLD.outstanding_interest_cents, 'expected_payoff_date', OLD.expected_payoff_date, 'loan_name', OLD.loan_name, 'loan_status_type', OLD.loan_status_type, 'loan_status_end_date', OLD.loan_status_end_date, 'repayment_plan_type', OLD.repayment_plan_type, 'repayment_plan_description', OLD.repayment_plan_description, 'servicer_name', OLD.servicer_name, 'ytd_interest_paid_cents', OLD.ytd_interest_paid_cents, 'ytd_principal_paid_cents', OLD.ytd_principal_paid_cents, 'mortgage_rate_pct', OLD.mortgage_rate_pct, 'mortgage_rate_type', OLD.mortgage_rate_type, 'loan_term', OLD.loan_term, 'maturity_date', OLD.maturity_date, 'origination_date', OLD.origination_date, 'escrow_balance_cents', OLD.escrow_balance_cents, 'has_pmi', OLD.has_pmi, 'has_prepayment_penalty', OLD.has_prepayment_penalty, 'next_monthly_payment_cents', OLD.next_monthly_payment_cents, 'past_due_amount_cents', OLD.past_due_amount_cents, 'current_late_fee_cents', OLD.current_late_fee_cents, 'property_address_json', OLD.property_address_json, 'raw_plaid_json', OLD.raw_plaid_json, 'fetched_at', OLD.fetched_at, 'updated_at', OLD.updated_at), json_object('id', NEW.id, 'account_id', NEW.account_id, 'liability_type', NEW.liability_type, 'is_active', NEW.is_active, 'last_seen_at', NEW.last_seen_at, 'is_overdue', NEW.is_overdue, 'last_payment_amount_cents', NEW.last_payment_amount_cents, 'last_payment_date', NEW.last_payment_date, 'last_statement_balance_cents', NEW.last_statement_balance_cents, 'last_statement_issue_date', NEW.last_statement_issue_date, 'minimum_payment_cents', NEW.minimum_payment_cents, 'next_payment_due_date', NEW.next_payment_due_date, 'apr_purchase', NEW.apr_purchase, 'apr_balance_transfer', NEW.apr_balance_transfer, 'apr_cash_advance', NEW.apr_cash_advance, 'interest_rate_pct', NEW.interest_rate_pct, 'origination_principal_cents', NEW.origination_principal_cents, 'outstanding_interest_cents', NEW.outstanding_interest_cents, 'expected_payoff_date', NEW.expected_payoff_date, 'loan_name', NEW.loan_name, 'loan_status_type', NEW.loan_status_type, 'loan_status_end_date', NEW.loan_status_end_date, 'repayment_plan_type', NEW.repayment_plan_type, 'repayment_plan_description', NEW.repayment_plan_description, 'servicer_name', NEW.servicer_name, 'ytd_interest_paid_cents', NEW.ytd_interest_paid_cents, 'ytd_principal_paid_cents', NEW.ytd_principal_paid_cents, 'mortgage_rate_pct', NEW.mortgage_rate_pct, 'mortgage_rate_type', NEW.mortgage_rate_type, 'loan_term', NEW.loan_term, 'maturity_date', NEW.maturity_date, 'origination_date', NEW.origination_date, 'escrow_balance_cents', NEW.escrow_balance_cents, 'has_pmi', NEW.has_pmi, 'has_prepayment_penalty', NEW.has_prepayment_penalty, 'next_monthly_payment_cents', NEW.next_monthly_payment_cents, 'past_due_amount_cents', NEW.past_due_amount_cents, 'current_late_fee_cents', NEW.current_late_fee_cents, 'property_address_json', NEW.property_address_json, 'raw_plaid_json', NEW.raw_plaid_json, 'fetched_at', NEW.fetched_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_liabilities_delete
AFTER DELETE ON liabilities
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('liabilities', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'account_id', OLD.account_id, 'liability_type', OLD.liability_type, 'is_active', OLD.is_active, 'last_seen_at', OLD.last_seen_at, 'is_overdue', OLD.is_overdue, 'last_payment_amount_cents', OLD.last_payment_amount_cents, 'last_payment_date', OLD.last_payment_date, 'last_statement_balance_cents', OLD.last_statement_balance_cents, 'last_statement_issue_date', OLD.last_statement_issue_date, 'minimum_payment_cents', OLD.minimum_payment_cents, 'next_payment_due_date', OLD.next_payment_due_date, 'apr_purchase', OLD.apr_purchase, 'apr_balance_transfer', OLD.apr_balance_transfer, 'apr_cash_advance', OLD.apr_cash_advance, 'interest_rate_pct', OLD.interest_rate_pct, 'origination_principal_cents', OLD.origination_principal_cents, 'outstanding_interest_cents', OLD.outstanding_interest_cents, 'expected_payoff_date', OLD.expected_payoff_date, 'loan_name', OLD.loan_name, 'loan_status_type', OLD.loan_status_type, 'loan_status_end_date', OLD.loan_status_end_date, 'repayment_plan_type', OLD.repayment_plan_type, 'repayment_plan_description', OLD.repayment_plan_description, 'servicer_name', OLD.servicer_name, 'ytd_interest_paid_cents', OLD.ytd_interest_paid_cents, 'ytd_principal_paid_cents', OLD.ytd_principal_paid_cents, 'mortgage_rate_pct', OLD.mortgage_rate_pct, 'mortgage_rate_type', OLD.mortgage_rate_type, 'loan_term', OLD.loan_term, 'maturity_date', OLD.maturity_date, 'origination_date', OLD.origination_date, 'escrow_balance_cents', OLD.escrow_balance_cents, 'has_pmi', OLD.has_pmi, 'has_prepayment_penalty', OLD.has_prepayment_penalty, 'next_monthly_payment_cents', OLD.next_monthly_payment_cents, 'past_due_amount_cents', OLD.past_due_amount_cents, 'current_late_fee_cents', OLD.current_late_fee_cents, 'property_address_json', OLD.property_address_json, 'raw_plaid_json', OLD.raw_plaid_json, 'fetched_at', OLD.fetched_at, 'updated_at', OLD.updated_at), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_import_batches_insert
AFTER INSERT ON import_batches
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('import_batches', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'source_type', NEW.source_type, 'file_path', NEW.file_path, 'file_hash_sha256', NEW.file_hash_sha256, 'bank_parser', NEW.bank_parser, 'statement_period', NEW.statement_period, 'extracted_count', NEW.extracted_count, 'imported_count', NEW.imported_count, 'skipped_count', NEW.skipped_count, 'reconcile_status', NEW.reconcile_status, 'statement_total_cents', NEW.statement_total_cents, 'extracted_total_cents', NEW.extracted_total_cents, 'created_at', NEW.created_at, 'ai_raw_output_json', NEW.ai_raw_output_json, 'ai_validation_json', NEW.ai_validation_json, 'ai_model', NEW.ai_model, 'ai_prompt_version', NEW.ai_prompt_version, 'ai_prompt_hash', NEW.ai_prompt_hash, 'content_hash_sha256', NEW.content_hash_sha256, 'total_charges_cents', NEW.total_charges_cents, 'total_payments_cents', NEW.total_payments_cents, 'new_balance_cents', NEW.new_balance_cents, 'expected_transaction_count', NEW.expected_transaction_count));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_import_batches_update
AFTER UPDATE ON import_batches
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('import_batches', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'source_type', OLD.source_type, 'file_path', OLD.file_path, 'file_hash_sha256', OLD.file_hash_sha256, 'bank_parser', OLD.bank_parser, 'statement_period', OLD.statement_period, 'extracted_count', OLD.extracted_count, 'imported_count', OLD.imported_count, 'skipped_count', OLD.skipped_count, 'reconcile_status', OLD.reconcile_status, 'statement_total_cents', OLD.statement_total_cents, 'extracted_total_cents', OLD.extracted_total_cents, 'created_at', OLD.created_at, 'ai_raw_output_json', OLD.ai_raw_output_json, 'ai_validation_json', OLD.ai_validation_json, 'ai_model', OLD.ai_model, 'ai_prompt_version', OLD.ai_prompt_version, 'ai_prompt_hash', OLD.ai_prompt_hash, 'content_hash_sha256', OLD.content_hash_sha256, 'total_charges_cents', OLD.total_charges_cents, 'total_payments_cents', OLD.total_payments_cents, 'new_balance_cents', OLD.new_balance_cents, 'expected_transaction_count', OLD.expected_transaction_count), json_object('id', NEW.id, 'source_type', NEW.source_type, 'file_path', NEW.file_path, 'file_hash_sha256', NEW.file_hash_sha256, 'bank_parser', NEW.bank_parser, 'statement_period', NEW.statement_period, 'extracted_count', NEW.extracted_count, 'imported_count', NEW.imported_count, 'skipped_count', NEW.skipped_count, 'reconcile_status', NEW.reconcile_status, 'statement_total_cents', NEW.statement_total_cents, 'extracted_total_cents', NEW.extracted_total_cents, 'created_at', NEW.created_at, 'ai_raw_output_json', NEW.ai_raw_output_json, 'ai_validation_json', NEW.ai_validation_json, 'ai_model', NEW.ai_model, 'ai_prompt_version', NEW.ai_prompt_version, 'ai_prompt_hash', NEW.ai_prompt_hash, 'content_hash_sha256', NEW.content_hash_sha256, 'total_charges_cents', NEW.total_charges_cents, 'total_payments_cents', NEW.total_payments_cents, 'new_balance_cents', NEW.new_balance_cents, 'expected_transaction_count', NEW.expected_transaction_count));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_import_batches_delete
AFTER DELETE ON import_batches
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('import_batches', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'source_type', OLD.source_type, 'file_path', OLD.file_path, 'file_hash_sha256', OLD.file_hash_sha256, 'bank_parser', OLD.bank_parser, 'statement_period', OLD.statement_period, 'extracted_count', OLD.extracted_count, 'imported_count', OLD.imported_count, 'skipped_count', OLD.skipped_count, 'reconcile_status', OLD.reconcile_status, 'statement_total_cents', OLD.statement_total_cents, 'extracted_total_cents', OLD.extracted_total_cents, 'created_at', OLD.created_at, 'ai_raw_output_json', OLD.ai_raw_output_json, 'ai_validation_json', OLD.ai_validation_json, 'ai_model', OLD.ai_model, 'ai_prompt_version', OLD.ai_prompt_version, 'ai_prompt_hash', OLD.ai_prompt_hash, 'content_hash_sha256', OLD.content_hash_sha256, 'total_charges_cents', OLD.total_charges_cents, 'total_payments_cents', OLD.total_payments_cents, 'new_balance_cents', OLD.new_balance_cents, 'expected_transaction_count', OLD.expected_transaction_count), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_category_mappings_insert
AFTER INSERT ON category_mappings
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('category_mappings', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'source_category', NEW.source_category, 'source', NEW.source, 'category_id', NEW.category_id, 'created_by', NEW.created_by, 'confidence', NEW.confidence, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched, 'is_enabled', NEW.is_enabled, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_category_mappings_update
AFTER UPDATE ON category_mappings
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('category_mappings', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'source_category', OLD.source_category, 'source', OLD.source, 'category_id', OLD.category_id, 'created_by', OLD.created_by, 'confidence', OLD.confidence, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched, 'is_enabled', OLD.is_enabled, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at), json_object('id', NEW.id, 'source_category', NEW.source_category, 'source', NEW.source, 'category_id', NEW.category_id, 'created_by', NEW.created_by, 'confidence', NEW.confidence, 'match_count', NEW.match_count, 'last_matched', NEW.last_matched, 'is_enabled', NEW.is_enabled, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_category_mappings_delete
AFTER DELETE ON category_mappings
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('category_mappings', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'source_category', OLD.source_category, 'source', OLD.source, 'category_id', OLD.category_id, 'created_by', OLD.created_by, 'confidence', OLD.confidence, 'match_count', OLD.match_count, 'last_matched', OLD.last_matched, 'is_enabled', OLD.is_enabled, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_notification_channels_insert
AFTER INSERT ON notification_channels
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('notification_channels', 'INSERT', json_object('channel', NEW.channel), NULL, json_object('channel', NEW.channel, 'config', NEW.config, 'label', NEW.label, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_notification_channels_update
AFTER UPDATE OF channel, config, label ON notification_channels
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('notification_channels', 'UPDATE', json_object('channel', NEW.channel), json_object('channel', OLD.channel, 'config', OLD.config, 'label', OLD.label, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at), json_object('channel', NEW.channel, 'config', NEW.config, 'label', NEW.label, 'created_at', NEW.created_at, 'updated_at', CASE WHEN NEW.updated_at = OLD.updated_at THEN datetime('now') ELSE NEW.updated_at END));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_notification_channels_delete
AFTER DELETE ON notification_channels
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('notification_channels', 'DELETE', json_object('channel', OLD.channel), json_object('channel', OLD.channel, 'config', OLD.config, 'label', OLD.label, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_mileage_log_insert
AFTER INSERT ON mileage_log
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('mileage_log', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'trip_date', NEW.trip_date, 'miles', NEW.miles, 'destination', NEW.destination, 'business_purpose', NEW.business_purpose, 'vehicle_name', NEW.vehicle_name, 'tax_year', NEW.tax_year, 'round_trip', NEW.round_trip, 'notes', NEW.notes, 'created_at', NEW.created_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_mileage_log_update
AFTER UPDATE ON mileage_log
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('mileage_log', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'trip_date', OLD.trip_date, 'miles', OLD.miles, 'destination', OLD.destination, 'business_purpose', OLD.business_purpose, 'vehicle_name', OLD.vehicle_name, 'tax_year', OLD.tax_year, 'round_trip', OLD.round_trip, 'notes', OLD.notes, 'created_at', OLD.created_at), json_object('id', NEW.id, 'trip_date', NEW.trip_date, 'miles', NEW.miles, 'destination', NEW.destination, 'business_purpose', NEW.business_purpose, 'vehicle_name', NEW.vehicle_name, 'tax_year', NEW.tax_year, 'round_trip', NEW.round_trip, 'notes', NEW.notes, 'created_at', NEW.created_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_mileage_log_delete
AFTER DELETE ON mileage_log
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('mileage_log', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'trip_date', OLD.trip_date, 'miles', OLD.miles, 'destination', OLD.destination, 'business_purpose', OLD.business_purpose, 'vehicle_name', OLD.vehicle_name, 'tax_year', OLD.tax_year, 'round_trip', OLD.round_trip, 'notes', OLD.notes, 'created_at', OLD.created_at), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_contractors_insert
AFTER INSERT ON contractors
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('contractors', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'name', NEW.name, 'tin_last4', NEW.tin_last4, 'entity_type', NEW.entity_type, 'is_active', NEW.is_active, 'notes', NEW.notes, 'created_at', NEW.created_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_contractors_update
AFTER UPDATE ON contractors
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('contractors', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'name', OLD.name, 'tin_last4', OLD.tin_last4, 'entity_type', OLD.entity_type, 'is_active', OLD.is_active, 'notes', OLD.notes, 'created_at', OLD.created_at), json_object('id', NEW.id, 'name', NEW.name, 'tin_last4', NEW.tin_last4, 'entity_type', NEW.entity_type, 'is_active', NEW.is_active, 'notes', NEW.notes, 'created_at', NEW.created_at));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_contractors_delete
AFTER DELETE ON contractors
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('contractors', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'name', OLD.name, 'tin_last4', OLD.tin_last4, 'entity_type', OLD.entity_type, 'is_active', OLD.is_active, 'notes', OLD.notes, 'created_at', OLD.created_at), NULL);
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_contractor_payments_insert
AFTER INSERT ON contractor_payments
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('contractor_payments', 'INSERT', json_object('id', NEW.id), NULL, json_object('id', NEW.id, 'contractor_id', NEW.contractor_id, 'transaction_id', NEW.transaction_id, 'tax_year', NEW.tax_year, 'created_at', NEW.created_at, 'paid_via_card', NEW.paid_via_card));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_contractor_payments_update
AFTER UPDATE ON contractor_payments
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('contractor_payments', 'UPDATE', json_object('id', NEW.id), json_object('id', OLD.id, 'contractor_id', OLD.contractor_id, 'transaction_id', OLD.transaction_id, 'tax_year', OLD.tax_year, 'created_at', OLD.created_at, 'paid_via_card', OLD.paid_via_card), json_object('id', NEW.id, 'contractor_id', NEW.contractor_id, 'transaction_id', NEW.transaction_id, 'tax_year', NEW.tax_year, 'created_at', NEW.created_at, 'paid_via_card', NEW.paid_via_card));
END;

CREATE TRIGGER IF NOT EXISTS _sync_log_contractor_payments_delete
AFTER DELETE ON contractor_payments
FOR EACH ROW
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json)
    VALUES ('contractor_payments', 'DELETE', json_object('id', OLD.id), json_object('id', OLD.id, 'contractor_id', OLD.contractor_id, 'transaction_id', OLD.transaction_id, 'tax_year', OLD.tax_year, 'created_at', OLD.created_at, 'paid_via_card', OLD.paid_via_card), NULL);
END;
