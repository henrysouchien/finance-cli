ALTER TABLE plaid_items ADD COLUMN last_sync_at TEXT;
ALTER TABLE plaid_items ADD COLUMN last_balance_refresh_at TEXT;
ALTER TABLE plaid_items ADD COLUMN last_liabilities_fetch_at TEXT;
