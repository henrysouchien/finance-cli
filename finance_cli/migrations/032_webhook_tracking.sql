ALTER TABLE plaid_items ADD COLUMN last_webhook_at TEXT;
ALTER TABLE plaid_items ADD COLUMN needs_reauth INTEGER NOT NULL DEFAULT 0 CHECK (needs_reauth IN (0, 1));
