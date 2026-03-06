ALTER TABLE plaid_items ADD COLUMN institution_id TEXT;

CREATE INDEX IF NOT EXISTS idx_plaid_items_institution_status
ON plaid_items(institution_id, status);
