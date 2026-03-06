CREATE INDEX IF NOT EXISTS idx_txn_recurring ON transactions(is_recurring, date);

ALTER TABLE subscriptions
ADD COLUMN is_auto_detected INTEGER NOT NULL DEFAULT 1 CHECK (is_auto_detected IN (0, 1));
