ALTER TABLE transactions ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_txn_idempotency
    ON transactions(idempotency_key) WHERE idempotency_key IS NOT NULL;

ALTER TABLE manual_loans ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_loan_idempotency
    ON manual_loans(idempotency_key) WHERE idempotency_key IS NOT NULL;

ALTER TABLE subscriptions ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_subs_idempotency
    ON subscriptions(idempotency_key) WHERE idempotency_key IS NOT NULL;
