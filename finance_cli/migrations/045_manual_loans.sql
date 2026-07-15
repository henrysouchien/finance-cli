-- 045_manual_loans.sql — Manual loan / informal liability tracking

CREATE TABLE IF NOT EXISTS manual_loans (
    id                      TEXT PRIMARY KEY,
    creditor_name           TEXT NOT NULL,
    description             TEXT,
    total_disbursed_cents   INTEGER NOT NULL CHECK (total_disbursed_cents >= 0),
    current_balance_cents   INTEGER NOT NULL CHECK (current_balance_cents >= 0),
    interest_rate_pct       REAL NOT NULL DEFAULT 0.0 CHECK (interest_rate_pct >= 0),
    interest_type           TEXT NOT NULL DEFAULT 'none'
                            CHECK (interest_type IN ('simple', 'compound', 'none')),
    monthly_payment_cents   INTEGER CHECK (monthly_payment_cents IS NULL OR monthly_payment_cents > 0),
    payment_due_day         INTEGER CHECK (payment_due_day IS NULL OR (payment_due_day >= 1 AND payment_due_day <= 31)),
    start_date              TEXT NOT NULL,
    expected_payoff_date    TEXT,
    use_type                TEXT NOT NULL DEFAULT 'Personal'
                            CHECK (use_type IN ('Personal', 'Business')),
    is_active               INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at              TEXT NOT NULL DEFAULT (datetime('now')),
    -- DB-level consistency: 'none' requires rate=0, non-zero rate requires non-'none'
    CHECK (
        (interest_type = 'none' AND interest_rate_pct = 0.0)
        OR (interest_type != 'none' AND interest_rate_pct > 0.0)
    )
);

CREATE INDEX IF NOT EXISTS idx_manual_loans_active
    ON manual_loans(is_active);

CREATE INDEX IF NOT EXISTS idx_manual_loans_active_due
    ON manual_loans(is_active, payment_due_day)
    WHERE payment_due_day IS NOT NULL;

CREATE TABLE IF NOT EXISTS loan_disbursements (
    id                TEXT PRIMARY KEY,
    loan_id           TEXT NOT NULL REFERENCES manual_loans(id) ON DELETE CASCADE,
    amount_cents      INTEGER NOT NULL CHECK (amount_cents > 0),
    disbursement_date TEXT NOT NULL,
    notes             TEXT,
    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_loan_disbursements_loan
    ON loan_disbursements(loan_id, disbursement_date);

CREATE TABLE IF NOT EXISTS loan_payments (
    id              TEXT PRIMARY KEY,
    loan_id         TEXT NOT NULL REFERENCES manual_loans(id) ON DELETE CASCADE,
    amount_cents    INTEGER NOT NULL CHECK (amount_cents > 0),
    payment_date    TEXT NOT NULL,
    transaction_id  TEXT REFERENCES transactions(id) ON DELETE SET NULL,
    notes           TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_loan_payments_loan
    ON loan_payments(loan_id, payment_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_loan_payments_txn_unique
    ON loan_payments(transaction_id)
    WHERE transaction_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS loan_events (
    id          TEXT PRIMARY KEY,
    loan_id     TEXT NOT NULL REFERENCES manual_loans(id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL CHECK (event_type IN ('adjust', 'close', 'forgive', 'reopen')),
    details     TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_loan_events_loan
    ON loan_events(loan_id);
