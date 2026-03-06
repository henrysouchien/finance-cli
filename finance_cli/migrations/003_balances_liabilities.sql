ALTER TABLE accounts ADD COLUMN balance_current_cents INTEGER;
ALTER TABLE accounts ADD COLUMN balance_available_cents INTEGER;
ALTER TABLE accounts ADD COLUMN balance_limit_cents INTEGER;
ALTER TABLE accounts ADD COLUMN iso_currency_code TEXT;
ALTER TABLE accounts ADD COLUMN unofficial_currency_code TEXT;
ALTER TABLE accounts ADD COLUMN balance_updated_at TEXT;

CREATE TABLE IF NOT EXISTS balance_snapshots (
    id                      TEXT PRIMARY KEY,
    account_id              TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    balance_current_cents   INTEGER,
    balance_available_cents INTEGER,
    balance_limit_cents     INTEGER,
    source                  TEXT NOT NULL CHECK (source IN ('sync', 'refresh', 'manual')),
    snapshot_date           TEXT NOT NULL,
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_balance_snap_dedup
    ON balance_snapshots(account_id, snapshot_date, source);

CREATE INDEX IF NOT EXISTS idx_balance_snap_account_date
    ON balance_snapshots(account_id, snapshot_date);

CREATE TABLE IF NOT EXISTS liabilities (
    id                            TEXT PRIMARY KEY,
    account_id                    TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    liability_type                TEXT NOT NULL CHECK (liability_type IN ('credit', 'student', 'mortgage')),
    is_active                     INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    last_seen_at                  TEXT,
    is_overdue                    INTEGER CHECK (is_overdue IS NULL OR is_overdue IN (0, 1)),
    last_payment_amount_cents     INTEGER,
    last_payment_date             TEXT,
    last_statement_balance_cents  INTEGER,
    last_statement_issue_date     TEXT,
    minimum_payment_cents         INTEGER,
    next_payment_due_date         TEXT,
    apr_purchase                  REAL,
    apr_balance_transfer          REAL,
    apr_cash_advance              REAL,
    interest_rate_pct             REAL,
    origination_principal_cents   INTEGER,
    outstanding_interest_cents    INTEGER,
    expected_payoff_date          TEXT,
    loan_name                     TEXT,
    loan_status_type              TEXT,
    loan_status_end_date          TEXT,
    repayment_plan_type           TEXT,
    repayment_plan_description    TEXT,
    servicer_name                 TEXT,
    ytd_interest_paid_cents       INTEGER,
    ytd_principal_paid_cents      INTEGER,
    mortgage_rate_pct             REAL,
    mortgage_rate_type            TEXT CHECK (mortgage_rate_type IS NULL OR mortgage_rate_type IN ('fixed', 'variable')),
    loan_term                     TEXT,
    maturity_date                 TEXT,
    origination_date              TEXT,
    escrow_balance_cents          INTEGER,
    has_pmi                       INTEGER CHECK (has_pmi IS NULL OR has_pmi IN (0, 1)),
    has_prepayment_penalty        INTEGER CHECK (has_prepayment_penalty IS NULL OR has_prepayment_penalty IN (0, 1)),
    next_monthly_payment_cents    INTEGER,
    past_due_amount_cents         INTEGER,
    current_late_fee_cents        INTEGER,
    property_address_json         TEXT,
    raw_plaid_json                TEXT,
    fetched_at                    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at                    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(account_id, liability_type)
);

CREATE INDEX IF NOT EXISTS idx_liabilities_due_date
    ON liabilities(next_payment_due_date);

CREATE INDEX IF NOT EXISTS idx_liabilities_active
    ON liabilities(is_active);
