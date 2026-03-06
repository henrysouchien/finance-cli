CREATE TABLE IF NOT EXISTS accounts (
    id                TEXT PRIMARY KEY,
    plaid_account_id  TEXT UNIQUE,
    plaid_item_id     TEXT,
    institution_name  TEXT NOT NULL,
    account_name      TEXT,
    account_type      TEXT NOT NULL CHECK (account_type IN ('checking', 'savings', 'credit_card', 'investment', 'loan')),
    card_ending       TEXT,
    is_active         INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS plaid_items (
    id                  TEXT PRIMARY KEY,
    plaid_item_id       TEXT UNIQUE NOT NULL,
    institution_name    TEXT NOT NULL,
    access_token_ref    TEXT,
    status              TEXT NOT NULL CHECK (status IN ('active', 'error', 'disconnected', 'pending')),
    error_code          TEXT,
    consented_products  TEXT,
    sync_cursor         TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS categories (
    id          TEXT PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    parent_id   TEXT REFERENCES categories(id) ON DELETE SET NULL,
    is_income   INTEGER NOT NULL DEFAULT 0 CHECK (is_income IN (0, 1)),
    is_system   INTEGER NOT NULL DEFAULT 0 CHECK (is_system IN (0, 1)),
    sort_order  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS projects (
    id                   TEXT PRIMARY KEY,
    name                 TEXT UNIQUE NOT NULL,
    description          TEXT,
    target_amount_cents  INTEGER,
    is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS vendor_memory (
    id                   TEXT PRIMARY KEY,
    description_pattern  TEXT NOT NULL,
    canonical_name       TEXT,
    category_id          TEXT REFERENCES categories(id) ON DELETE SET NULL,
    use_type             TEXT NOT NULL DEFAULT 'Any' CHECK (use_type IN ('Business', 'Personal', 'Any')),
    confidence           REAL NOT NULL DEFAULT 1.0,
    priority             INTEGER NOT NULL DEFAULT 0,
    is_enabled           INTEGER NOT NULL DEFAULT 1 CHECK (is_enabled IN (0, 1)),
    is_confirmed         INTEGER NOT NULL DEFAULT 1 CHECK (is_confirmed IN (0, 1)),
    match_count          INTEGER NOT NULL DEFAULT 0,
    last_matched         TEXT,
    UNIQUE(description_pattern, use_type)
);

CREATE TABLE IF NOT EXISTS transactions (
    id                   TEXT PRIMARY KEY,
    account_id           TEXT REFERENCES accounts(id) ON DELETE SET NULL,
    plaid_txn_id         TEXT UNIQUE,
    dedupe_key           TEXT UNIQUE,
    date                 TEXT NOT NULL,
    description          TEXT NOT NULL,
    amount_cents         INTEGER NOT NULL,
    category_id          TEXT REFERENCES categories(id) ON DELETE SET NULL,
    category_source      TEXT CHECK (category_source IS NULL OR category_source IN ('user', 'vendor_memory', 'plaid', 'auto_prefix', 'ambiguous')),
    category_confidence  REAL,
    category_rule_id     TEXT REFERENCES vendor_memory(id) ON DELETE SET NULL,
    use_type             TEXT CHECK (use_type IN ('Business', 'Personal')),
    is_payment           INTEGER NOT NULL DEFAULT 0 CHECK (is_payment IN (0, 1)),
    is_recurring         INTEGER NOT NULL DEFAULT 0 CHECK (is_recurring IN (0, 1)),
    is_reviewed          INTEGER NOT NULL DEFAULT 0 CHECK (is_reviewed IN (0, 1)),
    is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    removed_at           TEXT,
    project_id           TEXT REFERENCES projects(id) ON DELETE SET NULL,
    notes                TEXT,
    source               TEXT NOT NULL CHECK (source IN ('plaid', 'csv_import', 'manual')),
    raw_plaid_json       TEXT,
    created_at           TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS budgets (
    id            TEXT PRIMARY KEY,
    category_id   TEXT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    period        TEXT NOT NULL CHECK (period IN ('monthly', 'weekly', 'yearly')),
    amount_cents  INTEGER NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id            TEXT PRIMARY KEY,
    vendor_name   TEXT,
    category_id   TEXT REFERENCES categories(id) ON DELETE SET NULL,
    amount_cents  INTEGER NOT NULL,
    frequency     TEXT NOT NULL CHECK (frequency IN ('weekly', 'biweekly', 'monthly', 'quarterly', 'yearly')),
    next_expected TEXT,
    account_id    TEXT REFERENCES accounts(id) ON DELETE SET NULL,
    is_active     INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    use_type      TEXT CHECK (use_type IN ('Business', 'Personal'))
);

CREATE TABLE IF NOT EXISTS recurring_flows (
    id           TEXT PRIMARY KEY,
    name         TEXT,
    flow_type    TEXT NOT NULL CHECK (flow_type IN ('income', 'expense')),
    amount_cents INTEGER NOT NULL,
    frequency    TEXT NOT NULL CHECK (frequency IN ('weekly', 'biweekly', 'monthly', 'quarterly', 'yearly')),
    day_of_month INTEGER,
    account_id   TEXT REFERENCES accounts(id) ON DELETE SET NULL,
    category_id  TEXT REFERENCES categories(id) ON DELETE SET NULL,
    is_active    INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS monthly_plans (
    id                      TEXT PRIMARY KEY,
    month                   TEXT UNIQUE NOT NULL,
    expected_income_cents   INTEGER,
    expected_expenses_cents INTEGER,
    savings_target_cents    INTEGER,
    investment_target_cents INTEGER,
    notes                   TEXT
);

CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_txn_category ON transactions(category_id);
CREATE INDEX IF NOT EXISTS idx_txn_reviewed_date ON transactions(is_reviewed, date);
CREATE INDEX IF NOT EXISTS idx_txn_account ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_txn_source_date ON transactions(source, date);
CREATE INDEX IF NOT EXISTS idx_txn_use_type_date ON transactions(use_type, date);
CREATE INDEX IF NOT EXISTS idx_txn_project ON transactions(project_id);
CREATE INDEX IF NOT EXISTS idx_txn_active_date ON transactions(is_active, date);
CREATE INDEX IF NOT EXISTS idx_vendor_memory_lookup ON vendor_memory(description_pattern, use_type, is_enabled);

CREATE VIRTUAL TABLE IF NOT EXISTS txn_fts USING fts5(
    description,
    content='transactions',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS txn_ai AFTER INSERT ON transactions BEGIN
    INSERT INTO txn_fts(rowid, description) VALUES (new.rowid, new.description);
END;

CREATE TRIGGER IF NOT EXISTS txn_ad AFTER DELETE ON transactions BEGIN
    INSERT INTO txn_fts(txn_fts, rowid, description) VALUES ('delete', old.rowid, old.description);
END;

CREATE TRIGGER IF NOT EXISTS txn_au AFTER UPDATE OF description ON transactions BEGIN
    INSERT INTO txn_fts(txn_fts, rowid, description) VALUES ('delete', old.rowid, old.description);
    INSERT INTO txn_fts(rowid, description) VALUES (new.rowid, new.description);
END;

CREATE TRIGGER IF NOT EXISTS budgets_no_overlap_insert
BEFORE INSERT ON budgets
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM budgets b
    WHERE b.category_id = NEW.category_id
      AND b.period = NEW.period
      AND date(b.effective_from) <= date(COALESCE(NEW.effective_to, '9999-12-31'))
      AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(NEW.effective_from)
)
BEGIN
    SELECT RAISE(ABORT, 'budget range overlap');
END;

CREATE TRIGGER IF NOT EXISTS budgets_no_overlap_update
BEFORE UPDATE ON budgets
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM budgets b
    WHERE b.category_id = NEW.category_id
      AND b.period = NEW.period
      AND b.id <> OLD.id
      AND date(b.effective_from) <= date(COALESCE(NEW.effective_to, '9999-12-31'))
      AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(NEW.effective_from)
)
BEGIN
    SELECT RAISE(ABORT, 'budget range overlap');
END;
