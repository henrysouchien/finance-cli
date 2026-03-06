PRAGMA foreign_keys = OFF;

DROP TRIGGER IF EXISTS txn_ai;
DROP TRIGGER IF EXISTS txn_ad;
DROP TRIGGER IF EXISTS txn_au;

DROP INDEX IF EXISTS idx_txn_date;
DROP INDEX IF EXISTS idx_txn_category;
DROP INDEX IF EXISTS idx_txn_reviewed_date;
DROP INDEX IF EXISTS idx_txn_account;
DROP INDEX IF EXISTS idx_txn_source_date;
DROP INDEX IF EXISTS idx_txn_use_type_date;
DROP INDEX IF EXISTS idx_txn_project;
DROP INDEX IF EXISTS idx_txn_active_date;
DROP INDEX IF EXISTS idx_txn_split_group;
DROP INDEX IF EXISTS idx_txn_parent;

ALTER TABLE transactions RENAME TO transactions_old;

CREATE TABLE transactions (
    id                    TEXT PRIMARY KEY,
    account_id            TEXT REFERENCES accounts(id) ON DELETE SET NULL,
    plaid_txn_id          TEXT UNIQUE,
    dedupe_key            TEXT UNIQUE,
    date                  TEXT NOT NULL,
    description           TEXT NOT NULL,
    amount_cents          INTEGER NOT NULL,
    category_id           TEXT REFERENCES categories(id) ON DELETE SET NULL,
    category_source       TEXT CHECK (
        category_source IS NULL
        OR category_source IN (
            'user', 'vendor_memory', 'plaid', 'auto_prefix', 'ambiguous',
            'keyword_rule', 'ai', 'pdf_import'
        )
    ),
    category_confidence   REAL,
    category_rule_id      TEXT REFERENCES vendor_memory(id) ON DELETE SET NULL,
    use_type              TEXT CHECK (use_type IN ('Business', 'Personal')),
    is_payment            INTEGER NOT NULL DEFAULT 0 CHECK (is_payment IN (0, 1)),
    is_recurring          INTEGER NOT NULL DEFAULT 0 CHECK (is_recurring IN (0, 1)),
    is_reviewed           INTEGER NOT NULL DEFAULT 0 CHECK (is_reviewed IN (0, 1)),
    is_active             INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    removed_at            TEXT,
    project_id            TEXT REFERENCES projects(id) ON DELETE SET NULL,
    notes                 TEXT,
    source                TEXT NOT NULL CHECK (source IN ('plaid', 'csv_import', 'manual', 'pdf_import')),
    raw_plaid_json        TEXT,
    split_group_id        TEXT,
    parent_transaction_id TEXT REFERENCES transactions(id) ON DELETE SET NULL,
    split_pct             REAL,
    split_note            TEXT,
    created_at            TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at            TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO transactions (
    id,
    account_id,
    plaid_txn_id,
    dedupe_key,
    date,
    description,
    amount_cents,
    category_id,
    category_source,
    category_confidence,
    category_rule_id,
    use_type,
    is_payment,
    is_recurring,
    is_reviewed,
    is_active,
    removed_at,
    project_id,
    notes,
    source,
    raw_plaid_json,
    split_group_id,
    parent_transaction_id,
    split_pct,
    split_note,
    created_at,
    updated_at
)
SELECT
    id,
    account_id,
    plaid_txn_id,
    dedupe_key,
    date,
    description,
    amount_cents,
    category_id,
    category_source,
    category_confidence,
    category_rule_id,
    use_type,
    is_payment,
    is_recurring,
    is_reviewed,
    is_active,
    removed_at,
    project_id,
    notes,
    source,
    raw_plaid_json,
    NULL,
    NULL,
    NULL,
    NULL,
    created_at,
    updated_at
FROM transactions_old;

DROP TABLE transactions_old;

CREATE TABLE IF NOT EXISTS ai_categorization_log (
    id              TEXT PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    transaction_id  TEXT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    provider        TEXT NOT NULL,
    model           TEXT NOT NULL,
    category_name   TEXT,
    use_type        TEXT,
    confidence      REAL,
    reasoning       TEXT,
    prompt_hash     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ai_log_batch ON ai_categorization_log(batch_id);
CREATE INDEX IF NOT EXISTS idx_ai_log_txn ON ai_categorization_log(transaction_id);

CREATE TABLE IF NOT EXISTS import_batches (
    id                    TEXT PRIMARY KEY,
    source_type           TEXT NOT NULL CHECK (source_type IN ('pdf', 'csv', 'income_csv')),
    file_path             TEXT,
    file_hash_sha256      TEXT,
    bank_parser           TEXT,
    statement_period      TEXT,
    extracted_count       INTEGER,
    imported_count        INTEGER,
    skipped_count         INTEGER,
    reconcile_status      TEXT CHECK (
        reconcile_status IS NULL
        OR reconcile_status IN ('matched', 'mismatch', 'no_totals')
    ),
    statement_total_cents INTEGER,
    extracted_total_cents INTEGER,
    created_at            TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_import_batches_hash
    ON import_batches(file_hash_sha256) WHERE file_hash_sha256 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_txn_category ON transactions(category_id);
CREATE INDEX IF NOT EXISTS idx_txn_reviewed_date ON transactions(is_reviewed, date);
CREATE INDEX IF NOT EXISTS idx_txn_account ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_txn_source_date ON transactions(source, date);
CREATE INDEX IF NOT EXISTS idx_txn_use_type_date ON transactions(use_type, date);
CREATE INDEX IF NOT EXISTS idx_txn_project ON transactions(project_id);
CREATE INDEX IF NOT EXISTS idx_txn_active_date ON transactions(is_active, date);
CREATE INDEX IF NOT EXISTS idx_vendor_memory_lookup ON vendor_memory(description_pattern, use_type, is_enabled);
CREATE INDEX IF NOT EXISTS idx_txn_split_group ON transactions(split_group_id);
CREATE INDEX IF NOT EXISTS idx_txn_parent ON transactions(parent_transaction_id);

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

INSERT INTO txn_fts(txn_fts) VALUES('rebuild');

PRAGMA foreign_keys = ON;
