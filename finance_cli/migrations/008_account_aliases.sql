CREATE TABLE IF NOT EXISTS account_aliases (
    hash_account_id   TEXT NOT NULL REFERENCES accounts(id),
    canonical_id      TEXT NOT NULL REFERENCES accounts(id),
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (hash_account_id)
);

