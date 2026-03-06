CREATE TABLE IF NOT EXISTS tax_config (
    tax_year     INTEGER NOT NULL,
    config_key   TEXT NOT NULL,
    config_value TEXT NOT NULL,
    updated_at   TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (tax_year, config_key)
);
