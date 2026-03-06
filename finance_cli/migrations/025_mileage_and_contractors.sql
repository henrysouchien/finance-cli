CREATE TABLE IF NOT EXISTS mileage_log (
    id               TEXT PRIMARY KEY,
    trip_date        TEXT NOT NULL,
    miles            REAL NOT NULL CHECK (miles > 0),
    destination      TEXT NOT NULL,
    business_purpose TEXT NOT NULL,
    vehicle_name     TEXT DEFAULT 'primary',
    tax_year         INTEGER NOT NULL,
    round_trip       INTEGER NOT NULL DEFAULT 0 CHECK (round_trip IN (0, 1)),
    notes            TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS mileage_rates (
    tax_year   INTEGER PRIMARY KEY,
    rate_cents INTEGER NOT NULL CHECK (rate_cents > 0)
);

INSERT OR IGNORE INTO mileage_rates (tax_year, rate_cents) VALUES
    (2024, 67),
    (2025, 70),
    (2026, 70);

CREATE TABLE IF NOT EXISTS contractors (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    tin_last4   TEXT CHECK (
        tin_last4 IS NULL
        OR (
            length(tin_last4) = 4
            AND tin_last4 GLOB '[0-9][0-9][0-9][0-9]'
        )
    ),
    entity_type TEXT NOT NULL DEFAULT 'individual'
                CHECK (entity_type IN ('individual', 'llc', 'partnership', 'corporation')),
    is_active   INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    notes       TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS contractor_payments (
    id             TEXT PRIMARY KEY,
    contractor_id  TEXT NOT NULL REFERENCES contractors(id),
    transaction_id TEXT NOT NULL REFERENCES transactions(id),
    tax_year       INTEGER NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    paid_via_card  INTEGER NOT NULL DEFAULT 0 CHECK (paid_via_card IN (0, 1)),
    UNIQUE(contractor_id, transaction_id),
    UNIQUE(transaction_id)
);
