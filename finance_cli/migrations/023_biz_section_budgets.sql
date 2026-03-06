CREATE TABLE IF NOT EXISTS biz_section_budgets (
    id             TEXT PRIMARY KEY,
    pl_section     TEXT NOT NULL CHECK (pl_section IN (
        'cogs', 'opex_marketing', 'opex_technology',
        'opex_professional', 'opex_facilities', 'opex_people', 'opex_other'
    )),
    amount_cents   INTEGER NOT NULL CHECK (amount_cents >= 0),
    period         TEXT NOT NULL DEFAULT 'monthly' CHECK (period IN ('monthly', 'quarterly', 'yearly')),
    effective_from TEXT NOT NULL,
    effective_to   TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
