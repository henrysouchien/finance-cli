-- Note: SQLite does not parse underscore numeric literals — use plain digits here.
INSERT OR IGNORE INTO cost_limits (provider, period, limit_usd6, action)
VALUES ('plaid', 'daily', 1000000, 'warn');
