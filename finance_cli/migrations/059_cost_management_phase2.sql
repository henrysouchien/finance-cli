-- 1. Per-user credit balance (singleton row)
CREATE TABLE credit_balance (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    balance_usd6 INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
INSERT INTO credit_balance (id, balance_usd6, updated_at) VALUES (1, 0, datetime('now'));

-- 2. Credit ledger (audit + idempotency)
CREATE TABLE credit_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK(source IN ('topup','consume','refund','promo','adjustment')),
    amount_usd6 INTEGER NOT NULL,
    stripe_payment_intent_id TEXT,
    refund_idempotency_key TEXT,
    cost_ledger_idempotency_key TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    notes TEXT
);
CREATE UNIQUE INDEX idx_credit_ledger_pi ON credit_ledger(stripe_payment_intent_id) WHERE stripe_payment_intent_id IS NOT NULL;
CREATE UNIQUE INDEX idx_credit_ledger_refund ON credit_ledger(refund_idempotency_key) WHERE refund_idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX idx_credit_ledger_consume ON credit_ledger(cost_ledger_idempotency_key) WHERE source = 'consume';

-- 3. cost_ledger: BYOK + attribution columns
ALTER TABLE cost_ledger ADD COLUMN is_byok INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cost_ledger ADD COLUMN allowance_debit_usd6 INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cost_ledger ADD COLUMN credits_debit_usd6 INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cost_ledger ADD COLUMN overflow_unattributed_usd6 INTEGER NOT NULL DEFAULT 0;

-- 4. cost_limits: rewrite to make limit_usd6 NULLABLE + add system_limit_usd6
CREATE TABLE cost_limits_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'openai', 'plaid', 'all')),
    period TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
    limit_usd6 INTEGER,                  -- NOW NULLABLE
    system_limit_usd6 INTEGER,           -- NEW
    action TEXT NOT NULL DEFAULT 'warn' CHECK (action IN ('warn', 'block')),
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(provider, period)
);
INSERT INTO cost_limits_new (id, provider, period, limit_usd6, system_limit_usd6, action, is_active, created_at)
SELECT id, provider, period, limit_usd6, NULL, action, is_active, created_at FROM cost_limits;
DROP TABLE cost_limits;
ALTER TABLE cost_limits_new RENAME TO cost_limits;

-- 5. Phase 1 sentinel normalization happens via the ops CLI (plan-caps-reseed),
--    NOT here. The CLI joins per-user SQLite against PostgreSQL `users` to know
--    each user's actual `lifetime_deal`/`tier`. SQL migrations don't have that
--    cross-DB context, so doing the cleanup blind by sentinel value is unsafe
--    (could destroy a real user-set $10M block cap). See "Deploy step" below.

-- 6. Recreate _sync_log_cost_limits_* triggers (DROP'd by table rewrite at step 4).
--    Bodies match migration 058's exact shape: lowercase trigger names,
--    `current_session_id() != '__STREAM__'` guard, `_sync_changelog` columns
--    `(table_name, op, pk_json, old_json, new_json, origin_session_id)`,
--    `json(json_object(...))` wrapping, INSERT has `old_json=NULL`, DELETE has
--    `new_json=NULL`. The only delta from 058's own cost_limits triggers is the
--    addition of `system_limit_usd6` to the new_json/old_json payloads.
DROP TRIGGER IF EXISTS _sync_log_cost_limits_insert;
DROP TRIGGER IF EXISTS _sync_log_cost_limits_update;
DROP TRIGGER IF EXISTS _sync_log_cost_limits_delete;

CREATE TRIGGER _sync_log_cost_limits_insert
AFTER INSERT ON cost_limits
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('cost_limits', 'INSERT',
        json(json_object('id', NEW.id)),
        NULL,
        json(json_object(
            'id', NEW.id, 'provider', NEW.provider, 'period', NEW.period,
            'limit_usd6', NEW.limit_usd6,
            'system_limit_usd6', NEW.system_limit_usd6,
            'action', NEW.action, 'is_active', NEW.is_active, 'created_at', NEW.created_at
        )),
        current_session_id());
END;

CREATE TRIGGER _sync_log_cost_limits_update
AFTER UPDATE ON cost_limits
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('cost_limits', 'UPDATE',
        json(json_object('id', NEW.id)),
        json(json_object(
            'id', OLD.id, 'provider', OLD.provider, 'period', OLD.period,
            'limit_usd6', OLD.limit_usd6,
            'system_limit_usd6', OLD.system_limit_usd6,
            'action', OLD.action, 'is_active', OLD.is_active, 'created_at', OLD.created_at
        )),
        json(json_object(
            'id', NEW.id, 'provider', NEW.provider, 'period', NEW.period,
            'limit_usd6', NEW.limit_usd6,
            'system_limit_usd6', NEW.system_limit_usd6,
            'action', NEW.action, 'is_active', NEW.is_active, 'created_at', NEW.created_at
        )),
        current_session_id());
END;

CREATE TRIGGER _sync_log_cost_limits_delete
AFTER DELETE ON cost_limits
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES ('cost_limits', 'DELETE',
        json(json_object('id', OLD.id)),
        json(json_object(
            'id', OLD.id, 'provider', OLD.provider, 'period', OLD.period,
            'limit_usd6', OLD.limit_usd6,
            'system_limit_usd6', OLD.system_limit_usd6,
            'action', OLD.action, 'is_active', OLD.is_active, 'created_at', OLD.created_at
        )),
        NULL,
        current_session_id());
END;
