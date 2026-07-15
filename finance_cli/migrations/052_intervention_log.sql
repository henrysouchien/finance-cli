-- finance_cli/migrations/052_intervention_log.sql

CREATE TABLE IF NOT EXISTS intervention_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_id      TEXT NOT NULL,
    fired_at        TEXT NOT NULL DEFAULT (datetime('now')),
    surface         TEXT NOT NULL CHECK (surface IN ('dashboard', 'action_queue', 'agent_prompt', 'chat', 'telegram', 'email', 'cli')),
    user_action     TEXT NOT NULL DEFAULT 'pending'
                    CHECK (user_action IN ('pending', 'acted', 'dismissed', 'snoozed', 'ignored')),
    acted_at        TEXT,
    dollar_impact_cents INTEGER,
    goal_link       TEXT,
    headline        TEXT NOT NULL,
    payload         TEXT CHECK (payload IS NULL OR json_valid(payload)),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_intervention_log_pattern_fired
    ON intervention_log(pattern_id, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_intervention_log_pattern_dismissed
    ON intervention_log(pattern_id, acted_at DESC)
    WHERE user_action = 'dismissed';

CREATE INDEX IF NOT EXISTS idx_intervention_log_action_fired
    ON intervention_log(user_action, fired_at);

CREATE TRIGGER IF NOT EXISTS intervention_log_touch_updated_at
    AFTER UPDATE ON intervention_log
    FOR EACH ROW
BEGIN
    UPDATE intervention_log SET updated_at = datetime('now') WHERE id = NEW.id;
END;
