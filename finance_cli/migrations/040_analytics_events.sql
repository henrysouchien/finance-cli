CREATE TABLE IF NOT EXISTS analytics_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event           TEXT NOT NULL,
    domain          TEXT NOT NULL,
    outcome         TEXT NOT NULL DEFAULT 'succeeded' CHECK (outcome IN ('started','succeeded','failed','abandoned')),
    properties      TEXT CHECK (properties IS NULL OR json_valid(properties)),
    source          TEXT NOT NULL DEFAULT 'api' CHECK (source IN ('web','telegram','cli','api','cron')),
    request_id      TEXT,
    session_id      TEXT,
    conversation_id TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_ae_event_created ON analytics_events(event, created_at);
CREATE INDEX idx_ae_domain_created ON analytics_events(domain, created_at);
CREATE INDEX idx_ae_source_created ON analytics_events(source, created_at);
CREATE INDEX idx_ae_session ON analytics_events(session_id);
CREATE INDEX idx_ae_request ON analytics_events(request_id);
