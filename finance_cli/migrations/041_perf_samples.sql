CREATE TABLE IF NOT EXISTS perf_samples (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    source     TEXT NOT NULL CHECK (source IN ('http','tool','query','frontend','ai')),
    metric     TEXT NOT NULL,
    value_ms   INTEGER NOT NULL,
    is_error   INTEGER NOT NULL DEFAULT 0,
    request_id TEXT,
    tags       TEXT CHECK (tags IS NULL OR json_valid(tags)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_perf_metric_created ON perf_samples(metric, created_at);
CREATE INDEX IF NOT EXISTS idx_perf_source_created ON perf_samples(source, created_at);
CREATE INDEX IF NOT EXISTS idx_perf_request ON perf_samples(request_id);
