ALTER TABLE subscriptions ADD COLUMN sub_type TEXT NOT NULL DEFAULT 'fixed'
    CHECK (sub_type IN ('fixed', 'metered'));
