ALTER TABLE backup_log ADD COLUMN bundle_format_version INTEGER DEFAULT 1;
ALTER TABLE backup_log ADD COLUMN dek_secret_ref TEXT;
ALTER TABLE backup_log ADD COLUMN signing_key_secret_ref TEXT;
ALTER TABLE backup_log ADD COLUMN signature_verified_at TEXT;
ALTER TABLE backup_log ADD COLUMN bundle_id TEXT;
ALTER TABLE backup_log ADD COLUMN user_id TEXT;
