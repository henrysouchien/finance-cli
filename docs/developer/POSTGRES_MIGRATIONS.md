# PostgreSQL Migration Policy

Finance-web migrations in `finance-web/server/migrations/` normally run through
the app as the `finance_web` role. Operator-run migrations may use an admin role
for emergency deploys, so migrations that create tables or sequences should be
safe in both paths.

For any migration that creates PostgreSQL tables or sequences:

- Prefer applying it as `finance_web`.
- If an operator may apply it as `postgres`, include a conditional role handoff:
  `ALTER TABLE ... OWNER TO finance_web`, `ALTER SEQUENCE ... OWNER TO
  finance_web`, and matching table/sequence `GRANT`s inside a `DO $$` block that
  first checks `pg_roles`.
- Keep the block conditional so local test databases without a `finance_web`
  role still run the migration.

This prevents app-start replays of `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX
IF NOT EXISTS` from failing with `must be owner of table` after an admin-applied
migration.
