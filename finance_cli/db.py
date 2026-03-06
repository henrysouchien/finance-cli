"""SQLite connection and migration runner."""

from __future__ import annotations

from datetime import datetime
import re
import shutil
import sqlite3
from pathlib import Path

from .config import get_db_path

MIGRATION_RE = re.compile(r"^(?P<version>\d+)_.*\.sql$")
RUNTIME_RESET_TABLES: tuple[str, ...] = (
    "ai_categorization_log",
    "transactions",
    "import_batches",
    "balance_snapshots",
    "liabilities",
    "account_aliases",
    "subscriptions",
    "recurring_flows",
    "monthly_plans",
    "accounts",
)


class MigrationError(RuntimeError):
    """Raised when a DB migration fails."""


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    resolved = (db_path or get_db_path()).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(resolved))
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")


def initialize_database(db_path: Path | None = None) -> None:
    with connect(db_path) as conn:
        _ensure_schema_version_table(conn)
        _run_pending_migrations(conn)


def _ensure_schema_version_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version     INTEGER PRIMARY KEY,
            applied_at  TEXT DEFAULT (datetime('now')),
            description TEXT
        )
        """
    )


def _migration_dir() -> Path:
    return Path(__file__).resolve().parent / "migrations"


def _list_migrations() -> list[tuple[int, Path]]:
    migrations: list[tuple[int, Path]] = []
    for path in _migration_dir().glob("*.sql"):
        match = MIGRATION_RE.match(path.name)
        if not match:
            continue
        migrations.append((int(match.group("version")), path))
    migrations.sort(key=lambda x: x[0])
    return migrations


def _connected_main_db_path(conn: sqlite3.Connection) -> Path | None:
    rows = conn.execute("PRAGMA database_list").fetchall()
    for row in rows:
        name = str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        if name != "main":
            continue
        raw_path = str(row["file"] if isinstance(row, sqlite3.Row) else row[2]).strip()
        if not raw_path:
            return None
        return Path(raw_path).expanduser().resolve()
    return None


def _run_pending_migrations(conn: sqlite3.Connection) -> None:
    applied = {
        int(row["version"])
        for row in conn.execute("SELECT version FROM schema_version").fetchall()
    }
    migration_backup_created = False

    for version, path in _list_migrations():
        if version in applied:
            continue

        if version >= 15 and not migration_backup_created:
            db_path = _connected_main_db_path(conn)
            if db_path and db_path.exists():
                try:
                    backup_database(conn=conn, db_path=db_path)
                except Exception as exc:
                    raise MigrationError(f"Failed creating pre-migration backup before {path.name}: {exc}") from exc
            migration_backup_created = True

        sql = path.read_text(encoding="utf-8")
        # Append the version record to the migration script so executescript()
        # commits both the DDL/DML and the schema_version row atomically.
        sql_with_version = (
            sql.rstrip().rstrip(";")
            + f";\nINSERT INTO schema_version (version, description) VALUES ({version}, '{path.name}');\n"
        )
        try:
            conn.executescript(sql_with_version)
        except sqlite3.Error as exc:
            conn.rollback()
            raise MigrationError(f"Failed applying migration {path.name}: {exc}") from exc


def query_all(conn: sqlite3.Connection, sql: str, params: tuple | None = None) -> list[sqlite3.Row]:
    cursor = conn.execute(sql, params or ())
    return cursor.fetchall()


def query_one(conn: sqlite3.Connection, sql: str, params: tuple | None = None) -> sqlite3.Row | None:
    cursor = conn.execute(sql, params or ())
    return cursor.fetchone()


def _resolve_backup_target_path(source_db_path: Path, destination: Path | None = None) -> Path:
    source_db_path = source_db_path.expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_name = f"{source_db_path.stem}_backup_{timestamp}{source_db_path.suffix or '.db'}"

    if destination is None:
        target = source_db_path.with_name(default_name)
    else:
        resolved = destination.expanduser().resolve()
        # If destination is an existing directory, or a path without a suffix,
        # treat it as a target directory and generate a timestamped filename.
        if resolved.is_dir() or not resolved.suffix:
            target = resolved / default_name
        else:
            target = resolved

    if not target.exists():
        return target

    base = target.with_suffix("")
    suffix = target.suffix
    counter = 1
    candidate = Path(f"{base}_{counter}{suffix}")
    while candidate.exists():
        counter += 1
        candidate = Path(f"{base}_{counter}{suffix}")
    return candidate


def backup_database(
    *,
    conn: sqlite3.Connection | None = None,
    db_path: Path | None = None,
    destination: Path | None = None,
) -> Path:
    source_db_path = (db_path or get_db_path()).expanduser().resolve()
    source_db_path.parent.mkdir(parents=True, exist_ok=True)
    if not source_db_path.exists():
        raise FileNotFoundError(f"Database not found: {source_db_path}")

    backup_path = _resolve_backup_target_path(source_db_path, destination=destination)
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    if conn is not None:
        with sqlite3.connect(str(backup_path)) as backup_conn:
            conn.backup(backup_conn)
            backup_conn.commit()
        return backup_path

    shutil.copy2(source_db_path, backup_path)
    return backup_path


def reset_plaid_sync_metadata(conn: sqlite3.Connection) -> int:
    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(plaid_items)").fetchall()
    }
    if not columns:
        return 0

    reset_columns = [
        column
        for column in (
            "sync_cursor",
            "last_sync_at",
            "last_balance_refresh_at",
            "last_liabilities_fetch_at",
        )
        if column in columns
    ]
    if not reset_columns:
        return 0

    assignments = ", ".join(f"{column} = NULL" for column in reset_columns)
    cursor = conn.execute(
        f"""
        UPDATE plaid_items
           SET {assignments},
               updated_at = datetime('now')
        """
    )
    return int(cursor.rowcount or 0)


def wipe_runtime_data(conn: sqlite3.Connection, *, preserve_plaid_items: bool = True) -> dict[str, int]:
    report: dict[str, int] = {}
    for table in RUNTIME_RESET_TABLES:
        cursor = conn.execute(f"DELETE FROM {table}")
        report[table] = int(cursor.rowcount or 0)

    if preserve_plaid_items:
        report["plaid_items_reset"] = reset_plaid_sync_metadata(conn)
    else:
        cursor = conn.execute("DELETE FROM plaid_items")
        report["plaid_items_deleted"] = int(cursor.rowcount or 0)

    return report
