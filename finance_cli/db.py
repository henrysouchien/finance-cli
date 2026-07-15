"""SQLite connection and migration runner.

SQLCipher keys are applied via raw hex (`PRAGMA key = "x'...'"`), not
passphrase mode. Do not switch this to passphrase mode: raw hex avoids the
default SQLCipher PBKDF2 cost on every connection open.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
import logging
import os
import re
import shutil
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import get_db_path, runtime_cli_settings
from . import db_keys
from .exceptions import TenantMismatchError
from .settings_base import (
    DB_ENCRYPTION_MODE_ENV,
    VALID_DB_ENCRYPTION_MODES,
    normalize_db_encryption_mode,
)
from . import storage_lease

try:
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    _fcntl = None

if TYPE_CHECKING:  # pragma: no cover - type-checker only
    from .storage_client.connection import StorageConnection

try:  # psycopg2 is optional in local CLI environments.
    from psycopg2 import OperationalError as _PsycopgOperationalError
except Exception:  # pragma: no cover - exercised where psycopg2 is absent
    class _PsycopgOperationalError(Exception):
        pass

try:
    import sqlcipher3 as sqlcipher
except ImportError:  # pragma: no cover - fallback for environments without wheels
    from pysqlcipher3 import dbapi2 as sqlcipher

_SQLCIPHER_CONNECTION_TYPE = getattr(sqlcipher, "Connection", sqlite3.Connection)
log = logging.getLogger(__name__)

MIGRATION_RE = re.compile(r"^(?P<version>\d+)_.*\.sql$")
SCHEMA_VERSION = 79
DEFAULT_BACKUP_RETENTION_COUNT = 20
BACKUP_RETENTION_ENV = "FINANCE_CLI_BACKUP_RETENTION"
ENCRYPTION_MODE_ENV = DB_ENCRYPTION_MODE_ENV
VALID_ENCRYPTION_MODES = VALID_DB_ENCRYPTION_MODES
_DB_ENCRYPTION_MODE_OVERRIDE: str | None = None
_HEX_KEY_RE = re.compile(r"[0-9a-f]{64}")
_SQLITE_CORRUPTION_HINTS = (
    "database disk image is malformed",
    "disk i/o error",
)
# SECURITY: table names must be string literals in this tuple, never dynamic.
# This tuple is the allowlist for wipe_runtime_data(). Do not construct
# table names from user input or runtime values.
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
PREFERENCE_TABLES: tuple[str, ...] = (
    "budgets",
    "goals",
    "vendor_memory",
    "manual_loans",
    "debt_balance_portions",
    "category_mappings",
    "user_strategy_preferences",
    "account_alert_rules",
    "retirement_contribution_targets",
)
RESET_KEEP_TABLES = frozenset(
    {
        "schema_version",
        "mileage_rates",
        "pl_section_map",
        "schedule_c_map",
    }
)
_AI_LOG_NEW_COLUMNS: dict[str, str] = {
    "input_tokens": "INTEGER NOT NULL DEFAULT 0",
    "output_tokens": "INTEGER NOT NULL DEFAULT 0",
    "elapsed_ms": "INTEGER NOT NULL DEFAULT 0",
}
_BOT_SESSION_COLUMNS: dict[str, str] = {
    "bot_chat_messages": "bot_session_id TEXT",
    "bot_requests": "bot_session_id TEXT",
}
_PLAID_RESET_ALLOWED = frozenset(
    {
        "sync_cursor",
        "last_sync_at",
        "last_balance_refresh_at",
        "last_liabilities_fetch_at",
        "last_investment_sync_at",
    }
)
_SYNC_CHANGELOG_TRACKED_TABLES: tuple[str, ...] = (
    "transactions",
    "categories",
    "vendor_memory",
    "budgets",
    "subscriptions",
    "goals",
    "manual_loans",
    "debt_balance_portions",
    "accounts",
    "balance_snapshots",
    "liabilities",
    "import_batches",
    "category_mappings",
    "notification_channels",
    "mileage_log",
    "contractors",
    "contractor_payments",
    "contractor_tax_prep_flags",
    "reminders",
    "user_strategy_preferences",
    "account_alert_rules",
    "spending_freeze_flags",
    "card_paydown_flags",
    "retirement_contribution_targets",
    "hysa_transfer_flags",
    "savings_automations",
    "transaction_dispute_workflows",
)
_SYNC_CHANGELOG_UPDATE_COLUMN_FILTERS: dict[str, tuple[str, ...]] = {
    "notification_channels": ("channel", "config", "label"),
}
_SQLITE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_STORAGE_SESSION_POOL_ENV = "FINANCE_CLI_STORAGE_SESSION_POOL"
_install_id_var: ContextVar[str] = ContextVar("_install_id", default="")


class MigrationError(RuntimeError):
    """Raised when a DB migration fails."""


def _storage_dispatch():
    from .storage_client import _dispatch as storage_dispatch

    return storage_dispatch


def _storage_errors():
    from .storage_client import errors as storage_errors

    return storage_errors


def _storage_connection_type():
    from .storage_client.connection import StorageConnection

    return StorageConnection


def __getattr__(name: str) -> Any:
    if name == "StorageConnection":
        value = _storage_connection_type()
        globals()[name] = value
        return value
    raise AttributeError(name)


def _is_storage_connection(conn: Any) -> bool:
    if conn is None:
        return False
    cls = conn.__class__
    if not any(
        base.__name__ == "StorageConnection"
        and str(base.__module__).startswith("finance_cli.storage_client")
        for base in cls.__mro__
    ):
        return False
    return isinstance(conn, _storage_connection_type())


class CompatRow:
    """Row wrapper that supports both tuple and mapping-style access."""

    def __init__(self, columns: tuple[str, ...], values: tuple[object, ...]) -> None:
        self._columns = columns
        self._values = values
        self._index = {name: idx for idx, name in enumerate(columns)}

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._values[self._index[key]]
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def get(self, key: str, default=None):
        index = self._index.get(key)
        if index is None:
            return default
        return self._values[index]

    def keys(self) -> tuple[str, ...]:
        return self._columns


def COMPAT_ROW_FACTORY(cursor, row) -> CompatRow:
    columns = tuple(str(description[0]) for description in cursor.description or ())
    return CompatRow(columns, tuple(row))


def _normalize_encryption_mode(raw_value: object, *, source: str) -> str:
    return normalize_db_encryption_mode(raw_value, source=source)


def set_db_encryption_mode_override(mode: str | None) -> str | None:
    """Override DB encryption mode for the current process.

    ``None`` restores env-based behavior. Entry points that intentionally own
    a local plaintext DB can call this instead of mutating
    ``FINANCE_CLI_REQUIRE_DB_ENCRYPTION`` globally.
    """
    global _DB_ENCRYPTION_MODE_OVERRIDE
    previous = _DB_ENCRYPTION_MODE_OVERRIDE
    _DB_ENCRYPTION_MODE_OVERRIDE = (
        None
        if mode is None
        else _normalize_encryption_mode(mode, source="DB_ENCRYPTION_MODE_OVERRIDE")
    )
    return previous


def db_encryption_mode() -> str:
    """Return the configured DB encryption mode."""
    if _DB_ENCRYPTION_MODE_OVERRIDE is not None:
        return _DB_ENCRYPTION_MODE_OVERRIDE
    return runtime_cli_settings().db_encryption_mode


def _resolve_connection_user_id(
    path: Path,
    *,
    user_id: str | None = None,
    expected_user_id: str | None = None,
    allow_env: bool = True,
) -> str:
    if expected_user_id is not None:
        return str(expected_user_id)
    if user_id is not None and str(user_id).strip():
        return str(user_id).strip()
    if allow_env:
        env_user_id = str(os.getenv("FINANCE_CLI_USER_ID") or "").strip()
        if env_user_id:
            return env_user_id

    from .user_provisioning import user_id_from_db_path

    return user_id_from_db_path(path)


def _resolve_dispatch_user_id(
    path: Path,
    *,
    user_id: str | None,
    expected_user_id: str | None,
) -> str | None:
    try:
        return _resolve_connection_user_id(
            path,
            user_id=user_id,
            expected_user_id=expected_user_id,
            allow_env=False,
        )
    except (RuntimeError, ValueError):
        return None


def _storage_connection_for_dispatch(
    *,
    resolved: Path,
    user_id: str | None,
    expected_user_id: str | None,
    auth_provider=None,
    channel_pool=None,
    session_manager=None,
    product: str = "finance_cli",
    storage_mode_override: str | None = None,
) -> StorageConnection | None:
    storage_dispatch = _storage_dispatch()
    target = storage_dispatch.storage_server_target()
    if not target:
        return None
    if not storage_dispatch.storage_client_enabled():
        return None

    resolved_user_id = _resolve_dispatch_user_id(
        resolved,
        user_id=user_id,
        expected_user_id=expected_user_id,
    )
    if resolved_user_id is None:
        return None

    mode = (
        str(storage_mode_override).strip().lower()
        if storage_mode_override is not None
        else storage_dispatch.storage_mode_for_user(
            resolved_user_id,
            session_manager=session_manager,
        )
    )
    if mode == "local":
        return None
    if mode in {"migrating", "replaying"}:
        storage_dispatch.invalidate_storage_mode(resolved_user_id)
        storage_errors = _storage_errors()
        reason = f"user {resolved_user_id!r} is in storage_mode={mode}"
        _evict_storage_sessions_for_user(
            target=target,
            product=product,
            user_id=resolved_user_id,
        )
        storage_errors.record_storage_client_error(
            "Connect",
            "MAINTENANCE_MODE",
            reason=reason,
        )
        raise storage_errors.MaintenanceModeError(reason)
    if mode != "remote":
        return None

    storage_connection_cls = globals().get("StorageConnection")
    if storage_connection_cls is None:
        storage_connection_cls = _storage_connection_type()
    return storage_connection_cls(
        target,
        user_id=resolved_user_id,
        product=product,
        auth_provider=auth_provider,
        channel_pool=channel_pool,
        session_pool=_storage_session_pool_if_enabled(),
    )


def _default_storage_session_manager():
    return _storage_dispatch()._default_session_manager()


def _compatible_connection_factory(
    factory: object | None,
    driver_connection_type: type,
) -> type | None:
    if factory is None:
        return None
    if isinstance(factory, type) and issubclass(factory, driver_connection_type):
        return factory
    log.warning(
        "Ignoring incompatible DB connection factory %r for driver %s",
        factory,
        driver_connection_type.__name__,
    )
    return None


def _make_lease_connection_factory(
    base_factory: object | None,
    *,
    driver_connection_type: type = sqlite3.Connection,
):
    compatible_factory = _compatible_connection_factory(base_factory, driver_connection_type)
    base = compatible_factory or driver_connection_type

    class LeasedConnection(base):  # type: ignore[misc, valid-type]
        def close(self) -> None:
            try:
                return super().close()
            finally:
                cleanup = getattr(self, "_storage_lease_cleanup", None)
                if cleanup is not None:
                    self._storage_lease_cleanup = None
                    cleanup()

        def __exit__(self, exc_type, exc, tb):
            try:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
            finally:
                self.close()
            return None

    return LeasedConnection


def _connection_factory_for_driver(
    factory: object | None,
    *,
    driver_connection_type: type,
    lease_scope: storage_lease.LeaseScope | None,
) -> type | None:
    if lease_scope is None:
        return _compatible_connection_factory(factory, driver_connection_type)
    return _make_lease_connection_factory(
        factory,
        driver_connection_type=driver_connection_type,
    )


def _attach_lease_cleanup(conn: Any, scope: storage_lease.LeaseScope | None) -> None:
    if scope is None:
        return
    try:
        conn._storage_lease_cleanup = scope.close
        if hasattr(conn, "_close_on_context_exit"):
            conn._close_on_context_exit = True
    except Exception:
        scope.close()
        raise


def _acquire_connection_lease(
    *,
    resolved: Path,
    user_id: str | None,
    expected_user_id: str | None,
    session_manager,
    product: str = "finance_cli",
) -> storage_lease.LeaseScope | None:
    if storage_lease.current_lease_scope() is not None:
        return None
    if storage_lease.lease_enforcement_enabled():
        raise storage_lease.LeaseMissingError("db.connect() requires an active storage lease")

    resolved_user_id = _resolve_dispatch_user_id(
        resolved,
        user_id=user_id,
        expected_user_id=expected_user_id,
    )
    if resolved_user_id is None:
        return None

    manager = session_manager if session_manager is not None else _default_storage_session_manager()
    if manager is None:
        return None

    try:
        result = storage_lease.acquire_or_route(
            resolved_user_id,
            session_manager=manager,
            operation="request",
            metadata={"source": "db.connect", "path": str(resolved)},
        )
    except storage_lease.LeaseUnavailableError:
        # Lease table/session routing can be unavailable in local CLI mode.
        return None
    except _PsycopgOperationalError:
        # PostgreSQL can be unreachable while local SQLite fallback remains valid.
        log.debug("storage lease auto-acquire skipped: postgres unavailable", exc_info=True)
        return None
    except AttributeError:
        # Test/dummy session managers may lack the PG session API entirely.
        log.debug("storage lease auto-acquire skipped: session manager unavailable", exc_info=True)
        return None
    except ValueError as exc:
        if "DATABASE_URL" not in str(exc):
            raise
        # The default PG session manager exists but is unconfigured in local CLI/test runs.
        log.debug("storage lease auto-acquire skipped: session manager unconfigured", exc_info=True)
        return None
    if isinstance(result, storage_lease.Queued):
        reason = f"user {resolved_user_id!r} is in storage_mode={result.storage_mode}"
        target = _storage_dispatch().storage_server_target()
        if target:
            _evict_storage_sessions_for_user(
                target=target,
                product=product,
                user_id=resolved_user_id,
            )
        storage_errors = _storage_errors()
        storage_errors.record_storage_client_error(
            "Connect",
            "MAINTENANCE_MODE",
            reason=reason,
        )
        raise storage_errors.MaintenanceModeError(reason)
    scope = storage_lease.LeaseScope(
        user_id=resolved_user_id,
        lease=result,
        session_manager=manager,
        owns_lease=True,
        heartbeat=False,
    )
    scope.__enter__()
    return scope


def _open_plaintext_connection(
    path: Path,
    *,
    check_same_thread: bool,
    factory: object | None = None,
) -> sqlite3.Connection | StorageConnection:
    connect_kwargs: dict[str, object] = {}
    if factory is not None:
        connect_kwargs["factory"] = factory
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(
            str(path),
            check_same_thread=check_same_thread,
            **connect_kwargs,
        )
        conn.row_factory = sqlite3.Row
        _apply_pragmas(conn)
        return conn
    except sqlite3.Error as exc:
        if conn is not None:
            conn.close()
        raise _rewrite_sqlite_open_error(path, exc) from exc


def _immutable_sqlite_uri(path: Path) -> str:
    return f"{path.expanduser().resolve().as_uri()}?immutable=1"


def _probe_plaintext_sqlite(path: Path, *, immutable: bool = False, integrity_check: bool = False) -> bool:
    sql = "PRAGMA integrity_check" if integrity_check else "SELECT count(*) FROM sqlite_master"
    target = _immutable_sqlite_uri(path) if immutable else str(path)
    try:
        with sqlite3.connect(target, uri=immutable) as probe_conn:
            probe_conn.execute(sql).fetchone()
        return True
    except sqlite3.Error:
        return False


def _rewrite_sqlite_open_error(path: Path, exc: sqlite3.Error) -> sqlite3.Error:
    message = str(exc).strip() or exc.__class__.__name__
    wal_path = path.with_name(f"{path.name}-wal")
    shm_path = path.with_name(f"{path.name}-shm")
    lowered = message.lower()
    if (
        wal_path.exists()
        and any(hint in lowered for hint in _SQLITE_CORRUPTION_HINTS)
        and _probe_plaintext_sqlite(path, immutable=True, integrity_check=True)
    ):
        return exc.__class__(
            "SQLite open failed because the WAL sidecar appears corrupt. "
            f"Main DB {path} is still readable without WAL. "
            f"Back up the DB, then archive/remove {wal_path} and {shm_path} "
            "to recover the checkpointed main DB; uncheckpointed WAL writes may be lost. "
            f"Original SQLite error: {message}"
        )
    return exc.__class__(f"SQLite open failed for {path}: {message}")


def _is_plaintext_sqlite(path: Path) -> bool:
    return _probe_plaintext_sqlite(path) or _probe_plaintext_sqlite(path, immutable=True)


def open_encrypted_connection(
    path: Path,
    *,
    user_id: str,
    check_same_thread: bool,
    factory: object | None = None,
    data_dir: Path | None = None,
) -> sqlite3.Connection:
    connect_kwargs: dict[str, object] = {}
    if factory is not None:
        connect_kwargs["factory"] = factory
    conn = sqlcipher.connect(
        str(path),
        check_same_thread=check_same_thread,
        **connect_kwargs,
    )
    try:
        dek = db_keys.get_user_db_key(str(user_id), data_dir=data_dir)
        hex_key = dek.hex()
        assert _HEX_KEY_RE.fullmatch(hex_key)
        conn.execute(f"PRAGMA key = \"x'{hex_key}'\"")
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        conn.row_factory = COMPAT_ROW_FACTORY
        _apply_pragmas(conn)
        return conn
    except Exception:
        conn.close()
        raise


def _db_dek_data_dir_for_path(path: Path, user_id: str) -> Path | None:
    if path.name == "finance.db" and path.parent.name == str(user_id):
        return path.parent.parent
    return None


def connect(
    db_path: Path | None = None,
    busy_timeout: int | None = None,
    check_same_thread: bool = True,
    expected_user_id: str | None = None,
    user_id: str | None = None,
    session_id: str | None = None,
    storage_auth_provider=None,
    storage_channel_pool=None,
    storage_session_manager=None,
    storage_product: str = "finance_cli",
) -> sqlite3.Connection | StorageConnection:
    resolved = (db_path or get_db_path()).expanduser().resolve()
    existing_scope = storage_lease.current_lease_scope()
    lease_scope: storage_lease.LeaseScope | None = None
    active_scope = existing_scope
    if active_scope is None:
        lease_scope = _acquire_connection_lease(
            resolved=resolved,
            user_id=user_id,
            expected_user_id=expected_user_id,
            session_manager=storage_session_manager,
            product=storage_product,
        )
        active_scope = lease_scope

    if active_scope is not None:
        resolved_user_id = _resolve_dispatch_user_id(
            resolved,
            user_id=user_id,
            expected_user_id=expected_user_id,
        )
        if resolved_user_id is not None and str(resolved_user_id) != str(active_scope.user_id):
            if lease_scope is not None:
                lease_scope.close()
            raise storage_lease.LeaseMissingError(
                f"db.connect() for user {resolved_user_id!r} is outside active lease for user {active_scope.user_id!r}"
            )

    try:
        remote_conn = _storage_connection_for_dispatch(
            resolved=resolved,
            user_id=user_id,
            expected_user_id=expected_user_id,
            auth_provider=storage_auth_provider,
            channel_pool=storage_channel_pool,
            session_manager=(
                active_scope.session_manager
                if active_scope is not None
                else storage_session_manager
            ),
            product=storage_product,
            storage_mode_override=(
                active_scope.storage_mode if active_scope is not None else None
            ),
        )
        if remote_conn is not None:
            remote_conn.row_factory = sqlite3.Row
            if hasattr(remote_conn, "_close_on_context_exit"):
                remote_conn._close_on_context_exit = True
            _attach_lease_cleanup(remote_conn, lease_scope)
            effective_session = session_id if session_id is not None else _install_id_var.get()
            remote_conn.create_function(
                "current_session_id",
                0,
                lambda: effective_session,
                deterministic=True,
            )
            return remote_conn

        if expected_user_id is not None:
            if not resolved.exists():
                raise TenantMismatchError(
                    f"DB file missing for expected user {expected_user_id!r}: {resolved}",
                    expected_user_id=expected_user_id,
                    db_path=str(resolved),
                    reason="missing_file",
                )
        else:
            resolved.parent.mkdir(parents=True, exist_ok=True)
        raw_slow_query_ms = str(os.getenv("FINANCE_CLI_SLOW_QUERY_MS", "0")).strip()
        try:
            slow_query_ms = max(int(raw_slow_query_ms), 0)
        except ValueError:
            slow_query_ms = 0
        factory: object | None = None
        if slow_query_ms > 0:
            from .perf import TimedConnection

            factory = TimedConnection

        encryption_mode = db_encryption_mode()
        conn: sqlite3.Connection
        resolved_user_id: str | None = None
        if encryption_mode == "off" and not resolved.exists():
            conn = _open_plaintext_connection(
                resolved,
                check_same_thread=check_same_thread,
                factory=_connection_factory_for_driver(
                    factory,
                    driver_connection_type=sqlite3.Connection,
                    lease_scope=lease_scope,
                ),
            )
        elif encryption_mode == "off" and _is_plaintext_sqlite(resolved):
            conn = _open_plaintext_connection(
                resolved,
                check_same_thread=check_same_thread,
                factory=_connection_factory_for_driver(
                    factory,
                    driver_connection_type=sqlite3.Connection,
                    lease_scope=lease_scope,
                ),
            )
        else:
            try:
                resolved_user_id = _resolve_connection_user_id(
                    resolved,
                    user_id=user_id,
                    expected_user_id=expected_user_id,
                )
                conn = open_encrypted_connection(
                    resolved,
                    user_id=resolved_user_id,
                    check_same_thread=check_same_thread,
                    data_dir=_db_dek_data_dir_for_path(resolved, resolved_user_id),
                    factory=_connection_factory_for_driver(
                        factory,
                        driver_connection_type=_SQLCIPHER_CONNECTION_TYPE,
                        lease_scope=lease_scope,
                    ),
                )
            except (RuntimeError, ValueError) as exc:
                if encryption_mode != "off":
                    raise
                log.info(
                    "DB encryption disabled fallback path=%s reason=%s",
                    resolved,
                    exc,
                )
                conn = _open_plaintext_connection(
                    resolved,
                    check_same_thread=check_same_thread,
                    factory=factory,
                )
            except (sqlite3.DatabaseError, sqlcipher.DatabaseError) as exc:
                crypto_failure = TenantMismatchError(
                    f"Failed decrypting DB for user {resolved_user_id!r}: {resolved}",
                    expected_user_id=resolved_user_id,
                    db_path=str(resolved),
                    reason="crypto_failure",
                )
                if encryption_mode != "off":
                    raise crypto_failure from exc
                log.info(
                    "DB encryption disabled fallback path=%s reason=%s",
                    resolved,
                    crypto_failure.reason,
                )
                conn = _open_plaintext_connection(
                    resolved,
                    check_same_thread=check_same_thread,
                    factory=factory,
                )
        _attach_lease_cleanup(conn, lease_scope)
        if busy_timeout is not None:
            conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout)}")
        if expected_user_id is not None:
            try:
                _verify_tenant_marker(conn, expected_user_id, resolved)
            except TenantMismatchError:
                conn.close()
                raise
        effective_session = session_id if session_id is not None else _install_id_var.get()
        conn.create_function(
            "current_session_id",
            0,
            lambda: effective_session,
            deterministic=True,
        )
        return conn
    except Exception:
        if lease_scope is not None:
            lease_scope.close()
        raise


def _verify_tenant_marker(
    conn: sqlite3.Connection,
    expected_user_id: str,
    db_path: Path,
) -> None:
    try:
        row = conn.execute(
            "SELECT user_id FROM tenant_marker WHERE singleton = 1"
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise TenantMismatchError(
            f"DB missing tenant marker table for expected user {expected_user_id!r}: {db_path}",
            expected_user_id=expected_user_id,
            db_path=str(db_path),
            reason="missing_table",
        ) from exc

    if row is None:
        raise TenantMismatchError(
            f"DB missing tenant marker row for expected user {expected_user_id!r}: {db_path}",
            expected_user_id=expected_user_id,
            db_path=str(db_path),
            reason="missing_row",
        )

    actual_user_id = str(row[0])
    if actual_user_id != str(expected_user_id):
        raise TenantMismatchError(
            f"DB tenant marker {actual_user_id!r} does not match expected user {expected_user_id!r}: {db_path}",
            expected_user_id=str(expected_user_id),
            actual_user_id=actual_user_id,
            db_path=str(db_path),
            reason="mismatch",
        )


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")


def _storage_session_pool_if_enabled():
    value = str(os.getenv(_STORAGE_SESSION_POOL_ENV, "disabled")).strip().lower()
    if value != "enabled":
        return None
    from .storage_client import session_pool as storage_session_pool

    return storage_session_pool.get_default_pool()


def _evict_storage_sessions_for_user(
    *,
    target: str,
    product: str,
    user_id: str,
    reason: str = "maintenance_mode",
) -> None:
    pool = _storage_session_pool_if_enabled()
    if pool is None:
        return
    count = pool.evict_user(target, product, user_id)
    try:
        _storage_errors().record_storage_session_pool_event(
            "session_pool_evict",
            user_id=str(user_id),
            reason=reason,
            count=count,
            pool_size=pool.size(),
        )
    except Exception:
        pass


def initialize_database(
    db_path: Path | None = None,
    *,
    expected_user_id: str | None = None,
    storage_session_manager=None,
) -> None:
    with connect(
        db_path,
        expected_user_id=expected_user_id,
        storage_session_manager=storage_session_manager,
    ) as conn:
        initialize_connection(conn)


def initialize_connection(
    conn: sqlite3.Connection,
    *,
    create_migration_backup: bool = True,
) -> None:
    """Initialize schema objects on an already-open sqlite-compatible connection."""
    _ensure_schema_version_table(conn)
    _run_pending_migrations(conn, create_migration_backup=create_migration_backup)


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


@contextmanager
def _migration_file_lock(conn: sqlite3.Connection):
    if _fcntl is None or _is_storage_connection(conn):
        yield
        return

    db_path = _connected_main_db_path(conn)
    if db_path is None:
        yield
        return

    lock_path = db_path.with_name(f"{db_path.name}.migration.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_file:
        _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_EX)
        try:
            yield
        finally:
            _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_UN)


def _run_pending_migrations(
    conn: sqlite3.Connection,
    *,
    create_migration_backup: bool = True,
) -> None:
    with _migration_file_lock(conn):
        _run_pending_migrations_locked(
            conn,
            create_migration_backup=create_migration_backup,
        )


def _run_pending_migrations_locked(
    conn: sqlite3.Connection,
    *,
    create_migration_backup: bool = True,
) -> None:
    applied = {
        int(row["version"])
        for row in conn.execute("SELECT version FROM schema_version").fetchall()
    }
    migration_backup_created = False

    def _raise_migration_error(exc: Exception, message: str, migration_name: str) -> None:
        from .error_capture import capture_error

        capture_error(exc, source="startup", endpoint=Path(migration_name).stem)
        migration_error = MigrationError(message)
        try:
            setattr(migration_error, "_b3_captured", True)
        except Exception:
            pass
        raise migration_error from exc

    for version, path in _list_migrations():
        if version in applied:
            continue

        if create_migration_backup and version >= 15 and not migration_backup_created:
            db_path = _connected_main_db_path(conn)
            if db_path and db_path.exists():
                try:
                    backup_database(conn=conn, db_path=db_path)
                except Exception as exc:
                    _raise_migration_error(
                        exc,
                        f"Failed creating pre-migration backup before {path.name}: {exc}",
                        path.name,
                    )
            migration_backup_created = True

        sql = path.read_text(encoding="utf-8")
        # Keep the version insert in the same script to minimize the window
        # where migration SQL succeeds but the schema_version insert does not.
        escaped_desc = path.name.replace("'", "''")
        sql_with_version = (
            sql.rstrip().rstrip(";")
            + f";\nINSERT INTO schema_version (version, description) VALUES ({int(version)}, '{escaped_desc}');\n"
        )
        try:
            if _apply_special_migration(conn, version, path.name):
                continue
            conn.executescript(sql_with_version)
        except sqlite3.Error as exc:
            conn.rollback()
            _raise_migration_error(
                exc,
                f"Failed applying migration {path.name}: {exc}",
                path.name,
            )

    _repair_special_migration_invariants(conn)


def _apply_special_migration(conn: sqlite3.Connection, version: int, description: str) -> bool:
    if version == 33:
        _apply_ai_log_token_tracking(conn)
    elif version == 34:
        _apply_bot_sessions(conn)
    elif version == 44:
        _apply_cost_backfill(conn)
    elif version == 51:
        _apply_scrub_card_ending_migration(conn, description)
    elif version == 56:
        _apply_sync_changelog_migration(conn)
    elif version == 58 and _change_feed_requires_compat_path(conn):
        _apply_change_feed_migration_compat(conn)
    elif version == 61:
        _apply_cost_ledger_downstream_sync_migration(conn)
    elif version == 65:
        _apply_liability_intro_apr_end_date_migration(conn)
    elif version == 66:
        _apply_reminders_migration(conn)
    elif version == 68:
        _apply_user_strategy_preferences_migration(conn)
    elif version == 69:
        _apply_account_alert_rules_migration(conn)
    elif version == 70:
        _apply_contractor_tax_prep_flags_migration(conn)
    elif version == 71:
        _apply_spending_freeze_flags_migration(conn)
    elif version == 72:
        _apply_card_paydown_flags_migration(conn)
    elif version == 73:
        _apply_retirement_contribution_targets_migration(conn)
    elif version == 74:
        _apply_hysa_transfer_flags_migration(conn)
    elif version == 75:
        _apply_savings_automations_migration(conn)
    elif version == 76:
        _apply_transaction_dispute_workflows_migration(conn)
    elif version == 78:
        _apply_plaid_consent_expiration_migration(conn)
    elif version == 79:
        _apply_debt_balance_portions_migration(conn)
    else:
        return False

    conn.execute(
        "INSERT INTO schema_version (version, description) VALUES (?, ?)",
        (version, description),
    )
    return True


def _repair_special_migration_invariants(conn: sqlite3.Connection) -> None:
    """Repair idempotent special migrations whose version row already exists.

    Some migrations are implemented in Python so they can create sync triggers.
    If a DB restore or prior failed run leaves the schema_version row present but
    the actual table absent, the normal pending-migration loop will skip them and
    runtime code will fail later. These appliers are CREATE-IF-NOT-EXISTS and
    trigger-recreate safe, so rerunning them is the least surprising repair.
    """

    applied = {
        int(row["version"])
        for row in conn.execute("SELECT version FROM schema_version").fetchall()
    }
    for version, (table_name, apply_migration) in _SPECIAL_MIGRATION_INVARIANTS.items():
        if version in applied and not _table_exists(conn, table_name):
            apply_migration(conn)

    if 78 in applied and _table_exists(conn, "plaid_items"):
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(plaid_items)").fetchall()
        }
        if "consent_expiration_time" not in columns:
            _apply_plaid_consent_expiration_migration(conn)


def _apply_scrub_card_ending_migration(conn: sqlite3.Connection, description: str) -> None:
    tables = {
        str(row["name"])
        for row in conn.execute(
            """
            SELECT name
              FROM sqlite_master
             WHERE type = 'table'
            """
        ).fetchall()
    }
    if "accounts" not in tables or "account_aliases" not in tables:
        return

    migration_path = _migration_dir() / description
    conn.executescript(migration_path.read_text(encoding="utf-8"))


def _validate_sqlite_identifier(identifier: str) -> str:
    if not _SQLITE_IDENT_RE.fullmatch(identifier):
        raise ValueError(f"Unsafe SQLite identifier: {identifier!r}")
    return identifier


def _quote_sqlite_identifier(identifier: str) -> str:
    if "\x00" in identifier:
        raise ValueError(f"Unsafe SQLite identifier: {identifier!r}")
    return '"' + identifier.replace('"', '""') + '"'


def _sync_changelog_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    table_name = _validate_sqlite_identifier(table_name)
    rows = conn.execute(
        f"PRAGMA table_info({_quote_sqlite_identifier(table_name)})"
    ).fetchall()
    return [_validate_sqlite_identifier(str(row["name"])) for row in rows]


def _sync_changelog_pk_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    table_name = _validate_sqlite_identifier(table_name)
    rows = conn.execute(
        f"PRAGMA table_info({_quote_sqlite_identifier(table_name)})"
    ).fetchall()
    pk_rows = sorted(
        (row for row in rows if int(row["pk"] or 0) > 0),
        key=lambda row: int(row["pk"]),
    )
    return [_validate_sqlite_identifier(str(row["name"])) for row in pk_rows]


def _sync_changelog_json_object(
    columns: list[str],
    row_prefix: str,
    *,
    overrides: dict[str, str] | None = None,
) -> str:
    pairs: list[str] = []
    for column in columns:
        expr = (overrides or {}).get(column, f"{row_prefix}.{column}")
        pairs.append(f"'{column}'")
        pairs.append(expr)
    return "json(json_object(" + ", ".join(pairs) + "))"


def _sync_changelog_has_origin_session_id(conn: sqlite3.Connection) -> bool:
    rows = conn.execute("PRAGMA table_info(_sync_changelog)").fetchall()
    return any(str(row["name"]) == "origin_session_id" for row in rows)


def _create_sync_changelog_trigger_set(conn: sqlite3.Connection, table_name: str) -> None:
    table_name = _validate_sqlite_identifier(table_name)
    if not _table_exists(conn, table_name):
        return

    quoted_table_name = _quote_sqlite_identifier(table_name)
    has_origin_session_id = _sync_changelog_has_origin_session_id(conn)
    columns = _sync_changelog_columns(conn, table_name)
    pk_columns = _sync_changelog_pk_columns(conn, table_name)
    if not columns or not pk_columns:
        return

    pk_new = _sync_changelog_json_object(pk_columns, "NEW")
    pk_old = _sync_changelog_json_object(pk_columns, "OLD")
    old_json = _sync_changelog_json_object(columns, "OLD")
    insert_new_json = _sync_changelog_json_object(columns, "NEW")
    update_new_overrides: dict[str, str] | None = None
    update_target = f"AFTER UPDATE ON {quoted_table_name}"
    if table_name == "notification_channels":
        filtered_columns = _SYNC_CHANGELOG_UPDATE_COLUMN_FILTERS[table_name]
        filtered_columns_sql = ", ".join(
            _quote_sqlite_identifier(_validate_sqlite_identifier(column))
            for column in filtered_columns
        )
        update_target = f"AFTER UPDATE OF {filtered_columns_sql} ON {quoted_table_name}"
        update_new_overrides = {
            "updated_at": (
                "CASE WHEN NEW.updated_at = OLD.updated_at "
                "THEN datetime('now') ELSE NEW.updated_at END"
            ),
        }
    update_new_json = _sync_changelog_json_object(
        columns,
        "NEW",
        overrides=update_new_overrides,
    )
    changelog_columns = "table_name, op, pk_json, old_json, new_json"
    changelog_values_suffix = ""
    when_clause = ""
    if has_origin_session_id:
        changelog_columns += ", origin_session_id"
        changelog_values_suffix = ", current_session_id()"
        when_clause = "\n        WHEN current_session_id() != '__STREAM__'"
    insert_trigger = _quote_sqlite_identifier(f"_sync_log_{table_name}_insert")
    update_trigger = _quote_sqlite_identifier(f"_sync_log_{table_name}_update")
    delete_trigger = _quote_sqlite_identifier(f"_sync_log_{table_name}_delete")

    conn.executescript(
        f"""
        CREATE TRIGGER IF NOT EXISTS {insert_trigger}
        AFTER INSERT ON {quoted_table_name}
        FOR EACH ROW
        {when_clause}
        BEGIN
            INSERT INTO _sync_changelog ({changelog_columns})
            VALUES ('{table_name}', 'INSERT', {pk_new}, NULL, {insert_new_json}{changelog_values_suffix});
        END;

        CREATE TRIGGER IF NOT EXISTS {update_trigger}
        {update_target}
        FOR EACH ROW
        {when_clause}
        BEGIN
            INSERT INTO _sync_changelog ({changelog_columns})
            VALUES ('{table_name}', 'UPDATE', {pk_new}, {old_json}, {update_new_json}{changelog_values_suffix});
        END;

        CREATE TRIGGER IF NOT EXISTS {delete_trigger}
        AFTER DELETE ON {quoted_table_name}
        FOR EACH ROW
        {when_clause}
        BEGIN
            INSERT INTO _sync_changelog ({changelog_columns})
            VALUES ('{table_name}', 'DELETE', {pk_old}, {old_json}, NULL{changelog_values_suffix});
        END;
        """
    )


def _apply_sync_changelog_migration(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _sync_changelog (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            op         TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')),
            pk_json    TEXT NOT NULL CHECK (json_valid(pk_json)),
            old_json   TEXT CHECK (old_json IS NULL OR json_valid(old_json)),
            new_json   TEXT CHECK (new_json IS NULL OR json_valid(new_json)),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    for table_name in _SYNC_CHANGELOG_TRACKED_TABLES:
        _create_sync_changelog_trigger_set(conn, table_name)


def _change_feed_requires_compat_path(conn: sqlite3.Connection) -> bool:
    from .sync_protocol import CHANGELOG_TABLES

    for table_name in CHANGELOG_TABLES:
        if table_name == "_meta_state":
            continue
        if not _table_exists(conn, table_name):
            return True
    return False


def _apply_change_feed_migration_compat(conn: sqlite3.Connection) -> None:
    from .sync_protocol import CHANGELOG_TABLES

    if not _sync_changelog_has_origin_session_id(conn):
        conn.execute(
            "ALTER TABLE _sync_changelog ADD COLUMN origin_session_id TEXT NOT NULL DEFAULT ''"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _meta_state (
            key TEXT PRIMARY KEY,
            sha256 TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 0),
            last_applied_op_id INTEGER NOT NULL DEFAULT 0,
            install_id TEXT NOT NULL DEFAULT '',
            subscriber_status TEXT NOT NULL DEFAULT 'healthy'
                CHECK (subscriber_status IN ('healthy','degraded','bootstrapping'))
        )
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO sync_state (id, last_applied_op_id, install_id) VALUES (0, 0, '')"
    )
    for table_name in sorted(CHANGELOG_TABLES):
        for op in ("insert", "update", "delete"):
            trigger_name = _quote_sqlite_identifier(
                f"_sync_log_{_validate_sqlite_identifier(table_name)}_{op}"
            )
            conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    for table_name in sorted(CHANGELOG_TABLES):
        _create_sync_changelog_trigger_set(conn, table_name)


def _apply_cost_ledger_downstream_sync_migration(conn: sqlite3.Connection) -> None:
    for op in ("insert", "update", "delete"):
        trigger_name = _quote_sqlite_identifier(f"_sync_log_cost_ledger_{op}")
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    _create_sync_changelog_trigger_set(conn, "cost_ledger")


def _apply_reminders_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS reminders (
            id                TEXT PRIMARY KEY,
            kind              TEXT NOT NULL,
            title             TEXT NOT NULL,
            body              TEXT NOT NULL,
            due_at            TEXT NOT NULL,
            channel           TEXT CHECK (channel IS NULL OR channel IN ('telegram', 'imessage')),
            status            TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'sent', 'cancelled', 'failed')),
            payload_json      TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(payload_json)),
            idempotency_key   TEXT UNIQUE,
            sent_at           TEXT,
            cancelled_at      TEXT,
            last_error        TEXT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_reminders_status_due
            ON reminders(status, due_at);

        CREATE INDEX IF NOT EXISTS idx_reminders_kind
            ON reminders(kind);
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_reminders_{op}"')
    _create_sync_changelog_trigger_set(conn, "reminders")


def _apply_user_strategy_preferences_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS user_strategy_preferences (
            domain        TEXT PRIMARY KEY CHECK (length(domain) > 0),
            strategy      TEXT NOT NULL CHECK (length(strategy) > 0),
            rationale     TEXT,
            source        TEXT NOT NULL DEFAULT 'user'
                          CHECK (source IN ('user', 'agent', 'inferred', 'artifact', 'system')),
            evidence_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(evidence_json)),
            created_at    TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_user_strategy_preferences_source
            ON user_strategy_preferences(source, updated_at);

        CREATE TRIGGER IF NOT EXISTS user_strategy_preferences_touch_updated_at
        AFTER UPDATE ON user_strategy_preferences
        FOR EACH ROW
        BEGIN
            UPDATE user_strategy_preferences
               SET updated_at = datetime('now')
             WHERE domain = NEW.domain;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_user_strategy_preferences_{op}"')
    _create_sync_changelog_trigger_set(conn, "user_strategy_preferences")


def _apply_account_alert_rules_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS account_alert_rules (
            id                TEXT PRIMARY KEY,
            rule_type         TEXT NOT NULL CHECK (rule_type IN ('low_balance')),
            account_id        TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            threshold_cents   INTEGER NOT NULL CHECK (threshold_cents > 0),
            channel           TEXT CHECK (channel IS NULL OR channel IN ('telegram', 'imessage')),
            label             TEXT,
            status            TEXT NOT NULL DEFAULT 'active'
                              CHECK (status IN ('active', 'paused', 'cancelled')),
            cooldown_hours    INTEGER NOT NULL DEFAULT 24 CHECK (cooldown_hours BETWEEN 1 AND 720),
            last_triggered_at TEXT,
            last_error        TEXT,
            payload_json      TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(payload_json)),
            idempotency_key   TEXT NOT NULL UNIQUE,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_account_alert_rules_status
            ON account_alert_rules(status, rule_type);

        CREATE INDEX IF NOT EXISTS idx_account_alert_rules_account
            ON account_alert_rules(account_id);

        CREATE TRIGGER IF NOT EXISTS account_alert_rules_touch_updated_at
        AFTER UPDATE ON account_alert_rules
        FOR EACH ROW
        BEGIN
            UPDATE account_alert_rules
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_account_alert_rules_{op}"')
    _create_sync_changelog_trigger_set(conn, "account_alert_rules")


def _apply_contractor_tax_prep_flags_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS contractor_tax_prep_flags (
            id                    TEXT PRIMARY KEY,
            contractor_id         TEXT NOT NULL REFERENCES contractors(id) ON DELETE CASCADE,
            tax_year              INTEGER NOT NULL CHECK (tax_year BETWEEN 2000 AND 2100),
            flag_type             TEXT NOT NULL DEFAULT 'january_1099_prep'
                                  CHECK (flag_type IN ('january_1099_prep')),
            status                TEXT NOT NULL DEFAULT 'active'
                                  CHECK (status IN ('active', 'resolved', 'cancelled')),
            reason                TEXT,
            source                TEXT NOT NULL DEFAULT 'agent'
                                  CHECK (source IN ('user', 'agent', 'system')),
            payment_snapshot_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(payment_snapshot_json)),
            resolved_at           TEXT,
            created_at            TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at            TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(contractor_id, tax_year, flag_type)
        );

        CREATE INDEX IF NOT EXISTS idx_contractor_tax_prep_flags_status
            ON contractor_tax_prep_flags(status, tax_year);

        CREATE INDEX IF NOT EXISTS idx_contractor_tax_prep_flags_contractor
            ON contractor_tax_prep_flags(contractor_id, tax_year);

        CREATE TRIGGER IF NOT EXISTS contractor_tax_prep_flags_touch_updated_at
        AFTER UPDATE ON contractor_tax_prep_flags
        FOR EACH ROW
        BEGIN
            UPDATE contractor_tax_prep_flags
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_contractor_tax_prep_flags_{op}"')
    _create_sync_changelog_trigger_set(conn, "contractor_tax_prep_flags")


def _apply_spending_freeze_flags_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS spending_freeze_flags (
            id                         TEXT PRIMARY KEY,
            scope                      TEXT NOT NULL DEFAULT 'discretionary'
                                       CHECK (scope IN ('discretionary', 'all_nonessential', 'category', 'account')),
            status                     TEXT NOT NULL DEFAULT 'active'
                                       CHECK (status IN ('active', 'resolved', 'cancelled')),
            account_id                 TEXT REFERENCES accounts(id) ON DELETE SET NULL,
            category_id                TEXT REFERENCES categories(id) ON DELETE SET NULL,
            reason                     TEXT NOT NULL CHECK (length(reason) > 0),
            bill_name                  TEXT,
            bill_amount_cents          INTEGER CHECK (bill_amount_cents IS NULL OR bill_amount_cents >= 0),
            due_date                   TEXT,
            hold_until                 TEXT NOT NULL,
            target_balance_after_cents INTEGER,
            source                     TEXT NOT NULL DEFAULT 'agent'
                                       CHECK (source IN ('user', 'agent', 'system')),
            payload_json               TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(payload_json)),
            idempotency_key            TEXT NOT NULL UNIQUE,
            resolved_at                TEXT,
            created_at                 TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at                 TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_spending_freeze_flags_status
            ON spending_freeze_flags(status, hold_until);

        CREATE INDEX IF NOT EXISTS idx_spending_freeze_flags_scope
            ON spending_freeze_flags(scope, account_id, category_id);

        CREATE TRIGGER IF NOT EXISTS spending_freeze_flags_touch_updated_at
        AFTER UPDATE ON spending_freeze_flags
        FOR EACH ROW
        BEGIN
            UPDATE spending_freeze_flags
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_spending_freeze_flags_{op}"')
    _create_sync_changelog_trigger_set(conn, "spending_freeze_flags")


def _apply_card_paydown_flags_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS card_paydown_flags (
            id                          TEXT PRIMARY KEY,
            account_id                  TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            liability_id                TEXT REFERENCES liabilities(id) ON DELETE SET NULL,
            status                      TEXT NOT NULL DEFAULT 'active'
                                        CHECK (status IN ('active', 'resolved', 'cancelled')),
            reason                      TEXT,
            suggested_payment_cents     INTEGER NOT NULL DEFAULT 0 CHECK (suggested_payment_cents >= 0),
            cash_source_account_id      TEXT REFERENCES accounts(id) ON DELETE SET NULL,
            interest_saved_annual_cents INTEGER,
            source                      TEXT NOT NULL DEFAULT 'agent'
                                        CHECK (source IN ('user', 'agent', 'system')),
            snapshot_json               TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(snapshot_json)),
            idempotency_key             TEXT NOT NULL UNIQUE,
            resolved_at                 TEXT,
            created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at                  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_card_paydown_flags_status
            ON card_paydown_flags(status, updated_at);

        CREATE INDEX IF NOT EXISTS idx_card_paydown_flags_account
            ON card_paydown_flags(account_id);

        CREATE TRIGGER IF NOT EXISTS card_paydown_flags_touch_updated_at
        AFTER UPDATE ON card_paydown_flags
        FOR EACH ROW
        BEGIN
            UPDATE card_paydown_flags
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_card_paydown_flags_{op}"')
    _create_sync_changelog_trigger_set(conn, "card_paydown_flags")


def _apply_retirement_contribution_targets_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS retirement_contribution_targets (
            id                          TEXT PRIMARY KEY,
            tax_year                    INTEGER NOT NULL CHECK (tax_year BETWEEN 2000 AND 2100),
            account_type                TEXT NOT NULL
                                        CHECK (account_type IN (
                                            'roth_ira',
                                            'traditional_ira',
                                            'sep_ira',
                                            'solo_401k',
                                            'employer_401k',
                                            'other_retirement'
                                        )),
            status                      TEXT NOT NULL DEFAULT 'active'
                                        CHECK (status IN ('active', 'resolved', 'cancelled')),
            monthly_target_cents        INTEGER NOT NULL CHECK (monthly_target_cents > 0),
            start_month                 TEXT NOT NULL CHECK (length(start_month) = 7),
            end_month                   TEXT NOT NULL CHECK (length(end_month) = 7),
            room_remaining_cents        INTEGER CHECK (room_remaining_cents IS NULL OR room_remaining_cents >= 0),
            annual_limit_cents          INTEGER CHECK (annual_limit_cents IS NULL OR annual_limit_cents >= 0),
            contributed_ytd_cents       INTEGER CHECK (contributed_ytd_cents IS NULL OR contributed_ytd_cents >= 0),
            estimated_tax_savings_cents INTEGER CHECK (
                                            estimated_tax_savings_cents IS NULL
                                            OR estimated_tax_savings_cents >= 0
                                        ),
            deadline                    TEXT,
            reason                      TEXT,
            source                      TEXT NOT NULL DEFAULT 'agent'
                                        CHECK (source IN ('user', 'agent', 'system')),
            payload_json                TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(payload_json)),
            idempotency_key             TEXT NOT NULL UNIQUE,
            resolved_at                 TEXT,
            created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at                  TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (start_month <= end_month)
        );

        CREATE INDEX IF NOT EXISTS idx_retirement_contribution_targets_status
            ON retirement_contribution_targets(status, tax_year);

        CREATE INDEX IF NOT EXISTS idx_retirement_contribution_targets_account
            ON retirement_contribution_targets(account_type, tax_year);

        CREATE TRIGGER IF NOT EXISTS retirement_contribution_targets_touch_updated_at
        AFTER UPDATE ON retirement_contribution_targets
        FOR EACH ROW
        BEGIN
            UPDATE retirement_contribution_targets
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_retirement_contribution_targets_{op}"')
    _create_sync_changelog_trigger_set(conn, "retirement_contribution_targets")


def _apply_hysa_transfer_flags_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS hysa_transfer_flags (
            id                           TEXT PRIMARY KEY,
            account_id                   TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            status                       TEXT NOT NULL DEFAULT 'active'
                                         CHECK (status IN ('active', 'resolved', 'cancelled')),
            current_balance_cents        INTEGER NOT NULL CHECK (current_balance_cents >= 0),
            suggested_transfer_cents     INTEGER NOT NULL CHECK (suggested_transfer_cents > 0),
            retained_buffer_cents        INTEGER NOT NULL DEFAULT 0 CHECK (retained_buffer_cents >= 0),
            minimum_balance_cents        INTEGER NOT NULL DEFAULT 200000 CHECK (minimum_balance_cents > 0),
            current_apy_bps              INTEGER NOT NULL DEFAULT 0 CHECK (current_apy_bps >= 0),
            hysa_apy_bps                 INTEGER NOT NULL CHECK (hysa_apy_bps > 0),
            estimated_annual_yield_cents INTEGER NOT NULL CHECK (estimated_annual_yield_cents >= 0),
            observed_since               TEXT NOT NULL,
            lookback_days                INTEGER NOT NULL DEFAULT 90 CHECK (lookback_days BETWEEN 1 AND 3650),
            reason                       TEXT,
            source                       TEXT NOT NULL DEFAULT 'agent'
                                         CHECK (source IN ('user', 'agent', 'system')),
            snapshot_json                TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(snapshot_json)),
            idempotency_key              TEXT NOT NULL UNIQUE,
            resolved_at                  TEXT,
            created_at                   TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at                   TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (hysa_apy_bps > current_apy_bps)
        );

        CREATE INDEX IF NOT EXISTS idx_hysa_transfer_flags_status
            ON hysa_transfer_flags(status);

        CREATE INDEX IF NOT EXISTS idx_hysa_transfer_flags_account
            ON hysa_transfer_flags(account_id);

        CREATE TRIGGER IF NOT EXISTS hysa_transfer_flags_touch_updated_at
        AFTER UPDATE ON hysa_transfer_flags
        FOR EACH ROW
        BEGIN
            UPDATE hysa_transfer_flags
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_hysa_transfer_flags_{op}"')
    _create_sync_changelog_trigger_set(conn, "hysa_transfer_flags")


def _apply_savings_automations_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS savings_automations (
            id                          TEXT PRIMARY KEY,
            goal_id                     TEXT NOT NULL REFERENCES goals(id) ON DELETE CASCADE,
            status                      TEXT NOT NULL DEFAULT 'active'
                                        CHECK (status IN ('active', 'paused', 'cancelled')),
            funding_method              TEXT NOT NULL
                                        CHECK (funding_method IN (
                                            'auto_transfer',
                                            'paycheck_split',
                                            'percentage_of_paycheck',
                                            'windfall_capture',
                                            'hybrid'
                                        )),
            cadence                     TEXT NOT NULL
                                        CHECK (cadence IN ('weekly', 'biweekly', 'monthly', 'paycheck')),
            amount_cents                INTEGER NOT NULL CHECK (amount_cents > 0),
            start_date                  TEXT NOT NULL,
            day_of_month                INTEGER CHECK (day_of_month IS NULL OR day_of_month BETWEEN 1 AND 31),
            source_account_id           TEXT REFERENCES accounts(id) ON DELETE SET NULL,
            destination_account_id      TEXT REFERENCES accounts(id) ON DELETE SET NULL,
            target_amount_cents         INTEGER CHECK (target_amount_cents IS NULL OR target_amount_cents >= 0),
            projected_end_balance_cents INTEGER CHECK (
                                            projected_end_balance_cents IS NULL
                                            OR projected_end_balance_cents >= 0
                                        ),
            goal_date                   TEXT,
            reason                      TEXT,
            source                      TEXT NOT NULL DEFAULT 'agent'
                                        CHECK (source IN ('user', 'agent', 'system')),
            snapshot_json               TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(snapshot_json)),
            idempotency_key             TEXT NOT NULL UNIQUE,
            created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at                  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_savings_automations_status
            ON savings_automations(status);

        CREATE INDEX IF NOT EXISTS idx_savings_automations_goal
            ON savings_automations(goal_id);

        CREATE TRIGGER IF NOT EXISTS savings_automations_touch_updated_at
        AFTER UPDATE ON savings_automations
        FOR EACH ROW
        BEGIN
            UPDATE savings_automations
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_savings_automations_{op}"')
    _create_sync_changelog_trigger_set(conn, "savings_automations")


def _apply_transaction_dispute_workflows_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS transaction_dispute_workflows (
            id                       TEXT PRIMARY KEY,
            transaction_id           TEXT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
            duplicate_transaction_id TEXT REFERENCES transactions(id) ON DELETE SET NULL,
            account_id               TEXT REFERENCES accounts(id) ON DELETE SET NULL,
            status                   TEXT NOT NULL DEFAULT 'active'
                                     CHECK (status IN ('active', 'submitted', 'resolved', 'cancelled')),
            dispute_reason           TEXT NOT NULL
                                     CHECK (dispute_reason IN (
                                         'duplicate_charge',
                                         'unrecognized_merchant',
                                         'incorrect_amount',
                                         'unauthorized',
                                         'other'
                                     )),
            amount_cents             INTEGER NOT NULL CHECK (amount_cents > 0),
            merchant_name            TEXT NOT NULL,
            transaction_date         TEXT NOT NULL,
            duplicate_date           TEXT,
            note                     TEXT,
            source                   TEXT NOT NULL DEFAULT 'agent'
                                     CHECK (source IN ('user', 'agent', 'system')),
            snapshot_json            TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(snapshot_json)),
            idempotency_key          TEXT NOT NULL UNIQUE,
            created_at               TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at               TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_transaction_dispute_workflows_status
            ON transaction_dispute_workflows(status);

        CREATE INDEX IF NOT EXISTS idx_transaction_dispute_workflows_transaction
            ON transaction_dispute_workflows(transaction_id);

        CREATE TRIGGER IF NOT EXISTS transaction_dispute_workflows_touch_updated_at
        AFTER UPDATE ON transaction_dispute_workflows
        FOR EACH ROW
        BEGIN
            UPDATE transaction_dispute_workflows
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_transaction_dispute_workflows_{op}"')
    _create_sync_changelog_trigger_set(conn, "transaction_dispute_workflows")


def _apply_debt_balance_portions_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS debt_balance_portions (
            id                    TEXT PRIMARY KEY,
            account_id            TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            label                 TEXT NOT NULL,
            portion_type          TEXT NOT NULL DEFAULT 'installment'
                                  CHECK (portion_type IN (
                                      'purchase',
                                      'installment',
                                      'balance_transfer',
                                      'cash_advance',
                                      'promotional',
                                      'fee',
                                      'other'
                                  )),
            principal_cents       INTEGER NOT NULL CHECK (principal_cents >= 0),
            apr_pct               REAL CHECK (apr_pct IS NULL OR apr_pct >= 0),
            monthly_payment_cents INTEGER CHECK (
                                      monthly_payment_cents IS NULL
                                      OR monthly_payment_cents >= 0
                                  ),
            start_date            TEXT,
            promo_end_date        TEXT,
            source                TEXT NOT NULL DEFAULT 'manual'
                                  CHECK (source IN ('manual', 'statement', 'import', 'agent')),
            is_active             INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
            notes                 TEXT,
            created_at            TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at            TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_debt_balance_portions_account_active
            ON debt_balance_portions(account_id, is_active);

        CREATE INDEX IF NOT EXISTS idx_debt_balance_portions_active_updated
            ON debt_balance_portions(is_active, updated_at);

        CREATE TRIGGER IF NOT EXISTS debt_balance_portions_touch_updated_at
        AFTER UPDATE ON debt_balance_portions
        FOR EACH ROW
        BEGIN
            UPDATE debt_balance_portions
               SET updated_at = datetime('now')
             WHERE id = NEW.id;
        END;
        """
    )
    if not _table_exists(conn, "_sync_changelog"):
        return
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_debt_balance_portions_{op}"')
    _create_sync_changelog_trigger_set(conn, "debt_balance_portions")


_SPECIAL_MIGRATION_INVARIANTS = {
    68: ("user_strategy_preferences", _apply_user_strategy_preferences_migration),
    69: ("account_alert_rules", _apply_account_alert_rules_migration),
    70: ("contractor_tax_prep_flags", _apply_contractor_tax_prep_flags_migration),
    71: ("spending_freeze_flags", _apply_spending_freeze_flags_migration),
    72: ("card_paydown_flags", _apply_card_paydown_flags_migration),
    73: (
        "retirement_contribution_targets",
        _apply_retirement_contribution_targets_migration,
    ),
    74: ("hysa_transfer_flags", _apply_hysa_transfer_flags_migration),
    75: ("savings_automations", _apply_savings_automations_migration),
    76: ("transaction_dispute_workflows", _apply_transaction_dispute_workflows_migration),
    79: ("debt_balance_portions", _apply_debt_balance_portions_migration),
}


def _apply_liability_intro_apr_end_date_migration(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "liabilities"):
        return

    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(liabilities)").fetchall()
    }
    if "intro_apr_end_date" not in columns:
        conn.execute("ALTER TABLE liabilities ADD COLUMN intro_apr_end_date TEXT")

    if not _table_exists(conn, "_sync_changelog"):
        return
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_liabilities_{op}"')
    _create_sync_changelog_trigger_set(conn, "liabilities")


def _apply_plaid_consent_expiration_migration(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "plaid_items"):
        return

    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(plaid_items)").fetchall()
    }
    if "consent_expiration_time" not in columns:
        conn.execute("ALTER TABLE plaid_items ADD COLUMN consent_expiration_time TEXT")

    if not _table_exists(conn, "_sync_changelog"):
        return
    for op in ("insert", "update", "delete"):
        conn.execute(f'DROP TRIGGER IF EXISTS "_sync_log_plaid_items_{op}"')
    _create_sync_changelog_trigger_set(conn, "plaid_items")


def _apply_ai_log_token_tracking(conn: sqlite3.Connection) -> None:
    table_row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'ai_categorization_log'
        """
    ).fetchone()
    if table_row is None:
        conn.executescript(
            """
            CREATE TABLE ai_categorization_log (
                id              TEXT PRIMARY KEY,
                batch_id        TEXT NOT NULL,
                transaction_id  TEXT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
                provider        TEXT NOT NULL,
                model           TEXT NOT NULL,
                category_name   TEXT,
                use_type        TEXT,
                confidence      REAL,
                reasoning       TEXT,
                prompt_hash     TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                input_tokens    INTEGER NOT NULL DEFAULT 0,
                output_tokens   INTEGER NOT NULL DEFAULT 0,
                elapsed_ms      INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_ai_log_batch ON ai_categorization_log(batch_id);
            CREATE INDEX IF NOT EXISTS idx_ai_log_txn ON ai_categorization_log(transaction_id);
            """
        )
        return

    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(ai_categorization_log)").fetchall()
    }
    for column_name, column_def in _AI_LOG_NEW_COLUMNS.items():
        if column_name in columns:
            continue
        conn.execute(
            f"ALTER TABLE ai_categorization_log ADD COLUMN {column_name} {column_def}"
        )


def _apply_bot_sessions(conn: sqlite3.Connection) -> None:
    """Create bot_sessions table and add bot_session_id columns."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS bot_sessions (
            session_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL DEFAULT (datetime('now')),
            ended_at TEXT,
            end_reason TEXT CHECK (end_reason IN ('idle', 'reset', 'restart')),
            last_activity_at TEXT NOT NULL DEFAULT (datetime('now')),
            message_count INTEGER DEFAULT 0,
            request_count INTEGER DEFAULT 0,
            total_cost REAL DEFAULT 0.0
        );
        CREATE INDEX IF NOT EXISTS idx_bot_sessions_started ON bot_sessions(started_at);
        CREATE INDEX IF NOT EXISTS idx_bot_sessions_ended ON bot_sessions(ended_at);
        """
    )

    for table, column_spec in _BOT_SESSION_COLUMNS.items():
        table_name = _validate_sqlite_identifier(table)
        column_name = _validate_sqlite_identifier(column_spec.split()[0])
        quoted_table_name = _quote_sqlite_identifier(table_name)
        columns = {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({quoted_table_name})").fetchall()
        }
        if column_name in columns:
            continue
        conn.execute(f"ALTER TABLE {quoted_table_name} ADD COLUMN {column_spec}")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_chat_messages_session ON bot_chat_messages(bot_session_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_requests_session ON bot_requests(bot_session_id)"
    )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _apply_cost_backfill(conn: sqlite3.Connection) -> None:
    from .cost_tracking import estimate_ai_cost_usd6

    if not _table_exists(conn, "cost_ledger"):
        return

    if _table_exists(conn, "bot_requests"):
        conn.execute(
            """
            INSERT OR IGNORE INTO cost_ledger (
                provider,
                operation,
                cost_usd6,
                input_tokens,
                output_tokens,
                cache_creation_tokens,
                cache_read_tokens,
                model,
                request_id,
                is_estimated,
                idempotency_key,
                created_at
            )
            SELECT
                'claude',
                'chat',
                CAST(ROUND(estimated_cost * 1000000) AS INTEGER),
                input_tokens,
                output_tokens,
                cache_creation_tokens,
                cache_read_tokens,
                model,
                request_id,
                0,
                'backfill_bot_' || request_id,
                created_at
            FROM bot_requests
            WHERE COALESCE(estimated_cost, 0) > 0
            """
        )

    if not _table_exists(conn, "ai_categorization_log"):
        return

    rows = conn.execute(
        """
        SELECT
            batch_id,
            MIN(provider) AS provider,
            MIN(model) AS model,
            MAX(input_tokens) AS input_tokens,
            MAX(output_tokens) AS output_tokens,
            MIN(created_at) AS created_at
        FROM ai_categorization_log
        WHERE COALESCE(input_tokens, 0) > 0
           OR COALESCE(output_tokens, 0) > 0
        GROUP BY batch_id
        """
    ).fetchall()

    if not rows:
        return

    payload: list[tuple[object, ...]] = []
    for row in rows:
        provider = str(row["provider"] or "").strip().lower()
        model = str(row["model"] or "").strip() or None
        input_tokens = int(row["input_tokens"] or 0)
        output_tokens = int(row["output_tokens"] or 0)
        cost_usd6 = estimate_ai_cost_usd6(
            provider,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        if cost_usd6 <= 0:
            continue
        payload.append(
            (
                provider,
                "categorize",
                cost_usd6,
                input_tokens,
                output_tokens,
                model,
                f"backfill_aicat_{row['batch_id']}",
                row["created_at"],
            )
        )

    if not payload:
        return

    conn.executemany(
        """
        INSERT OR IGNORE INTO cost_ledger (
            provider,
            operation,
            cost_usd6,
            input_tokens,
            output_tokens,
            model,
            is_estimated,
            idempotency_key,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        payload,
    )


def query_all(conn: sqlite3.Connection, sql: str, params: tuple | None = None) -> list[sqlite3.Row]:
    cursor = conn.execute(sql, params or ())
    return cursor.fetchall()


def query_one(conn: sqlite3.Connection, sql: str, params: tuple | None = None) -> sqlite3.Row | None:
    cursor = conn.execute(sql, params or ())
    return cursor.fetchone()


def _default_backup_dir(source_db_path: Path) -> Path:
    return source_db_path.parent / "backups"


def _backup_retention_count() -> int:
    raw = str(os.getenv(BACKUP_RETENTION_ENV) or "").strip()
    if not raw:
        return DEFAULT_BACKUP_RETENTION_COUNT
    try:
        return max(int(raw), 0)
    except ValueError:
        return DEFAULT_BACKUP_RETENTION_COUNT


def _generated_backup_pattern(source_db_path: Path) -> re.Pattern[str]:
    stem = re.escape(source_db_path.stem)
    suffix = re.escape(source_db_path.suffix or ".db")
    return re.compile(rf"^{stem}_backup_\d{{8}}_\d{{6}}(?:_\d+)?{suffix}$")


def _delete_backup_artifacts(path: Path) -> None:
    related = (
        path,
        path.with_name(f"{path.name}-shm"),
        path.with_name(f"{path.name}-wal"),
    )
    for candidate in related:
        try:
            candidate.unlink()
        except FileNotFoundError:
            continue


def _prune_generated_backups(source_db_path: Path, backup_dir: Path) -> None:
    keep_count = _backup_retention_count()
    if keep_count <= 0 or not backup_dir.exists():
        return

    pattern = _generated_backup_pattern(source_db_path)
    backups = sorted(
        path
        for path in backup_dir.iterdir()
        if path.is_file() and pattern.match(path.name)
    )
    for path in backups[:-keep_count]:
        _delete_backup_artifacts(path)


def _resolve_backup_target_path(source_db_path: Path, destination: Path | None = None) -> Path:
    source_db_path = source_db_path.expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_name = f"{source_db_path.stem}_backup_{timestamp}{source_db_path.suffix or '.db'}"

    if destination is None:
        target = _default_backup_dir(source_db_path) / default_name
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
    if _is_storage_connection(conn):
        # Remote backups are created on the storage server. Keep the local sqlite
        # backup path unchanged, but use the path-returning proxy API here.
        remote_target = None if destination is None else str(destination)
        return Path(conn.backup(target_path=remote_target))

    source_db_path = db_path
    if source_db_path is None and conn is not None:
        source_db_path = _connected_main_db_path(conn)
    source_db_path = (source_db_path or get_db_path()).expanduser().resolve()
    source_db_path.parent.mkdir(parents=True, exist_ok=True)
    if not source_db_path.exists():
        raise FileNotFoundError(f"Database not found: {source_db_path}")

    backup_path = _resolve_backup_target_path(source_db_path, destination=destination)
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    if conn is not None:
        encryption_mode = db_encryption_mode()
        if encryption_mode == "off":
            backup_result = _backup_sqlite_connection_to_path(conn, str(backup_path))
            backup_path = Path(backup_result)
        else:
            source_user_id = _resolve_connection_user_id(source_db_path)
            backup_result = _backup_sqlite_connection_to_path(
                conn,
                str(backup_path),
                encrypted_user_id=source_user_id,
            )
            backup_path = Path(backup_result)
    else:
        shutil.copy2(source_db_path, backup_path)

    default_backup_dir = _default_backup_dir(source_db_path)
    if backup_path.parent == default_backup_dir:
        _prune_generated_backups(source_db_path, default_backup_dir)

    return backup_path


def _backup_sqlite_connection_to_path(
    conn: sqlite3.Connection,
    target_path: str,
    *,
    encrypted_user_id: str | None = None,
) -> str:
    if encrypted_user_id is None:
        with sqlite3.connect(str(target_path)) as backup_conn:
            conn.backup(backup_conn)
            backup_conn.commit()
        return str(target_path)

    with open_encrypted_connection(
        Path(target_path),
        user_id=encrypted_user_id,
        check_same_thread=True,
    ) as backup_conn:
        conn.backup(backup_conn)
        backup_conn.commit()
    return str(target_path)


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
            "last_investment_sync_at",
        )
        if column in columns
    ]
    if not reset_columns:
        return 0

    for column in reset_columns:
        if column not in _PLAID_RESET_ALLOWED:
            raise ValueError(f"Cannot reset unauthorized column: {column}")

    assignments = ", ".join(f"{column} = NULL" for column in reset_columns)
    cursor = conn.execute(
        f"""
        UPDATE plaid_items
           SET {assignments},
               updated_at = datetime('now')
        """
    )
    return int(cursor.rowcount or 0)


def _delete_all_rows(conn: sqlite3.Connection, table_name: str) -> sqlite3.Cursor:
    return conn.execute(f"DELETE FROM {_quote_sqlite_identifier(table_name)}")


def wipe_runtime_data(
    conn: sqlite3.Connection,
    *,
    preserve_plaid_items: bool = True,
    full: bool = False,
) -> dict[str, int]:
    report: dict[str, int] = {}
    if full:
        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            tables = [
                str(row["name"])
                for row in conn.execute(
                    """
                    SELECT name
                      FROM sqlite_master
                     WHERE type = 'table'
                       AND name NOT LIKE 'sqlite_%'
                     ORDER BY name
                    """
                ).fetchall()
            ]
            for table in tables:
                if table.startswith("txn_fts") or table in RESET_KEEP_TABLES:
                    continue
                if table == "categories":
                    cursor = conn.execute("DELETE FROM categories WHERE is_system = 0")
                    report[table] = int(cursor.rowcount or 0)
                    continue
                if table == "plaid_items":
                    if preserve_plaid_items:
                        report["plaid_items_reset"] = reset_plaid_sync_metadata(conn)
                    else:
                        cursor = conn.execute("DELETE FROM plaid_items")
                        report["plaid_items_deleted"] = int(cursor.rowcount or 0)
                    continue
                cursor = _delete_all_rows(conn, table)
                report[table] = int(cursor.rowcount or 0)
        finally:
            if conn.in_transaction:
                conn.commit()
            conn.execute("PRAGMA foreign_keys = ON")
        return report

    for table in RUNTIME_RESET_TABLES:
        cursor = _delete_all_rows(conn, table)
        report[table] = int(cursor.rowcount or 0)

    if preserve_plaid_items:
        report["plaid_items_reset"] = reset_plaid_sync_metadata(conn)
    else:
        cursor = conn.execute("DELETE FROM plaid_items")
        report["plaid_items_deleted"] = int(cursor.rowcount or 0)

    if full:
        for table in PREFERENCE_TABLES:
            cursor = _delete_all_rows(conn, table)
            report[table] = int(cursor.rowcount or 0)

    return report
