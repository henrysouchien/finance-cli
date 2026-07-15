"""Shared per-user workspace provisioning helpers."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from finance_cli import crypto_envelope, db_keys
from finance_cli.category_seed import seed_canonical_categories
from finance_cli.db import connect, db_encryption_mode, initialize_database
from finance_cli.db_keys import provision_user_db_key
from finance_cli.exceptions import TenantMismatchError

log = logging.getLogger(__name__)


def user_dir(data_root: Path, user_id: str | int) -> Path:
    """Resolve a user workspace directory with path traversal protection."""
    uid = str(user_id)
    if "/" in uid or "\\" in uid or uid in {".", ".."} or ".." in uid:
        raise ValueError("Invalid user_id: contains path separators or dot segments")

    resolved_root = Path(data_root).expanduser().resolve()
    resolved_dir = (resolved_root / uid).resolve()
    if resolved_dir.parent != resolved_root:
        raise ValueError("Invalid user_id: path traversal detected")
    return resolved_dir


def user_db_path(data_root: Path, user_id: str | int) -> Path:
    return user_dir(data_root, user_id) / "finance.db"


def user_rules_path(data_root: Path, user_id: str | int) -> Path:
    return user_dir(data_root, user_id) / "rules.yaml"


def user_id_from_db_path(path: Path) -> str:
    resolved = Path(path).expanduser().resolve()
    if resolved.name != "finance.db":
        raise ValueError(f"DB path does not match per-user finance.db layout: {resolved}")
    parent = resolved.parent
    data_root = parent.parent
    configured_root = str(os.getenv("FINANCE_WEB_DATA_ROOT") or "").strip()
    if configured_root:
        if data_root != Path(configured_root).expanduser().resolve():
            raise ValueError(f"DB path does not live under FINANCE_WEB_DATA_ROOT: {resolved}")
    elif data_root.name != "users":
        raise ValueError(f"DB path does not match per-user finance.db layout: {resolved}")
    user_id = resolved.parent.name
    if user_id in {"", ".", ".."}:
        raise ValueError(f"DB path does not contain a valid user_id: {resolved}")
    return user_id


def _stamp_tenant_marker(
    db_path: Path,
    user_id: str | int,
    *,
    storage_session_manager=None,
) -> None:
    with connect(db_path=db_path, storage_session_manager=storage_session_manager) as conn:
        stamp_tenant_marker(conn, user_id, db_path=db_path)


def stamp_tenant_marker(conn, user_id: str | int, *, db_path: Path | str | None = None) -> None:
    """Stamp or verify the singleton tenant marker on an open connection."""
    expected_user_id = str(user_id)
    existing = conn.execute(
        "SELECT user_id FROM tenant_marker WHERE singleton = 1"
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO tenant_marker (singleton, user_id) VALUES (1, ?)",
            (expected_user_id,),
        )
        conn.commit()
        return

    actual_user_id = str(existing[0])
    if actual_user_id != expected_user_id:
        raise TenantMismatchError(
            f"Provisioning {expected_user_id!r} against DB stamped for {actual_user_id!r}",
            expected_user_id=expected_user_id,
            actual_user_id=actual_user_id,
            db_path=str(db_path) if db_path is not None else "<connection>",
            reason="mismatch",
        )


def _seed_user_categories(db_path: Path, user_id: str | int) -> None:
    with connect(db_path=db_path, expected_user_id=str(user_id)) as conn:
        seed_canonical_categories(conn, dry_run=False)


def ensure_tenant_marker(
    *,
    data_root: Path,
    user_id: str | int,
    storage_session_manager=None,
) -> None:
    """Stamp or verify the tenant marker for an existing per-user database."""
    _stamp_tenant_marker(
        user_db_path(data_root, user_id),
        user_id,
        storage_session_manager=storage_session_manager,
    )


def _provision_db_dek_envelope(*, data_root: Path, db_path: Path, user_id: str | int) -> None:
    resolved_user_id = str(user_id)
    if not crypto_envelope.has_db_dek(resolved_user_id, data_dir=data_root):
        legacy_dek = db_keys.get_user_db_key(resolved_user_id)
        crypto_envelope.provision_db_dek(resolved_user_id, dek=legacy_dek, data_dir=data_root)

    readback = crypto_envelope.get_db_dek(resolved_user_id, data_dir=data_root)
    if len(readback) != 32:
        raise ValueError(f"db-dek.enc for user {resolved_user_id!r} has unexpected length")

    from finance_cli.backup import can_hard_delete_db_dek_sm

    with connect(db_path=db_path, expected_user_id=resolved_user_id) as conn:
        cleanup_allowed = can_hard_delete_db_dek_sm(
            resolved_user_id,
            conn=conn,
            data_dir=data_root / resolved_user_id,
        )

    if cleanup_allowed:
        db_keys.delete_user_db_key(resolved_user_id)
    else:
        log.info("Deferred legacy SM db-key cleanup for user_id=%s; durability gate did not pass", resolved_user_id)


def provision_user(
    *,
    data_root: Path,
    user_id: str | int,
    template_rules_path: Path,
    ensure_canonical_categories: bool = False,
) -> dict[str, str]:
    encryption_mode = db_encryption_mode()
    encryption_enabled = encryption_mode != "off"

    root = user_dir(data_root, user_id)
    root.mkdir(parents=True, exist_ok=True)

    db_path = user_db_path(data_root, user_id)
    if encryption_enabled:
        provision_user_db_key(str(user_id), data_dir=data_root)
    initialize_database(db_path=db_path)
    _stamp_tenant_marker(db_path, user_id)
    if ensure_canonical_categories:
        _seed_user_categories(db_path, user_id)
    if encryption_enabled:
        _provision_db_dek_envelope(data_root=data_root, db_path=db_path, user_id=user_id)

    rules_path = user_rules_path(data_root, user_id)
    if not rules_path.exists():
        shutil.copyfile(template_rules_path, rules_path)

    return {
        "user_dir": str(root),
        "db_path": str(db_path),
        "rules_path": str(rules_path),
    }
