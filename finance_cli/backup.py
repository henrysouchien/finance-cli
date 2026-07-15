"""Backup bundle create/list/verify/restore/prune helpers.

``backup.py`` generally treats ``data_dir`` as the per-user workspace where
``backups/`` lives. ``crypto_envelope`` treats ``data_dir`` as the root that
contains user subdirectories. Use ``_envelope_data_dir`` at that boundary.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import os
import shutil
import sqlite3
import tarfile
import tempfile
import time
import uuid
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

from finance_cli import __version__
from finance_cli import backup_crypto
from finance_cli import crypto_envelope
from finance_cli import config as config_module
from finance_cli.config import ensure_data_dir, get_db_path
from finance_cli.db import backup_database, connect, open_encrypted_connection
from finance_cli.exceptions import FinanceCLIError, TenantMismatchError
from finance_cli.sync.subscriber_lock import acquire_install_lock_for_restore
from finance_cli.user_rules import resolve_rules_path

log = logging.getLogger(__name__)

_ARCHIVE_SUFFIX = ".bundle"


@dataclass
class BackupResult:
    bundle_path: Path
    bundle_sha256: str
    bundle_size: int
    db_sha256: str
    migration_ver: int
    duration_ms: int
    files: list[dict]


@dataclass
class VerifyResult:
    valid: bool
    manifest: dict
    errors: list[str]
    warnings: list[str]


@dataclass
class RestoreResult:
    restored: bool
    dry_run: bool
    bundle_path: Path
    manifest: dict
    warnings: list[str]


@dataclass
class PruneResult:
    dry_run: bool
    kept: int
    deleted: int
    deleted_paths: list[str]
    freed_bytes: int
    scheduled_key_deletions: int = 0


@dataclass(frozen=True)
class BackupBundleInfo:
    bundle_path: Path
    format_version: int
    mode: str | None
    recovery_db_dek_present: bool
    bundle_id: str | None
    user_id: str | None
    created_at: str | None


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _default_user_id() -> str:
    value = str(getattr(config_module, "default_user_id", "") or "").strip()
    return value or "default"


def _provided_user_id(user_id: str | None) -> str | None:
    value = str(user_id or "").strip()
    return value or None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_fileobj(fileobj) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: fileobj.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def _normalize_data_dir(data_dir: Path | None = None) -> Path:
    return (
        data_dir.expanduser().resolve() if data_dir is not None else ensure_data_dir()
    )


def _envelope_data_dir(data_dir: Path, user_id: str) -> Path:
    """Return the data_dir that ``crypto_envelope`` expects.

    ``backup.py`` receives the per-user data directory in multi-tenant
    deployments, e.g. ``/data/finance/users/1``. ``crypto_envelope`` expects
    the multi-tenant root containing ``<user_id>/`` subdirectories, e.g.
    ``/data/finance/users``.

    Detection is intentionally narrow: prefer ``FINANCE_WEB_DATA_ROOT`` when
    it exactly parents this user directory, then the canonical ``users/<id>``
    layout, otherwise preserve single-tenant behavior by returning ``data_dir``.
    """

    resolved = data_dir.expanduser().resolve()
    resolved_user_id = str(user_id)
    web_root_raw = os.getenv("FINANCE_WEB_DATA_ROOT")
    if web_root_raw:
        web_root = Path(web_root_raw).expanduser().resolve()
        if resolved == (web_root / resolved_user_id).resolve():
            return web_root
    if resolved.name == resolved_user_id and resolved.parent.name == "users":
        return resolved.parent
    return resolved


def _default_envelope_data_dir(user_id: str) -> Path | None:
    """Return an env-derived crypto-envelope root when no data_dir was supplied."""

    resolved_user_id = str(user_id)
    for env_name in ("FINANCE_WEB_DATA_ROOT", "FINANCE_CLI_DATA_DIR"):
        raw = str(os.getenv(env_name) or "").strip()
        if raw:
            return _envelope_data_dir(Path(raw), resolved_user_id)
    return None


def _normalize_rules_path(
    rules_path: Path | None = None, *, data_dir: Path | None = None
) -> Path:
    if rules_path is not None:
        return rules_path.expanduser().resolve()
    if data_dir is not None:
        return (data_dir / "rules.yaml").expanduser().resolve()
    return resolve_rules_path()


def _sidecar_path(data_dir: Path | None = None) -> Path:
    return _normalize_data_dir(data_dir) / "backups" / "backup_audit.jsonl"


def canonical_install_db_path() -> Path:
    return (Path.home() / ".cashnerd" / "data" / "finance.db").expanduser().resolve()


def install_subscriber_lock_path() -> Path:
    return canonical_install_db_path().parent.parent / "subscriber.lock"


def is_canonical_install_db_path(path: Path) -> bool:
    return path.expanduser().resolve() == canonical_install_db_path()


def _append_sidecar(entry: dict, data_dir: Path | None = None) -> None:
    sidecar = _sidecar_path(data_dir)
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    with sidecar.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True, default=str) + "\n")


def _read_sidecar(data_dir: Path | None = None) -> list[dict]:
    sidecar = _sidecar_path(data_dir)
    if not sidecar.exists():
        return []

    entries: list[dict] = []
    for line_no, raw_line in enumerate(
        sidecar.read_text(encoding="utf-8").splitlines(), start=1
    ):
        candidate = raw_line.strip()
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            log.warning(
                "Skipping invalid backup sidecar line %s in %s", line_no, sidecar
            )
            continue
        if isinstance(payload, dict):
            entries.append(payload)
    return entries


def _log_to_db(conn: sqlite3.Connection | None, entry: dict) -> None:
    if conn is None:
        return
    try:
        conn.execute(
            """
            INSERT INTO backup_log (
                backup_type,
                status,
                bundle_path,
                bundle_sha256,
                bundle_size,
                db_sha256,
                migration_ver,
                duration_ms,
                bundle_format_version,
                dek_secret_ref,
                signing_key_secret_ref,
                signature_verified_at,
                bundle_id,
                user_id,
                mode,
                recovery_db_dek_present,
                error_message,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.get("backup_type"),
                entry.get("status"),
                entry.get("bundle_path"),
                entry.get("bundle_sha256"),
                entry.get("bundle_size"),
                entry.get("db_sha256"),
                entry.get("migration_ver"),
                entry.get("duration_ms"),
                entry.get("bundle_format_version"),
                entry.get("dek_secret_ref"),
                entry.get("signing_key_secret_ref"),
                entry.get("signature_verified_at"),
                entry.get("bundle_id"),
                entry.get("user_id"),
                entry.get("mode"),
                entry.get("recovery_db_dek_present"),
                entry.get("error_message"),
                entry.get("created_at") or _utc_now_iso(),
            ),
        )
    except Exception:
        log.warning("Failed mirroring backup audit entry to DB", exc_info=True)


def _get_migration_ver(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    except sqlite3.OperationalError:
        return 0
    if row is None:
        return 0
    value = row[0] if not isinstance(row, sqlite3.Row) else row[0]
    return int(value or 0)


def _bundle_name_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


def _resolve_bundle_destination(destination: Path | None, *, data_dir: Path) -> Path:
    default_name = f"finance_backup_{_bundle_name_timestamp()}{_ARCHIVE_SUFFIX}"

    if destination is None:
        target = data_dir / "backups" / default_name
    else:
        resolved = destination.expanduser().resolve()
        if resolved.is_dir() or not resolved.suffix:
            target = resolved / default_name
        else:
            target = resolved

    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        return target

    if target.name.endswith(_ARCHIVE_SUFFIX):
        stem = target.name[: -len(_ARCHIVE_SUFFIX)]
        suffix = _ARCHIVE_SUFFIX
    else:
        stem = target.stem
        suffix = target.suffix

    counter = 1
    while True:
        candidate = target.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _record_event(
    entry: dict, *, conn: sqlite3.Connection | None, data_dir: Path | None
) -> None:
    _append_sidecar(entry, data_dir=data_dir)
    _log_to_db(conn, entry)


def _file_entries(root: Path, *, include_sha256: bool = True) -> list[dict]:
    entries: list[dict] = []
    for file_path in sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    ):
        entry = {
            "path": file_path.relative_to(root).as_posix(),
            "size_bytes": int(file_path.stat().st_size),
        }
        if include_sha256:
            entry["sha256"] = _sha256(file_path)
        entries.append(entry)
    return entries


def _member_names(manifest: dict) -> set[str]:
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        return set()
    return {
        str(item.get("path", "")).strip()
        for item in raw_files
        if isinstance(item, dict)
    }


def _member_exists(tar: tarfile.TarFile, member_name: str) -> bool:
    try:
        tar.getmember(member_name)
    except KeyError:
        return False
    return True


def _extract_member_to_path(
    tar: tarfile.TarFile, member_name: str, target_path: Path
) -> bool:
    try:
        member = tar.getmember(member_name)
    except KeyError:
        return False
    fileobj = tar.extractfile(member)
    if fileobj is None:
        return False
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("wb") as handle:
        shutil.copyfileobj(fileobj, handle)
    return True


def _read_member_bytes(tar: tarfile.TarFile, member_name: str) -> bytes | None:
    try:
        member = tar.getmember(member_name)
    except KeyError:
        return None
    fileobj = tar.extractfile(member)
    if fileobj is None:
        return None
    return fileobj.read()


def _read_manifest_from_tar(tar: tarfile.TarFile) -> dict[str, Any]:
    try:
        member = tar.getmember("manifest.json")
    except KeyError as exc:
        raise ValueError("Backup bundle is missing manifest.json") from exc
    fileobj = tar.extractfile(member)
    if fileobj is None:
        raise ValueError("Backup bundle manifest.json is unreadable")
    payload = json.load(fileobj)
    if not isinstance(payload, dict):
        raise ValueError("Backup bundle manifest.json must contain an object")
    return payload


def _parse_created_at(raw_value: Any) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    for parser in (datetime.fromisoformat,):
        try:
            parsed = parser(text)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _entry_sort_key(entry: dict) -> datetime:
    return _parse_created_at(entry.get("created_at")) or datetime.min.replace(
        tzinfo=timezone.utc
    )


def _sidecar_completed_entries(entries: list[dict]) -> list[dict]:
    pruned_paths = {
        str(entry.get("bundle_path", "")).strip()
        for entry in entries
        if entry.get("action") == "pruned" and str(entry.get("bundle_path", "")).strip()
    }
    deduped: dict[tuple[str, str], dict] = {}
    for entry in entries:
        if entry.get("status") != "completed":
            continue
        bundle_path = str(entry.get("bundle_path", "")).strip()
        backup_type = str(entry.get("backup_type", "")).strip()
        if not bundle_path or not backup_type or bundle_path in pruned_paths:
            continue
        deduped[(bundle_path, backup_type)] = dict(entry)
    return list(deduped.values())


def _latest_sidecar_entry(
    data_dir: Path,
    *,
    bundle_path: Path,
    backup_type: str,
    status: str = "completed",
) -> dict | None:
    bundle_path_str = str(bundle_path.expanduser().resolve())
    match: dict | None = None
    for entry in _read_sidecar(data_dir):
        if (
            str(entry.get("bundle_path", "")).strip() == bundle_path_str
            and str(entry.get("backup_type", "")).strip() == backup_type
            and str(entry.get("status", "")).strip() == status
        ):
            match = dict(entry)
    return match


def _open_plaintext_backup_conn(db_path: Path) -> sqlite3.Connection:
    uri = f"{db_path.expanduser().resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _is_plaintext_backup_db(db_path: Path) -> bool:
    try:
        with _open_plaintext_backup_conn(db_path) as conn:
            conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        return True
    except sqlite3.Error:
        return False


@contextmanager
def _open_backup_db_for_read(
    db_path: Path,
    *,
    user_id: str | None = None,
    data_dir: Path | None = None,
):
    if _is_plaintext_backup_db(db_path):
        with _open_plaintext_backup_conn(db_path) as conn:
            yield conn
        return
    if user_id is not None:
        with open_encrypted_connection(
            db_path,
            user_id=user_id,
            check_same_thread=True,
            data_dir=data_dir,
        ) as conn:
            yield conn
        return
    with _open_plaintext_backup_conn(db_path) as conn:
        yield conn


def _verify_integrity(
    db_path: Path,
    *,
    user_id: str | None = None,
    data_dir: Path | None = None,
) -> str:
    resolved_user_id = _provided_user_id(user_id)
    with _open_backup_db_for_read(
        db_path,
        user_id=resolved_user_id,
        data_dir=data_dir,
    ) as conn:
        row = conn.execute("PRAGMA integrity_check").fetchone()
    return str(row[0] if row is not None else "")


def _is_v2_bundle(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            return handle.read(len(backup_crypto.MAGIC)) == backup_crypto.MAGIC
    except OSError:
        return False


def _is_v3_bundle(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            return handle.read(len(backup_crypto.MAGIC_V3)) == backup_crypto.MAGIC_V3
    except OSError:
        return False


def _is_encrypted_bundle(path: Path) -> bool:
    return _is_v2_bundle(path) or _is_v3_bundle(path)


def _resolve_backup_mode(
    *,
    portable: bool = False,
    compact: bool = False,
    mode: str | None = None,
) -> str:
    if portable and compact:
        raise ValueError("--portable and --compact are mutually exclusive")
    if portable:
        return "portable"
    if compact:
        return "compact"
    raw = (
        str(mode or os.getenv("FINANCE_CLI_BACKUP_DEFAULT_MODE", "compact"))
        .strip()
        .lower()
    )
    if raw not in {"portable", "compact"}:
        raise ValueError("Backup mode must be 'portable' or 'compact'")
    return raw


def tar_contains(tar_bytes: bytes, member_name: str) -> bool:
    """Return True when tar_bytes contains member_name."""

    try:
        with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r:gz") as tar:
            return _member_exists(tar, member_name)
    except tarfile.TarError:
        return False


def create_backup(
    conn: sqlite3.Connection,
    *,
    destination: Path | None = None,
    include_offhost: bool = False,
    backup_type: str = "local",
    data_dir: Path | None = None,
    rules_path: Path | None = None,
    user_id: str | None = None,
    portable: bool = False,
    compact: bool = False,
    mode: str | None = None,
) -> BackupResult:
    """Create a bundled backup containing the DB and local config state."""

    started_at = time.perf_counter()
    resolved_data_dir = _normalize_data_dir(data_dir)
    resolved_rules_path = _normalize_rules_path(rules_path, data_dir=resolved_data_dir)
    bundle_path = _resolve_bundle_destination(destination, data_dir=resolved_data_dir)
    resolved_user_id = _provided_user_id(user_id) or _default_user_id()
    bundle_id = str(uuid.uuid4())
    backup_mode = _resolve_backup_mode(portable=portable, compact=compact, mode=mode)
    envelope_data_dir = _envelope_data_dir(resolved_data_dir, resolved_user_id)
    backend = crypto_envelope.select_backend(resolved_user_id, envelope_data_dir)
    db_dek_blob = backend.get(resolved_user_id, kind="db-dek")
    bundle_format_version = 3 if db_dek_blob is not None else 2
    temp_dir = Path(tempfile.mkdtemp(prefix="finance_backup_"))

    started_entry = {
        "backup_type": backup_type,
        "status": "started",
        "bundle_path": str(bundle_path),
        "bundle_format_version": bundle_format_version,
        "bundle_id": bundle_id,
        "user_id": resolved_user_id,
        "mode": backup_mode if bundle_format_version == 3 else None,
        "created_at": _utc_now_iso(),
    }
    _record_event(started_entry, conn=conn, data_dir=resolved_data_dir)
    conn.commit()

    try:
        db_copy_path = temp_dir / "finance.db"
        backup_database(conn=conn, destination=db_copy_path)

        db_integrity_check = _verify_integrity(
            db_copy_path,
            user_id=resolved_user_id,
            data_dir=envelope_data_dir,
        )
        if db_integrity_check.lower() != "ok":
            log.warning(
                "Backup copy integrity check returned %s for %s",
                db_integrity_check,
                db_copy_path,
            )
        try:
            with sqlite3.connect(str(db_copy_path)) as probe_conn:
                probe_conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
            sqlcipher_db = False
        except sqlite3.Error:
            sqlcipher_db = True

        if resolved_rules_path.exists():
            shutil.copy2(resolved_rules_path, temp_dir / "rules.yaml")

        agent_memory_path = resolved_data_dir / "agent_memory.md"
        if agent_memory_path.exists():
            shutil.copy2(agent_memory_path, temp_dir / "agent_memory.md")

        sessions_dir = resolved_data_dir / "sessions"
        if sessions_dir.is_dir():
            shutil.copytree(sessions_dir, temp_dir / "sessions")

        if db_dek_blob is None and backup_mode == "portable":
            log.info(
                "Cannot write portable v3 bundle: db-dek.enc missing for user %s; "
                "falling back to v2",
                resolved_user_id,
            )
        embed_recovery = db_dek_blob is not None and backup_mode == "portable"
        if embed_recovery:
            recovery_path = temp_dir / "recovery" / "db-dek.enc"
            recovery_path.parent.mkdir(parents=True, exist_ok=True)
            recovery_path.write_bytes(db_dek_blob)

        files = _file_entries(temp_dir, include_sha256=False)
        db_file = next(
            (entry for entry in files if entry["path"] == "finance.db"), None
        )
        if db_file is None:
            raise ValueError("Backup bundle staging area is missing finance.db")
        db_sha256 = _sha256(db_copy_path)

        migration_ver = _get_migration_ver(conn)
        manifest = {
            "schema_version": 2,
            "bundle_id": bundle_id,
            "user_id": resolved_user_id,
            "created_at": _utc_now_iso(),
            "finance_cli_version": __version__,
            "sqlcipher_db": sqlcipher_db,
            "db_integrity_check": db_integrity_check,
            "migration_version": migration_ver,
            "files": files,
        }
        manifest = backup_crypto.sign_manifest(manifest, resolved_user_id)
        signing_key_secret_ref = str(
            manifest.get("signature", {}).get("key_ref") or ""
        ).strip()
        (temp_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        staged_bundle_path = temp_dir / "bundle.tar.gz"
        with tarfile.open(staged_bundle_path, "w:gz") as tar:
            for path in sorted(
                candidate for candidate in temp_dir.rglob("*") if candidate.is_file()
            ):
                if path == staged_bundle_path:
                    continue
                tar.add(path, arcname=path.relative_to(temp_dir).as_posix())

        tar_bytes = staged_bundle_path.read_bytes()
        recovery_present_in_tar = tar_contains(tar_bytes, "recovery/db-dek.enc")
        if embed_recovery and not recovery_present_in_tar:
            raise ValueError(
                "Recovery payload requested but not present in tar; refusing to write header"
            )

        if bundle_format_version == 3:
            encrypted_bundle = backup_crypto.encrypt_bundle_v3(
                tar_bytes,
                resolved_user_id,
                bundle_id,
                mode=backup_mode,
                recovery_db_dek_present=recovery_present_in_tar,
            )
            header = backup_crypto.parse_bundle_header_v3(encrypted_bundle)
        else:
            encrypted_bundle = backup_crypto.encrypt_bundle(
                tar_bytes,
                resolved_user_id,
                bundle_id,
            )
            header = backup_crypto.parse_bundle_header(encrypted_bundle)
        bundle_path.write_bytes(encrypted_bundle)

        bundle_sha256 = _sha256(bundle_path)
        bundle_size = int(bundle_path.stat().st_size)
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        completed_entry = {
            "backup_type": backup_type,
            "status": "completed",
            "bundle_path": str(bundle_path),
            "bundle_sha256": bundle_sha256,
            "bundle_size": bundle_size,
            "db_sha256": db_sha256,
            "migration_ver": migration_ver,
            "duration_ms": duration_ms,
            "bundle_format_version": bundle_format_version,
            "dek_secret_ref": header.get("dek_secret_ref"),
            "signing_key_secret_ref": signing_key_secret_ref,
            "bundle_id": bundle_id,
            "user_id": resolved_user_id,
            "mode": header.get("mode"),
            "recovery_db_dek_present": header.get("recovery_db_dek_present"),
            "created_at": _utc_now_iso(),
        }
        _record_event(completed_entry, conn=conn, data_dir=resolved_data_dir)
        conn.commit()

        if include_offhost:
            log.warning(
                "Off-host backup upload is not implemented in Phase 1; created local bundle only"
            )

        return BackupResult(
            bundle_path=bundle_path,
            bundle_sha256=bundle_sha256,
            bundle_size=bundle_size,
            db_sha256=db_sha256,
            migration_ver=migration_ver,
            duration_ms=duration_ms,
            files=files,
        )
    except Exception as exc:
        failed_entry = {
            "backup_type": backup_type,
            "status": "failed",
            "bundle_path": str(bundle_path),
            "duration_ms": int((time.perf_counter() - started_at) * 1000),
            "error_message": str(exc),
            "bundle_format_version": bundle_format_version,
            "bundle_id": bundle_id,
            "user_id": resolved_user_id,
            "mode": backup_mode if bundle_format_version == 3 else None,
            "created_at": _utc_now_iso(),
        }
        _record_event(failed_entry, conn=conn, data_dir=resolved_data_dir)
        conn.commit()
        raise
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def list_backups(
    conn: sqlite3.Connection | None = None,
    *,
    backup_type: str | None = None,
    limit: int = 20,
    s3_bucket: str | None = None,
    data_dir: Path | None = None,
) -> list[dict]:
    """List recent completed backup entries, most recent first."""

    if s3_bucket and backup_type == "offhost":
        log.warning("S3 backup listing is not implemented in Phase 1")
        return []

    entries: list[dict] = []
    sidecar_entries = _read_sidecar(data_dir)
    if sidecar_entries:
        entries = _sidecar_completed_entries(sidecar_entries)

    if not entries and conn is not None:
        rows = conn.execute(
            """
            SELECT *
              FROM backup_log
             WHERE status = 'completed'
            """
        ).fetchall()
        entries = [dict(row) for row in rows]

    if s3_bucket and not entries:
        log.warning("S3 backup listing is not implemented in Phase 1")
        return []

    if backup_type is not None:
        entries = [
            entry for entry in entries if entry.get("backup_type") == backup_type
        ]

    entries.sort(key=_entry_sort_key, reverse=True)
    return entries[: max(int(limit), 0)]


def _backup_log_columns(conn: sqlite3.Connection) -> set[str]:
    try:
        rows = conn.execute("PRAGMA table_info(backup_log)").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) for row in rows
    }


def _parse_v3_header_metadata(bundle_path: Path) -> tuple[str | None, bool]:
    try:
        header = backup_crypto.parse_bundle_header_v3(
            bundle_path.expanduser().read_bytes()
        )
    except Exception:
        return None, False
    mode = str(header.get("mode") or "").strip().lower() or None
    recovery_present = bool(header.get("recovery_db_dek_present"))
    return mode, recovery_present


def list_bundles_for_user(
    user_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    data_dir: Path | None = None,
) -> list[BackupBundleInfo]:
    """Return tracked backup bundle metadata for hard-delete gating."""

    owns_conn = False
    active_conn = conn
    if active_conn is None:
        db_path = (
            ((data_dir / "finance.db") if data_dir is not None else get_db_path())
            .expanduser()
            .resolve()
        )
        if not db_path.exists():
            return []
        try:
            active_conn = connect(db_path, user_id=str(user_id))
            owns_conn = True
        except Exception:
            return []

    try:
        columns = _backup_log_columns(active_conn)
        if not columns:
            return []
        wanted = [
            "bundle_path",
            "bundle_format_version",
            "mode",
            "recovery_db_dek_present",
            "bundle_id",
            "user_id",
            "created_at",
        ]
        select_exprs = [
            column if column in columns else f"NULL AS {column}" for column in wanted
        ]
        rows = active_conn.execute(
            f"""
            SELECT {", ".join(select_exprs)}
              FROM backup_log
             WHERE status = 'completed'
               AND backup_type IN ('local', 'offhost')
               AND (user_id = ? OR user_id IS NULL)
             ORDER BY datetime(created_at) DESC, id DESC
            """,
            (str(user_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        if owns_conn and active_conn is not None:
            active_conn.close()

    bundles: list[BackupBundleInfo] = []
    for row in rows:
        raw_path = str(row["bundle_path"] or "").strip()
        if not raw_path:
            continue
        format_version = int(row["bundle_format_version"] or 1)
        raw_mode = row["mode"]
        mode = (
            str(raw_mode).strip().lower()
            if raw_mode is not None and str(raw_mode).strip()
            else None
        )
        raw_recovery = row["recovery_db_dek_present"]
        recovery_present = bool(raw_recovery) if raw_recovery is not None else False
        if format_version == 3 and (mode is None or raw_recovery is None):
            parsed_mode, parsed_recovery = _parse_v3_header_metadata(Path(raw_path))
            if mode is None:
                mode = parsed_mode
            if raw_recovery is None:
                recovery_present = parsed_recovery
        bundles.append(
            BackupBundleInfo(
                bundle_path=Path(raw_path).expanduser(),
                format_version=format_version,
                mode=mode,
                recovery_db_dek_present=recovery_present,
                bundle_id=str(row["bundle_id"])
                if row["bundle_id"] is not None
                else None,
                user_id=str(row["user_id"]) if row["user_id"] is not None else None,
                created_at=str(row["created_at"])
                if row["created_at"] is not None
                else None,
            )
        )
    return bundles


def server_durability_preflight() -> tuple[bool, str]:
    """Require a recent AWS Backup recovery point before server-side SM hard-delete."""

    backup_vault_name = str(
        os.getenv("FINANCE_CLI_AWS_BACKUP_VAULT_NAME") or ""
    ).strip()
    if not backup_vault_name:
        return (
            False,
            "AWS Backup vault name not configured for finance-web; refuse hard-delete",
        )
    resource_arn = str(os.getenv("FINANCE_CLI_AWS_BACKUP_RESOURCE_ARN") or "").strip()
    if not resource_arn:
        return (
            False,
            "AWS Backup resource ARN not configured for finance-web; refuse hard-delete",
        )

    try:
        response = (
            importlib.import_module("boto3")
            .client(
                "backup",
                region_name=str(os.getenv("AWS_REGION") or "us-east-1").strip()
                or "us-east-1",
            )
            .list_recovery_points_by_backup_vault(
                BackupVaultName=backup_vault_name,
                ByResourceArn=resource_arn,
            )
        )
    except Exception as exc:
        return False, f"AWS Backup preflight failed: {exc}"

    recovery_points = response.get("RecoveryPoints") or []
    if not recovery_points:
        return False, "No recovery point found in AWS Backup vault; refuse hard-delete"
    latest_creation: datetime | None = None
    for point in recovery_points:
        created = point.get("CreationDate") if isinstance(point, dict) else None
        if not isinstance(created, datetime):
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        else:
            created = created.astimezone(timezone.utc)
        if latest_creation is None or created > latest_creation:
            latest_creation = created
    if latest_creation is None:
        return (
            False,
            "No recovery point creation timestamp found in AWS Backup vault; refuse hard-delete",
        )
    age_hours = (datetime.now(timezone.utc) - latest_creation).total_seconds() / 3600
    if age_hours > 48:
        return (
            False,
            f"Latest recovery point is {age_hours:.1f}h old (>48h); refuse hard-delete",
        )
    return True, "ok"


def _require_portable_bundle_for_delete() -> bool:
    """Resolve the DB-DEK SM retirement gate mode for this deployment.

    Local CLI installs default to portable-bundle proof. Web/prod deployments
    have a server data root and should instead prove server-side durability.
    """

    raw = os.getenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE")
    if raw is not None and raw.strip():
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return not bool(str(os.getenv("FINANCE_WEB_DATA_ROOT") or "").strip())


def can_hard_delete_db_dek_sm(
    user_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    data_dir: Path | None = None,
) -> bool:
    """Return whether this deployment has enough durable recovery to hard-delete SM."""

    try:
        envelope_data_dir = (
            _envelope_data_dir(data_dir, str(user_id))
            if data_dir is not None
            else _default_envelope_data_dir(str(user_id))
        )
        if not crypto_envelope.has_db_dek(str(user_id), data_dir=envelope_data_dir):
            return False
    except Exception:
        return False

    require_portable = _require_portable_bundle_for_delete()
    if not require_portable:
        ok, _reason = server_durability_preflight()
        return ok

    bundles = list_bundles_for_user(str(user_id), conn=conn, data_dir=data_dir)
    if not bundles:
        return True
    return any(
        bundle.format_version == 3
        and bundle.mode == "portable"
        and bundle.recovery_db_dek_present
        for bundle in bundles
    )


def verify_backup(
    bundle_path: Path,
    *,
    conn: sqlite3.Connection | None = None,
    user_id: str | None = None,
) -> VerifyResult:
    """Verify a bundled backup archive and its embedded SQLite database."""

    resolved_bundle_path = bundle_path.expanduser().resolve()
    manifest: dict[str, Any] = {}
    errors: list[str] = []
    warnings: list[str] = []
    requested_user_id = _provided_user_id(user_id)

    if not resolved_bundle_path.exists():
        return VerifyResult(
            valid=False,
            manifest=manifest,
            errors=[f"Backup not found: {resolved_bundle_path}"],
            warnings=warnings,
        )

    if _is_encrypted_bundle(resolved_bundle_path):
        try:
            encrypted_bundle = resolved_bundle_path.read_bytes()
            header = backup_crypto.parse_bundle_header_any_version(encrypted_bundle)
        except (OSError, ValueError, FinanceCLIError) as exc:
            return VerifyResult(
                valid=False, manifest=manifest, errors=[str(exc)], warnings=warnings
            )

        header_user_id = str(header.get("user_id") or "").strip()
        decrypt_user_id = requested_user_id or header_user_id
        if not decrypt_user_id:
            return VerifyResult(
                valid=False,
                manifest=manifest,
                errors=["Backup bundle header is missing user_id"],
                warnings=warnings,
            )
        if header_user_id != decrypt_user_id:
            return VerifyResult(
                valid=False,
                manifest=manifest,
                errors=[
                    f"Backup bundle belongs to {header_user_id!r}, not {decrypt_user_id!r}"
                ],
                warnings=warnings,
            )

        try:
            tar_bytes, header = backup_crypto.decrypt_bundle_any_version(
                encrypted_bundle, decrypt_user_id
            )
        except (ValueError, FinanceCLIError) as exc:
            return VerifyResult(
                valid=False, manifest=manifest, errors=[str(exc)], warnings=warnings
            )

        with tempfile.TemporaryDirectory(prefix="verify_backup_") as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            extracted_db = temp_dir / "finance.db"

            try:
                with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r:gz") as tar:
                    try:
                        manifest = _read_manifest_from_tar(tar)
                    except ValueError as exc:
                        errors.append(str(exc))
                        return VerifyResult(
                            valid=False,
                            manifest=manifest,
                            errors=errors,
                            warnings=warnings,
                        )

                    if manifest.get("schema_version") != 2:
                        errors.append(
                            f"Unsupported backup manifest schema_version: {manifest.get('schema_version')!r}"
                        )

                    if not backup_crypto.verify_manifest(manifest):
                        errors.append("Backup manifest signature is invalid")

                    manifest_files = manifest.get("files")
                    if not isinstance(manifest_files, list):
                        errors.append("Backup manifest files list is invalid")
                        manifest_files = []

                    for file_entry in manifest_files:
                        if not isinstance(file_entry, dict):
                            errors.append(
                                "Backup manifest contains an invalid file entry"
                            )
                            continue
                        member_name = str(file_entry.get("path", "")).strip()
                        if not member_name:
                            errors.append(
                                "Backup manifest contains a file entry without a path"
                            )
                            continue
                        if not _member_exists(tar, member_name):
                            errors.append(f"Backup archive is missing {member_name}")
                            continue
                        member = tar.getmember(member_name)
                        expected_size = int(file_entry.get("size_bytes") or 0)
                        if expected_size and int(member.size or 0) != expected_size:
                            errors.append(f"Size mismatch for {member_name}")

                    if not _extract_member_to_path(tar, "finance.db", extracted_db):
                        errors.append("Backup archive is missing finance.db")
            except tarfile.TarError as exc:
                errors.append(f"Backup archive could not be opened: {exc}")

            sqlcipher_db = bool(manifest.get("sqlcipher_db"))
            manifest_migration_ver = int(manifest.get("migration_version") or 0)
            extracted_migration_ver = manifest_migration_ver if sqlcipher_db else 0
            if extracted_db.exists() and not sqlcipher_db:
                try:
                    integrity_result = _verify_integrity(
                        extracted_db, user_id=decrypt_user_id
                    )
                    if integrity_result.lower() != "ok":
                        errors.append(
                            f"Restored DB integrity check failed: {integrity_result}"
                        )
                    with _open_backup_db_for_read(
                        extracted_db, user_id=decrypt_user_id
                    ) as extracted_conn:
                        extracted_migration_ver = _get_migration_ver(extracted_conn)
                except (
                    FinanceCLIError,
                    RuntimeError,
                    ValueError,
                    sqlite3.Error,
                ) as exc:
                    errors.append(f"Backup database could not be opened: {exc}")

            if (
                extracted_db.exists()
                and not sqlcipher_db
                and manifest_migration_ver != extracted_migration_ver
            ):
                errors.append(
                    "Manifest migration version does not match embedded database "
                    f"({manifest_migration_ver} != {extracted_migration_ver})"
                )

            current_migration_ver: int | None = None
            if conn is not None:
                current_migration_ver = _get_migration_ver(conn)
            else:
                current_db_path = get_db_path().expanduser().resolve()
                if current_db_path.exists():
                    try:
                        with connect(
                            current_db_path,
                            user_id=requested_user_id or _default_user_id(),
                        ) as current_conn:
                            current_migration_ver = _get_migration_ver(current_conn)
                    except (FinanceCLIError, RuntimeError, ValueError, sqlite3.Error):
                        current_migration_ver = None

            compare_migration_ver = (
                manifest_migration_ver if sqlcipher_db else extracted_migration_ver
            )
            if (
                current_migration_ver is not None
                and compare_migration_ver != current_migration_ver
            ):
                warnings.append(
                    f"Backup migration version {compare_migration_ver} differs from current database {current_migration_ver}"
                )

        if not errors and conn is not None:
            conn.execute(
                """
                UPDATE backup_log
                   SET signature_verified_at = ?
                 WHERE bundle_path = ?
                   AND bundle_format_version = ?
                """,
                (
                    _utc_now_iso(),
                    str(resolved_bundle_path),
                    int(header.get("version") or 2),
                ),
            )

        return VerifyResult(
            valid=not errors, manifest=manifest, errors=errors, warnings=warnings
        )

    if not tarfile.is_tarfile(resolved_bundle_path):
        return VerifyResult(
            valid=False,
            manifest=manifest,
            errors=[f"Backup is not a valid tar archive: {resolved_bundle_path}"],
            warnings=warnings,
        )

    with tempfile.TemporaryDirectory(prefix="verify_backup_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        extracted_db = temp_dir / "finance.db"

        try:
            with tarfile.open(resolved_bundle_path, "r:gz") as tar:
                try:
                    manifest = _read_manifest_from_tar(tar)
                except ValueError as exc:
                    errors.append(str(exc))
                    return VerifyResult(
                        valid=False, manifest=manifest, errors=errors, warnings=warnings
                    )

                if manifest.get("version") != 1:
                    errors.append(
                        f"Unsupported backup manifest version: {manifest.get('version')!r}"
                    )

                manifest_files = manifest.get("files")
                if not isinstance(manifest_files, list):
                    errors.append("Backup manifest files list is invalid")
                    manifest_files = []

                for file_entry in manifest_files:
                    if not isinstance(file_entry, dict):
                        errors.append("Backup manifest contains an invalid file entry")
                        continue
                    member_name = str(file_entry.get("path", "")).strip()
                    if not member_name:
                        errors.append(
                            "Backup manifest contains a file entry without a path"
                        )
                        continue
                    if not _member_exists(tar, member_name):
                        errors.append(f"Backup archive is missing {member_name}")
                        continue
                    member = tar.getmember(member_name)
                    fileobj = tar.extractfile(member)
                    if fileobj is None:
                        errors.append(f"Backup archive could not read {member_name}")
                        continue
                    actual_sha = _sha256_fileobj(fileobj)
                    expected_sha = str(file_entry.get("sha256", "")).strip()
                    if actual_sha != expected_sha:
                        errors.append(f"Checksum mismatch for {member_name}")
                    expected_size = int(file_entry.get("size_bytes") or 0)
                    if expected_size and int(member.size or 0) != expected_size:
                        errors.append(f"Size mismatch for {member_name}")

                if not _extract_member_to_path(tar, "finance.db", extracted_db):
                    errors.append("Backup archive is missing finance.db")
        except tarfile.TarError as exc:
            errors.append(f"Backup archive could not be opened: {exc}")

        extracted_migration_ver = 0
        if extracted_db.exists():
            try:
                integrity_result = _verify_integrity(extracted_db)
                if integrity_result.lower() != "ok":
                    errors.append(
                        f"Restored DB integrity check failed: {integrity_result}"
                    )
                with _open_backup_db_for_read(extracted_db) as extracted_conn:
                    extracted_migration_ver = _get_migration_ver(extracted_conn)
            except (FinanceCLIError, sqlite3.Error) as exc:
                errors.append(f"Backup database could not be opened: {exc}")

        manifest_migration_ver = int(manifest.get("migration_version") or 0)
        if extracted_db.exists() and manifest_migration_ver != extracted_migration_ver:
            errors.append(
                "Manifest migration version does not match embedded database "
                f"({manifest_migration_ver} != {extracted_migration_ver})"
            )

        current_migration_ver: int | None = None
        if conn is not None:
            current_migration_ver = _get_migration_ver(conn)
        else:
            current_db_path = get_db_path().expanduser().resolve()
            if current_db_path.exists():
                try:
                    with connect(
                        current_db_path,
                        user_id=requested_user_id or _default_user_id(),
                    ) as current_conn:
                        current_migration_ver = _get_migration_ver(current_conn)
                except (FinanceCLIError, sqlite3.Error):
                    current_migration_ver = None

        if (
            current_migration_ver is not None
            and extracted_migration_ver != current_migration_ver
        ):
            warnings.append(
                f"Backup migration version {extracted_migration_ver} differs from current database {current_migration_ver}"
            )

    return VerifyResult(
        valid=not errors, manifest=manifest, errors=errors, warnings=warnings
    )


def restore_backup(
    bundle_path: Path,
    *,
    conn: sqlite3.Connection | None = None,
    target_db_path: Path | None = None,
    target_data_dir: Path | None = None,
    dry_run: bool = True,
    data_dir: Path | None = None,
    rules_path: Path | None = None,
    expected_user_id: str | None = None,
    user_id: str | None = None,
) -> RestoreResult:
    """Restore the current workspace from a bundled backup."""

    resolved_bundle_path = bundle_path.expanduser().resolve()
    requested_user_id = _provided_user_id(user_id) or _provided_user_id(
        expected_user_id
    )
    resolved_data_dir = (
        target_data_dir.expanduser().resolve()
        if target_data_dir is not None
        else _normalize_data_dir(data_dir)
    )
    resolved_data_dir.mkdir(parents=True, exist_ok=True)
    resolved_db_path = (target_db_path or get_db_path()).expanduser().resolve()
    resolved_rules_path = (
        rules_path.expanduser().resolve()
        if rules_path is not None
        else (resolved_data_dir / "rules.yaml").resolve()
    )
    encrypted_bundle: bytes | None = None
    bundle_header: dict[str, Any] | None = None
    if _is_encrypted_bundle(resolved_bundle_path):
        encrypted_bundle = resolved_bundle_path.read_bytes()
        bundle_header = backup_crypto.parse_bundle_header_any_version(encrypted_bundle)
        header_user_id = str(bundle_header.get("user_id") or "").strip()
        guard_user_id = _provided_user_id(expected_user_id) or requested_user_id
        if guard_user_id and header_user_id != guard_user_id:
            raise TenantMismatchError(
                "Backup belongs to a different user",
                expected_user_id=str(guard_user_id),
                actual_user_id=header_user_id,
                db_path=str(resolved_bundle_path),
                reason="header_mismatch",
            )
        requested_user_id = requested_user_id or header_user_id

    verify_result = verify_backup(
        resolved_bundle_path, conn=conn, user_id=requested_user_id
    )
    if not verify_result.valid:
        raise ValueError(
            "Backup verification failed: " + "; ".join(verify_result.errors)
        )

    if verify_result.manifest.get("schema_version") == 2:
        manifest_user_id = str(verify_result.manifest.get("user_id") or "")
        if expected_user_id and manifest_user_id != str(expected_user_id):
            raise TenantMismatchError(
                "Backup belongs to a different user",
                expected_user_id=str(expected_user_id),
                actual_user_id=manifest_user_id,
                reason="manifest_mismatch",
            )

    manifest_members = _member_names(verify_result.manifest)
    warnings = list(verify_result.warnings)

    if dry_run:
        warnings.append(f"Would overwrite {resolved_db_path}")
        if "rules.yaml" in manifest_members:
            warnings.append(f"Would overwrite {resolved_rules_path}")
        if "agent_memory.md" in manifest_members:
            warnings.append(f"Would overwrite {resolved_data_dir / 'agent_memory.md'}")
        if any(member.startswith("sessions/") for member in manifest_members):
            warnings.append(f"Would overwrite {resolved_data_dir / 'sessions'}")
        return RestoreResult(
            restored=False,
            dry_run=True,
            bundle_path=resolved_bundle_path,
            manifest=verify_result.manifest,
            warnings=warnings,
        )

    lock_context = (
        acquire_install_lock_for_restore(install_subscriber_lock_path())
        if is_canonical_install_db_path(resolved_db_path)
        else nullcontext()
    )
    with lock_context:
        pre_restore_entry: dict | None = None
        restore_started_at = time.perf_counter()
        if conn is not None:
            pre_restore_result = create_backup(
                conn,
                backup_type="pre_restore",
                data_dir=resolved_data_dir,
                rules_path=resolved_rules_path,
                user_id=requested_user_id,
            )
            pre_restore_entry = _latest_sidecar_entry(
                resolved_data_dir,
                bundle_path=pre_restore_result.bundle_path,
                backup_type="pre_restore",
            )
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.close()

        for stale_path in (
            resolved_db_path.with_name(f"{resolved_db_path.name}-wal"),
            resolved_db_path.with_name(f"{resolved_db_path.name}-shm"),
        ):
            try:
                stale_path.unlink()
            except FileNotFoundError:
                continue

        resolved_db_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="restore_backup_", dir=str(resolved_db_path.parent)
        ) as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            staged_db_path = temp_dir / f"{resolved_db_path.name}.restore"
            decrypted_tar_path = temp_dir / "bundle.tar.gz"

            if encrypted_bundle is not None:
                decrypt_user_id = requested_user_id or str(
                    bundle_header.get("user_id") or ""
                )
                tar_bytes, bundle_header = backup_crypto.decrypt_bundle_any_version(
                    encrypted_bundle, decrypt_user_id
                )
                decrypted_tar_path.write_bytes(tar_bytes)
                tar_path = decrypted_tar_path
                tar_mode = "r:gz"
            else:
                tar_path = resolved_bundle_path
                tar_mode = "r:gz"

            with tarfile.open(tar_path, tar_mode) as tar:
                if not _extract_member_to_path(tar, "finance.db", staged_db_path):
                    raise ValueError("Backup archive is missing finance.db")

                if (
                    isinstance(bundle_header, dict)
                    and int(bundle_header.get("version") or 0) == 3
                    and _member_exists(tar, "recovery/db-dek.enc")
                ):
                    blob = _read_member_bytes(tar, "recovery/db-dek.enc")
                    if blob:
                        install_user_id = str(
                            verify_result.manifest.get("user_id")
                            or requested_user_id
                            or ""
                        )
                        try:
                            crypto_envelope.install_db_dek_blob(
                                install_user_id,
                                blob,
                                data_dir=_envelope_data_dir(
                                    resolved_data_dir, install_user_id
                                ),
                            )
                        except NotImplementedError:
                            log.info(
                                "Recovery db-dek install is not implemented in Phase 1; skipped"
                            )
                            warnings.append(
                                "Recovery db-dek install is not implemented in Phase 1; skipped"
                            )

                if expected_user_id is not None:
                    with connect(
                        staged_db_path,
                        expected_user_id=str(expected_user_id),
                        user_id=requested_user_id,
                    ):
                        pass

                resolved_db_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staged_db_path, resolved_db_path)

                if "rules.yaml" in manifest_members:
                    _extract_member_to_path(tar, "rules.yaml", resolved_rules_path)

                if "agent_memory.md" in manifest_members:
                    _extract_member_to_path(
                        tar, "agent_memory.md", resolved_data_dir / "agent_memory.md"
                    )

                session_members = sorted(
                    member
                    for member in manifest_members
                    if member.startswith("sessions/")
                )
                if session_members:
                    sessions_target = resolved_data_dir / "sessions"
                    shutil.rmtree(sessions_target, ignore_errors=True)
                    for member_name in session_members:
                        _extract_member_to_path(
                            tar, member_name, resolved_data_dir / member_name
                        )

        restored_conn_kwargs: dict[str, str] = {}
        if expected_user_id is not None:
            restored_conn_kwargs["expected_user_id"] = str(expected_user_id)
        if requested_user_id is not None:
            restored_conn_kwargs["user_id"] = requested_user_id
        with connect(resolved_db_path, **restored_conn_kwargs) as restored_conn:
            integrity_result = _verify_integrity(
                resolved_db_path,
                user_id=requested_user_id,
                data_dir=(
                    _envelope_data_dir(resolved_data_dir, requested_user_id)
                    if requested_user_id is not None
                    else None
                ),
            )
            if integrity_result.lower() != "ok":
                raise ValueError(
                    f"Restored DB integrity check failed: {integrity_result}"
                )

            restore_manifest_user_id = str(
                verify_result.manifest.get("user_id") or requested_user_id or ""
            )
            restore_bundle_format_version = (
                int(bundle_header.get("version") or 0) if bundle_header else 1
            )
            if restore_bundle_format_version <= 0:
                restore_bundle_format_version = (
                    2 if verify_result.manifest.get("schema_version") == 2 else 1
                )
            restore_signature = verify_result.manifest.get("signature")
            restore_db_sha256 = ""
            if verify_result.manifest.get("schema_version") == 2:
                restore_db_sha256 = _sha256(resolved_db_path)
            else:
                restore_db_sha256 = str(verify_result.manifest.get("db_sha256", ""))
            restore_entry = {
                "backup_type": "restore",
                "status": "completed",
                "bundle_path": str(resolved_bundle_path),
                "bundle_sha256": _sha256(resolved_bundle_path),
                "bundle_size": int(resolved_bundle_path.stat().st_size),
                "db_sha256": restore_db_sha256,
                "migration_ver": _get_migration_ver(restored_conn),
                "duration_ms": int((time.perf_counter() - restore_started_at) * 1000),
                "bundle_format_version": restore_bundle_format_version,
                "signature_verified_at": _utc_now_iso()
                if restore_bundle_format_version in {2, 3}
                else None,
                "bundle_id": verify_result.manifest.get("bundle_id"),
                "user_id": restore_manifest_user_id,
                "signing_key_secret_ref": (
                    str(restore_signature.get("key_ref") or "").strip()
                    if isinstance(restore_signature, dict)
                    else None
                ),
                "dek_secret_ref": bundle_header.get("dek_secret_ref")
                if isinstance(bundle_header, dict)
                else None,
                "mode": bundle_header.get("mode")
                if isinstance(bundle_header, dict)
                else None,
                "recovery_db_dek_present": (
                    bundle_header.get("recovery_db_dek_present")
                    if isinstance(bundle_header, dict)
                    else None
                ),
                "created_at": _utc_now_iso(),
            }
            if pre_restore_entry is not None:
                _log_to_db(restored_conn, pre_restore_entry)
            _log_to_db(restored_conn, restore_entry)
            restored_conn.commit()

    _append_sidecar(restore_entry, data_dir=resolved_data_dir)

    return RestoreResult(
        restored=True,
        dry_run=False,
        bundle_path=resolved_bundle_path,
        manifest=verify_result.manifest,
        warnings=warnings,
    )


def prune_backups(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = True,
    data_dir: Path | None = None,
    user_id: str | None = None,
) -> PruneResult:
    """Apply daily/weekly/monthly retention to completed backup bundles."""

    resolved_data_dir = _normalize_data_dir(data_dir)
    sidecar_entries = _read_sidecar(resolved_data_dir)
    entries = [
        entry
        for entry in _sidecar_completed_entries(sidecar_entries)
        if entry.get("backup_type") in {"local", "offhost"}
    ]
    entries.sort(key=_entry_sort_key, reverse=True)

    today = datetime.now(timezone.utc).date()
    kept_buckets: set[tuple[str, str]] = set()
    to_delete: list[dict] = []

    for entry in entries:
        created_at = _parse_created_at(entry.get("created_at"))
        if created_at is None:
            continue
        age_days = max((today - created_at.date()).days, 0)
        if age_days < 7:
            bucket = ("daily", created_at.date().isoformat())
        elif age_days <= 30:
            sunday = created_at.date() - timedelta(days=(created_at.weekday() + 1) % 7)
            bucket = ("weekly", sunday.isoformat())
        elif age_days <= 365:
            bucket = ("monthly", f"{created_at.year:04d}-{created_at.month:02d}")
        else:
            bucket = None

        if bucket is None:
            to_delete.append(entry)
            continue
        if bucket in kept_buckets:
            to_delete.append(entry)
            continue
        kept_buckets.add(bucket)

    deleted_paths: list[str] = []
    freed_bytes = 0
    scheduled_key_deletions = 0
    for entry in to_delete:
        bundle_path = str(entry.get("bundle_path", "")).strip()
        if not bundle_path:
            continue
        deleted_paths.append(bundle_path)
        size_bytes = int(entry.get("bundle_size") or 0)
        candidate_path = Path(bundle_path).expanduser()
        if candidate_path.exists():
            try:
                size_bytes = int(candidate_path.stat().st_size)
            except OSError:
                pass
        freed_bytes += size_bytes

        if dry_run:
            continue

        if candidate_path.exists():
            try:
                candidate_path.unlink()
            except FileNotFoundError:
                pass
        if int(entry.get("bundle_format_version") or 1) == 2:
            entry_user_id = str(entry.get("user_id") or user_id or "").strip()
            bundle_id = str(entry.get("bundle_id") or "").strip()
            if entry_user_id and bundle_id:
                try:
                    backup_crypto.delete_bundle_key(entry_user_id, bundle_id)
                    scheduled_key_deletions += 1
                except Exception:
                    log.warning(
                        "Failed scheduling bundle key deletion user_id=%s bundle_id=%s",
                        entry_user_id,
                        bundle_id,
                        exc_info=True,
                    )
        conn.execute(
            "DELETE FROM backup_log WHERE bundle_path = ? AND backup_type = ?",
            (bundle_path, entry.get("backup_type")),
        )
        _append_sidecar(
            {
                "action": "pruned",
                "bundle_path": bundle_path,
                "backup_type": entry.get("backup_type"),
                "pruned_at": _utc_now_iso(),
            },
            data_dir=resolved_data_dir,
        )

    if not dry_run:
        conn.commit()

    return PruneResult(
        dry_run=dry_run,
        kept=max(len(entries) - len(to_delete), 0),
        deleted=len(to_delete),
        deleted_paths=deleted_paths,
        freed_bytes=freed_bytes,
        scheduled_key_deletions=scheduled_key_deletions,
    )
