"""One-shot KMS4 P2 migration: existing users' DB DEKs from SM → db-dek.enc envelope.

Per `docs/planning/PLAN_SEC_KMS4_VAULT_MIGRATION.md` §8.2, P2 provisions
`db-dek.enc` (KMS-encrypted blob wrapping the SQLCipher DEK) for every user.
The deployed `provision_user()` flow handles new users automatically. This
script handles existing users that were created pre-P2.

This script does not soft-delete the legacy SM `db-key` entry during normal
migration. SM cleanup is deferred until BOTH the server-side and the local CLI
side have provisioned their own `db-dek.enc` blobs (each wrapping the SAME
plaintext DEK obtained from SM) and the deployment's durability gate passes.
Premature SM deletion would break whichever side hasn't yet synced/migrated.

Operational sequence for a multi-side deployment:

    1. Run on the server: ``python3 -m finance_cli.scripts.migrate_db_dek_to_vault``
    2. Force a sync on the local CLI so the server-provisioned ``db-dek.enc``
       lands locally via ``SYNCED_SIDECAR_FILES`` + ``install_db_dek_blob``.
       Alternatively, run this script on the local CLI as well — the same
       SM-resolved DEK gets wrapped into a fresh local ``db-dek.enc``.
    3. Verify both sides successfully open their finance.db via the vault.
    4. After confidence (e.g., 24h normal operation) and durability gate
       verification, schedule SM soft-delete with ``aws secretsmanager
       delete-secret`` (7-day recovery window). Or re-run this script with
       ``--cleanup-sm`` to do the gated soft-delete.

Idempotent. Re-running on a user that already has ``db-dek.enc`` is a no-op
(reports ``skip``). Safe to invoke as part of operational runbooks.

Usage::

    # Dry run — report what would change without applying
    python3 -m finance_cli.scripts.migrate_db_dek_to_vault --dry-run

    # Migrate every user under FINANCE_WEB_DATA_ROOT
    python3 -m finance_cli.scripts.migrate_db_dek_to_vault

    # Migrate a single user
    python3 -m finance_cli.scripts.migrate_db_dek_to_vault --user-id 1

    # Soft-delete SM db-key entries for users that already have db-dek.enc
    # (run this only after BOTH sides are confirmed migrated)
    python3 -m finance_cli.scripts.migrate_db_dek_to_vault --cleanup-sm

Exit codes:
    0  all targets succeeded (or were already migrated)
    1  one or more targets failed or were blocked; SM entries left intact
    2  argument or environment error
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from finance_cli import crypto_envelope, db_keys, secrets_backend
from finance_cli.backup import can_hard_delete_db_dek_sm
from finance_cli.db import connect


def _resolve_data_root(explicit: str | None) -> Path:
    raw = explicit or os.getenv("FINANCE_WEB_DATA_ROOT") or os.getenv("FINANCE_CLI_DATA_DIR")
    if not raw:
        raise SystemExit(
            "error: --data-root not given and neither FINANCE_WEB_DATA_ROOT nor "
            "FINANCE_CLI_DATA_DIR is set"
        )
    resolved = Path(raw).expanduser().resolve()
    if not resolved.exists():
        raise SystemExit(f"error: data root does not exist: {resolved}")
    return resolved


def _list_user_ids(data_root: Path) -> list[str]:
    """Return sorted user_ids that have a finance.db file under data_root/<user_id>/."""
    out: list[str] = []
    for child in sorted(data_root.iterdir()):
        if child.is_dir() and (child / "finance.db").exists():
            out.append(child.name)
    return out


def _verify_db_open(*, data_root: Path, user_id: str) -> None:
    """Open the user's SQLCipher DB to confirm the vault-resolved DEK works.

    Uses the standard ``connect`` helper, which calls ``db_keys.get_user_db_key``
    internally — that's now vault-first per P2, so this verifies the new path.
    """
    db_path = data_root / user_id / "finance.db"
    with connect(db_path=db_path, expected_user_id=user_id):
        pass


def _format_deleted_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _legacy_sm_db_key_state(user_id: str) -> tuple[str, str]:
    """Return ``(state, detail)`` for the legacy SM db-key.

    States: ``active``, ``pending-delete``, ``missing``, ``error``.
    """

    ref = db_keys.user_db_key_secret_ref(user_id)
    try:
        described = secrets_backend.describe_secret(ref, missing_ok=True)
    except Exception as exc:
        return ("error", f"could not describe SM db-key {ref}: {type(exc).__name__}: {exc}")
    if described is None:
        return ("missing", f"legacy SM db-key {ref} is already missing")
    deleted_at = described.get("DeletedDate")
    if deleted_at is not None:
        return (
            "pending-delete",
            f"legacy SM db-key {ref} is already scheduled for deletion at {_format_deleted_date(deleted_at)}",
        )
    return ("active", f"legacy SM db-key {ref} is active")


def _cleanup_gate_allows_sm_delete(*, user_id: str, data_root: Path) -> tuple[bool, str]:
    db_path = data_root / user_id / "finance.db"
    if not db_path.exists():
        return (False, f"no finance.db at expected path: {db_path}")
    try:
        with connect(db_path=db_path, expected_user_id=user_id) as conn:
            allowed = can_hard_delete_db_dek_sm(
                user_id,
                conn=conn,
                data_dir=data_root / user_id,
            )
    except Exception as exc:
        return (False, f"durability gate check failed: {type(exc).__name__}: {exc}")
    if not allowed:
        return (False, "durability gate did not pass")
    return (True, "durability gate passed")


def migrate_user(*, user_id: str, data_root: Path, dry_run: bool) -> tuple[str, str]:
    """Provision db-dek.enc for one user. Returns ``(status, detail)``."""
    db_path = data_root / user_id / "finance.db"
    if not db_path.exists():
        return ("skip", "no finance.db at expected path")

    if crypto_envelope.has_db_dek(user_id, data_dir=data_root):
        return ("skip", "db-dek.enc already exists")

    if dry_run:
        return ("would-migrate", "would provision db-dek.enc from legacy SM DEK")

    try:
        legacy_dek = db_keys.get_user_db_key(user_id)
    except Exception as exc:
        return ("error", f"could not fetch DEK from SM: {type(exc).__name__}: {exc}")

    try:
        crypto_envelope.provision_db_dek(user_id, dek=legacy_dek, data_dir=data_root)
    except Exception as exc:
        return ("error", f"provision_db_dek failed: {type(exc).__name__}: {exc}")

    try:
        readback = crypto_envelope.get_db_dek(user_id, data_dir=data_root)
    except Exception as exc:
        return ("error", f"vault read-back failed: {type(exc).__name__}: {exc}")
    if readback != legacy_dek:
        return ("error", "vault DEK does not match SM DEK (refusing to proceed)")

    try:
        _verify_db_open(data_root=data_root, user_id=user_id)
    except Exception as exc:
        return ("error", f"DB open with vault DEK failed: {type(exc).__name__}: {exc}")

    return ("migrated", "db-dek.enc provisioned; SM entry untouched")


def cleanup_sm_user(*, user_id: str, data_root: Path, dry_run: bool) -> tuple[str, str]:
    """Soft-delete the legacy SM db-key entry for a user, only if vault exists.

    Refuses to delete if ``db-dek.enc`` is missing — that would break the user.
    Default 7-day recovery window via ``db_keys.delete_user_db_key``.
    """
    if not crypto_envelope.has_db_dek(user_id, data_dir=data_root):
        return ("skip", "no db-dek.enc — refusing to soft-delete SM (would break user)")

    state, detail = _legacy_sm_db_key_state(user_id)
    if state == "error":
        return ("error", detail)
    if state in {"missing", "pending-delete"}:
        return ("skip", detail)

    gate_ok, gate_detail = _cleanup_gate_allows_sm_delete(user_id=user_id, data_root=data_root)
    if not gate_ok:
        return ("blocked", f"{detail}; refusing to soft-delete SM: {gate_detail}")

    if dry_run:
        return (
            "would-cleanup",
            f"{detail}; {gate_detail}; would schedule SM db-key for soft-delete (7d recovery)",
        )

    try:
        db_keys.delete_user_db_key(user_id)
    except Exception as exc:
        return ("error", f"SM soft-delete failed: {type(exc).__name__}: {exc}")

    return ("cleaned-up", "SM db-key scheduled for soft-delete (7d recovery window)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate existing users' DB DEKs from Secrets Manager to db-dek.enc.",
        epilog="See module docstring for the full operational sequence.",
    )
    parser.add_argument("--data-root", help="Per-user data root (default: $FINANCE_WEB_DATA_ROOT or $FINANCE_CLI_DATA_DIR)")
    parser.add_argument("--user-id", help="Migrate this user only (default: all users under data root)")
    parser.add_argument("--dry-run", action="store_true", help="Report planned actions without applying")
    parser.add_argument(
        "--cleanup-sm",
        action="store_true",
        help="Schedule SM db-key entries for soft-delete (only for users that already have db-dek.enc)",
    )
    args = parser.parse_args()

    if not os.getenv("FINANCE_CLI_KMS_KEY_ARN"):
        print("error: FINANCE_CLI_KMS_KEY_ARN must be set", file=sys.stderr)
        return 2

    data_root = _resolve_data_root(args.data_root)
    if args.user_id:
        target_users = [args.user_id]
    else:
        target_users = _list_user_ids(data_root)
        if not target_users:
            print(f"No users found under {data_root}", file=sys.stderr)
            return 1

    mode = "cleanup-sm" if args.cleanup_sm else "migrate"
    print(f"data_root: {data_root}")
    print(f"users:     {len(target_users)} ({', '.join(target_users)})")
    print(f"mode:      {mode}")
    print(f"dry_run:   {args.dry_run}")
    print()

    counts: dict[str, int] = {}
    error_count = 0
    for user_id in target_users:
        if args.cleanup_sm:
            status, detail = cleanup_sm_user(user_id=user_id, data_root=data_root, dry_run=args.dry_run)
        else:
            status, detail = migrate_user(user_id=user_id, data_root=data_root, dry_run=args.dry_run)
        counts[status] = counts.get(status, 0) + 1
        if status in {"error", "blocked"}:
            error_count += 1
        markers = {
            "migrated": "✓",
            "would-migrate": "→",
            "skip": "·",
            "cleaned-up": "✓",
            "would-cleanup": "→",
            "blocked": "!",
            "error": "✗",
        }
        marker = markers.get(status, "?")
        print(f"  {marker} user={user_id} {status}: {detail}")

    print()
    print(f"Summary: {counts}")
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
