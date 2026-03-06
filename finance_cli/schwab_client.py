"""Schwab brokerage balance sync client for finance_cli."""

from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import logging
import os
import sqlite3
import sys
import types
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from .models import dollars_to_cents
from .provider_routing import check_provider_allowed

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchwabConfigStatus:
    configured: bool
    has_sdk: bool
    missing_env: list[str]
    token_path: str
    token_exists: bool


class _NoopLogRedactor:
    def register(self, _string: Any, _label: Any) -> None:
        return None

    def redact(self, msg: Any) -> str:
        return str(msg)


def _load_schwab_auth_module() -> Any:
    """Load schwab.auth without importing schwab.__init__ (which imports streaming)."""
    module_name = "schwab.auth"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing

    pkg_name = "schwab"
    pkg_module = sys.modules.get(pkg_name)
    if pkg_module is None:
        pkg_spec = importlib.util.find_spec(pkg_name)
        if pkg_spec is None or not pkg_spec.origin:
            raise ImportError("schwab package not found")

        pkg_dir = Path(pkg_spec.origin).resolve().parent
        pkg_module = types.ModuleType(pkg_name)
        pkg_module.__file__ = str(pkg_dir / "__init__.py")
        pkg_module.__package__ = pkg_name
        pkg_module.__path__ = [str(pkg_dir)]  # type: ignore[attr-defined]
        pkg_module.__spec__ = importlib.machinery.ModuleSpec(
            name=pkg_name,
            loader=None,
            is_package=True,
        )
        pkg_module.LOG_REDACTOR = _NoopLogRedactor()
        sys.modules[pkg_name] = pkg_module

    pkg_paths = getattr(pkg_module, "__path__", None)
    if not pkg_paths:
        raise ImportError("schwab package path unavailable")

    auth_path = Path(pkg_paths[0]) / "auth.py"
    spec = importlib.util.spec_from_file_location(module_name, auth_path)
    if spec is None or spec.loader is None:
        raise ImportError("could not load schwab.auth module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    setattr(pkg_module, "auth", module)
    return module


def _load_json_response(response: Any) -> Any:
    if response is None:
        return None
    if isinstance(response, (dict, list)):
        return response
    body = getattr(response, "body", None)
    if body is not None:
        return body
    if hasattr(response, "json"):
        try:
            return response.json()
        except Exception:
            return None
    return None


def _has_schwab_sdk() -> bool:
    try:
        import schwab  # noqa: F401

        return True
    except Exception:
        return False


def _token_path() -> str:
    return os.path.expanduser(os.getenv("SCHWAB_TOKEN_PATH", "~/.schwab_token.json"))


def _mask_account_number(account_number: str) -> str:
    digits = "".join(ch for ch in str(account_number or "") if ch.isdigit())
    if not digits:
        return "****"
    return f"****{digits[-4:]}"


def _sanitize_sync_error(message: str, account_number: str, account_hash: str) -> str:
    text = str(message or "")
    if account_number:
        text = text.replace(account_number, _mask_account_number(account_number))
    if account_hash:
        text = text.replace(account_hash, "<account_hash>")
    return text


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def config_status() -> SchwabConfigStatus:
    missing_env = [name for name in ("SCHWAB_APP_KEY", "SCHWAB_APP_SECRET") if not os.getenv(name)]
    has_sdk = _has_schwab_sdk()
    token_path = _token_path()
    token_exists = os.path.exists(token_path)
    return SchwabConfigStatus(
        configured=not missing_env and has_sdk and token_exists,
        has_sdk=has_sdk,
        missing_env=missing_env,
        token_path=token_path,
        token_exists=token_exists,
    )


def _client_from_token_file() -> Any:
    app_key = os.getenv("SCHWAB_APP_KEY")
    app_secret = os.getenv("SCHWAB_APP_SECRET")
    if not app_key or not app_secret:
        raise ValueError("Missing SCHWAB_APP_KEY or SCHWAB_APP_SECRET in environment")

    token_path = _token_path()
    if not os.path.exists(token_path):
        raise FileNotFoundError(
            f"Schwab token file not found at {token_path}. Run `python3 run_schwab.py login`."
        )

    auth = _load_schwab_auth_module()
    try:
        return auth.client_from_token_file(
            token_path=token_path,
            api_key=app_key,
            app_secret=app_secret,
            enforce_enums=False,
        )
    except TypeError:
        return auth.client_from_token_file(token_path, app_key, app_secret)


def _get_account_hashes(client: Any) -> dict[str, str]:
    payload = _load_json_response(client.get_account_numbers())
    rows = payload if isinstance(payload, list) else []
    mapping: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        account_number = str(row.get("accountNumber") or row.get("account_number") or "").strip()
        account_hash = str(row.get("hashValue") or row.get("hash_value") or "").strip()
        if account_number and account_hash:
            mapping[account_number] = account_hash
    return mapping


def _schwab_account_id(account_number: str) -> str:
    normalized = str(account_number or "").strip()
    if not normalized:
        raise ValueError("account_number is required")
    return sha256(f"schwab:{normalized}".encode("utf-8")).hexdigest()[:32]


def _extract_portfolio_value(payload: dict[str, Any] | None) -> float | None:
    if not isinstance(payload, dict):
        return None

    balances = payload.get("currentBalances") or {}
    if isinstance(balances, dict):
        liquidation_value = _to_float_or_none(balances.get("liquidationValue"))
        if liquidation_value is not None:
            return liquidation_value

    total = 0.0
    has_value = False
    positions = payload.get("positions")
    if isinstance(positions, list):
        for position in positions:
            if not isinstance(position, dict):
                continue
            market_value = _to_float_or_none(position.get("marketValue"))
            if market_value is None:
                continue
            total += market_value
            has_value = True

    if isinstance(balances, dict):
        cash_balance = _to_float_or_none(balances.get("cashBalance"))
        if cash_balance is not None:
            total += cash_balance
            has_value = True

    return total if has_value else None


def check_token_health() -> dict[str, Any]:
    token_path = _token_path()
    health: dict[str, Any] = {
        "token_path": token_path,
        "token_file_exists": os.path.exists(token_path),
        "token_permissions_octal": None,
        "token_age_seconds": None,
        "refresh_token_expires_at": None,
        "refresh_token_days_remaining": None,
        "near_refresh_expiry": False,
        "warnings": [],
    }
    if not os.path.exists(token_path):
        health["warnings"].append("Token file missing. Run `python3 run_schwab.py login`.")
        return health

    token_blob: dict[str, Any] = {}
    try:
        with open(token_path, "r", encoding="utf-8") as handle:
            token_blob = json.load(handle)
    except Exception as exc:
        health["warnings"].append(f"Could not parse token file JSON: {exc}")

    try:
        file_mode = os.stat(token_path).st_mode & 0o777
        health["token_permissions_octal"] = f"{file_mode:04o}"
        if file_mode & 0o077:
            health["warnings"].append(
                "Token file permissions are too open; restrict to owner-only (chmod 600)."
            )
    except Exception:
        pass

    try:
        client = _client_from_token_file()
        token_age = getattr(client, "token_age", None)
        if token_age is not None:
            if callable(token_age):
                token_age = token_age()
            if isinstance(token_age, timedelta):
                health["token_age_seconds"] = token_age.total_seconds()
            else:
                health["token_age_seconds"] = float(token_age)
    except Exception as exc:
        health["warnings"].append(f"Client health check failed: {exc}")

    try:
        mtime = os.path.getmtime(token_path)
        created_dt = datetime.fromtimestamp(mtime, tz=UTC)
        blob_ts = token_blob.get("creation_timestamp")
        if blob_ts is not None:
            blob_dt = datetime.fromtimestamp(float(blob_ts), tz=UTC)
            if blob_dt > created_dt:
                created_dt = blob_dt

        refresh_expiry = created_dt + timedelta(days=7)
        remaining_days = (refresh_expiry - datetime.now(tz=UTC)).total_seconds() / 86400
        health["refresh_token_expires_at"] = refresh_expiry.isoformat()
        health["refresh_token_days_remaining"] = round(remaining_days, 2)
        if remaining_days <= 1.0:
            health["near_refresh_expiry"] = True
            health["warnings"].append(
                "Refresh token near expiry (<=1 day). Re-run `python3 run_schwab.py login` soon."
            )
    except Exception:
        pass

    return health


def sync_schwab_balances(conn: sqlite3.Connection) -> dict[str, Any]:
    allowed, designated = check_provider_allowed(conn, "Charles Schwab", "schwab")
    if not allowed:
        logger.info("Schwab balance sync skipped designated_provider=%s", designated)
        return {
            "accounts_requested": 0,
            "accounts_synced": 0,
            "snapshots_upserted": 0,
            "accounts_failed": 0,
            "total_value_cents": 0,
            "accounts": [],
            "errors": [],
            "skipped_reason": f"institution routed to {designated}",
        }

    client = _client_from_token_file()
    account_hashes = _get_account_hashes(client)
    result: dict[str, Any] = {
        "accounts_requested": len(account_hashes),
        "accounts_synced": 0,
        "snapshots_upserted": 0,
        "accounts_failed": 0,
        "total_value_cents": 0,
        "accounts": [],
        "errors": [],
    }

    for account_number, account_hash in sorted(account_hashes.items()):
        masked_account = _mask_account_number(account_number)
        conn.execute("SAVEPOINT schwab_sync_account")
        try:
            try:
                response = client.get_account(account_hash, fields=["positions"])
            except TypeError:
                response = client.get_account(account_hash)

            payload = _load_json_response(response) or {}
            account_payload = payload.get("securitiesAccount") if isinstance(payload, dict) else None
            if not isinstance(account_payload, dict) and isinstance(payload, dict):
                account_root = payload.get("account")
                if isinstance(account_root, dict):
                    account_payload = account_root.get("securitiesAccount")
            if not isinstance(account_payload, dict):
                raise ValueError("missing securitiesAccount payload")

            portfolio_value_dollars = _extract_portfolio_value(account_payload)
            if portfolio_value_dollars is None:
                raise ValueError("missing portfolio value")

            balance_current_cents = dollars_to_cents(portfolio_value_dollars)
            account_id = _schwab_account_id(account_number)
            account_name = f"Brokerage {masked_account}"

            conn.execute(
                """
                INSERT INTO accounts (
                    id, institution_name, account_name, account_type, source,
                    balance_current_cents, iso_currency_code, balance_updated_at, is_active
                ) VALUES (?, 'Charles Schwab', ?, 'investment', 'schwab', ?, 'USD', datetime('now'), 1)
                ON CONFLICT(id) DO UPDATE SET
                    balance_current_cents = excluded.balance_current_cents,
                    balance_updated_at = excluded.balance_updated_at,
                    is_active = 1,
                    source = 'schwab',
                    account_name = excluded.account_name
                """,
                (account_id, account_name, balance_current_cents),
            )

            conn.execute(
                """
                INSERT INTO balance_snapshots (
                    id, account_id, balance_current_cents, source, snapshot_date
                ) VALUES (?, ?, ?, 'refresh', date('now'))
                ON CONFLICT(account_id, snapshot_date, source) DO UPDATE SET
                    balance_current_cents = excluded.balance_current_cents
                """,
                (uuid.uuid4().hex, account_id, balance_current_cents),
            )

            conn.execute("RELEASE SAVEPOINT schwab_sync_account")
            result["accounts_synced"] += 1
            result["snapshots_upserted"] += 1
            result["total_value_cents"] += balance_current_cents
            result["accounts"].append(
                {
                    "account_id": account_id,
                    "account_name": account_name,
                    "account_masked": masked_account,
                    "balance_current_cents": balance_current_cents,
                }
            )
            logger.info(
                "Schwab balance synced account=%s balance_current_cents=%s",
                masked_account,
                balance_current_cents,
            )
        except Exception as exc:
            conn.execute("ROLLBACK TO SAVEPOINT schwab_sync_account")
            conn.execute("RELEASE SAVEPOINT schwab_sync_account")
            message = _sanitize_sync_error(str(exc), account_number, account_hash)
            result["accounts_failed"] += 1
            result["errors"].append({"account": masked_account, "error": message})
            logger.warning("Schwab balance sync failed account=%s error=%s", masked_account, message)

    return result
