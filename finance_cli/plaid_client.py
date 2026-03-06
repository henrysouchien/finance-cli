"""Plaid integration boundary for finance_cli.

This module owns all external Plaid API calls and the corresponding local state
transitions in SQLite. Keep API/DB contracts explicit here so command handlers
can stay thin.

Primary references:
- `docs/plaid/PLAID_API_REFERENCE.md` for endpoint payloads and product behavior.
- `docs/architecture/DESIGN.md` and `docs/overview/PROJECT_GUIDE.md` for
  local schema and lifecycle rules.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from typing import Any, Callable

from .categorizer import map_plaid_pfc_to_category, match_transaction
from .models import dollars_to_cents
from .provider_routing import check_provider_allowed

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlaidConfigStatus:
    configured: bool
    has_sdk: bool
    missing_env: list[str]
    env: str | None


class PlaidUnavailableError(RuntimeError):
    """Raised when Plaid operations are requested but setup is incomplete."""


class PlaidSyncError(RuntimeError):
    """Raised when Plaid sync fails."""


SUPPORTED_PLAID_LINK_PRODUCTS = ("transactions", "liabilities", "investments")
_COOLDOWN_DEFAULTS = {"sync": 300, "balance": 600, "liabilities": 3600, "investments": 300}
_COOLDOWN_COLUMN_BY_TYPE = {
    "sync": "last_sync_at",
    "balance": "last_balance_refresh_at",
    "liabilities": "last_liabilities_fetch_at",
    "investments": "last_investment_sync_at",
}
# PFC detailed codes that represent investment income when seen on investment accounts.
# Plaid sometimes tags brokerage dividends as TRANSFER_IN rather than INCOME.
_INVESTMENT_INCOME_PFC_CODES: frozenset[str] = frozenset({
    "TRANSFER_IN_CASH_ADVANCES_AND_LOANS",
})

# Plaid investment transaction subtype -> (category_name, is_payment)
_INVESTMENT_SUBTYPE_MAP: dict[str, tuple[str, bool]] = {
    # Income
    "dividend": ("Income: Other", False),
    "qualified dividend": ("Income: Other", False),
    "non-qualified dividend": ("Income: Other", False),
    "interest": ("Income: Other", False),
    "interest receivable": ("Income: Other", False),
    "long-term capital gain": ("Income: Other", False),
    "short-term capital gain": ("Income: Other", False),
    "return of principal": ("Income: Other", False),
    # Sells (proceeds — is_payment=True since it's internal portfolio movement,
    # the cash stays in the brokerage account)
    "sell": ("Payments & Transfers", True),
    "sell short": ("Payments & Transfers", True),
    # Fees
    "account fee": ("Bank Charges & Fees", False),
    "fund fee": ("Bank Charges & Fees", False),
    "management fee": ("Bank Charges & Fees", False),
    "transfer fee": ("Bank Charges & Fees", False),
    "trust fee": ("Bank Charges & Fees", False),
    "legal fee": ("Bank Charges & Fees", False),
    "miscellaneous fee": ("Bank Charges & Fees", False),
    "margin expense": ("Bank Charges & Fees", False),
    # Buys/reinvestments (internal movement)
    "buy": ("Payments & Transfers", True),
    "buy to cover": ("Payments & Transfers", True),
    "dividend reinvestment": ("Payments & Transfers", True),
    "interest reinvestment": ("Payments & Transfers", True),
    # Transfers/contributions
    "contribution": ("Payments & Transfers", True),
    "deposit": ("Payments & Transfers", True),
    "withdrawal": ("Payments & Transfers", True),
    "transfer": ("Payments & Transfers", True),
    # Tax-related
    "tax": ("Bank Charges & Fees", False),
    "tax withheld": ("Bank Charges & Fees", False),
    # Other activity
    "adjustment": ("Payments & Transfers", True),
    "distribution": ("Income: Other", False),
    "stock distribution": ("Income: Other", False),
    "spin off": ("Payments & Transfers", True),
    "split": ("Payments & Transfers", True),
    "merger": ("Payments & Transfers", True),
    "exercise": ("Payments & Transfers", True),
    "expire": ("Payments & Transfers", True),
    "assignment": ("Payments & Transfers", True),
    "rebalance": ("Payments & Transfers", True),
    "loan payment": ("Payments & Transfers", True),
    "pending credit": ("Payments & Transfers", True),
    "pending debit": ("Payments & Transfers", True),
    "send": ("Payments & Transfers", True),
}

# Type-level fallbacks for subtypes not in the map
_INVESTMENT_TYPE_MAP: dict[str, tuple[str, bool]] = {
    "buy": ("Payments & Transfers", True),
    "sell": ("Payments & Transfers", True),
    "cancel": ("Payments & Transfers", True),
    "cash": ("Payments & Transfers", True),
    "fee": ("Bank Charges & Fees", False),
    "transfer": ("Payments & Transfers", True),
}


def _get_cooldown_seconds(call_type: str) -> int:
    """Resolve cooldown duration from environment with safe fallbacks."""
    env_map = {
        "sync": "PLAID_SYNC_COOLDOWN",
        "balance": "PLAID_BALANCE_COOLDOWN",
        "liabilities": "PLAID_LIABILITIES_COOLDOWN",
        "investments": "PLAID_INVESTMENTS_COOLDOWN",
    }
    env_name = env_map.get(call_type, "")
    env_value = os.environ.get(env_name, "")
    if env_value.strip():
        try:
            return max(0, int(env_value))
        except ValueError:
            return _COOLDOWN_DEFAULTS.get(call_type, 300)
    return _COOLDOWN_DEFAULTS.get(call_type, 300)


def _plaid_items_column_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(plaid_items)").fetchall()
    }


def _item_within_cooldown(
    conn: sqlite3.Connection,
    plaid_item_id: str,
    call_type: str,
    cooldown_seconds: int,
    plaid_item_columns: set[str] | None = None,
) -> tuple[bool, str | None]:
    """Return whether an item is fresh enough to skip an API call."""
    col = _COOLDOWN_COLUMN_BY_TYPE[call_type]
    columns = plaid_item_columns if plaid_item_columns is not None else _plaid_items_column_names(conn)
    if col not in columns:
        # Backward compatibility for pre-cooldown schemas.
        return (False, None)

    try:
        row = conn.execute(
            f"SELECT {col} FROM plaid_items WHERE plaid_item_id = ?",
            (plaid_item_id,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such column" in str(exc).lower():
            return (False, None)
        raise

    if not row or row[0] is None:
        return (False, None)

    last_fetched = str(row[0])
    is_fresh = conn.execute(
        "SELECT datetime(?) > datetime('now', ? || ' seconds')",
        (last_fetched, f"-{cooldown_seconds}"),
    ).fetchone()[0]
    return (bool(is_fresh), last_fetched)


def _touch_item_cooldown(
    conn: sqlite3.Connection,
    plaid_item_id: str,
    call_type: str,
    plaid_item_columns: set[str] | None = None,
) -> None:
    """Set per-item cooldown timestamp when schema supports it."""
    col = _COOLDOWN_COLUMN_BY_TYPE[call_type]
    columns = plaid_item_columns if plaid_item_columns is not None else _plaid_items_column_names(conn)
    if col not in columns:
        return
    conn.execute(
        f"UPDATE plaid_items SET {col} = datetime('now') WHERE plaid_item_id = ?",
        (plaid_item_id,),
    )


def _has_plaid_sdk() -> bool:
    try:
        import plaid  # noqa: F401

        return True
    except Exception:
        return False


def _has_boto3() -> bool:
    try:
        import boto3  # noqa: F401

        return True
    except Exception:
        return False


def config_status() -> PlaidConfigStatus:
    required = ["PLAID_CLIENT_ID", "PLAID_SECRET", "PLAID_ENV"]
    missing = [name for name in required if not os.getenv(name)]
    return PlaidConfigStatus(
        configured=len(missing) == 0,
        has_sdk=_has_plaid_sdk(),
        missing_env=missing,
        env=os.getenv("PLAID_ENV"),
    )


def _coerce_product_names(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    products: list[str] = []
    for raw in values:
        text = str(raw or "").strip().lower()
        if text:
            products.append(text)
    return products


def resolve_requested_products(
    requested_products: list[str] | None = None,
    include_balance: bool = False,
    include_liabilities: bool = False,
) -> list[str]:
    """Normalize CLI product intent into a Link-safe `products` list.

    Notes:
    - `balance` is intentionally ignored because Plaid Balance is implicit and
      must not be sent in Link `products`.
    - `transactions` is always included and forced to first position.
    """
    normalized: list[str] = []
    seen: set[str] = set()

    def _add(product_name: str) -> None:
        clean = str(product_name or "").strip().lower()
        if not clean:
            return
        if clean == "balance":
            # Balance is implicit and must not be sent in Link products.
            return
        if clean not in SUPPORTED_PLAID_LINK_PRODUCTS:
            raise PlaidSyncError(
                f"Unsupported Plaid product '{clean}'. Supported products: {', '.join(SUPPORTED_PLAID_LINK_PRODUCTS)}"
            )
        if clean in seen:
            return
        normalized.append(clean)
        seen.add(clean)

    for value in requested_products or []:
        _add(value)

    if include_balance:
        _add("balance")
    if include_liabilities:
        _add("liabilities")

    _add("transactions")
    if "transactions" in normalized:
        normalized = ["transactions"] + [name for name in normalized if name != "transactions"]

    return normalized


def _parse_stored_products(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return _coerce_product_names(raw)
    text = str(raw).strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _coerce_product_names(payload)


def _extract_products_from_item_payload(item_payload: dict[str, Any]) -> list[str]:
    item = item_payload.get("item")
    if not isinstance(item, dict):
        item = {}

    for field in ("consented_products", "billed_products", "products"):
        products = _coerce_product_names(item.get(field))
        if products:
            # Preserve order from Plaid payload while removing duplicates.
            deduped: list[str] = []
            seen: set[str] = set()
            for product in products:
                if product in seen:
                    continue
                seen.add(product)
                deduped.append(product)
            return deduped
    return []


def _slugify_institution(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "unknown-institution"


def _looks_like_email(value: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value))


def sanitize_client_user_id(user_id: str) -> str:
    """Normalize `client_user_id` into a Plaid-safe, non-PII identifier."""
    raw = str(user_id or "").strip()
    if not raw:
        raise PlaidSyncError("client user id is required")

    if _looks_like_email(raw):
        digest = sha256(raw.lower().encode("utf-8")).hexdigest()
        return f"user_{digest[:32]}"

    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", raw).strip("._-")
    if not sanitized:
        digest = sha256(raw.encode("utf-8")).hexdigest()
        sanitized = f"user_{digest[:32]}"
    return sanitized[:128]


def _flat_secret_name(user_id: str, institution: str) -> str:
    return f"plaid_token_{user_id}_{_slugify_institution(institution)}"


def _legacy_path_secret_name(user_id: str, institution: str) -> str:
    return f"plaid/access_token/{user_id}/{_slugify_institution(institution)}"


def secret_name_candidates(user_id: str, institution: str) -> list[str]:
    # Support both key formats to preserve compatibility across tools/environments.
    return [_flat_secret_name(user_id, institution), _legacy_path_secret_name(user_id, institution)]


def _flat_item_secret_name(user_id: str, item_id: str) -> str:
    return f"plaid_token_{user_id}_item_{_slugify_institution(item_id)}"


def _legacy_item_path_secret_name(user_id: str, item_id: str) -> str:
    return f"plaid/access_token/{user_id}/item/{_slugify_institution(item_id)}"


def secret_name_candidates_for_item(user_id: str, item_id: str) -> list[str]:
    """Return deterministic per-item secret names (flat + legacy path)."""
    return [_flat_item_secret_name(user_id, item_id), _legacy_item_path_secret_name(user_id, item_id)]


def _aws_region(region_name: str | None = None) -> str:
    region = region_name or os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    if not region:
        raise PlaidUnavailableError("AWS region missing (set AWS_DEFAULT_REGION or AWS_REGION)")
    return region


def _boto_secrets_client(region_name: str | None = None):
    if not _has_boto3():
        raise PlaidUnavailableError("boto3 not installed")
    import boto3

    return boto3.session.Session().client("secretsmanager", region_name=_aws_region(region_name))


def secret_name_for_institution(user_id: str, institution: str) -> str:
    return secret_name_candidates(user_id, institution)[0]


def _secret_exists(client, secret_name: str) -> bool:
    try:
        client.describe_secret(SecretId=secret_name)
        return True
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code == "ResourceNotFoundException":
            return False
        raise


def _put_or_create_secret(client, secret_name: str, payload: dict[str, Any]) -> None:
    try:
        client.put_secret_value(SecretId=secret_name, SecretString=json.dumps(payload))
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code != "ResourceNotFoundException":
            raise
        client.create_secret(Name=secret_name, SecretString=json.dumps(payload))


def store_plaid_token(
    user_id: str,
    institution: str,
    access_token: str,
    item_id: str,
    region_name: str | None = None,
    secret_name: str | None = None,
    secret_names: list[str] | None = None,
) -> str:
    candidate_names = [name for name in (secret_names or []) if str(name).strip()]
    if not candidate_names:
        candidate_names = [secret_name] if secret_name else secret_name_candidates(user_id, institution)
    payload = {
        "access_token": access_token,
        "item_id": item_id,
        "institution": institution,
        "user_id": user_id,
    }

    client = _boto_secrets_client(region_name)

    chosen_name = candidate_names[0]
    for name in candidate_names:
        if _secret_exists(client, name):
            chosen_name = name
            break

    _put_or_create_secret(client, chosen_name, payload)

    # Keep alternate key formats synchronized as best-effort aliases.
    for alias_name in candidate_names:
        if alias_name == chosen_name:
            continue
        try:
            _put_or_create_secret(client, alias_name, payload)
        except Exception:
            # Alias update should not block primary token storage.
            pass

    return chosen_name


def get_secret_payload(secret_name: str, region_name: str | None = None) -> dict[str, Any]:
    client = _boto_secrets_client(region_name)
    response = client.get_secret_value(SecretId=secret_name)
    raw = response.get("SecretString")
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {"access_token": raw}
    if isinstance(value, dict):
        return value
    return {"access_token": str(value)}


def delete_secret(secret_name: str, region_name: str | None = None) -> None:
    client = _boto_secrets_client(region_name)
    try:
        client.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code == "ResourceNotFoundException":
            return
        raise


def list_user_tokens(user_id: str, region_name: str | None = None) -> list[str]:
    legacy_prefix = f"plaid/access_token/{user_id}/"
    flat_prefix = f"plaid_token_{user_id}_"
    client = _boto_secrets_client(region_name)

    tokens: list[str] = []
    paginator = client.get_paginator("list_secrets")
    for page in paginator.paginate():
        for secret in page.get("SecretList", []):
            name = str(secret.get("Name") or "")
            if name.startswith(legacy_prefix) or name.startswith(flat_prefix):
                tokens.append(name)
    return tokens


def _create_plaid_api_client():
    status = config_status()
    if not status.has_sdk:
        raise PlaidUnavailableError("plaid-python not installed")
    if not status.configured:
        raise PlaidUnavailableError("Missing env: " + ",".join(status.missing_env))

    from plaid import ApiClient, Configuration, Environment
    from plaid.api import plaid_api
    import certifi

    env = str(os.getenv("PLAID_ENV", "production")).capitalize()
    host = getattr(Environment, env, None)
    if host is None:
        raise PlaidUnavailableError(f"Unsupported PLAID_ENV '{os.getenv('PLAID_ENV')}'")

    config = Configuration(
        host=host,
        api_key={
            "clientId": os.getenv("PLAID_CLIENT_ID"),
            "secret": os.getenv("PLAID_SECRET"),
        },
        ssl_ca_cert=certifi.where(),
    )
    return plaid_api.PlaidApi(ApiClient(config))


def _remove_remote_item(access_token: str) -> None:
    client = _create_plaid_api_client()
    from plaid.model.item_remove_request import ItemRemoveRequest

    client.item_remove(ItemRemoveRequest(access_token=access_token))


def _extract_error_code(exc: Exception) -> str | None:
    code = getattr(exc, "error_code", None)
    if code:
        return str(code)

    body = getattr(exc, "body", None)
    if not body:
        return None

    try:
        payload = json.loads(body)
    except Exception:
        return None

    value = payload.get("error_code")
    return str(value) if value else None


def _extract_error_message(exc: Exception) -> str:
    body = getattr(exc, "body", None)
    if not body:
        return str(exc)
    try:
        payload = json.loads(body)
    except Exception:
        return str(exc)
    return str(payload.get("error_message") or payload.get("display_message") or exc)


def list_plaid_items(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, plaid_item_id, institution_name, status, error_code,
               consented_products, access_token_ref, sync_cursor,
               last_sync_at, last_balance_refresh_at, last_liabilities_fetch_at,
               created_at, updated_at
          FROM plaid_items
         ORDER BY created_at DESC
        """
    ).fetchall()

    items = []
    for row in rows:
        item = dict(row)
        item["has_token_ref"] = bool(item.get("access_token_ref"))
        items.append(item)
    return items


def _parse_token_payload(secret_payload: dict[str, Any]) -> tuple[str, str | None]:
    token = str(secret_payload.get("access_token") or "").strip()
    item_id = secret_payload.get("item_id")
    return token, str(item_id) if item_id else None


def _get_access_token_for_item(item_row: sqlite3.Row | dict, region_name: str | None = None) -> str:
    access_token_ref = (item_row["access_token_ref"] if isinstance(item_row, sqlite3.Row) else item_row.get("access_token_ref"))
    plaid_item_id = (item_row["plaid_item_id"] if isinstance(item_row, sqlite3.Row) else item_row.get("plaid_item_id"))
    if not access_token_ref:
        raise PlaidSyncError("plaid item has no access_token_ref")

    payload = get_secret_payload(str(access_token_ref), region_name=region_name)
    token, payload_item_id = _parse_token_payload(payload)
    item_id_text = str(plaid_item_id or "").strip()
    if payload_item_id and item_id_text and payload_item_id != item_id_text:
        raise PlaidSyncError(
            f"secret token item mismatch for {item_id_text}: secret references {payload_item_id}"
        )
    if not token:
        raise PlaidSyncError("secret payload missing access_token")
    return token


def _account_type_from_plaid(account: dict | None) -> str:
    if not account:
        return "checking"

    plaid_type = str(account.get("type") or "").lower()
    subtype = str(account.get("subtype") or "").lower()

    if plaid_type == "depository":
        if subtype == "savings":
            return "savings"
        return "checking"
    if plaid_type == "credit":
        return "credit_card"
    if plaid_type == "investment":
        return "investment"
    if plaid_type == "loan":
        return "loan"
    return "checking"


def _to_cents_or_none(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return dollars_to_cents(value)
    except Exception:
        return None


def _to_iso_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text[:10]


def _to_bool_int(value: Any) -> int | None:
    if value is None:
        return None
    return 1 if bool(value) else 0


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _extract_balance_fields(account_payload: dict | None) -> dict[str, Any]:
    if not isinstance(account_payload, dict):
        return {
            "balance_current_cents": None,
            "balance_available_cents": None,
            "balance_limit_cents": None,
            "iso_currency_code": None,
            "unofficial_currency_code": None,
            "has_balance_data": False,
            "has_balance_amounts": False,
        }

    balances = account_payload.get("balances") or {}
    if not isinstance(balances, dict):
        balances = {}

    current_cents = _to_cents_or_none(balances.get("current"))
    available_cents = _to_cents_or_none(balances.get("available"))
    limit_cents = _to_cents_or_none(balances.get("limit"))
    iso_currency_code = str(balances.get("iso_currency_code") or "").strip() or None
    unofficial_currency_code = str(balances.get("unofficial_currency_code") or "").strip() or None

    return {
        "balance_current_cents": current_cents,
        "balance_available_cents": available_cents,
        "balance_limit_cents": limit_cents,
        "iso_currency_code": iso_currency_code,
        "unofficial_currency_code": unofficial_currency_code,
        "has_balance_data": any(
            value is not None
            for value in (current_cents, available_cents, limit_cents, iso_currency_code, unofficial_currency_code)
        ),
        "has_balance_amounts": any(value is not None for value in (current_cents, available_cents, limit_cents)),
    }


def _record_balance_snapshot(conn: sqlite3.Connection, account_id: str, account_payload: dict | None, source: str) -> bool:
    """Upsert one daily balance snapshot row.

    Returns `True` only when at least one numeric balance field is present.
    Snapshot dedupe key is `(account_id, snapshot_date, source)`.
    """
    balance = _extract_balance_fields(account_payload)
    if not balance["has_balance_amounts"]:
        return False

    conn.execute(
        """
        INSERT INTO balance_snapshots (
            id,
            account_id,
            balance_current_cents,
            balance_available_cents,
            balance_limit_cents,
            source,
            snapshot_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_id, snapshot_date, source) DO UPDATE SET
            balance_current_cents = excluded.balance_current_cents,
            balance_available_cents = excluded.balance_available_cents,
            balance_limit_cents = excluded.balance_limit_cents
        """,
        (
            uuid.uuid4().hex,
            account_id,
            balance["balance_current_cents"],
            balance["balance_available_cents"],
            balance["balance_limit_cents"],
            source,
            date.today().isoformat(),
        ),
    )
    return True


def _get_or_create_system_category(conn: sqlite3.Connection, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row:
        return str(row["id"])

    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 1)",
        (category_id, name),
    )
    return category_id


def _ensure_default_plaid_categories(conn: sqlite3.Connection) -> None:
    for name in [
        "Home Improvement",
        "Donations",
        "Taxes",
        "Insurance",
        "Childcare",
        "Coffee",
        "Income: Salary",
        "Income: Business",
        "Income: Other",
        "Utilities",
        "Rent",
        "Travel",
        "Transportation",
        "Entertainment",
        "Dining",
        "Groceries",
        "Shopping",
        "Software & Subscriptions",
        "Professional Fees",
        "Health & Wellness",
        "Personal Expense",
        "Bank Charges & Fees",
    ]:
        _get_or_create_system_category(conn, name)


def _selective_raw_plaid_json(transaction: dict) -> str:
    pfc = transaction.get("personal_finance_category") or {}
    location = transaction.get("location") or {}
    counterparties = transaction.get("counterparties") or []

    payload = {
        "merchant_name": transaction.get("merchant_name"),
        "merchant_entity_id": transaction.get("merchant_entity_id"),
        "payment_channel": transaction.get("payment_channel"),
        "personal_finance_category": {
            "primary": pfc.get("primary"),
            "detailed": pfc.get("detailed"),
            "confidence_level": pfc.get("confidence_level"),
            "version": pfc.get("version"),
        },
        "location": {
            "city": location.get("city"),
            "region": location.get("region") or location.get("state"),
        },
        "counterparties": [
            {
                "name": cp.get("name"),
                "type": cp.get("type"),
            }
            for cp in counterparties[:10]
            if isinstance(cp, dict)
        ],
        "logo_url": transaction.get("logo_url"),
        "pending": bool(transaction.get("pending")),
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _json_dumps_safe(value: Any) -> str:
    # Plaid payloads may include date/datetime objects in nested fields.
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _description_from_plaid(transaction: dict) -> str:
    return (
        str(transaction.get("merchant_name") or "").strip()
        or str(transaction.get("name") or "").strip()
        or str(transaction.get("original_description") or "").strip()
        or "Plaid transaction"
    )


def _ensure_account(
    conn: sqlite3.Connection,
    plaid_item_id: str,
    institution_name: str,
    plaid_account_id: str,
    account_payload: dict | None,
) -> str | None:
    """Create/update local `accounts` row for a Plaid account and return local id.

    Contract:
    - Non-balance identity fields are patch-style updates (`COALESCE`) to avoid
      dropping names/masks when partial payloads are returned.
    - Balance/currency fields are authoritative replacement values; `NULL`
      clears stale previously stored balances.
    """
    allowed, designated = check_provider_allowed(conn, institution_name, "plaid")
    if not allowed:
        logger.info(
            "Plaid account upsert skipped institution=%s plaid_account_id=%s designated_provider=%s",
            institution_name,
            plaid_account_id,
            designated,
        )
        return None

    existing = conn.execute(
        "SELECT id, account_type_override FROM accounts WHERE plaid_account_id = ?",
        (plaid_account_id,),
    ).fetchone()

    account_name = None
    card_ending = None
    account_type = "checking"

    if account_payload:
        account_name = str(account_payload.get("name") or account_payload.get("official_name") or "").strip() or None
        card_ending = str(account_payload.get("mask") or "").strip() or None
        account_type = _account_type_from_plaid(account_payload)
    balance = _extract_balance_fields(account_payload)

    if existing:
        if existing["account_type_override"]:
            account_type = None
        account_id = str(existing["id"])
        conn.execute(
            """
            UPDATE accounts
               SET plaid_item_id = ?,
                   source = 'plaid',
                   institution_name = ?,
                   account_name = COALESCE(?, account_name),
                   account_type = COALESCE(?, account_type),
                   card_ending = COALESCE(?, card_ending),
                   balance_current_cents = ?,
                   balance_available_cents = ?,
                   balance_limit_cents = ?,
                   iso_currency_code = ?,
                   unofficial_currency_code = ?,
                   balance_updated_at = CASE WHEN ? THEN datetime('now') ELSE balance_updated_at END,
                   is_active = 1,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (
                plaid_item_id,
                institution_name,
                account_name,
                account_type,
                card_ending,
                balance["balance_current_cents"],
                balance["balance_available_cents"],
                balance["balance_limit_cents"],
                balance["iso_currency_code"],
                balance["unofficial_currency_code"],
                1 if balance["has_balance_data"] else 0,
                account_id,
            ),
        )
        return account_id

    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id,
            plaid_account_id,
            plaid_item_id,
            institution_name,
            account_name,
            account_type,
            card_ending,
            source,
            balance_current_cents,
            balance_available_cents,
            balance_limit_cents,
            iso_currency_code,
            unofficial_currency_code,
            balance_updated_at,
            is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'plaid', ?, ?, ?, ?, ?, CASE WHEN ? THEN datetime('now') ELSE NULL END, 1)
        """,
        (
            account_id,
            plaid_account_id,
            plaid_item_id,
            institution_name,
            account_name,
            account_type,
            card_ending,
            balance["balance_current_cents"],
            balance["balance_available_cents"],
            balance["balance_limit_cents"],
            balance["iso_currency_code"],
            balance["unofficial_currency_code"],
            1 if balance["has_balance_data"] else 0,
        ),
    )
    return account_id


def _apply_upsert_transaction(
    conn: sqlite3.Connection,
    plaid_item: sqlite3.Row | dict,
    transaction: dict,
    account_map: dict[str, dict],
    local_account_ids: dict[str, str | None] | None,
    mode: str,
) -> str:
    plaid_txn_id = str(transaction.get("transaction_id") or "").strip()
    if not plaid_txn_id:
        return "skipped"

    plaid_account_id = str(transaction.get("account_id") or "").strip()
    if not plaid_account_id:
        return "skipped"

    institution = str(plaid_item["institution_name"] if isinstance(plaid_item, sqlite3.Row) else plaid_item.get("institution_name") or "Unknown Institution")
    item_id = str(plaid_item["plaid_item_id"] if isinstance(plaid_item, sqlite3.Row) else plaid_item.get("plaid_item_id"))

    has_cached_account_id = local_account_ids is not None and plaid_account_id in local_account_ids
    account_id = (local_account_ids or {}).get(plaid_account_id)
    if has_cached_account_id and account_id is None:
        return "skipped"

    if not has_cached_account_id:
        account_payload = account_map.get(plaid_account_id)
        account_id = _ensure_account(
            conn,
            plaid_item_id=item_id,
            institution_name=institution,
            plaid_account_id=plaid_account_id,
            account_payload=account_payload,
        )
        if local_account_ids is not None:
            local_account_ids[plaid_account_id] = account_id
        if account_id is None:
            return "skipped"

    amount_cents = -dollars_to_cents(transaction.get("amount") or 0)
    category_name, is_payment_from_pfc = map_plaid_pfc_to_category(transaction.get("personal_finance_category"))
    pfc = transaction.get("personal_finance_category") or {}
    source_category = str(pfc.get("detailed") or "").strip() or None
    # On investment accounts, reclassify known dividend PFC codes as income.
    account_payload = account_map.get(plaid_account_id)
    if (
        _account_type_from_plaid(account_payload) == "investment"
        and str(pfc.get("detailed") or "").strip().upper() in _INVESTMENT_INCOME_PFC_CODES
    ):
        is_payment_from_pfc = False
        category_name = category_name or "Income: Other"

    description = _description_from_plaid(transaction)
    try:
        result = match_transaction(
            conn,
            description,
            use_type=None,
            source_category=source_category,
            is_payment=is_payment_from_pfc,
        )
    except Exception as exc:
        logger.warning("match_transaction() failed for %r: %s", description, exc)
        result = None

    if result and result.category_id:
        category_id = result.category_id
        category_source = result.category_source
        category_confidence = result.category_confidence
        category_rule_id = result.category_rule_id
    else:
        category_id = _get_or_create_system_category(conn, category_name) if category_name else None
        category_source = "plaid" if category_id else None
        category_confidence = 0.3 if category_id else None
        category_rule_id = None

    tx_date = str(transaction.get("date") or "")[:10]
    raw_plaid_json = _selective_raw_plaid_json(transaction)

    existing = conn.execute(
        "SELECT id FROM transactions WHERE plaid_txn_id = ?",
        (plaid_txn_id,),
    ).fetchone()

    if result is not None and result.category_source != "ambiguous":
        is_payment = 1 if result.is_payment else 0
    else:
        is_payment = 1 if is_payment_from_pfc else 0

    if existing:
        conn.execute(
            """
            UPDATE transactions
               SET account_id = ?,
                   date = ?,
                   description = ?,
                   amount_cents = ?,
                   category_id = CASE
                       WHEN category_source IN ('user', 'vendor_memory', 'keyword_rule', 'ai', 'auto_prefix', 'category_mapping')
                       THEN category_id ELSE ? END,
                   source_category = COALESCE(source_category, ?),
                   category_source = CASE
                       WHEN category_source IN ('user', 'vendor_memory', 'keyword_rule', 'ai', 'auto_prefix', 'category_mapping')
                       THEN category_source ELSE ? END,
                   category_confidence = CASE
                       WHEN category_source IN ('user', 'vendor_memory', 'keyword_rule', 'ai', 'auto_prefix', 'category_mapping')
                       THEN category_confidence ELSE ? END,
                   is_payment = ?,
                   raw_plaid_json = ?,
                   source = 'plaid',
                   is_active = 1,
                   removed_at = NULL,
                   updated_at = datetime('now')
             WHERE plaid_txn_id = ?
            """,
            (
                account_id,
                tx_date,
                description,
                amount_cents,
                category_id,
                source_category,
                category_source,
                category_confidence,
                is_payment,
                raw_plaid_json,
                plaid_txn_id,
            ),
        )
        return "modified"

    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id,
            account_id,
            plaid_txn_id,
            dedupe_key,
            date,
            description,
            amount_cents,
            category_id,
            source_category,
            category_source,
            category_confidence,
            category_rule_id,
            is_payment,
            source,
            raw_plaid_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'plaid', ?)
        """,
        (
            txn_id,
            account_id,
            plaid_txn_id,
            f"plaid:{plaid_txn_id}",
            tx_date,
            description,
            amount_cents,
            category_id,
            source_category,
            category_source,
            category_confidence,
            category_rule_id,
            is_payment,
            raw_plaid_json,
        ),
    )
    return "added" if mode == "added" else "modified"


def apply_sync_updates(
    conn: sqlite3.Connection,
    plaid_item: sqlite3.Row | dict,
    added: list[dict],
    modified: list[dict],
    removed: list[dict],
    accounts: list[dict],
    next_cursor: str | None,
) -> dict[str, int]:
    """Apply one `/transactions/sync` batch into local DB.

    Side effects:
    - Upserts all accounts from `accounts[]` first (dormant accounts still get
      fresh balances/snapshots even with no transaction mutations).
    - Applies added/modified/removed transaction deltas.
    - Advances `plaid_items.sync_cursor`.
    """
    account_map = {
        str(account.get("account_id") or ""): account
        for account in accounts
        if isinstance(account, dict)
    }

    counts = {
        "added": 0,
        "modified": 0,
        "removed": 0,
        "skipped": 0,
    }

    institution = str(plaid_item["institution_name"] if isinstance(plaid_item, sqlite3.Row) else plaid_item.get("institution_name") or "Unknown Institution")
    item_id = str(plaid_item["plaid_item_id"] if isinstance(plaid_item, sqlite3.Row) else plaid_item.get("plaid_item_id"))
    local_account_ids: dict[str, str | None] = {}

    # Account upsert pass must run before transaction mutations so balance data
    # is not tied to transaction activity.
    for plaid_account_id, account_payload in account_map.items():
        if not plaid_account_id:
            continue
        account_id = _ensure_account(
            conn,
            plaid_item_id=item_id,
            institution_name=institution,
            plaid_account_id=plaid_account_id,
            account_payload=account_payload,
        )
        local_account_ids[plaid_account_id] = account_id
        if account_id is None:
            continue
        _record_balance_snapshot(conn, account_id, account_payload, source="sync")

    for tx in added:
        status = _apply_upsert_transaction(conn, plaid_item, tx, account_map, local_account_ids, mode="added")
        counts[status] = counts.get(status, 0) + 1

    for tx in modified:
        status = _apply_upsert_transaction(conn, plaid_item, tx, account_map, local_account_ids, mode="modified")
        counts[status] = counts.get(status, 0) + 1

    removed_ids: list[str] = []
    for removed_tx in removed:
        txn_id = str(removed_tx.get("transaction_id") or "").strip()
        if txn_id:
            removed_ids.append(txn_id)
        else:
            counts["skipped"] += 1

    if removed_ids:
        placeholders = ",".join("?" for _ in removed_ids)
        existing_rows = conn.execute(
            f"SELECT plaid_txn_id FROM transactions WHERE plaid_txn_id IN ({placeholders})",
            tuple(removed_ids),
        ).fetchall()
        existing_ids = {str(row["plaid_txn_id"]) for row in existing_rows}

        conn.executemany(
            """
            UPDATE transactions
               SET is_active = 0,
                   removed_at = datetime('now'),
                   updated_at = datetime('now')
             WHERE plaid_txn_id = ?
            """,
            [(txn_id,) for txn_id in removed_ids],
        )

        counts["removed"] += len(existing_ids)
        counts["skipped"] += len(removed_ids) - len(existing_ids)

    conn.execute(
        """
        UPDATE plaid_items
           SET sync_cursor = ?,
               status = 'active',
               error_code = NULL,
               updated_at = datetime('now')
         WHERE plaid_item_id = ?
        """,
        (next_cursor, item_id),
    )

    return counts


def _investment_description(inv_txn: dict, securities_map: dict[str, dict]) -> str:
    security_id = str(inv_txn.get("security_id") or "").strip()
    security = securities_map.get(security_id, {})
    ticker = str(security.get("ticker_symbol") or "").strip()
    security_name = str(security.get("name") or "").strip()
    txn_name = str(inv_txn.get("name") or "").strip()
    subtype = str(inv_txn.get("subtype") or inv_txn.get("type") or "").strip()

    parts = []
    if subtype:
        parts.append(subtype.upper())
    if ticker:
        parts.append(ticker)
    elif security_name:
        parts.append(security_name)
    if txn_name and txn_name not in parts:
        parts.append(txn_name)
    return " - ".join(parts) if parts else "Investment transaction"


def _selective_raw_investment_json(inv_txn: dict, security: dict | None) -> str:
    payload = {
        "investment_transaction_id": inv_txn.get("investment_transaction_id"),
        "type": inv_txn.get("type"),
        "subtype": inv_txn.get("subtype"),
        "quantity": inv_txn.get("quantity"),
        "price": inv_txn.get("price"),
        "fees": inv_txn.get("fees"),
        "security_id": inv_txn.get("security_id"),
    }
    if security:
        payload["security"] = {
            "ticker_symbol": security.get("ticker_symbol"),
            "name": security.get("name"),
            "type": security.get("type"),
            "close_price": security.get("close_price"),
            "cusip": security.get("cusip"),
        }
    return json.dumps(payload, default=str)


def _apply_investment_transaction(
    conn: sqlite3.Connection,
    plaid_item: sqlite3.Row | dict,
    inv_txn: dict,
    securities_map: dict[str, dict],
    account_map: dict[str, dict],
    local_account_ids: dict[str, str | None],
    consumed_crossfeed_ids: set[str] | None = None,  # required for one-to-one dedup; None only for testing individual calls
) -> str:
    inv_txn_id = str(inv_txn.get("investment_transaction_id") or "").strip()
    if not inv_txn_id:
        return "skipped"

    plaid_account_id = str(inv_txn.get("account_id") or "").strip()
    if not plaid_account_id:
        return "skipped"

    # Resolve local account_id (same pattern as _apply_upsert_transaction)
    institution = str(
        plaid_item["institution_name"] if isinstance(plaid_item, sqlite3.Row) else plaid_item.get("institution_name") or "Unknown Institution"
    )
    item_id = str(plaid_item["plaid_item_id"] if isinstance(plaid_item, sqlite3.Row) else plaid_item.get("plaid_item_id"))

    has_cached = local_account_ids is not None and plaid_account_id in local_account_ids
    account_id = (local_account_ids or {}).get(plaid_account_id)
    if has_cached and account_id is None:
        return "skipped"
    if not has_cached:
        account_payload = account_map.get(plaid_account_id)
        account_id = _ensure_account(
            conn,
            plaid_item_id=item_id,
            institution_name=institution,
            plaid_account_id=plaid_account_id,
            account_payload=account_payload,
        )
        if local_account_ids is not None:
            local_account_ids[plaid_account_id] = account_id
        if account_id is None:
            return "skipped"

    # Amount: negate (Plaid positive = outflow -> our negative = expense)
    amount_cents = -dollars_to_cents(inv_txn.get("amount") or 0)

    # Category from investment type map
    inv_type = str(inv_txn.get("type") or "").strip().lower()
    inv_subtype = str(inv_txn.get("subtype") or "").strip().lower()
    source_category = f"investment:{inv_type}:{inv_subtype}" if inv_subtype else f"investment:{inv_type}"

    cat_entry = _INVESTMENT_SUBTYPE_MAP.get(inv_subtype) or _INVESTMENT_TYPE_MAP.get(inv_type)
    if cat_entry:
        default_category_name, default_is_payment = cat_entry
    else:
        default_category_name, default_is_payment = None, False

    # Description
    description = _investment_description(inv_txn, securities_map)

    # Run through standard categorization pipeline
    try:
        result = match_transaction(
            conn,
            description,
            use_type=None,
            source_category=source_category,
            is_payment=default_is_payment,
        )
    except Exception as exc:
        logger.warning("match_transaction() failed for investment %r: %s", description, exc)
        result = None

    if result and result.category_id:
        category_id = result.category_id
        category_source = result.category_source
        category_confidence = result.category_confidence
        category_rule_id = result.category_rule_id
        is_payment = 1 if result.is_payment else 0
    else:
        category_id = _get_or_create_system_category(conn, default_category_name) if default_category_name else None
        category_source = "plaid" if category_id else None
        category_confidence = 0.5 if category_id else None
        category_rule_id = None
        is_payment = 1 if default_is_payment else 0

    tx_date = str(inv_txn.get("date") or "")[:10]
    security = securities_map.get(str(inv_txn.get("security_id") or "").strip())
    raw_json = _selective_raw_investment_json(inv_txn, security)

    # Check for existing by investment_transaction_id
    existing = conn.execute(
        "SELECT id FROM transactions WHERE plaid_txn_id = ?",
        (inv_txn_id,),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE transactions
               SET account_id = ?, date = ?, description = ?, amount_cents = ?,
                   category_id = CASE WHEN category_source IN ('user','vendor_memory','keyword_rule','ai','auto_prefix','category_mapping')
                       THEN category_id ELSE ? END,
                   source_category = COALESCE(source_category, ?),
                   category_source = CASE WHEN category_source IN ('user','vendor_memory','keyword_rule','ai','auto_prefix','category_mapping')
                       THEN category_source ELSE ? END,
                   category_confidence = CASE WHEN category_source IN ('user','vendor_memory','keyword_rule','ai','auto_prefix','category_mapping')
                       THEN category_confidence ELSE ? END,
                   is_payment = ?, raw_plaid_json = ?, source = 'plaid',
                   is_active = 1, removed_at = NULL, updated_at = datetime('now')
             WHERE plaid_txn_id = ?
            """,
            (
                account_id,
                tx_date,
                description,
                amount_cents,
                category_id,
                source_category,
                category_source,
                category_confidence,
                is_payment,
                raw_json,
                inv_txn_id,
            ),
        )
        return "modified"

    # Cross-feed dedup: same account + date + amount from regular /transactions/sync?
    # One-to-one matching: each regular-feed txn can only consume one investment txn.
    # This prevents false-skipping when multiple investment txns have the same date/amount.
    if consumed_crossfeed_ids is None:
        consumed_crossfeed_ids = set()
    cross_dups = conn.execute(
        """
        SELECT id FROM transactions
         WHERE account_id = ? AND date = ? AND amount_cents = ?
           AND plaid_txn_id IS NOT NULL AND plaid_txn_id != ?
           AND is_active = 1
           AND (source_category IS NULL OR source_category NOT LIKE 'investment:%')
        """,
        (account_id, tx_date, amount_cents, inv_txn_id),
    ).fetchall()
    for dup_row in cross_dups:
        dup_id = str(dup_row["id"])
        if dup_id not in consumed_crossfeed_ids:
            consumed_crossfeed_ids.add(dup_id)
            return "skipped"
    # No unconsumed match found — insert normally

    # Insert new
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (id, account_id, plaid_txn_id, dedupe_key, date, description,
            amount_cents, category_id, source_category, category_source, category_confidence,
            category_rule_id, is_payment, source, raw_plaid_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'plaid', ?)
        """,
        (
            txn_id,
            account_id,
            inv_txn_id,
            f"plaid:{inv_txn_id}",
            tx_date,
            description,
            amount_cents,
            category_id,
            source_category,
            category_source,
            category_confidence,
            category_rule_id,
            is_payment,
            raw_json,
        ),
    )
    return "added"


def _fetch_investment_transactions(
    client,
    access_token: str,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
    from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions

    all_transactions: list[dict] = []
    securities_by_id: dict[str, dict] = {}
    accounts_by_id: dict[str, dict] = {}
    offset = 0
    page_size = 100  # Plaid max for /investments/transactions/get

    while True:
        request = InvestmentsTransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=InvestmentsTransactionsGetRequestOptions(count=page_size, offset=offset),
        )
        response = client.investments_transactions_get(request)
        page = response.to_dict() if hasattr(response, "to_dict") else dict(response)

        inv_txns = page.get("investment_transactions") or []
        all_transactions.extend(inv_txns)

        for sec in page.get("securities") or []:
            sec_id = str(sec.get("security_id") or "").strip()
            if sec_id:
                securities_by_id[sec_id] = sec

        for acct in page.get("accounts") or []:
            aid = str(acct.get("account_id") or "").strip()
            if aid:
                accounts_by_id[aid] = acct

        total = page.get("total_investment_transactions", len(all_transactions))
        offset += len(inv_txns)

        if offset >= total or len(inv_txns) < page_size:
            break

    return {
        "investment_transactions": all_transactions,
        "securities": securities_by_id,
        "accounts": accounts_by_id,
        "total": len(all_transactions),
    }


def _sync_investment_transactions(
    conn: sqlite3.Connection,
    client,
    item: sqlite3.Row,
    plaid_item_columns: set[str],
    force_refresh: bool,
    region_name: str | None,
) -> dict[str, Any]:
    plaid_item_id = str(item["plaid_item_id"])
    institution_name = str(item["institution_name"] or "Unknown Institution")

    # Check consented products — if metadata is stale, user can run
    # `plaid backfill-products` to refresh from Plaid's item_get API
    products = _parse_stored_products(item["consented_products"])
    if "investments" not in products:
        return {"status": "skipped_no_product", "added": 0, "modified": 0, "skipped": 0}

    # Cooldown check
    inv_cooldown = _get_cooldown_seconds("investments")
    if not force_refresh and str(item["status"] or "") != "error" and inv_cooldown > 0:
        within_cooldown, _ = _item_within_cooldown(
            conn,
            plaid_item_id,
            "investments",
            inv_cooldown,
            plaid_item_columns,
        )
        if within_cooldown:
            return {"status": "skipped_cooldown", "added": 0, "modified": 0, "skipped": 0}

    access_token = _get_access_token_for_item(item, region_name=region_name)

    # Determine date range
    last_sync_at = None
    if "last_investment_sync_at" in plaid_item_columns:
        row = conn.execute(
            "SELECT last_investment_sync_at FROM plaid_items WHERE plaid_item_id = ?",
            (plaid_item_id,),
        ).fetchone()
        if row and row[0]:
            last_sync_at = str(row[0])

    from datetime import timedelta

    max_history = date.today() - timedelta(days=730)  # Plaid max: 2 years
    if last_sync_at:
        try:
            start_date = max(date.fromisoformat(last_sync_at[:10]) - timedelta(days=7), max_history)
        except (ValueError, TypeError):
            logger.warning("Malformed last_investment_sync_at=%r, falling back to full history", last_sync_at)
            start_date = max_history
    else:
        start_date = max_history
    end_date = date.today()

    # --force resets to full 2-year history for recovery
    if force_refresh:
        start_date = max_history

    # Fetch all pages
    batch = _fetch_investment_transactions(client, access_token, start_date, end_date)

    # Upsert accounts first (for balance data)
    account_map = batch["accounts"]
    securities_map = batch["securities"]
    local_account_ids: dict[str, str | None] = {}

    for plaid_acct_id, account_payload in account_map.items():
        acct_id = _ensure_account(
            conn,
            plaid_item_id=plaid_item_id,
            institution_name=institution_name,
            plaid_account_id=plaid_acct_id,
            account_payload=account_payload,
        )
        local_account_ids[plaid_acct_id] = acct_id
        if acct_id:
            _record_balance_snapshot(conn, acct_id, account_payload, source="sync")

    counts = {"added": 0, "modified": 0, "skipped": 0}
    # Track regular-feed txn IDs already consumed by cross-feed dedup (one-to-one matching)
    consumed_crossfeed_ids: set[str] = set()
    for inv_txn in batch["investment_transactions"]:
        status = _apply_investment_transaction(
            conn,
            item,
            inv_txn,
            securities_map,
            account_map,
            local_account_ids,
            consumed_crossfeed_ids=consumed_crossfeed_ids,
        )
        counts[status] = counts.get(status, 0) + 1

    _touch_item_cooldown(conn, plaid_item_id, "investments", plaid_item_columns=plaid_item_columns)

    return {"status": "synced", **counts, "total_fetched": batch["total"]}


def _transactions_sync_request_payload(
    access_token: str,
    cursor: str | None,
    days_requested: int | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "access_token": access_token,
        "count": 500,
        "options": {
            "include_original_description": True,
        },
    }

    if cursor:
        payload["cursor"] = cursor
    elif days_requested:
        payload["options"]["days_requested"] = max(1, min(days_requested, 730))

    pfc_version = os.getenv("PLAID_PFC_VERSION")
    if pfc_version:
        payload["options"]["personal_finance_category_version"] = pfc_version

    return payload


def _fetch_sync_page(client, access_token: str, cursor: str | None, days_requested: int | None) -> dict[str, Any]:
    payload = _transactions_sync_request_payload(access_token, cursor, days_requested)

    from plaid.model.transactions_sync_request import TransactionsSyncRequest

    request = TransactionsSyncRequest(**payload)
    response = client.transactions_sync(request)
    return response.to_dict() if hasattr(response, "to_dict") else dict(response)


def collect_transactions_sync_pages(
    fetch_page: Callable[[str | None], dict[str, Any]],
    starting_cursor: str | None,
    max_restarts: int = 2,
) -> dict[str, Any]:
    last_mutation_error: Exception | None = None
    for attempt in range(max_restarts + 1):
        cursor = starting_cursor
        all_added: list[dict] = []
        all_modified: list[dict] = []
        all_removed: list[dict] = []
        account_by_id: dict[str, dict] = {}

        try:
            while True:
                page = fetch_page(cursor)
                all_added.extend(page.get("added") or [])
                all_modified.extend(page.get("modified") or [])
                all_removed.extend(page.get("removed") or [])

                for account in page.get("accounts") or []:
                    aid = str(account.get("account_id") or "")
                    if aid:
                        account_by_id[aid] = account

                cursor = page.get("next_cursor") or cursor
                if not page.get("has_more"):
                    return {
                        "added": all_added,
                        "modified": all_modified,
                        "removed": all_removed,
                        "accounts": list(account_by_id.values()),
                        "next_cursor": cursor,
                    }
        except Exception as exc:
            if _extract_error_code(exc) == "TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION" and attempt < max_restarts:
                last_mutation_error = exc
                continue
            if _extract_error_code(exc) == "TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION":
                last_mutation_error = exc
                break
            raise

    if last_mutation_error is not None:
        raise PlaidSyncError("TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION retries exhausted") from last_mutation_error
    raise PlaidSyncError("Failed to complete sync pagination")


def _mark_item_error(conn: sqlite3.Connection, plaid_item_id: str, message: str) -> None:
    conn.execute(
        """
        UPDATE plaid_items
           SET status = 'error',
               error_code = ?,
               updated_at = datetime('now')
         WHERE plaid_item_id = ?
        """,
        (message[:255], plaid_item_id),
    )
    conn.commit()


def run_sync(
    conn: sqlite3.Connection,
    days: int | None = None,
    item_id: str | None = None,
    force_refresh: bool = False,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Sync transactions for active Plaid items via `/transactions/sync`.

    Behavior:
    - Uses stored cursor when present; otherwise requests historical window
      (`days` bounded to Plaid limits).
    - Handles mutation-during-pagination restarts inside page collection.
    - Marks failing items with `status='error'` and preserves per-item errors.
    """
    status = config_status()
    if not status.configured or not status.has_sdk:
        missing_parts = []
        if not status.has_sdk:
            missing_parts.append("plaid-python not installed")
        if status.missing_env:
            missing_parts.append("missing env: " + ",".join(status.missing_env))
        raise PlaidUnavailableError("Plaid sync unavailable: " + "; ".join(missing_parts))

    client = _create_plaid_api_client()

    where = ["status IN ('active', 'pending', 'error')"]
    params: list[Any] = []
    if item_id:
        where.append("plaid_item_id = ?")
        params.append(item_id)

    items = conn.execute(
        f"""
        SELECT *
          FROM plaid_items
         WHERE {' AND '.join(where)}
         ORDER BY created_at ASC
        """,
        tuple(params),
    ).fetchall()

    if not items:
        return {
            "items_requested": 0,
            "items_synced": 0,
            "items_skipped": 0,
            "items_failed": 0,
            "added": 0,
            "modified": 0,
            "removed": 0,
            "total_elapsed_ms": 0,
            "errors": [],
            "items": [],
        }

    totals = {
        "items_requested": len(items),
        "items_synced": 0,
        "items_skipped": 0,
        "items_failed": 0,
        "added": 0,
        "modified": 0,
        "removed": 0,
        "total_elapsed_ms": 0,
        "errors": [],
        "items": [],
    }

    _ensure_default_plaid_categories(conn)
    conn.commit()
    sync_cooldown_seconds = _get_cooldown_seconds("sync")
    plaid_item_columns = _plaid_items_column_names(conn)

    for item in items:
        plaid_item_id = str(item["plaid_item_id"])
        institution_name = str(item["institution_name"] or "Unknown Institution")
        item_result = {
            "plaid_item_id": plaid_item_id,
            "institution_name": institution_name,
            "added": 0,
            "modified": 0,
            "removed": 0,
            "elapsed_ms": 0,
        }
        item_status = str(item["status"] or "")

        if not force_refresh and item_status != "error" and sync_cooldown_seconds > 0:
            within_cooldown, last_sync_at = _item_within_cooldown(
                conn,
                plaid_item_id=plaid_item_id,
                call_type="sync",
                cooldown_seconds=sync_cooldown_seconds,
                plaid_item_columns=plaid_item_columns,
            )
            if within_cooldown:
                logger.info(
                    "Plaid sync skipped by cooldown item_id=%s institution=%s last_sync_at=%s cooldown_seconds=%s",
                    plaid_item_id,
                    institution_name,
                    last_sync_at,
                    sync_cooldown_seconds,
                )
                totals["items_skipped"] += 1
                totals["items"].append(
                    {
                        "plaid_item_id": plaid_item_id,
                        "institution_name": institution_name,
                        "status": "skipped_cooldown",
                        "last_sync_at": last_sync_at,
                        "elapsed_ms": 0,
                    }
                )
                continue

        try:
            item_started_at = time.perf_counter()
            logger.info("Plaid sync starting item_id=%s institution=%s", plaid_item_id, institution_name)
            access_token = _get_access_token_for_item(item, region_name=region_name)

            start_cursor = item["sync_cursor"]

            def fetch_page(cursor_value: str | None) -> dict[str, Any]:
                return _fetch_sync_page(client, access_token, cursor_value, days)

            batch = collect_transactions_sync_pages(fetch_page, starting_cursor=start_cursor)

            counts = apply_sync_updates(
                conn,
                plaid_item=item,
                added=batch["added"],
                modified=batch["modified"],
                removed=batch["removed"],
                accounts=batch["accounts"],
                next_cursor=batch["next_cursor"],
            )
            _touch_item_cooldown(conn, plaid_item_id, "sync", plaid_item_columns=plaid_item_columns)
            conn.commit()

            # Investment transaction sync (non-fatal — regular sync already committed above)
            try:
                inv_result = _sync_investment_transactions(
                    conn,
                    client,
                    item,
                    plaid_item_columns,
                    force_refresh=force_refresh,
                    region_name=region_name,
                )
                if inv_result["status"] == "synced":
                    conn.commit()  # commit investment writes separately
                    item_result["investment_added"] = inv_result.get("added", 0)
                    item_result["investment_modified"] = inv_result.get("modified", 0)
                    totals["added"] += inv_result.get("added", 0)
                    totals["modified"] += inv_result.get("modified", 0)
            except Exception as inv_exc:
                conn.rollback()  # only rolls back uncommitted investment writes; regular sync already committed
                logger.warning("Investment sync failed item=%s: %s", plaid_item_id, _extract_error_message(inv_exc))
                item_result["investment_error"] = _extract_error_message(inv_exc)

            item_result.update(
                {
                    "added": counts.get("added", 0),
                    "modified": counts.get("modified", 0),
                    "removed": counts.get("removed", 0),
                    "next_cursor": batch.get("next_cursor"),
                    "status": "synced",
                    "elapsed_ms": int((time.perf_counter() - item_started_at) * 1000),
                }
            )

            totals["items_synced"] += 1
            totals["added"] += counts.get("added", 0)
            totals["modified"] += counts.get("modified", 0)
            totals["removed"] += counts.get("removed", 0)
            totals["total_elapsed_ms"] += int(item_result["elapsed_ms"])
            totals["items"].append(item_result)
            logger.info(
                "Plaid sync complete item_id=%s institution=%s added=%s modified=%s removed=%s",
                plaid_item_id,
                institution_name,
                counts.get("added", 0),
                counts.get("modified", 0),
                counts.get("removed", 0),
            )
        except Exception as exc:
            conn.rollback()
            message = _extract_error_message(exc)
            _mark_item_error(conn, plaid_item_id, message)
            logger.warning(
                "Plaid sync failed item_id=%s institution=%s error=%s",
                plaid_item_id,
                institution_name,
                message,
            )
            totals["items_failed"] += 1
            totals["errors"].append(
                {
                    "plaid_item_id": plaid_item_id,
                    "institution_name": institution_name,
                    "error": message,
                }
            )
            totals["items"].append(
                {
                    "plaid_item_id": plaid_item_id,
                    "institution_name": institution_name,
                    "status": "failed",
                    "error": message,
                    "elapsed_ms": 0,
                }
            )

    return totals


def refresh_balances(
    conn: sqlite3.Connection,
    item_id: str | None = None,
    force_refresh: bool = False,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Fetch real-time balances via `/accounts/balance/get`.

    Side effects per item:
    - Upserts account balance fields from Plaid response.
    - Upserts `balance_snapshots` rows with `source='refresh'`.
    - Clears item error state on success; marks item error on failure.
    """
    status = config_status()
    if not status.configured or not status.has_sdk:
        missing_parts = []
        if not status.has_sdk:
            missing_parts.append("plaid-python not installed")
        if status.missing_env:
            missing_parts.append("missing env: " + ",".join(status.missing_env))
        raise PlaidUnavailableError("Plaid balance refresh unavailable: " + "; ".join(missing_parts))

    client = _create_plaid_api_client()
    from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest

    where = ["status IN ('active', 'pending', 'error')"]
    params: list[Any] = []
    if item_id:
        where.append("plaid_item_id = ?")
        params.append(item_id)

    items = conn.execute(
        f"""
        SELECT *
          FROM plaid_items
         WHERE {' AND '.join(where)}
         ORDER BY created_at ASC
        """,
        tuple(params),
    ).fetchall()

    if not items:
        return {
            "items_requested": 0,
            "items_refreshed": 0,
            "items_skipped": 0,
            "items_failed": 0,
            "accounts_updated": 0,
            "snapshots_updated": 0,
            "errors": [],
            "items": [],
        }

    totals = {
        "items_requested": len(items),
        "items_refreshed": 0,
        "items_skipped": 0,
        "items_failed": 0,
        "accounts_updated": 0,
        "snapshots_updated": 0,
        "errors": [],
        "items": [],
    }
    balance_cooldown_seconds = _get_cooldown_seconds("balance")
    plaid_item_columns = _plaid_items_column_names(conn)

    for item in items:
        plaid_item_id = str(item["plaid_item_id"])
        institution_name = str(item["institution_name"] or "Unknown Institution")
        item_status = str(item["status"] or "")

        if not force_refresh and item_status != "error" and balance_cooldown_seconds > 0:
            within_cooldown, last_balance_refresh_at = _item_within_cooldown(
                conn,
                plaid_item_id=plaid_item_id,
                call_type="balance",
                cooldown_seconds=balance_cooldown_seconds,
                plaid_item_columns=plaid_item_columns,
            )
            if within_cooldown:
                logger.info(
                    "Plaid balance refresh skipped by cooldown item_id=%s institution=%s last_balance_refresh_at=%s cooldown_seconds=%s",
                    plaid_item_id,
                    institution_name,
                    last_balance_refresh_at,
                    balance_cooldown_seconds,
                )
                totals["items_skipped"] += 1
                totals["items"].append(
                    {
                        "plaid_item_id": plaid_item_id,
                        "institution_name": institution_name,
                        "status": "skipped_cooldown",
                        "last_balance_refresh_at": last_balance_refresh_at,
                    }
                )
                continue

        try:
            access_token = _get_access_token_for_item(item, region_name=region_name)
            response = client.accounts_balance_get(AccountsBalanceGetRequest(access_token=access_token))
            payload = response.to_dict() if hasattr(response, "to_dict") else dict(response)
            accounts = payload.get("accounts") or []

            accounts_updated = 0
            snapshots_updated = 0
            for account_payload in accounts:
                plaid_account_id = str(account_payload.get("account_id") or "").strip()
                if not plaid_account_id:
                    continue
                account_id_local = _ensure_account(
                    conn,
                    plaid_item_id=plaid_item_id,
                    institution_name=institution_name,
                    plaid_account_id=plaid_account_id,
                    account_payload=account_payload,
                )
                if account_id_local is None:
                    continue
                accounts_updated += 1
                if _record_balance_snapshot(conn, account_id_local, account_payload, source="refresh"):
                    snapshots_updated += 1

            conn.execute(
                """
                UPDATE plaid_items
                   SET status = 'active',
                       error_code = NULL,
                       updated_at = datetime('now')
                 WHERE plaid_item_id = ?
                """,
                (plaid_item_id,),
            )
            _touch_item_cooldown(conn, plaid_item_id, "balance", plaid_item_columns=plaid_item_columns)
            conn.commit()

            totals["items_refreshed"] += 1
            totals["accounts_updated"] += accounts_updated
            totals["snapshots_updated"] += snapshots_updated
            totals["items"].append(
                {
                    "plaid_item_id": plaid_item_id,
                    "accounts_updated": accounts_updated,
                    "snapshots_updated": snapshots_updated,
                }
            )
            logger.info(
                "Plaid balance refresh complete item_id=%s institution=%s accounts_updated=%s snapshots_updated=%s",
                plaid_item_id,
                institution_name,
                accounts_updated,
                snapshots_updated,
            )
        except Exception as exc:
            conn.rollback()
            message = _extract_error_message(exc)
            _mark_item_error(conn, plaid_item_id, message)
            logger.warning(
                "Plaid balance refresh failed item_id=%s institution=%s error=%s",
                plaid_item_id,
                institution_name,
                message,
            )
            totals["items_failed"] += 1
            totals["errors"].append({"plaid_item_id": plaid_item_id, "institution_name": institution_name, "error": message})

    return totals


_APR_TYPE_ALIASES = {"cash": "cash_advance"}


def _normalize_apr_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    if normalized.endswith("_apr"):
        normalized = normalized[:-4]
    return _APR_TYPE_ALIASES.get(normalized, normalized)


def _apr_percentage(aprs: Any, apr_type: str) -> float | None:
    if not isinstance(aprs, list):
        return None
    target_apr_type = _normalize_apr_type(apr_type)
    if not target_apr_type:
        return None
    for apr in aprs:
        if not isinstance(apr, dict):
            continue
        if _normalize_apr_type(apr.get("apr_type")) == target_apr_type:
            value = apr.get("apr_percentage")
            try:
                return float(value) if value is not None else None
            except Exception:
                return None
    return None


def _next_monthly_payment_cents(value: Any) -> int | None:
    if isinstance(value, dict):
        for key in ("amount", "total_payment_amount"):
            cents = _to_cents_or_none(value.get(key))
            if cents is not None:
                return cents
        return None
    return _to_cents_or_none(value)


def _normalize_mortgage_rate_type(value: Any) -> str | None:
    clean = str(value or "").strip().lower()
    if clean in {"fixed", "variable"}:
        return clean
    return None


def _upsert_liability(
    conn: sqlite3.Connection,
    account_id: str,
    liability_type: str,
    entry: dict[str, Any],
    sync_ts: str,
) -> None:
    """Upsert one liability row keyed by `(account_id, liability_type)`."""
    loan_status = entry.get("loan_status") or {}
    if not isinstance(loan_status, dict):
        loan_status = {}
    repayment_plan = entry.get("repayment_plan") or {}
    if not isinstance(repayment_plan, dict):
        repayment_plan = {}
    interest_rate = entry.get("interest_rate") or {}
    if not isinstance(interest_rate, dict):
        interest_rate = {}
    property_address = entry.get("property_address") or {}
    if not isinstance(property_address, dict):
        property_address = {}

    conn.execute(
        """
        INSERT INTO liabilities (
            id,
            account_id,
            liability_type,
            is_active,
            last_seen_at,
            is_overdue,
            last_payment_amount_cents,
            last_payment_date,
            last_statement_balance_cents,
            last_statement_issue_date,
            minimum_payment_cents,
            next_payment_due_date,
            apr_purchase,
            apr_balance_transfer,
            apr_cash_advance,
            interest_rate_pct,
            origination_principal_cents,
            outstanding_interest_cents,
            expected_payoff_date,
            loan_name,
            loan_status_type,
            loan_status_end_date,
            repayment_plan_type,
            repayment_plan_description,
            servicer_name,
            ytd_interest_paid_cents,
            ytd_principal_paid_cents,
            mortgage_rate_pct,
            mortgage_rate_type,
            loan_term,
            maturity_date,
            origination_date,
            escrow_balance_cents,
            has_pmi,
            has_prepayment_penalty,
            next_monthly_payment_cents,
            past_due_amount_cents,
            current_late_fee_cents,
            property_address_json,
            raw_plaid_json,
            fetched_at,
            updated_at
        ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(account_id, liability_type) DO UPDATE SET
            is_active = 1,
            last_seen_at = excluded.last_seen_at,
            is_overdue = excluded.is_overdue,
            last_payment_amount_cents = excluded.last_payment_amount_cents,
            last_payment_date = excluded.last_payment_date,
            last_statement_balance_cents = excluded.last_statement_balance_cents,
            last_statement_issue_date = excluded.last_statement_issue_date,
            minimum_payment_cents = excluded.minimum_payment_cents,
            next_payment_due_date = excluded.next_payment_due_date,
            apr_purchase = COALESCE(excluded.apr_purchase, apr_purchase),
            apr_balance_transfer = COALESCE(excluded.apr_balance_transfer, apr_balance_transfer),
            apr_cash_advance = COALESCE(excluded.apr_cash_advance, apr_cash_advance),
            interest_rate_pct = excluded.interest_rate_pct,
            origination_principal_cents = excluded.origination_principal_cents,
            outstanding_interest_cents = excluded.outstanding_interest_cents,
            expected_payoff_date = excluded.expected_payoff_date,
            loan_name = excluded.loan_name,
            loan_status_type = excluded.loan_status_type,
            loan_status_end_date = excluded.loan_status_end_date,
            repayment_plan_type = excluded.repayment_plan_type,
            repayment_plan_description = excluded.repayment_plan_description,
            servicer_name = excluded.servicer_name,
            ytd_interest_paid_cents = excluded.ytd_interest_paid_cents,
            ytd_principal_paid_cents = excluded.ytd_principal_paid_cents,
            mortgage_rate_pct = excluded.mortgage_rate_pct,
            mortgage_rate_type = excluded.mortgage_rate_type,
            loan_term = excluded.loan_term,
            maturity_date = excluded.maturity_date,
            origination_date = excluded.origination_date,
            escrow_balance_cents = excluded.escrow_balance_cents,
            has_pmi = excluded.has_pmi,
            has_prepayment_penalty = excluded.has_prepayment_penalty,
            next_monthly_payment_cents = excluded.next_monthly_payment_cents,
            past_due_amount_cents = excluded.past_due_amount_cents,
            current_late_fee_cents = excluded.current_late_fee_cents,
            property_address_json = excluded.property_address_json,
            raw_plaid_json = excluded.raw_plaid_json,
            fetched_at = datetime('now'),
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            account_id,
            liability_type,
            sync_ts,
            _to_bool_int(entry.get("is_overdue")),
            _to_cents_or_none(entry.get("last_payment_amount")),
            _to_iso_date(entry.get("last_payment_date")),
            _to_cents_or_none(entry.get("last_statement_balance")),
            _to_iso_date(entry.get("last_statement_issue_date")),
            _to_cents_or_none(entry.get("minimum_payment_amount")),
            _to_iso_date(entry.get("next_payment_due_date")),
            _apr_percentage(entry.get("aprs"), "purchase") if liability_type == "credit" else None,
            _apr_percentage(entry.get("aprs"), "balance_transfer") if liability_type == "credit" else None,
            _apr_percentage(entry.get("aprs"), "cash_advance") if liability_type == "credit" else None,
            _to_float_or_none(entry.get("interest_rate_percentage")),
            _to_cents_or_none(entry.get("origination_principal_amount")),
            _to_cents_or_none(entry.get("outstanding_interest_amount")),
            _to_iso_date(entry.get("expected_payoff_date")),
            str(entry.get("loan_name") or "").strip() or None,
            str(loan_status.get("type") or "").strip() or None,
            _to_iso_date(loan_status.get("end_date")),
            str(repayment_plan.get("type") or "").strip() or None,
            str(repayment_plan.get("description") or "").strip() or None,
            str(entry.get("servicer_name") or "").strip() or None,
            _to_cents_or_none(entry.get("ytd_interest_paid")),
            _to_cents_or_none(entry.get("ytd_principal_paid")),
            _to_float_or_none(interest_rate.get("percentage")),
            _normalize_mortgage_rate_type(interest_rate.get("type")),
            str(entry.get("loan_term") or "").strip() or None,
            _to_iso_date(entry.get("maturity_date")),
            _to_iso_date(entry.get("origination_date")),
            _to_cents_or_none(entry.get("escrow_balance")),
            _to_bool_int(entry.get("has_pmi")),
            _to_bool_int(entry.get("has_prepayment_penalty")),
            _next_monthly_payment_cents(entry.get("next_monthly_payment")),
            _to_cents_or_none(entry.get("past_due_amount")),
            _to_cents_or_none(entry.get("current_late_fee")),
            _json_dumps_safe(property_address) if property_address else None,
            _json_dumps_safe(entry),
        ),
    )


def fetch_liabilities(
    conn: sqlite3.Connection,
    item_id: str | None = None,
    force_refresh: bool = False,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Fetch liabilities via `/liabilities/get` and apply lifecycle updates.

    Processing order per item:
    1. Upsert accounts from payload `accounts[]` (balance fields included).
    2. Upsert liabilities and stamp `last_seen_at=sync_ts`.
    3. Deactivate stale liabilities for the item using local account scope.

    The stale-deactivation scope intentionally uses local `accounts` table,
    not response `accounts[]`, so disappeared accounts/liabilities still close.
    """
    status = config_status()
    if not status.configured or not status.has_sdk:
        missing_parts = []
        if not status.has_sdk:
            missing_parts.append("plaid-python not installed")
        if status.missing_env:
            missing_parts.append("missing env: " + ",".join(status.missing_env))
        raise PlaidUnavailableError("Plaid liabilities sync unavailable: " + "; ".join(missing_parts))

    client = _create_plaid_api_client()
    from plaid.model.liabilities_get_request import LiabilitiesGetRequest

    where = ["status IN ('active', 'pending', 'error')"]
    params: list[Any] = []
    if item_id:
        where.append("plaid_item_id = ?")
        params.append(item_id)

    candidate_items = conn.execute(
        f"""
        SELECT *
          FROM plaid_items
         WHERE {' AND '.join(where)}
         ORDER BY created_at ASC
        """,
        tuple(params),
    ).fetchall()

    items = [item for item in candidate_items if "liabilities" in _parse_stored_products(item["consented_products"])]

    if not items:
        return {
            "items_requested": 0,
            "items_synced": 0,
            "items_skipped": 0,
            "items_failed": 0,
            "liabilities_upserted": 0,
            "liabilities_deactivated": 0,
            "errors": [],
            "items": [],
        }

    totals = {
        "items_requested": len(items),
        "items_synced": 0,
        "items_skipped": 0,
        "items_failed": 0,
        "liabilities_upserted": 0,
        "liabilities_deactivated": 0,
        "errors": [],
        "items": [],
    }
    liabilities_cooldown_seconds = _get_cooldown_seconds("liabilities")
    plaid_item_columns = _plaid_items_column_names(conn)

    for item in items:
        plaid_item_id = str(item["plaid_item_id"])
        institution_name = str(item["institution_name"] or "Unknown Institution")
        item_status = str(item["status"] or "")

        if not force_refresh and item_status != "error" and liabilities_cooldown_seconds > 0:
            within_cooldown, last_liabilities_fetch_at = _item_within_cooldown(
                conn,
                plaid_item_id=plaid_item_id,
                call_type="liabilities",
                cooldown_seconds=liabilities_cooldown_seconds,
                plaid_item_columns=plaid_item_columns,
            )
            if within_cooldown:
                logger.info(
                    "Plaid liabilities fetch skipped by cooldown item_id=%s institution=%s last_liabilities_fetch_at=%s cooldown_seconds=%s",
                    plaid_item_id,
                    institution_name,
                    last_liabilities_fetch_at,
                    liabilities_cooldown_seconds,
                )
                totals["items_skipped"] += 1
                totals["items"].append(
                    {
                        "plaid_item_id": plaid_item_id,
                        "institution_name": institution_name,
                        "status": "skipped_cooldown",
                        "last_liabilities_fetch_at": last_liabilities_fetch_at,
                    }
                )
                continue

        try:
            access_token = _get_access_token_for_item(item, region_name=region_name)
            response = client.liabilities_get(LiabilitiesGetRequest(access_token=access_token))
            payload = response.to_dict() if hasattr(response, "to_dict") else dict(response)

            accounts = payload.get("accounts") or []
            liabilities = payload.get("liabilities") or {}
            if not isinstance(liabilities, dict):
                liabilities = {}
            sync_ts = conn.execute("SELECT datetime('now') AS ts").fetchone()["ts"]

            account_ids_by_plaid_id: dict[str, str | None] = {}
            for account_payload in accounts:
                plaid_account_id = str(account_payload.get("account_id") or "").strip()
                if not plaid_account_id:
                    continue
                account_id_local = _ensure_account(
                    conn,
                    plaid_item_id=plaid_item_id,
                    institution_name=institution_name,
                    plaid_account_id=plaid_account_id,
                    account_payload=account_payload,
                )
                account_ids_by_plaid_id[plaid_account_id] = account_id_local
                if account_id_local is None:
                    continue
                _record_balance_snapshot(conn, account_id_local, account_payload, source="refresh")
            upserted = 0

            for liability_type in ("credit", "student", "mortgage"):
                entries = liabilities.get(liability_type) or []
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    plaid_account_id = str(entry.get("account_id") or "").strip()
                    if not plaid_account_id:
                        continue
                    if plaid_account_id in account_ids_by_plaid_id:
                        account_id_local = account_ids_by_plaid_id.get(plaid_account_id)
                        if not account_id_local:
                            continue
                    else:
                        row = conn.execute(
                            "SELECT id FROM accounts WHERE plaid_account_id = ?",
                            (plaid_account_id,),
                        ).fetchone()
                        if not row:
                            continue
                        account_id_local = str(row["id"])
                    _upsert_liability(conn, account_id_local, liability_type, entry, sync_ts)
                    upserted += 1

            # Deactivate liabilities not seen in this sync for any local account
            # tied to this item (covers disappeared accounts in Plaid response).
            deactivated = conn.execute(
                """
                UPDATE liabilities
                   SET is_active = 0,
                       updated_at = datetime('now')
                 WHERE account_id IN (
                        SELECT id
                          FROM accounts
                         WHERE plaid_item_id = ?
                   )
                   AND raw_plaid_json IS NOT NULL
                   AND (last_seen_at IS NULL OR last_seen_at < ?)
                """,
                (plaid_item_id, sync_ts),
            ).rowcount

            conn.execute(
                """
                UPDATE plaid_items
                   SET status = 'active',
                       error_code = NULL,
                       updated_at = datetime('now')
                 WHERE plaid_item_id = ?
                """,
                (plaid_item_id,),
            )
            _touch_item_cooldown(conn, plaid_item_id, "liabilities", plaid_item_columns=plaid_item_columns)
            conn.commit()

            totals["items_synced"] += 1
            totals["liabilities_upserted"] += upserted
            totals["liabilities_deactivated"] += int(deactivated or 0)
            totals["items"].append(
                {
                    "plaid_item_id": plaid_item_id,
                    "liabilities_upserted": upserted,
                    "liabilities_deactivated": int(deactivated or 0),
                }
            )
            logger.info(
                "Plaid liabilities fetch complete item_id=%s institution=%s liabilities_upserted=%s liabilities_deactivated=%s",
                plaid_item_id,
                institution_name,
                upserted,
                int(deactivated or 0),
            )
        except Exception as exc:
            conn.rollback()
            message = _extract_error_message(exc)
            _mark_item_error(conn, plaid_item_id, message)
            logger.warning(
                "Plaid liabilities fetch failed item_id=%s institution=%s error=%s",
                plaid_item_id,
                institution_name,
                message,
            )
            totals["items_failed"] += 1
            totals["errors"].append({"plaid_item_id": plaid_item_id, "institution_name": institution_name, "error": message})

    return totals


def unlink_item(conn: sqlite3.Connection, item_id: str, region_name: str | None = None) -> bool:
    row = conn.execute(
        "SELECT plaid_item_id, access_token_ref FROM plaid_items WHERE plaid_item_id = ?",
        (item_id,),
    ).fetchone()
    if not row:
        return False

    access_token_ref = row["access_token_ref"]
    shared_active_ref = False
    if access_token_ref:
        shared_active_ref = bool(
            conn.execute(
                """
                SELECT 1
                  FROM plaid_items
                 WHERE access_token_ref = ?
                   AND plaid_item_id <> ?
                   AND status IN ('active', 'pending', 'error')
                 LIMIT 1
                """,
                (access_token_ref, item_id),
            ).fetchone()
        )

    # Best-effort remote Plaid item removal; skip if token ref is shared.
    if not shared_active_ref:
        try:
            status = config_status()
            if status.configured and status.has_sdk:
                access_token = _get_access_token_for_item(row, region_name=region_name)
                _remove_remote_item(access_token)
        except Exception:
            pass

    if access_token_ref and not shared_active_ref:
        try:
            delete_secret(str(access_token_ref), region_name=region_name)
        except Exception:
            pass

    conn.execute(
        "UPDATE accounts SET is_active = 0, updated_at = datetime('now') WHERE plaid_item_id = ?",
        (item_id,),
    )
    conn.execute(
        """
        UPDATE transactions
           SET is_active = 0,
               updated_at = datetime('now')
         WHERE account_id IN (
            SELECT id
              FROM accounts
             WHERE plaid_item_id = ?
         )
        """,
        (item_id,),
    )

    # If the item has no token ref (orphaned/dead link), delete the row entirely
    # rather than leaving a disconnected stub in the status list.
    if not access_token_ref:
        conn.execute("DELETE FROM plaid_items WHERE plaid_item_id = ?", (item_id,))
    else:
        conn.execute(
            """
            UPDATE plaid_items
               SET status = 'disconnected',
                   error_code = NULL,
                   updated_at = datetime('now')
             WHERE plaid_item_id = ?
            """,
            (item_id,),
        )

    conn.commit()
    return True


def create_hosted_link_session(
    conn: sqlite3.Connection,
    user_id: str,
    update_item_id: str | None = None,
    include_balance: bool = False,
    include_liabilities: bool = False,
    requested_products: list[str] | None = None,
) -> dict[str, Any]:
    """Create hosted Plaid Link token/session for new or update-mode linking.

    Returns a payload consumed directly by `plaid link` command handlers.
    """
    client = _create_plaid_api_client()

    from plaid.model.country_code import CountryCode
    from plaid.model.link_token_create_request import LinkTokenCreateRequest
    from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
    from plaid.model.products import Products

    requested_product_names = resolve_requested_products(
        requested_products=requested_products,
        include_balance=include_balance,
        include_liabilities=include_liabilities,
    )
    products = [Products(name) for name in requested_product_names]

    request_payload: dict[str, Any] = {
        "user": LinkTokenCreateRequestUser(client_user_id=user_id),
        "client_name": os.getenv("PLAID_CLIENT_NAME", "finance_cli"),
        "products": products,
        "country_codes": [CountryCode("US")],
        "language": "en",
        "hosted_link": {
            "completion_redirect_uri": os.getenv("PLAID_COMPLETION_REDIRECT_URI", "https://example.com/plaid/complete"),
            "is_mobile_app": False,
        },
    }

    webhook = os.getenv("PLAID_WEBHOOK_URL")
    if webhook:
        request_payload["webhook"] = webhook

    if update_item_id:
        item = conn.execute(
            "SELECT * FROM plaid_items WHERE plaid_item_id = ?",
            (update_item_id,),
        ).fetchone()
        if not item:
            raise PlaidSyncError(f"Plaid item {update_item_id} not found")
        request_payload["access_token"] = _get_access_token_for_item(item)
        request_payload["update"] = {"account_selection_enabled": True}
        non_tx_products = [product for product in products if product.value != "transactions"]
        if non_tx_products:
            request_payload["additional_consented_products"] = non_tx_products
        request_payload["products"] = [Products("transactions")]

    request = LinkTokenCreateRequest(**request_payload)
    response = client.link_token_create(request)
    data = response.to_dict() if hasattr(response, "to_dict") else dict(response)

    return {
        "link_token": data.get("link_token"),
        "hosted_link_url": data.get("hosted_link_url"),
        "expiration": data.get("expiration"),
        "requested_products": requested_product_names,
        "update_item_id": update_item_id,
    }


def wait_for_public_token(
    link_token: str,
    timeout_seconds: int = 300,
    poll_seconds: int = 10,
) -> str:
    client = _create_plaid_api_client()
    from plaid.model.link_token_get_request import LinkTokenGetRequest

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = client.link_token_get(LinkTokenGetRequest(link_token=link_token))
        payload = response.to_dict() if hasattr(response, "to_dict") else dict(response)

        sessions = payload.get("link_sessions") or []
        for session in sessions:
            item_results = ((session.get("results") or {}).get("item_add_results") or [])
            for result in item_results:
                token = result.get("public_token")
                if token:
                    return str(token)

        time.sleep(max(1, poll_seconds))

    raise PlaidSyncError("Timed out waiting for Plaid Link completion")


def complete_link_session(
    conn: sqlite3.Connection,
    user_id: str,
    link_token: str,
    timeout_seconds: int = 300,
    poll_seconds: int = 10,
    requested_products: list[str] | None = None,
    region_name: str | None = None,
    allow_duplicate_institution: bool = False,
) -> dict[str, Any]:
    """Complete hosted Link flow and persist local `plaid_items` metadata.

    Product source-of-truth order:
    1. Plaid `item_get` response (`consented_products`/`billed_products`)
    2. Requested products from link session
    3. Previously stored products
    """
    client = _create_plaid_api_client()

    from plaid.model.country_code import CountryCode
    from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
    from plaid.model.item_get_request import ItemGetRequest
    from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
    user_key = sanitize_client_user_id(user_id)

    public_token = wait_for_public_token(link_token, timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)

    exchange = client.item_public_token_exchange(ItemPublicTokenExchangeRequest(public_token=public_token))
    exchange_payload = exchange.to_dict() if hasattr(exchange, "to_dict") else dict(exchange)
    access_token = str(exchange_payload.get("access_token") or "")
    item_id = str(exchange_payload.get("item_id") or "")

    if not access_token or not item_id:
        raise PlaidSyncError("Plaid token exchange returned no access_token/item_id")

    item_resp = client.item_get(ItemGetRequest(access_token=access_token))
    item_payload = item_resp.to_dict() if hasattr(item_resp, "to_dict") else dict(item_resp)

    institution_id = str(((item_payload.get("item") or {}).get("institution_id")) or "").strip() or None
    institution_name = "Unknown Institution"

    if institution_id:
        inst_resp = client.institutions_get_by_id(
            InstitutionsGetByIdRequest(
                institution_id=institution_id,
                country_codes=[CountryCode("US")],
            )
        )
        inst_payload = inst_resp.to_dict() if hasattr(inst_resp, "to_dict") else dict(inst_resp)
        institution_name = (((inst_payload.get("institution") or {}).get("name")) or institution_name)

    existing = conn.execute(
        "SELECT id, consented_products FROM plaid_items WHERE plaid_item_id = ?",
        (item_id,),
    ).fetchone()
    plaid_item_columns = _plaid_items_column_names(conn)
    has_institution_id_column = "institution_id" in plaid_item_columns
    normalized_institution_name = institution_name.strip().lower()
    institution_identity_known = normalized_institution_name not in {"", "unknown institution"}
    can_dedupe = institution_identity_known or bool(institution_id and has_institution_id_column)
    if not existing and not allow_duplicate_institution and can_dedupe:
        if institution_id and has_institution_id_column:
            duplicates = conn.execute(
                """
                SELECT plaid_item_id
                  FROM plaid_items
                 WHERE plaid_item_id <> ?
                   AND status IN ('active', 'pending', 'error')
                   AND (
                        institution_id = ?
                        OR (institution_id IS NULL AND lower(trim(institution_name)) = lower(trim(?)))
                   )
                 ORDER BY created_at DESC
                """,
                (item_id, institution_id, institution_name),
            ).fetchall()
        else:
            duplicates = conn.execute(
                """
                SELECT plaid_item_id
                  FROM plaid_items
                 WHERE plaid_item_id <> ?
                   AND status IN ('active', 'pending', 'error')
                   AND lower(trim(institution_name)) = lower(trim(?))
                 ORDER BY created_at DESC
                """,
                (item_id, institution_name),
            ).fetchall()
        duplicate_item_ids = [str(row["plaid_item_id"]) for row in duplicates]
        if duplicate_item_ids:
            # Best-effort rollback for newly-created duplicate item.
            try:
                _remove_remote_item(access_token)
            except Exception:
                pass
            suggested_item = duplicate_item_ids[0]
            raise PlaidSyncError(
                f"Duplicate institution link blocked for '{institution_name}'. "
                f"Existing active item(s): {', '.join(duplicate_item_ids)}. "
                f"Use 'plaid link --update --item {suggested_item}' or rerun with '--allow-duplicate'."
            )

    secret_name = store_plaid_token(
        user_id=user_key,
        institution=institution_name,
        access_token=access_token,
        item_id=item_id,
        region_name=region_name,
        secret_names=secret_name_candidates_for_item(user_key, item_id),
    )

    consented_product_names = _extract_products_from_item_payload(item_payload)
    stored_products = _parse_stored_products(existing["consented_products"]) if existing else []

    if not consented_product_names and requested_products is not None:
        consented_product_names = resolve_requested_products(requested_products=requested_products)
        logger.warning(
            "consented_products not available from Plaid item payload; "
            "falling back to requested products (may overreport): %s",
            consented_product_names,
        )

    if not consented_product_names:
        consented_product_names = list(stored_products)
    else:
        for product in stored_products:
            if product not in consented_product_names:
                consented_product_names.append(product)

    if not consented_product_names:
        consented_product_names = ["transactions"]
    consented_products = json.dumps(consented_product_names)
    if existing:
        local_id = str(existing["id"])
        if has_institution_id_column:
            conn.execute(
                """
                UPDATE plaid_items
                   SET institution_id = ?,
                       institution_name = ?,
                       access_token_ref = ?,
                       status = 'active',
                       error_code = NULL,
                       consented_products = ?,
                       updated_at = datetime('now')
                 WHERE id = ?
                """,
                (institution_id, institution_name, secret_name, consented_products, local_id),
            )
        else:
            conn.execute(
                """
                UPDATE plaid_items
                   SET institution_name = ?,
                       access_token_ref = ?,
                       status = 'active',
                       error_code = NULL,
                       consented_products = ?,
                       updated_at = datetime('now')
                 WHERE id = ?
                """,
                (institution_name, secret_name, consented_products, local_id),
            )
    else:
        local_id = uuid.uuid4().hex
        if has_institution_id_column:
            conn.execute(
                """
                INSERT INTO plaid_items (
                    id,
                    plaid_item_id,
                    institution_id,
                    institution_name,
                    access_token_ref,
                    status,
                    error_code,
                    consented_products,
                    sync_cursor
                ) VALUES (?, ?, ?, ?, ?, 'active', NULL, ?, NULL)
                """,
                (local_id, item_id, institution_id, institution_name, secret_name, consented_products),
            )
        else:
            conn.execute(
                """
                INSERT INTO plaid_items (
                    id,
                    plaid_item_id,
                    institution_name,
                    access_token_ref,
                    status,
                    error_code,
                    consented_products,
                    sync_cursor
                ) VALUES (?, ?, ?, ?, 'active', NULL, ?, NULL)
                """,
                (local_id, item_id, institution_name, secret_name, consented_products),
            )

    conn.commit()

    return {
        "id": local_id,
        "plaid_item_id": item_id,
        "institution_name": institution_name,
        "access_token_ref": secret_name,
        "status": "active",
        "consented_products": consented_product_names,
    }


def backfill_item_products(
    conn: sqlite3.Connection,
    item_id: str | None = None,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Refresh stored `plaid_items.consented_products` from Plaid item metadata."""
    status = config_status()
    if not status.configured or not status.has_sdk:
        missing_parts = []
        if not status.has_sdk:
            missing_parts.append("plaid-python not installed")
        if status.missing_env:
            missing_parts.append("missing env: " + ",".join(status.missing_env))
        raise PlaidUnavailableError("Plaid products backfill unavailable: " + "; ".join(missing_parts))

    client = _create_plaid_api_client()
    from plaid.model.item_get_request import ItemGetRequest

    where = ["status IN ('active', 'pending', 'error')"]
    params: list[Any] = []
    if item_id:
        where.append("plaid_item_id = ?")
        params.append(item_id)

    items = conn.execute(
        f"""
        SELECT plaid_item_id, access_token_ref, consented_products
          FROM plaid_items
         WHERE {' AND '.join(where)}
         ORDER BY created_at ASC
        """,
        tuple(params),
    ).fetchall()

    if not items:
        return {
            "items_requested": 0,
            "items_updated": 0,
            "items_failed": 0,
            "errors": [],
            "items": [],
        }

    totals = {
        "items_requested": len(items),
        "items_updated": 0,
        "items_failed": 0,
        "errors": [],
        "items": [],
    }
    plaid_item_columns = _plaid_items_column_names(conn)
    has_institution_id_column = "institution_id" in plaid_item_columns

    for item in items:
        plaid_item_id = str(item["plaid_item_id"])
        try:
            access_token = _get_access_token_for_item(item, region_name=region_name)
            item_resp = client.item_get(ItemGetRequest(access_token=access_token))
            item_payload = item_resp.to_dict() if hasattr(item_resp, "to_dict") else dict(item_resp)
            institution_id = str(((item_payload.get("item") or {}).get("institution_id")) or "").strip() or None

            product_names = _extract_products_from_item_payload(item_payload)
            if not product_names:
                product_names = _parse_stored_products(item["consented_products"])

            if has_institution_id_column:
                conn.execute(
                    """
                    UPDATE plaid_items
                       SET consented_products = ?,
                           institution_id = COALESCE(?, institution_id),
                           updated_at = datetime('now')
                     WHERE plaid_item_id = ?
                    """,
                    (json.dumps(product_names), institution_id, plaid_item_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE plaid_items
                       SET consented_products = ?,
                           updated_at = datetime('now')
                     WHERE plaid_item_id = ?
                    """,
                    (json.dumps(product_names), plaid_item_id),
                )
            conn.commit()

            totals["items_updated"] += 1
            totals["items"].append(
                {
                    "plaid_item_id": plaid_item_id,
                    "consented_products": product_names,
                }
            )
        except Exception as exc:
            conn.rollback()
            message = _extract_error_message(exc)
            totals["items_failed"] += 1
            totals["errors"].append({"plaid_item_id": plaid_item_id, "error": message})

    return totals
