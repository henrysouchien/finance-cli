"""Institution-level provider routing helpers."""

from __future__ import annotations

import sqlite3

DEFAULT_PROVIDER = "plaid"

# Code-level defaults for institutions with fixed routing.
# DB overrides in provider_routing take precedence.
INSTITUTION_PROVIDER: dict[str, str] = {
    "Charles Schwab": "schwab",
    "Stripe": "stripe",
}


def _normalize_provider(provider: object) -> str:
    value = str(provider or "").strip().lower()
    return value or DEFAULT_PROVIDER


def get_provider_for_institution(conn: sqlite3.Connection, institution_name: str) -> str:
    """Return designated provider for an institution.

    Resolution order:
    1) DB override table (`provider_routing`)
    2) code defaults (`INSTITUTION_PROVIDER`)
    3) global default (`DEFAULT_PROVIDER`)
    """
    normalized_name = str(institution_name or "").strip()
    if normalized_name:
        try:
            row = conn.execute(
                "SELECT provider FROM provider_routing WHERE institution_name = ?",
                (normalized_name,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            # Backward compatibility for schemas before migration 017.
            if "no such table" in str(exc).lower():
                row = None
            else:
                raise
        if row and row["provider"]:
            return _normalize_provider(row["provider"])

        configured = INSTITUTION_PROVIDER.get(normalized_name)
        if configured:
            return _normalize_provider(configured)

    return DEFAULT_PROVIDER


def is_provider_active(conn: sqlite3.Connection, institution_name: str, source: str) -> bool:
    """Return True when source is the active provider for institution."""
    return get_provider_for_institution(conn, institution_name) == _normalize_provider(source)


def check_provider_allowed(conn: sqlite3.Connection, institution_name: str, source: str) -> tuple[bool, str]:
    """Generic provider guard for institution-level routing.

    Returns:
        (allowed, designated_provider)
    """
    designated_provider = get_provider_for_institution(conn, institution_name)
    allowed = designated_provider == _normalize_provider(source)
    return allowed, designated_provider
