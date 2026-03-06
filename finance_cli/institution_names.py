"""Shared institution name canonicalization."""

from __future__ import annotations

import re

_TOKEN_RE = re.compile(r"[^a-z0-9 ]")
_WS_RE = re.compile(r"\s+")

# Normalized input key -> canonical institution label.
CANONICAL_NAMES: dict[str, str] = {
    # Bank of America
    "bank of america": "Bank of America",
    "bofa": "Bank of America",
    "boa": "Bank of America",
    "bofa checking": "Bank of America",
    "boa checking": "Bank of America",
    "bank of america checking": "Bank of America",
    "bofa credit": "Bank of America",
    "boa credit": "Bank of America",
    "bank of america credit": "Bank of America",
    "bank of america credit card": "Bank of America",
    # American Express
    "amex": "American Express",
    "american express": "American Express",
    "american express company": "American Express",
    # Chase
    "chase": "Chase",
    "chase credit": "Chase",
    "chase credit card": "Chase",
    "chase checking": "Chase",
    "jpmorgan chase": "Chase",
    # Barclays
    "barclays": "Barclays",
    "barclays cards": "Barclays",
    "barclays card": "Barclays",
    "barclays us": "Barclays",
    # Apple Card
    "apple": "Apple Card",
    "apple card": "Apple Card",
    "apple card inc": "Apple Card",
    "apple card goldman sachs bank usa": "Apple Card",
    "goldman sachs": "Apple Card",
    "goldman sachs bank usa": "Apple Card",
    # Citi
    "citi": "Citi",
    "citibank": "Citi",
    # Other
    "bloomingdales": "Bloomingdale's",
    "bloomingdale s": "Bloomingdale's",
    "bloomingdale s citibank n a": "Bloomingdale's",
    "schwab": "Schwab",
    "charles schwab": "Schwab",
    "venmo": "Venmo",
    "venmo personal": "Venmo",
    "paypal": "PayPal",
    "paypal credit syncb": "PayPal",
    "paypal credit": "PayPal",
    "merrill": "Merrill",
}


def normalize_key(value: str) -> str:
    """Normalize institution name to lookup key format."""
    normalized = _TOKEN_RE.sub(" ", str(value or "").strip().lower())
    return _WS_RE.sub(" ", normalized).strip()


def canonicalize(institution_name: str) -> str:
    """Return canonical institution name or the stripped input if unknown."""
    stripped = str(institution_name or "").strip()
    if not stripped:
        return ""
    return CANONICAL_NAMES.get(normalize_key(stripped), stripped)


def is_known(institution_name: str) -> bool:
    """True when institution_name resolves through CANONICAL_NAMES."""
    stripped = str(institution_name or "").strip()
    if not stripped:
        return False
    return normalize_key(stripped) in CANONICAL_NAMES


def _tokenize(value: str) -> set[str]:
    normalized = normalize_key(value)
    if not normalized:
        return set()
    return {token for token in normalized.split(" ") if token}


def similar_names(name_a: str, name_b: str) -> bool:
    """Textual similarity heuristic for audit reporting."""
    norm_a = normalize_key(name_a)
    norm_b = normalize_key(name_b)
    if not norm_a or not norm_b:
        return False
    if norm_a in norm_b or norm_b in norm_a:
        return True

    tokens_a = _tokenize(norm_a)
    tokens_b = _tokenize(norm_b)
    if not tokens_a or not tokens_b:
        return False
    shared = len(tokens_a & tokens_b)
    smaller = min(len(tokens_a), len(tokens_b))
    return shared * 2 >= smaller
