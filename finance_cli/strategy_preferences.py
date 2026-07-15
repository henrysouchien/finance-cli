"""Durable user strategy preference helpers."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


VALID_STRATEGIES_BY_DOMAIN: dict[str, frozenset[str]] = {
    "debt": frozenset({"avalanche", "snowball", "hybrid", "minimum_commitment"}),
}
VALID_SOURCES: frozenset[str] = frozenset({"user", "agent", "inferred", "artifact", "system"})


def _normalize_domain(domain: str) -> str:
    normalized = str(domain or "").strip().lower().replace("-", "_")
    if normalized not in VALID_STRATEGIES_BY_DOMAIN:
        expected = ", ".join(sorted(VALID_STRATEGIES_BY_DOMAIN))
        raise ValueError(f"domain must be one of: {expected}")
    return normalized


def _normalize_strategy(domain: str, strategy: str) -> str:
    normalized = str(strategy or "").strip().lower().replace("-", "_")
    valid = VALID_STRATEGIES_BY_DOMAIN[domain]
    if normalized not in valid:
        expected = ", ".join(sorted(valid))
        raise ValueError(f"strategy for {domain} must be one of: {expected}")
    return normalized


def _normalize_source(source: str) -> str:
    normalized = str(source or "user").strip().lower()
    if normalized not in VALID_SOURCES:
        expected = ", ".join(sorted(VALID_SOURCES))
        raise ValueError(f"source must be one of: {expected}")
    return normalized


def _normalize_evidence(evidence: dict[str, Any] | None) -> str:
    if evidence is None:
        return "{}"
    if not isinstance(evidence, dict):
        raise ValueError("evidence must be a dict when provided")
    return json.dumps(evidence, sort_keys=True)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    evidence_raw = row["evidence_json"]
    evidence = json.loads(evidence_raw) if evidence_raw else {}
    return {
        "domain": str(row["domain"]),
        "strategy": str(row["strategy"]),
        "rationale": row["rationale"],
        "source": str(row["source"]),
        "evidence": evidence if isinstance(evidence, dict) else {},
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def get_strategy_preferences(conn: sqlite3.Connection, *, domain: str | None = None) -> dict[str, Any]:
    """Read durable strategy preferences."""
    if domain:
        normalized_domain = _normalize_domain(domain)
        rows = conn.execute(
            """
            SELECT domain, strategy, rationale, source, evidence_json, created_at, updated_at
              FROM user_strategy_preferences
             WHERE domain = ?
            """,
            (normalized_domain,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT domain, strategy, rationale, source, evidence_json, created_at, updated_at
              FROM user_strategy_preferences
             ORDER BY domain
            """
        ).fetchall()

    preferences = [_row_to_dict(row) for row in rows]
    return {
        "data": {
            "preferences": preferences,
            "by_domain": {item["domain"]: item for item in preferences},
            "valid_strategies_by_domain": {
                key: sorted(value) for key, value in VALID_STRATEGIES_BY_DOMAIN.items()
            },
        },
        "summary": {
            "count": len(preferences),
            "domain": domain or None,
        },
    }


def set_strategy_preference(
    conn: sqlite3.Connection,
    *,
    domain: str,
    strategy: str,
    rationale: str = "",
    source: str = "user",
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create or replace one durable strategy preference."""
    normalized_domain = _normalize_domain(domain)
    normalized_strategy = _normalize_strategy(normalized_domain, strategy)
    normalized_source = _normalize_source(source)
    evidence_json = _normalize_evidence(evidence)
    rationale_value = str(rationale or "").strip() or None

    conn.execute(
        """
        INSERT INTO user_strategy_preferences (
            domain, strategy, rationale, source, evidence_json
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(domain) DO UPDATE SET
            strategy = excluded.strategy,
            rationale = excluded.rationale,
            source = excluded.source,
            evidence_json = excluded.evidence_json,
            updated_at = datetime('now')
        """,
        (
            normalized_domain,
            normalized_strategy,
            rationale_value,
            normalized_source,
            evidence_json,
        ),
    )
    conn.commit()
    return get_strategy_preferences(conn, domain=normalized_domain)


def clear_strategy_preference(conn: sqlite3.Connection, *, domain: str) -> dict[str, Any]:
    """Delete one durable strategy preference."""
    normalized_domain = _normalize_domain(domain)
    cursor = conn.execute(
        "DELETE FROM user_strategy_preferences WHERE domain = ?",
        (normalized_domain,),
    )
    conn.commit()
    return {
        "data": {
            "domain": normalized_domain,
            "cleared": cursor.rowcount > 0,
        },
        "summary": {
            "domain": normalized_domain,
            "cleared": cursor.rowcount > 0,
        },
    }
