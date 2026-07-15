"""Usage analytics helpers."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any

from .db import _resolve_connection_user_id, connect
from .perf import get_conversation_id, get_request_id, get_session_id
from .storage_lease import optional_lease_scope

log = logging.getLogger(__name__)


class PropType(Enum):
    INT = "int"
    ENUM = "enum"
    BOOL = "bool"


PropertySpec = tuple[PropType, set[str] | None]
PropertyValue = int | str | bool


KNOWN_PROPERTIES: dict[str, dict[str, PropertySpec]] = {
    "onboarding.wizard": {
        "step": (PropType.ENUM, None),
        "context": (PropType.ENUM, None),
    },
    "onboarding.plaid_link": {
        "reason_code": (
            PropType.ENUM,
            {"user_cancelled", "institution_error", "timeout", "unknown"},
        ),
        "institution_type": (
            PropType.ENUM,
            {"bank", "credit_union", "brokerage", "other"},
        ),
    },
    "onboarding.csv_import": {
        "reason_code": (
            PropType.ENUM,
            {"parse_error", "empty_file", "duplicate", "unknown"},
        ),
        "row_count": (PropType.INT, None),
        "file_type": (PropType.ENUM, {"csv", "pdf"}),
    },
    "onboarding.first_categorization": {},
    "onboarding.profile_captured": {},
    "onboarding.focus_selected": {},
    "onboarding.setup_acknowledged": {},
    "onboarding.complete": {},
    "chat.session": {
        "message_count": (PropType.INT, None),
        "duration_min": (PropType.INT, None),
        "tool_call_count": (PropType.INT, None),
    },
    "feature.budget_set": {},
    "feature.goal_set": {},
    "feature.goal_abandoned": {
        "goal_id": (PropType.ENUM, None),
        "goal_name": (PropType.ENUM, None),
    },
    "feature.subscription_detected": {
        "count": (PropType.INT, None),
    },
    "feature.spending_trends_viewed": {},
    "feature.debt_simulated": {},
    "feature.export_generated": {
        "format": (PropType.ENUM, {"csv", "sheets", "wave"}),
    },
    "feature.plan_created": {},
    "feature.plan_abandoned": {
        "month": (PropType.ENUM, None),
    },
    "import.csv_completed": {
        "row_count": (PropType.INT, None),
        "account_type": (
            PropType.ENUM,
            {"checking", "savings", "credit_card", "brokerage", "other"},
        ),
    },
    "import.pdf_completed": {
        "page_count": (PropType.INT, None),
    },
    "import.plaid_synced": {
        "txn_count": (PropType.INT, None),
        "account_count": (PropType.INT, None),
    },
    "import.stripe_synced": {
        "txn_count": (PropType.INT, None),
    },
    "cost.limit_warning": {
        "provider": (PropType.ENUM, {"claude", "openai", "plaid", "all"}),
        "period": (PropType.ENUM, {"daily", "monthly"}),
        "spent_pct": (PropType.INT, None),
    },
    "account.deletion_scheduled": {},
    "account.deletion_immediate": {},
    "account.deletion_cancelled": {},
    "account.deletion_completed": {},
}

KNOWN_EVENTS: set[str] = set(KNOWN_PROPERTIES)


def _coerce_db_path(db_path: str | Path | None) -> str | None:
    if db_path is None:
        return None
    if isinstance(db_path, str) and (db_path == ":memory:" or db_path.startswith("file:")):
        return db_path
    return str(Path(db_path).expanduser().resolve())


def _filter_properties(
    event: str,
    properties: Mapping[str, Any] | None,
) -> dict[str, PropertyValue]:
    """Return only allowlisted properties with valid types."""
    if not properties:
        return {}

    specs = KNOWN_PROPERTIES.get(event, {})
    if not specs:
        return {}

    filtered: dict[str, PropertyValue] = {}
    for key, value in properties.items():
        spec = specs.get(str(key))
        if spec is None:
            continue

        prop_type, allowed_values = spec
        if prop_type is PropType.INT:
            if isinstance(value, bool) or not isinstance(value, int):
                continue
            filtered[str(key)] = int(value)
            continue

        if prop_type is PropType.BOOL:
            if isinstance(value, bool):
                filtered[str(key)] = value
            continue

        if not isinstance(value, str):
            continue

        enum_value = value.strip()
        if not enum_value:
            continue
        if allowed_values is not None and enum_value not in allowed_values:
            log.warning(
                "analytics enum property normalized to unknown",
                extra={"event": event, "property": key},
            )
            filtered[str(key)] = "unknown"
            continue
        filtered[str(key)] = enum_value

    return filtered


def log_event(
    db_path: str | Path | None,
    event: str,
    *,
    outcome: str = "succeeded",
    properties: Mapping[str, Any] | None = None,
    source: str = "api",
    request_id: str | None = None,
    session_id: str | None = None,
    conversation_id: str | None = None,
) -> None:
    """Write an analytics event using an isolated connection."""
    if event not in KNOWN_EVENTS:
        return

    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        return

    clean_props = _filter_properties(event, properties)
    domain = event.split(".")[0]
    effective_request_id = get_request_id() if request_id is None else request_id
    effective_session_id = get_session_id() if session_id is None else session_id
    effective_conversation_id = (
        get_conversation_id() if conversation_id is None else conversation_id
    )

    try:
        if resolved_db_path.startswith("file:"):
            conn = sqlite3.connect(resolved_db_path, uri=True)
        else:
            resolved_path = Path(resolved_db_path).expanduser().resolve()
            try:
                user_id = _resolve_connection_user_id(resolved_path)
            except ValueError:
                conn = sqlite3.connect(str(resolved_path))
            else:
                with optional_lease_scope(
                    user_id,
                    operation="analytics.log_event",
                    metadata={"source": "analytics.log_event", "event": event},
                ):
                    conn = connect(
                        db_path=resolved_path,
                        check_same_thread=True,
                        user_id=user_id,
                    )
                    with conn:
                        conn.execute(
                            """
                            INSERT INTO analytics_events (
                                event,
                                domain,
                                outcome,
                                properties,
                                source,
                                request_id,
                                session_id,
                                conversation_id
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                event,
                                domain,
                                outcome,
                                json.dumps(clean_props, sort_keys=True, separators=(",", ":"))
                                if clean_props
                                else None,
                                source,
                                effective_request_id,
                                effective_session_id,
                                effective_conversation_id,
                            ),
                        )
                        conn.commit()
                    return
        with conn:
            conn.execute(
                """
                INSERT INTO analytics_events (
                    event,
                    domain,
                    outcome,
                    properties,
                    source,
                    request_id,
                    session_id,
                    conversation_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event,
                    domain,
                    outcome,
                    json.dumps(clean_props, sort_keys=True, separators=(",", ":"))
                    if clean_props
                    else None,
                    source,
                    effective_request_id,
                    effective_session_id,
                    effective_conversation_id,
                ),
            )
            conn.commit()
    except Exception:
        return


def prune_analytics(conn: sqlite3.Connection, retention_days: int = 90) -> None:
    """Delete analytics rows older than the configured retention window."""
    conn.execute(
        """
        DELETE FROM analytics_events
        WHERE created_at < datetime('now', ?)
        """,
        (f"-{int(retention_days)} days",),
    )


__all__ = [
    "KNOWN_EVENTS",
    "KNOWN_PROPERTIES",
    "PropType",
    "_filter_properties",
    "log_event",
    "prune_analytics",
]
