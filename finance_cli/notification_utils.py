import json
import logging
import sqlite3

log = logging.getLogger(__name__)

_ALLOWED_KEYS: dict[str, set[str]] = {
    "telegram": {"chat_id"},
    "imessage": {"target", "service"},
}

_REQUIRED_KEYS: dict[str, str] = {
    "telegram": "chat_id",
    "imessage": "target",
}


def resolve_notification_creds(
    conn: sqlite3.Connection | None,
    channel: str,
    *,
    require: bool = False,
) -> dict[str, str]:
    """Resolve per-user notification creds from DB."""
    if conn is None:
        return {}

    allowed = _ALLOWED_KEYS.get(channel, set())
    required_key = _REQUIRED_KEYS.get(channel)

    try:
        row = conn.execute(
            "SELECT config FROM notification_channels WHERE channel = ?",
            (channel,),
        ).fetchone()
        if row:
            parsed = json.loads(row[0])
            filtered = {k: str(v) for k, v in parsed.items() if k in allowed}
            if required_key and required_key in filtered:
                return filtered
    except sqlite3.OperationalError:
        pass

    if channel == "telegram":
        try:
            tg_row = conn.execute(
                "SELECT chat_id FROM telegram_config WHERE id = 1 AND chat_id IS NOT NULL"
            ).fetchone()
            if tg_row and tg_row[0]:
                return {"chat_id": str(tg_row[0])}
        except sqlite3.OperationalError:
            pass

    if require:
        raise ValueError(
            f"No notification channel configured for '{channel}'. "
            "Use notify_channel_set to add one."
        )
    return {}
