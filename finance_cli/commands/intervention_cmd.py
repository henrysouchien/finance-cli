from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
from typing import Any

from ..exceptions import ValidationError
from ..intervention_engine import log_fires, record_action, run_engine, serialize
from ..interventions.registry import PATTERN_REGISTRY


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("interventions", parents=[format_parent], help="Rank intervention candidates")
    intervention_sub = parser.add_subparsers(dest="intervention_command", required=True)

    p_list = intervention_sub.add_parser("list", parents=[format_parent], help="List interventions for a surface")
    p_list.add_argument("--surface", choices=["dashboard", "action_queue", "agent_prompt"], default="dashboard")
    p_list.set_defaults(func=handle_list, command_name="interventions.list")

    p_act = intervention_sub.add_parser("act", parents=[format_parent], help="Mark an intervention as acted")
    p_act.add_argument("log_id", type=int)
    p_act.set_defaults(func=handle_act, command_name="interventions.act")

    p_dismiss = intervention_sub.add_parser("dismiss", parents=[format_parent], help="Dismiss an intervention")
    p_dismiss.add_argument("log_id", type=int)
    p_dismiss.set_defaults(func=handle_dismiss, command_name="interventions.dismiss")

    p_mute = intervention_sub.add_parser("mute", parents=[format_parent], help="Mute an intervention pattern")
    p_mute.add_argument("pattern_id")
    p_mute.add_argument("--reason", default="")
    p_mute.set_defaults(func=handle_mute, command_name="interventions.mute")

    p_unmute = intervention_sub.add_parser("unmute", parents=[format_parent], help="Unmute an intervention pattern")
    p_unmute.add_argument("pattern_id")
    p_unmute.set_defaults(func=handle_unmute, command_name="interventions.unmute")

    p_expire = intervention_sub.add_parser("expire", parents=[format_parent], help="Expire stale pending interventions")
    p_expire.set_defaults(func=handle_expire, command_name="interventions.expire")


def _build_cli_report(surface: str, interventions: list[dict[str, Any]]) -> str:
    if not interventions:
        return f"No interventions firing for {surface}."
    lines = [f"Interventions ({surface})", ""]
    for idx, intervention in enumerate(interventions, start=1):
        lines.append(f"{idx}. {intervention['pattern_id']} - {intervention['headline']}")
        if intervention.get("tier4_ladder"):
            lines.append(f"   {intervention['tier4_ladder']}")
    return "\n".join(lines)


def handle_list(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    engine_result = run_engine(conn, rules_path=rules_path)
    surfaced = engine_result.get_for_surface(args.surface)
    logged = log_fires(conn, surfaced, surface="cli")
    serialized = [serialize(item) for item in logged]
    return {
        "data": {
            "surface": args.surface,
            "log_surface": "cli",
            "interventions": serialized,
        },
        "summary": {
            "count": len(serialized),
            "surface": args.surface,
            "total_candidates": len(engine_result.interventions),
        },
        "cli_report": _build_cli_report(args.surface, serialized),
    }


def handle_get(args, conn, rules_path=None):
    """Read-only handler for the MCP interventions_get tool.

    PRAGMA query_only is connection-scoped. Keep it as the first statement so
    accidental writes in future refactors raise instead of touching
    intervention_log.
    """
    conn.execute("PRAGMA query_only = 1")
    from .. import intervention_engine

    engine_result, surfaced = intervention_engine.evaluate_for_surface(
        conn,
        args.surface,
        rules_path=rules_path,
        log_to_surface=None,
    )
    return intervention_engine.build_surface_envelope(engine_result, surfaced, args.surface)


def handle_act(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    del rules_path
    row = record_action(conn, args.log_id, "acted")
    return {
        "data": row,
        "summary": {"log_id": int(row["id"]), "action": "acted"},
        "cli_report": f"log_id={row['id']}\naction=acted",
    }


def handle_dismiss(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    del rules_path
    row = record_action(conn, args.log_id, "dismissed")
    return {
        "data": row,
        "summary": {"log_id": int(row["id"]), "action": "dismissed"},
        "cli_report": f"log_id={row['id']}\naction=dismissed",
    }


def handle_mute(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    del rules_path
    pattern_id = str(args.pattern_id)
    if pattern_id not in PATTERN_REGISTRY:
        raise ValidationError(f"Unknown intervention pattern: {pattern_id}")
    reason = str(getattr(args, "reason", "") or "")
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO intervention_mutes (pattern_id, reason)
        VALUES (?, ?)
        """,
        (pattern_id, reason),
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT id, pattern_id, muted_at, reason, created_at
          FROM intervention_mutes
         WHERE pattern_id = ?
        """,
        (pattern_id,),
    ).fetchone()
    data = dict(row) if row is not None else {"pattern_id": pattern_id, "reason": reason}
    created = bool(cursor.rowcount)
    return {
        "data": {**data, "created": created},
        "summary": {"pattern_id": pattern_id, "created": created},
        "cli_report": f"pattern_id={pattern_id}\ncreated={created}",
    }


def handle_unmute(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    del rules_path
    pattern_id = str(args.pattern_id)
    cursor = conn.execute("DELETE FROM intervention_mutes WHERE pattern_id = ?", (pattern_id,))
    conn.commit()
    deleted = bool(cursor.rowcount)
    return {
        "data": {"pattern_id": pattern_id, "deleted": deleted},
        "summary": {"deleted": deleted},
        "cli_report": f"pattern_id={pattern_id}\ndeleted={deleted}",
    }


def handle_expire(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    del args, rules_path
    now = (now or datetime.now()).replace(microsecond=0)
    cutoff = now - timedelta(days=7)
    cursor = conn.execute(
        """
        UPDATE intervention_log
           SET user_action = 'ignored',
               acted_at = ?
         WHERE user_action = 'pending'
           AND fired_at < ?
        """,
        (now.strftime("%Y-%m-%d %H:%M:%S"), cutoff.strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    expired = int(cursor.rowcount)
    return {
        "data": {
            "expired": expired,
            "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "summary": {"expired": expired},
        "cli_report": f"expired={expired}",
    }


def handle_surface(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    from .. import intervention_engine

    engine_result, surfaced = intervention_engine.evaluate_for_surface(
        conn,
        args.surface,
        rules_path=rules_path,
        log_to_surface=args.surface,
    )
    return intervention_engine.build_surface_envelope(engine_result, surfaced, args.surface)
