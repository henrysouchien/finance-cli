"""Provider routing commands."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..provider_routing import INSTITUTION_PROVIDER, get_provider_for_institution


def _sync_hint(provider: str) -> str:
    if provider == "plaid":
        return "plaid balance-refresh"
    if provider == "schwab":
        return "schwab sync"
    return f"{provider} sync"


def _provider_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'provider_routing'"
    ).fetchone()
    if not has_table:
        return []
    return conn.execute(
        """
        SELECT institution_name, provider, updated_at
          FROM provider_routing
         ORDER BY institution_name
        """
    ).fetchall()


def _status_cli_report(rows: list[dict[str, Any]]) -> str:
    lines = [f"institutions={len(rows)}"]
    for row in rows:
        account_counts = row.get("account_counts") or {}
        source_parts = [f"{source}:{int(count)}" for source, count in sorted(account_counts.items())]
        sources_text = ", ".join(source_parts) if source_parts else "none"
        lines.append(
            f"  {row['institution_name']}: provider={row['designated_provider']} "
            f"active_accounts={row['active_accounts']} sources=[{sources_text}]"
        )
    return "\n".join(lines)


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("provider", parents=[format_parent], help="Institution provider routing")
    provider_sub = parser.add_subparsers(dest="provider_command", required=True)

    p_status = provider_sub.add_parser("status", parents=[format_parent], help="Show provider routing status")
    p_status.set_defaults(func=handle_status, command_name="provider.status")

    p_switch = provider_sub.add_parser("switch", parents=[format_parent], help="Switch institution provider")
    p_switch.add_argument("institution")
    p_switch.add_argument("provider")
    p_switch.set_defaults(func=handle_switch, command_name="provider.switch")


def handle_status(args, conn: sqlite3.Connection) -> dict[str, Any]:
    del args
    counts_by_institution: dict[str, dict[str, int]] = {}
    active_by_institution: dict[str, int] = {}
    account_rows = conn.execute(
        """
        SELECT
            institution_name,
            COALESCE(source, 'unknown') AS source,
            COUNT(*) AS account_count,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_count
          FROM accounts
         GROUP BY institution_name, COALESCE(source, 'unknown')
         ORDER BY institution_name, source
        """
    ).fetchall()
    institutions: set[str] = {name for name in INSTITUTION_PROVIDER.keys() if str(name).strip()}
    for row in account_rows:
        institution_name = str(row["institution_name"] or "").strip()
        if not institution_name:
            continue
        institutions.add(institution_name)
        source = str(row["source"] or "unknown")
        counts_by_institution.setdefault(institution_name, {})[source] = int(row["account_count"] or 0)
        active_by_institution[institution_name] = active_by_institution.get(institution_name, 0) + int(
            row["active_count"] or 0
        )

    for row in conn.execute(
        """
        SELECT institution_name
          FROM plaid_items
         WHERE institution_name IS NOT NULL
           AND trim(institution_name) <> ''
        """
    ).fetchall():
        institutions.add(str(row["institution_name"]).strip())

    overrides = [dict(row) for row in _provider_rows(conn)]
    for row in overrides:
        institutions.add(str(row["institution_name"] or "").strip())

    rows: list[dict[str, Any]] = []
    for institution_name in sorted((name for name in institutions if name), key=lambda value: value.lower()):
        account_counts = dict(sorted(counts_by_institution.get(institution_name, {}).items()))
        rows.append(
            {
                "institution_name": institution_name,
                "designated_provider": get_provider_for_institution(conn, institution_name),
                "account_counts": account_counts,
                "total_accounts": int(sum(account_counts.values())),
                "active_accounts": int(active_by_institution.get(institution_name, 0)),
            }
        )

    data = {
        "institutions": rows,
        "db_overrides": overrides,
    }
    return {
        "data": data,
        "summary": {
            "institution_count": len(rows),
            "override_count": len(overrides),
        },
        "cli_report": _status_cli_report(rows),
    }


def handle_switch(args, conn: sqlite3.Connection) -> dict[str, Any]:
    institution = str(args.institution or "").strip()
    provider = str(args.provider or "").strip().lower()
    if not institution:
        raise ValueError("institution is required")
    if not provider:
        raise ValueError("provider is required")

    previous_provider = get_provider_for_institution(conn, institution)
    conn.execute(
        """
        INSERT INTO provider_routing (institution_name, provider, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(institution_name) DO UPDATE SET
            provider = excluded.provider,
            updated_at = datetime('now')
        """,
        (institution, provider),
    )

    accounts_to_deactivate = conn.execute(
        """
        SELECT id, source
          FROM accounts
         WHERE institution_name = ?
           AND COALESCE(source, '') <> ?
           AND is_active = 1
         ORDER BY source, id
        """,
        (institution, provider),
    ).fetchall()
    deactivated_accounts = [str(row["id"]) for row in accounts_to_deactivate]
    conn.execute(
        """
        UPDATE accounts
           SET is_active = 0,
               updated_at = datetime('now')
         WHERE institution_name = ?
           AND COALESCE(source, '') <> ?
           AND is_active = 1
        """,
        (institution, provider),
    )

    message = (
        f"{institution} switched to {provider}. "
        f"Run `{_sync_hint(provider)}` to sync."
    )
    return {
        "data": {
            "institution_name": institution,
            "previous_provider": previous_provider,
            "new_provider": provider,
            "deactivated_count": len(deactivated_accounts),
            "deactivated_account_ids": deactivated_accounts,
            "message": message,
        },
        "summary": {
            "deactivated_count": len(deactivated_accounts),
        },
        "cli_report": message,
    }
