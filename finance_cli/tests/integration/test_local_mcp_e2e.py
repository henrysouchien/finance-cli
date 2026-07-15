from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest

from finance_cli import db as db_module
from finance_cli.db import connect

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_pull_through_encrypted_snapshot_and_sync_token(
    in_process_server,
    mcp_client,
    seeded_token_file: Path,
) -> None:
    assert db_module.db_encryption_mode() == "provision"

    cashnerd_dir = seeded_token_file.parents[1]
    local_db_path = cashnerd_dir / "data" / "finance.db"

    result = await mcp_client.call_tool("db_status")

    assert result.is_error is False
    assert local_db_path.exists()
    assert int(result.data["data"]["transaction_counts"]["active"] or 0) >= 1

    token_payload = json.loads(seeded_token_file.read_text(encoding="utf-8"))
    assert token_payload["user_id"] == in_process_server.user_id
    assert token_payload["sync_token"]

    with psycopg2.connect(
        in_process_server.database_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, client_info
                  FROM sync_sessions
                 ORDER BY id DESC
                 LIMIT 1
                """
            )
            sync_row = cursor.fetchone()

    assert sync_row is not None
    assert str(sync_row["user_id"]) == in_process_server.user_id
    assert sync_row["client_info"]

    with pytest.raises(sqlite3.DatabaseError):
        with sqlite3.connect(str(in_process_server.server_db_path)) as plain_conn:
            plain_conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()

    with connect(
        in_process_server.server_db_path,
        expected_user_id=in_process_server.user_id,
    ) as server_conn:
        server_row = server_conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-server-seed'"
        ).fetchone()
        assert server_row["description"] == "Server Seed"

    with sqlite3.connect(str(local_db_path)) as local_conn:
        local_row = local_conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-server-seed'"
        ).fetchone()
        assert local_row == ("Server Seed",)


@pytest.mark.asyncio
async def test_write_tool_push_lands_in_server_db(
    in_process_server,
    mcp_client,
) -> None:
    await mcp_client.call_tool("db_status")

    result = await mcp_client.call_tool(
        "txn_add",
        {
            "amount": -12.34,
            "date": "2026-04-16",
            "description": "E2E",
            "idempotency_key": "e2e-push",
        },
    )

    assert result.is_error is False

    with connect(
        in_process_server.server_db_path,
        expected_user_id=in_process_server.user_id,
    ) as conn:
        row = conn.execute(
            """
            SELECT description, amount_cents, idempotency_key
              FROM transactions
             WHERE idempotency_key = ?
            """,
            ("e2e-push",),
        ).fetchone()

    assert row is not None
    assert row["description"] == "E2E"
    assert row["amount_cents"] == -1234
    assert row["idempotency_key"] == "e2e-push"


@pytest.mark.asyncio
async def test_local_mode_allows_ingest_outside_uploads_dir(
    mcp_client,
    test_home_dir: Path,
) -> None:
    csv_path = test_home_dir / "custom_location" / "import.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(
        (
            "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
            "02/17/2026,02/18/2026,UBER   *TRIP,Travel,Sale,-29.53,\n"
        ),
        encoding="utf-8",
    )

    result = await mcp_client.call_tool(
        "ingest_csv",
        {
            "file": str(csv_path),
            "institution": "auto",
            "commit": False,
        },
        raise_on_error=False,
    )

    assert result.is_error is False

    payload = dict(result.structured_content or {})
    data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    messages = [
        str(data.get("error") or ""),
        str(data.get("sync_auth_error") or ""),
        *[str(item) for item in summary.get("errors") or []],
    ]
    combined = " ".join(message for message in messages if message)

    assert "file path must be within the user uploads directory" not in combined
