from __future__ import annotations

import sqlite3
import uuid
from argparse import Namespace
from pathlib import Path

from finance_cli.commands import stripe_cmd
from finance_cli.stripe_client import StripeConfigStatus


class _FakeStripe:
    def __init__(self):
        self.api_key = None
        self.Account = type("_Account", (), {"retrieve": staticmethod(self._retrieve_account)})
        self.Balance = type("_Balance", (), {"retrieve": staticmethod(self._retrieve_balance)})
        self.BalanceTransaction = type("_BT", (), {"list": staticmethod(self._list)})

    @staticmethod
    def _retrieve_account():
        return {"id": "acct_cmd_123", "business_profile": {"name": "Cmd LLC"}}

    @staticmethod
    def _retrieve_balance():
        return {
            "available": [{"currency": "usd", "amount": 12345}],
            "pending": [{"currency": "usd", "amount": 500}],
        }

    @staticmethod
    def _list(**kwargs):
        del kwargs
        return {"data": [], "has_more": False}


def _migrated_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    for path in sorted(migration_dir.glob("*.sql")):
        conn.executescript(path.read_text(encoding="utf-8"))
    return conn


def _ns(**kwargs) -> Namespace:
    return Namespace(**kwargs)


def test_link_not_configured_returns_ready_false(monkeypatch) -> None:
    conn = _migrated_conn()
    try:
        monkeypatch.setattr(
            "finance_cli.commands.stripe_cmd.config_status",
            lambda conn: StripeConfigStatus(
                configured=False,
                has_sdk=True,
                missing_env=["STRIPE_API_KEY"],
                account_name=None,
                connection_count=0,
            ),
        )

        result = stripe_cmd.handle_link(_ns(), conn)
        assert result["data"]["ready"] is False
        assert "STRIPE_API_KEY" in result["data"]["missing_env"]
    finally:
        conn.close()


def test_link_success_creates_connection_and_account(monkeypatch) -> None:
    conn = _migrated_conn()
    try:
        fake = _FakeStripe()
        monkeypatch.setenv("STRIPE_API_KEY", "sk_test_cmd")
        monkeypatch.setattr("finance_cli.stripe_client._has_stripe_sdk", lambda: True)
        monkeypatch.setattr("finance_cli.stripe_client._import_stripe", lambda: fake)

        result = stripe_cmd.handle_link(_ns(), conn)
        assert result["summary"]["ready"] is True

        connection = conn.execute("SELECT * FROM stripe_connections WHERE id = 'default'").fetchone()
        assert connection is not None
        assert connection["account_id"] == "acct_cmd_123"

        account = conn.execute(
            """
            SELECT institution_name, account_name, source, is_business
              FROM accounts
             WHERE institution_name = 'Stripe' AND account_name = 'Stripe Balance'
            """
        ).fetchone()
        assert account is not None
        assert account["source"] == "stripe"
        assert int(account["is_business"] or 0) == 1
    finally:
        conn.close()


def test_sync_passes_days_and_force(monkeypatch) -> None:
    conn = _migrated_conn()
    try:
        captured: dict[str, object] = {}

        def _fake_run_sync(conn, days=None, force=False, backfill=False):
            captured["days"] = days
            captured["force"] = force
            captured["backfill"] = backfill
            return {
                "charges_added": 0,
                "fees_added": 0,
                "refunds_added": 0,
                "adjustments_added": 0,
                "payouts_matched": 0,
                "payouts_ambiguous": 0,
                "payouts_unmatched": 0,
                "skipped_existing": 0,
                "skipped_non_usd": 0,
                "skipped_unknown_type": 0,
                "errors": [],
                "skipped_cooldown": False,
            }

        monkeypatch.setattr("finance_cli.commands.stripe_cmd.run_sync", _fake_run_sync)

        stripe_cmd.handle_sync(_ns(days=14, force=True, backfill=False), conn)
        assert captured == {"days": 14, "force": True, "backfill": False}
    finally:
        conn.close()


def test_sync_passes_backfill(monkeypatch) -> None:
    conn = _migrated_conn()
    try:
        captured: dict[str, object] = {}

        def _fake_run_sync(conn, days=None, force=False, backfill=False):
            captured["days"] = days
            captured["force"] = force
            captured["backfill"] = backfill
            return {
                "charges_added": 0,
                "fees_added": 0,
                "refunds_added": 0,
                "adjustments_added": 0,
                "payouts_matched": 0,
                "payouts_ambiguous": 0,
                "payouts_unmatched": 0,
                "skipped_existing": 0,
                "skipped_non_usd": 0,
                "skipped_unknown_type": 0,
                "errors": [],
                "skipped_cooldown": False,
            }

        monkeypatch.setattr("finance_cli.commands.stripe_cmd.run_sync", _fake_run_sync)

        stripe_cmd.handle_sync(_ns(days=None, force=False, backfill=True), conn)
        assert captured == {"days": None, "force": False, "backfill": True}
    finally:
        conn.close()


def test_status_returns_connection_info(monkeypatch) -> None:
    conn = _migrated_conn()
    try:
        conn.execute(
            """
            INSERT INTO stripe_connections (
                id, account_id, account_name, api_key_ref, sync_cursor, last_sync_at, status
            ) VALUES ('default', 'acct_status_123', 'Status LLC', 'STRIPE_API_KEY', '1700000000', '2026-01-01 00:00:00', 'active')
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, dedupe_key, date, description, amount_cents, source, source_category, use_type
            ) VALUES (?, ?, '2026-01-10', 'Stripe charge', 1000, 'stripe', 'charge', 'Business')
            """,
            (uuid.uuid4().hex, f"stripe:{uuid.uuid4().hex}:charge"),
        )
        conn.commit()

        monkeypatch.setattr(
            "finance_cli.commands.stripe_cmd.config_status",
            lambda conn: StripeConfigStatus(
                configured=True,
                has_sdk=True,
                missing_env=[],
                account_name="Status LLC",
                connection_count=1,
            ),
        )
        monkeypatch.setattr(
            "finance_cli.commands.stripe_cmd.balance_status",
            lambda: {"available_cents": 12345, "pending_cents": 500},
        )

        result = stripe_cmd.handle_status(_ns(), conn)
        assert result["data"]["configured"] is True
        assert result["data"]["connection"]["account_id"] == "acct_status_123"
        assert result["data"]["transaction_count"] == 1
        assert result["data"]["balance"]["available_cents"] == 12345
    finally:
        conn.close()


def test_revenue_monthly_breakdown() -> None:
    conn = _migrated_conn()
    try:
        rows = [
            ("2026-01-10", 10_000, "charge"),
            ("2026-01-10", -300, "fee"),
            ("2026-01-15", -500, "refund"),
        ]
        for txn_date, amount_cents, source_category in rows:
            conn.execute(
                """
                INSERT INTO transactions (
                    id, dedupe_key, date, description, amount_cents, source, source_category, use_type
                ) VALUES (?, ?, ?, 'Stripe', ?, 'stripe', ?, 'Business')
                """,
                (uuid.uuid4().hex, f"stripe:{uuid.uuid4().hex}:{source_category}", txn_date, amount_cents, source_category),
            )
        conn.commit()

        result = stripe_cmd.handle_revenue(
            _ns(month="2026-01", quarter=None, year=None),
            conn,
        )
        assert result["summary"]["months"] == 1
        assert result["data"]["rows"][0]["gross_cents"] == 10_000
        assert result["data"]["rows"][0]["fees_cents"] == 300
        assert result["data"]["rows"][0]["refunds_cents"] == 500
        assert result["data"]["rows"][0]["net_cents"] == 9200
    finally:
        conn.close()


def test_revenue_empty_period() -> None:
    conn = _migrated_conn()
    try:
        result = stripe_cmd.handle_revenue(
            _ns(month="2026-02", quarter=None, year=None),
            conn,
        )
        assert result["summary"]["months"] == 0
        assert result["data"]["rows"] == []
        assert "No Stripe revenue data" in result["cli_report"]
    finally:
        conn.close()


def test_unlink_sets_disconnected() -> None:
    conn = _migrated_conn()
    try:
        conn.execute(
            """
            INSERT INTO stripe_connections (
                id, account_id, account_name, api_key_ref, sync_cursor, last_sync_at, status
            ) VALUES ('default', 'acct_unlink_123', 'Unlink LLC', 'STRIPE_API_KEY', NULL, NULL, 'active')
            """
        )
        conn.commit()

        result = stripe_cmd.handle_unlink(_ns(), conn)
        row = conn.execute("SELECT status FROM stripe_connections WHERE id = 'default'").fetchone()

        assert result["data"]["status"] == "disconnected"
        assert row["status"] == "disconnected"
    finally:
        conn.close()


def test_mcp_tools_registered(monkeypatch) -> None:
    import finance_cli.mcp_server as mcp_server
    from finance_cli.commands import stripe_cmd as stripe_cmd_module

    calls: list[tuple[str, dict]] = []

    def _fake_call(handler, ns_kwargs):
        calls.append((handler.__name__, dict(ns_kwargs)))
        return {"data": {}, "summary": {}}

    monkeypatch.setattr(mcp_server, "_call", _fake_call)

    mcp_server.stripe_link()
    mcp_server.stripe_sync(days=7, force=True, backfill=False)
    mcp_server.stripe_status()
    mcp_server.stripe_revenue(month="2026-01")
    mcp_server.stripe_unlink()

    assert hasattr(mcp_server, "stripe_link")
    assert hasattr(mcp_server, "stripe_sync")
    assert hasattr(mcp_server, "stripe_status")
    assert hasattr(mcp_server, "stripe_revenue")
    assert hasattr(mcp_server, "stripe_unlink")

    assert calls == [
        (stripe_cmd_module.handle_link.__name__, {}),
        (stripe_cmd_module.handle_sync.__name__, {"days": 7, "force": True, "backfill": False}),
        (stripe_cmd_module.handle_status.__name__, {}),
        (stripe_cmd_module.handle_revenue.__name__, {"month": "2026-01", "quarter": None, "year": None}),
        (stripe_cmd_module.handle_unlink.__name__, {}),
    ]
