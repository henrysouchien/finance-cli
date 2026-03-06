from __future__ import annotations

import sqlite3
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli.commands.biz_cmd import _schedule_c_snapshot
from finance_cli.stripe_client import (
    StripeConfigStatus,
    StripeUnavailableError,
    _dedup_payout_against_plaid,
    config_status,
    run_sync,
)


class _FakeStripe:
    def __init__(self, pages: list[dict], account_payload: dict | None = None, balance_payload: dict | None = None):
        self.api_key = None
        self._pages = pages
        self._page_index = 0
        self._account_payload = account_payload or {
            "id": "acct_test_123",
            "business_profile": {"name": "Acme LLC"},
        }
        self._balance_payload = balance_payload or {
            "available": [{"currency": "usd", "amount": 0}],
            "pending": [{"currency": "usd", "amount": 0}],
        }
        self.list_calls: list[dict] = []

        self.BalanceTransaction = SimpleNamespace(list=self._list)
        self.Account = SimpleNamespace(retrieve=self._account_retrieve)
        self.Balance = SimpleNamespace(retrieve=self._balance_retrieve)

    def _list(self, **kwargs):
        self.list_calls.append(dict(kwargs))
        if not self._pages:
            return {"data": [], "has_more": False}
        idx = min(self._page_index, len(self._pages) - 1)
        self._page_index += 1
        return self._pages[idx]

    def _account_retrieve(self):
        return self._account_payload

    def _balance_retrieve(self):
        return self._balance_payload


def _migrated_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    for path in sorted(migration_dir.glob("*.sql")):
        conn.executescript(path.read_text(encoding="utf-8"))
    return conn


@pytest.fixture()
def conn():
    c = _migrated_conn()
    try:
        yield c
    finally:
        c.close()


def _seed_account(
    conn: sqlite3.Connection,
    *,
    institution_name: str,
    account_name: str,
    source: str,
    is_business: int,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, source, is_active, is_business
        ) VALUES (?, ?, ?, 'checking', ?, 1, ?)
        """,
        (account_id, institution_name, account_name, source, is_business),
    )
    return account_id


def _seed_plaid_deposit(
    conn: sqlite3.Connection,
    *,
    amount_cents: int,
    txn_date: str,
    is_business: int = 1,
    notes: str | None = None,
) -> str:
    account_id = _seed_account(
        conn,
        institution_name="Chase",
        account_name="Business Checking" if is_business else "Personal Checking",
        source="plaid",
        is_business=is_business,
    )
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, dedupe_key, date, description, amount_cents, source, is_active, notes
        ) VALUES (?, ?, ?, ?, 'ORIG CO NAME:STRIPE', ?, 'plaid', 1, ?)
        """,
        (txn_id, account_id, f"plaid:{txn_id}", txn_date, int(amount_cents), notes),
    )
    return txn_id


def _seed_stripe_connection(
    conn: sqlite3.Connection,
    *,
    sync_cursor: str | None = None,
    last_sync_at: str | None = None,
    status: str = "active",
) -> None:
    conn.execute(
        """
        INSERT INTO stripe_connections (
            id, account_id, account_name, api_key_ref, sync_cursor, last_sync_at, status
        ) VALUES ('default', 'acct_test_123', 'Acme LLC', 'STRIPE_API_KEY', ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            sync_cursor = excluded.sync_cursor,
            last_sync_at = excluded.last_sync_at,
            status = excluded.status,
            updated_at = datetime('now')
        """,
        (sync_cursor, last_sync_at, status),
    )


def _seed_schedule_map(conn: sqlite3.Connection, category_name: str, line_number: str) -> None:
    row = conn.execute(
        "SELECT id FROM categories WHERE lower(trim(name)) = lower(trim(?)) LIMIT 1",
        (category_name,),
    ).fetchone()
    assert row is not None
    conn.execute(
        """
        INSERT INTO schedule_c_map (
            id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes
        ) VALUES (?, ?, ?, ?, 1.0, 2025, NULL)
        ON CONFLICT(category_id, tax_year) DO UPDATE SET
            line_number = excluded.line_number,
            schedule_c_line = excluded.schedule_c_line
        """,
        (uuid.uuid4().hex, str(row["id"]), category_name, line_number),
    )


def _txn_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT t.*, c.name AS category_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.source = 'stripe'
         ORDER BY t.date ASC, t.description ASC, t.id ASC
        """
    ).fetchall()


def _charge_txn(
    *,
    txn_id: str = "txn_charge_1",
    created: int = 1_738_368_000,
    amount: int = 10_000,
    fee: int = 329,
    currency: str = "usd",
    description: str = "Course sale",
) -> dict:
    return {
        "id": txn_id,
        "created": created,
        "currency": currency,
        "reporting_category": "charge",
        "type": "charge",
        "amount": amount,
        "fee": fee,
        "description": description,
        "source": {"id": "ch_123"},
    }


def _fee_txn(
    *,
    txn_id: str = "txn_fee_1",
    created: int = 1_738_368_100,
    amount: int = -300,
) -> dict:
    return {
        "id": txn_id,
        "created": created,
        "currency": "usd",
        "reporting_category": "fee",
        "type": "stripe_fee",
        "amount": amount,
        "fee": 0,
        "description": "Stripe processing fee",
        "source": {"id": "fee_123"},
    }


def _refund_txn(
    *,
    txn_id: str = "txn_refund_1",
    created: int = 1_738_368_200,
    amount: int = -2500,
    reporting_category: str = "refund",
) -> dict:
    return {
        "id": txn_id,
        "created": created,
        "currency": "usd",
        "reporting_category": reporting_category,
        "type": reporting_category,
        "amount": amount,
        "fee": 0,
        "description": f"Stripe {reporting_category}",
        "source": {"id": "re_123"},
    }


def _payout_txn(
    *,
    txn_id: str = "txn_payout_1",
    created: int = 1_738_368_300,
    amount: int = -9671,
    payout_id: str = "po_123",
) -> dict:
    return {
        "id": txn_id,
        "created": created,
        "currency": "usd",
        "reporting_category": "payout",
        "type": "payout",
        "amount": amount,
        "fee": 0,
        "description": "Stripe payout",
        "source": {"id": payout_id},
    }


def _patch_stripe(monkeypatch, fake: _FakeStripe) -> None:
    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_123")
    monkeypatch.setattr("finance_cli.stripe_client._has_stripe_sdk", lambda: True)
    monkeypatch.setattr("finance_cli.stripe_client._import_stripe", lambda: fake)


def test_config_status_sdk_missing(monkeypatch) -> None:
    monkeypatch.delenv("STRIPE_API_KEY", raising=False)
    monkeypatch.setattr("finance_cli.stripe_client._has_stripe_sdk", lambda: False)

    status = config_status()
    assert status == StripeConfigStatus(
        configured=False,
        has_sdk=False,
        missing_env=["STRIPE_API_KEY"],
        account_name=None,
        connection_count=0,
    )


def test_config_status_configured_with_connection(conn, monkeypatch) -> None:
    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_123")
    monkeypatch.setattr("finance_cli.stripe_client._has_stripe_sdk", lambda: True)
    _seed_stripe_connection(conn)
    conn.commit()

    status = config_status(conn)
    assert status.configured is True
    assert status.has_sdk is True
    assert status.missing_env == []
    assert status.connection_count == 1
    assert status.account_name == "Acme LLC"


def test_run_sync_charge_creates_two_rows(conn, monkeypatch) -> None:
    fake = _FakeStripe([{"data": [_charge_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)
    rows = _txn_rows(conn)

    assert result["charges_added"] == 1
    assert result["fees_added"] == 1
    assert len(rows) == 2

    charge_row = next(r for r in rows if r["dedupe_key"].endswith(":charge"))
    fee_row = next(r for r in rows if r["dedupe_key"].endswith(":fee"))

    assert charge_row["amount_cents"] == 10_000
    assert charge_row["category_name"] == "Income: Business"
    assert charge_row["stripe_txn_id"] == "txn_charge_1"
    assert charge_row["use_type"] == "Business"
    assert charge_row["source"] == "stripe"

    assert fee_row["amount_cents"] == -329
    assert fee_row["category_name"] == "Cost of Goods Sold"
    assert fee_row["stripe_txn_id"] is None
    assert fee_row["use_type"] == "Business"
    assert fee_row["source"] == "stripe"


def test_run_sync_fee_categorized_cogs(conn, monkeypatch) -> None:
    fake = _FakeStripe([{"data": [_fee_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)
    rows = _txn_rows(conn)

    assert result["fees_added"] == 1
    assert len(rows) == 1
    assert rows[0]["category_name"] == "Cost of Goods Sold"
    assert rows[0]["amount_cents"] == -300
    assert rows[0]["dedupe_key"] == "stripe:txn_fee_1:fee"


def test_run_sync_refund_negative_income(conn, monkeypatch) -> None:
    fake = _FakeStripe([{"data": [_refund_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)
    rows = _txn_rows(conn)

    assert result["refunds_added"] == 1
    assert len(rows) == 1
    assert rows[0]["category_name"] == "Income: Business"
    assert rows[0]["amount_cents"] == -2500
    assert rows[0]["dedupe_key"] == "stripe:txn_refund_1"


def test_run_sync_dispute_and_reversal(conn, monkeypatch) -> None:
    fake = _FakeStripe(
        [
            {
                "data": [
                    _refund_txn(txn_id="txn_dispute_1", amount=-5000, reporting_category="dispute"),
                    _refund_txn(txn_id="txn_dispute_rev_1", amount=5000, reporting_category="dispute_reversal"),
                ],
                "has_more": False,
            }
        ]
    )
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)
    rows = _txn_rows(conn)

    assert result["adjustments_added"] == 2
    assert len(rows) == 2
    assert {int(r["amount_cents"]) for r in rows} == {-5000, 5000}
    assert all(r["category_name"] == "Income: Business" for r in rows)


def test_run_sync_payout_matched_deactivates_plaid(conn, monkeypatch) -> None:
    _seed_plaid_deposit(conn, amount_cents=9671, txn_date="2025-01-31", is_business=1)
    fake = _FakeStripe([{"data": [_payout_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)

    plaid_row = conn.execute(
        "SELECT is_active, notes FROM transactions WHERE source = 'plaid'"
    ).fetchone()
    assert result["payouts_matched"] == 1
    assert result["payouts_ambiguous"] == 0
    assert result["payouts_unmatched"] == 0
    assert plaid_row["is_active"] == 0
    assert "Deduped: Stripe payout po_123" in str(plaid_row["notes"] or "")


def test_run_sync_payout_ambiguous(conn, monkeypatch) -> None:
    _seed_plaid_deposit(conn, amount_cents=9671, txn_date="2025-01-30", is_business=1)
    _seed_plaid_deposit(conn, amount_cents=9671, txn_date="2025-02-01", is_business=1)
    fake = _FakeStripe([{"data": [_payout_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)

    active_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM transactions WHERE source = 'plaid' AND is_active = 1"
    ).fetchone()
    assert result["payouts_ambiguous"] == 1
    assert active_rows["n"] == 2


def test_run_sync_payout_unmatched(conn, monkeypatch) -> None:
    fake = _FakeStripe([{"data": [_payout_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)

    assert result["payouts_matched"] == 0
    assert result["payouts_unmatched"] == 1


def test_dedup_sign_normalization_abs_match(conn) -> None:
    txn_id = _seed_plaid_deposit(conn, amount_cents=5000, txn_date="2025-01-31", is_business=1)
    status = _dedup_payout_against_plaid(
        conn,
        payout_amount_cents=-5000,
        payout_date="2025-01-31",
        payout_id="po_sign_test",
    )
    conn.commit()

    row = conn.execute("SELECT is_active FROM transactions WHERE id = ?", (txn_id,)).fetchone()
    assert status == "matched"
    assert row["is_active"] == 0


def test_run_sync_high_water_mark_cursor(conn, monkeypatch) -> None:
    _seed_stripe_connection(conn, sync_cursor="1738367900", last_sync_at="2020-01-01 00:00:00")
    conn.commit()

    fake = _FakeStripe(
        [
            {
                "data": [_charge_txn(created=1_738_367_915, txn_id="txn_charge_cursor")],
                "has_more": False,
            }
        ]
    )
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)
    cursor_row = conn.execute("SELECT sync_cursor FROM stripe_connections WHERE id = 'default'").fetchone()

    assert fake.list_calls[0]["created"]["gte"] == 1_738_367_900
    assert result["max_created"] == 1_738_367_915
    assert cursor_row["sync_cursor"] == "1738367915"


def test_run_sync_cooldown_skips(conn, monkeypatch) -> None:
    _seed_stripe_connection(conn, sync_cursor="0", last_sync_at="2999-01-01 00:00:00")
    conn.commit()
    fake = _FakeStripe([{"data": [_charge_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)

    assert result["skipped_cooldown"] is True
    assert fake.list_calls == []


def test_run_sync_force_bypasses_cooldown(conn, monkeypatch) -> None:
    _seed_stripe_connection(conn, sync_cursor="0", last_sync_at="2999-01-01 00:00:00")
    conn.commit()
    fake = _FakeStripe([{"data": [_charge_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn, force=True)

    assert result["skipped_cooldown"] is False
    assert len(fake.list_calls) == 1


def test_run_sync_backfill_ignores_cursor(conn, monkeypatch) -> None:
    _seed_stripe_connection(conn, sync_cursor="1739999999", last_sync_at="2020-01-01 00:00:00")
    conn.commit()
    fake = _FakeStripe([{"data": [_charge_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    run_sync(conn, backfill=True)

    assert fake.list_calls[0]["created"]["gte"] == 0


def test_run_sync_idempotent(conn, monkeypatch) -> None:
    fake = _FakeStripe([{"data": [_charge_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    first = run_sync(conn)
    second = run_sync(conn, force=True)
    rows = _txn_rows(conn)

    assert first["charges_added"] == 1
    assert first["fees_added"] == 1
    assert second["charges_added"] == 0
    assert second["fees_added"] == 0
    assert second["skipped_existing"] >= 2
    assert len(rows) == 2


def test_run_sync_unknown_reporting_category_skipped(conn, monkeypatch, caplog) -> None:
    fake = _FakeStripe(
        [
            {
                "data": [
                    {
                        "id": "txn_unknown_1",
                        "created": 1_738_368_000,
                        "currency": "usd",
                        "reporting_category": "mystery",
                        "type": "adjustment",
                        "amount": 100,
                        "fee": 0,
                        "description": "Unknown event",
                        "source": {"id": "src_1"},
                    }
                ],
                "has_more": False,
            }
        ]
    )
    _patch_stripe(monkeypatch, fake)

    with caplog.at_level("WARNING"):
        result = run_sync(conn)

    assert result["skipped_unknown_type"] == 1


def test_run_sync_non_usd_skipped(conn, monkeypatch, caplog) -> None:
    fake = _FakeStripe(
        [
            {
                "data": [
                    _charge_txn(txn_id="txn_eur", currency="eur", amount=5000, fee=100),
                ],
                "has_more": False,
            }
        ]
    )
    _patch_stripe(monkeypatch, fake)

    with caplog.at_level("WARNING"):
        result = run_sync(conn)

    assert result["skipped_non_usd"] == 1
    assert len(_txn_rows(conn)) == 0


def test_run_sync_payout_business_account_only(conn, monkeypatch) -> None:
    txn_id = _seed_plaid_deposit(conn, amount_cents=9671, txn_date="2025-01-31", is_business=0)
    fake = _FakeStripe([{"data": [_payout_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    result = run_sync(conn)

    row = conn.execute("SELECT is_active FROM transactions WHERE id = ?", (txn_id,)).fetchone()
    assert result["payouts_unmatched"] == 1
    assert row["is_active"] == 1


def test_dedup_date_window_plus_minus_two_days(conn) -> None:
    txn_id = _seed_plaid_deposit(conn, amount_cents=1000, txn_date="2025-01-03", is_business=1)
    status = _dedup_payout_against_plaid(
        conn,
        payout_amount_cents=-1000,
        payout_date="2025-01-01",
        payout_id="po_window",
    )
    conn.commit()

    row = conn.execute("SELECT is_active FROM transactions WHERE id = ?", (txn_id,)).fetchone()
    assert status == "matched"
    assert row["is_active"] == 0

    txn_id_far = _seed_plaid_deposit(conn, amount_cents=1000, txn_date="2025-01-05", is_business=1)
    status_far = _dedup_payout_against_plaid(
        conn,
        payout_amount_cents=-1000,
        payout_date="2025-01-01",
        payout_id="po_window_far",
    )
    conn.commit()

    row_far = conn.execute("SELECT is_active FROM transactions WHERE id = ?", (txn_id_far,)).fetchone()
    assert status_far == "unmatched"
    assert row_far["is_active"] == 1


def test_run_sync_auto_creates_business_stripe_balance_account(conn, monkeypatch) -> None:
    fake = _FakeStripe([{"data": [_charge_txn()], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    run_sync(conn)

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


def test_schedule_c_e2e_after_stripe_sync(conn, monkeypatch) -> None:
    fake = _FakeStripe([{"data": [_charge_txn(amount=10_000, fee=300)], "has_more": False}])
    _patch_stripe(monkeypatch, fake)

    run_sync(conn)

    _seed_schedule_map(conn, "Cost of Goods Sold", "42")
    conn.commit()

    snapshot = _schedule_c_snapshot(
        conn,
        start=date(2025, 1, 1),
        end=date(2025, 12, 31),
        tax_year=2025,
    )

    assert snapshot["line_1_gross_receipts_cents"] == 10_000
    assert snapshot["line_4_cogs_cents"] == 300


def test_run_sync_raises_when_not_configured(conn, monkeypatch) -> None:
    monkeypatch.delenv("STRIPE_API_KEY", raising=False)
    monkeypatch.setattr("finance_cli.stripe_client._has_stripe_sdk", lambda: True)

    with pytest.raises(StripeUnavailableError):
        run_sync(conn)
