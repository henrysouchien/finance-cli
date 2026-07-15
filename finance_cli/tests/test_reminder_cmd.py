from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli.commands import reminder_cmd
from finance_cli.db import connect, initialize_database


def _seed_card(
    conn,
    account_id: str,
    institution: str,
    name: str,
    *,
    balance_cents: int = 0,
    card_ending: str | None = None,
    is_business: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, card_ending,
            balance_current_cents, is_active, is_business
        ) VALUES (?, ?, ?, 'credit_card', ?, ?, 1, ?)
        """,
        (account_id, institution, name, card_ending, balance_cents, is_business),
    )
    conn.commit()


def _seed_checking(conn, account_id: str = "checking-1") -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Cash Bank', 'Checking', 'checking', 300000, 1)
        """,
        (account_id,),
    )
    conn.commit()


def _seed_credit_liability(conn, account_id: str, apr_purchase: float | None = 24.99) -> None:
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, 7500)
        """,
        (f"liability-{account_id}", account_id, apr_purchase),
    )
    conn.commit()


def _card_rotation_args(**overrides) -> Namespace:
    values = {
        "zero_apr_account_id": "card-zero",
        "paydown_account_id": "card-paydown",
        "intro_apr_end_date": "2026-06-30",
        "avg_monthly_spend_cents": 50_000,
        "estimated_interest_saved_cents": 7_500,
        "channel": "telegram",
        "days_before": 7,
        "dry_run": False,
    }
    values.update(overrides)
    return Namespace(**values)


def _balance_transfer_args(**overrides) -> Namespace:
    values = {
        "account_id": "card-transfer",
        "remind_on": "2099-06-02",
        "balance_transfer_fee_percent": 3.0,
        "channel": "telegram",
        "note": "",
        "dry_run": False,
    }
    values.update(overrides)
    return Namespace(**values)


def test_card_rotation_reminder_set_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_card(conn, "card-zero", "Promo Bank", "Zero")
        _seed_card(conn, "card-paydown", "High Bank", "Rewards")

        first = reminder_cmd.handle_card_rotation_set(_card_rotation_args(), conn)
        second = reminder_cmd.handle_card_rotation_set(
            _card_rotation_args(avg_monthly_spend_cents=60_000),
            conn,
        )
        rows = conn.execute("SELECT id, payload_json FROM reminders").fetchall()

    assert first["summary"]["scheduled"] == 1
    assert second["summary"]["id"] == first["summary"]["id"]
    assert len(rows) == 1
    payload = json.loads(rows[0]["payload_json"])
    assert payload["avg_monthly_spend_cents"] == 60_000
    assert payload["zero_apr_account_id"] == "card-zero"


def test_balance_transfer_reminder_set_is_idempotent_and_snapshots_card(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_card(
            conn,
            "card-transfer",
            "High Bank",
            "Rewards",
            balance_cents=-250_000,
            card_ending="1234",
        )
        _seed_credit_liability(conn, "card-transfer", apr_purchase=24.99)

        first = reminder_cmd.handle_balance_transfer_set(_balance_transfer_args(), conn)
        second = reminder_cmd.handle_balance_transfer_set(
            _balance_transfer_args(balance_transfer_fee_percent=5.0, note="Apply after statement closes."),
            conn,
        )
        rows = conn.execute("SELECT id, kind, due_at, payload_json FROM reminders").fetchall()

    assert first["summary"]["scheduled"] == 1
    assert first["summary"]["balance_transfer_fee_cents"] == 7_500
    assert first["summary"]["interest_avoided_12mo_cents"] == 62_475
    assert second["summary"]["id"] == first["summary"]["id"]
    assert len(rows) == 1
    assert rows[0]["kind"] == "balance_transfer"
    assert rows[0]["due_at"] == "2099-06-02 09:00:00"
    payload = json.loads(rows[0]["payload_json"])
    assert payload["account_id"] == "card-transfer"
    assert payload["account_label"] == "High Bank Rewards ending 1234"
    assert payload["balance_cents"] == 250_000
    assert payload["apr_purchase"] == 24.99
    assert payload["balance_transfer_fee_percent"] == 5.0
    assert payload["balance_transfer_fee_cents"] == 12_500
    assert payload["net_savings_12mo_cents"] == 49_975
    assert payload["meets_playbook_trigger"] is True
    assert payload["note"] == "Apply after statement closes."


def test_balance_transfer_reminder_reuses_pending_account_reminder_across_date_and_channel(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_card(conn, "card-transfer", "High Bank", "Rewards", balance_cents=-250_000)
        _seed_credit_liability(conn, "card-transfer", apr_purchase=24.99)

        first = reminder_cmd.handle_balance_transfer_set(_balance_transfer_args(), conn)
        second = reminder_cmd.handle_balance_transfer_set(
            _balance_transfer_args(remind_on="2099-07-03", channel="imessage"),
            conn,
        )
        rows = conn.execute("SELECT id, due_at, channel, idempotency_key FROM reminders").fetchall()

    assert second["summary"]["id"] == first["summary"]["id"]
    assert len(rows) == 1
    assert rows[0]["due_at"] == "2099-07-03 09:00:00"
    assert rows[0]["channel"] == "imessage"
    assert rows[0]["idempotency_key"] == "balance_transfer:card-transfer"


def test_balance_transfer_reminder_reuses_legacy_pending_alias_reminder(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_card(conn, "canonical-card", "High Bank", "Canonical", balance_cents=-250_000)
        _seed_card(conn, "hash-card", "High Bank", "Hash", balance_cents=-250_000)
        _seed_credit_liability(conn, "canonical-card", apr_purchase=24.99)
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            ("hash-card", "canonical-card"),
        )
        conn.execute(
            """
            INSERT INTO reminders (
                id, kind, title, body, due_at, channel, status, payload_json, idempotency_key
            ) VALUES ('legacy-reminder', 'balance_transfer', 'Old', 'Old',
                      '2099-06-02 09:00:00', 'telegram', 'pending', ?, ?)
            """,
            ('{"account_id":"hash-card"}', "balance_transfer:hash-card:2099-06-02:telegram"),
        )
        conn.commit()

        result = reminder_cmd.handle_balance_transfer_set(
            _balance_transfer_args(account_id="canonical-card", remind_on="2099-08-04"),
            conn,
        )
        rows = conn.execute("SELECT id, due_at, payload_json, idempotency_key FROM reminders").fetchall()

    assert result["summary"]["id"] == "legacy-reminder"
    assert len(rows) == 1
    assert rows[0]["due_at"] == "2099-08-04 09:00:00"
    payload = json.loads(rows[0]["payload_json"])
    assert payload["account_id"] == "canonical-card"
    assert rows[0]["idempotency_key"] == "balance_transfer:hash-card:2099-06-02:telegram"


def test_balance_transfer_reminder_dry_run_does_not_write(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_card(conn, "card-transfer", "High Bank", "Rewards", balance_cents=-250_000)
        _seed_credit_liability(conn, "card-transfer", apr_purchase=24.99)

        result = reminder_cmd.handle_balance_transfer_set(
            _balance_transfer_args(dry_run=True),
            conn,
        )
        row_count = conn.execute("SELECT COUNT(*) AS n FROM reminders").fetchone()["n"]

    assert result["summary"]["scheduled"] == 0
    assert result["data"]["dry_run"] is True
    assert row_count == 0


def test_balance_transfer_reminder_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_checking(conn)
        _seed_card(conn, "paid-off", "Zero Bank", "Paid Off", balance_cents=0)
        _seed_card(conn, "missing-apr", "High Bank", "No APR", balance_cents=-250_000)
        _seed_card(conn, "canonical-card", "High Bank", "Canonical", balance_cents=-250_000)
        _seed_card(conn, "hash-card", "High Bank", "Hash", balance_cents=-250_000)
        _seed_card(conn, "business-card", "Biz Bank", "Card", balance_cents=-250_000, is_business=1)
        _seed_card(conn, "small-balance", "Small Bank", "Card", balance_cents=-199_999)
        _seed_card(conn, "low-apr", "Low Bank", "Card", balance_cents=-250_000)
        _seed_card(conn, "high-fee", "Fee Bank", "Card", balance_cents=-250_000)
        _seed_credit_liability(conn, "missing-apr", apr_purchase=None)
        _seed_credit_liability(conn, "business-card", apr_purchase=24.99)
        _seed_credit_liability(conn, "small-balance", apr_purchase=24.99)
        _seed_credit_liability(conn, "low-apr", apr_purchase=17.99)
        _seed_credit_liability(conn, "high-fee", apr_purchase=18.0)
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            ("hash-card", "canonical-card"),
        )

        with pytest.raises(ValueError, match="account not found"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="missing"),
                conn,
            )
        with pytest.raises(ValueError, match="credit_card"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="checking-1"),
                conn,
            )
        with pytest.raises(ValueError, match="canonical account"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="hash-card"),
                conn,
            )
        with pytest.raises(ValueError, match="personal credit_card"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="business-card"),
                conn,
            )
        with pytest.raises(ValueError, match="positive balance"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="paid-off"),
                conn,
            )
        with pytest.raises(ValueError, match="at least \\$2,000"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="small-balance"),
                conn,
            )
        with pytest.raises(ValueError, match="apr_purchase"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="missing-apr"),
                conn,
            )
        with pytest.raises(ValueError, match="at least 18%"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(account_id="low-apr"),
                conn,
            )
        with pytest.raises(ValueError, match="positive estimated net savings"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(
                    account_id="high-fee",
                    balance_transfer_fee_percent=20,
                ),
                conn,
            )
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(remind_on="06/02/2099"),
                conn,
            )
        with pytest.raises(ValueError, match="between 0 and 20"):
            reminder_cmd.handle_balance_transfer_set(
                _balance_transfer_args(balance_transfer_fee_percent=25),
                conn,
            )


def test_send_due_reminders_dispatches_and_marks_sent(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    sent_messages: list[dict[str, object]] = []

    def fake_send(message: str, *, channel: str, **kwargs):
        sent_messages.append({"message": message, "channel": channel, "kwargs": kwargs})
        return {"ok": True}

    monkeypatch.setattr(reminder_cmd, "_HAS_ALERTS", True)
    monkeypatch.setattr(reminder_cmd, "alerts", SimpleNamespace(send=fake_send))

    with connect(db_path) as conn:
        _seed_card(conn, "card-zero", "Promo Bank", "Zero")
        _seed_card(conn, "card-paydown", "High Bank", "Rewards")
        conn.execute(
            """
            INSERT INTO notification_channels (channel, config, label)
            VALUES ('telegram', ?, 'primary')
            """,
            (json.dumps({"chat_id": "123"}),),
        )
        reminder_cmd.handle_card_rotation_set(
            _card_rotation_args(intro_apr_end_date="2026-05-16"),
            conn,
        )

        result = reminder_cmd.handle_send_due(
            Namespace(channel=None, now="2026-05-09T10:00:00", limit=50, dry_run=False),
            conn,
        )
        row = conn.execute("SELECT status, sent_at FROM reminders").fetchone()

    assert result["summary"] == {"due": 1, "sent": 1, "failed": 0, "previews": 0}
    assert row["status"] == "sent"
    assert row["sent_at"] == "2026-05-09 10:00:00"
    assert sent_messages == [
        {
            "message": (
                "0% APR card rotation reminder\n\n"
                "Promo Bank Zero's 0% APR period ends 2026-05-16. "
                "Stop routing new daily spend there before the promo expires and confirm the promo balance is paid off. "
                "Keep sending freed cash to High Bank Rewards. Recent spend estimate: $500/mo. "
                "Estimated interest avoided by rotating spend: $75."
            ),
            "channel": "telegram",
            "kwargs": {"chat_id": "123"},
        }
    ]


def test_send_due_reminders_dry_run_does_not_mark_sent(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_card(conn, "card-zero", "Promo Bank", "Zero")
        _seed_card(conn, "card-paydown", "High Bank", "Rewards")
        reminder_cmd.handle_card_rotation_set(
            _card_rotation_args(intro_apr_end_date="2026-05-16"),
            conn,
        )

        result = reminder_cmd.handle_send_due(
            Namespace(channel=None, now="2026-05-09T10:00:00", limit=50, dry_run=True),
            conn,
        )
        row = conn.execute("SELECT status, sent_at FROM reminders").fetchone()

    assert result["summary"] == {"due": 1, "sent": 0, "failed": 0, "previews": 1}
    assert row["status"] == "pending"
    assert row["sent_at"] is None


def test_balance_transfer_reminder_tool_is_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    assert "set_balance_transfer_reminder" in gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert "set_balance_transfer_reminder" in DB_WRITE_TOOLS
