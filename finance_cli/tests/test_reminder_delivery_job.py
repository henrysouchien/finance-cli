from __future__ import annotations

from argparse import Namespace
from datetime import datetime
from pathlib import Path

from finance_cli import account_alerts
from finance_cli.commands import reminder_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.scripts.reminder_delivery_job import (
    ReminderDeliverySettings,
    run_delivery,
)
from finance_cli.user_provisioning import ensure_tenant_marker, user_db_path


def test_reminder_delivery_job_processes_local_user_dry_run(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    user_id = "1"
    db_path = user_db_path(data_root, user_id)
    initialize_database(db_path)
    ensure_tenant_marker(data_root=data_root, user_id=user_id)

    with connect(db_path, expected_user_id=user_id) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('card-zero', 'Promo Bank', 'Zero', 'credit_card', 0, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('card-paydown', 'High Bank', 'Rewards', 'credit_card', 0, 1)
            """
        )
        conn.commit()
        reminder_cmd.handle_card_rotation_set(
            Namespace(
                zero_apr_account_id="card-zero",
                paydown_account_id="card-paydown",
                intro_apr_end_date="2026-05-16",
                avg_monthly_spend_cents=50_000,
                estimated_interest_saved_cents=7_500,
                channel="telegram",
                days_before=7,
                dry_run=False,
            ),
            conn,
        )
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('checking-1', 'Test Bank', 'Checking', 'checking', 40000, 1)
            """
        )
        conn.commit()
        account_alerts.set_low_balance_alert(
            conn,
            account_id="checking-1",
            threshold_cents=50_000,
        )

    summary = run_delivery(
        settings=ReminderDeliverySettings(data_root=data_root, database_url=""),
        user_id=user_id,
        now=datetime.fromisoformat("2026-05-09T10:00:00"),
        dry_run=True,
    )

    assert summary.user_count == 1
    assert summary.processed_users == 1
    assert summary.error_users == 0
    assert summary.due_reminders == 1
    assert summary.preview_reminders == 1
    assert summary.checked_account_alerts == 1
    assert summary.preview_account_alerts == 1
