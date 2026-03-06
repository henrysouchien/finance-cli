from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database
from finance_cli.plaid_client import PlaidConfigStatus, _ensure_account, apply_sync_updates, refresh_balances
from finance_cli.provider_routing import check_provider_allowed, get_provider_for_institution
from finance_cli.schwab_client import sync_schwab_balances


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _route(conn, institution_name: str, provider: str) -> None:
    conn.execute(
        """
        INSERT INTO provider_routing (institution_name, provider, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(institution_name) DO UPDATE SET
            provider = excluded.provider,
            updated_at = datetime('now')
        """,
        (institution_name, provider),
    )


def _seed_plaid_item(conn, plaid_item_id: str, institution_name: str = "Test Bank"):
    conn.execute(
        """
        INSERT INTO plaid_items (
            id,
            plaid_item_id,
            institution_name,
            access_token_ref,
            status,
            consented_products,
            sync_cursor
        ) VALUES (?, ?, ?, 'secret/ref', 'active', '["transactions"]', NULL)
        """,
        (uuid.uuid4().hex, plaid_item_id, institution_name),
    )
    conn.commit()
    return conn.execute("SELECT * FROM plaid_items WHERE plaid_item_id = ?", (plaid_item_id,)).fetchone()


def _insert_account(
    conn,
    *,
    account_id: str,
    institution_name: str,
    source: str,
    plaid_account_id: str | None = None,
    plaid_item_id: str | None = None,
    account_type: str = "checking",
    is_active: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, plaid_item_id, institution_name, account_name, account_type, source, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            account_id,
            plaid_account_id,
            plaid_item_id,
            institution_name,
            f"{institution_name} {source}",
            account_type,
            source,
            is_active,
        ),
    )


def test_default_provider_is_plaid(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        assert get_provider_for_institution(conn, "Unknown Institution") == "plaid"


def test_db_override_takes_precedence(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        assert get_provider_for_institution(conn, "Charles Schwab") == "schwab"
        _route(conn, "Charles Schwab", "plaid")
        assert get_provider_for_institution(conn, "Charles Schwab") == "plaid"


def test_code_default_routes_stripe_to_stripe_provider(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        assert get_provider_for_institution(conn, "Stripe") == "stripe"


def test_check_provider_allowed_true(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        allowed, designated = check_provider_allowed(conn, "Acme Bank", "plaid")
        assert allowed is True
        assert designated == "plaid"


def test_check_provider_allowed_false(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _route(conn, "Acme Bank", "schwab")
        allowed, designated = check_provider_allowed(conn, "Acme Bank", "plaid")
        assert allowed is False
        assert designated == "schwab"


def test_ensure_account_skips_both_paths_when_not_allowed(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _route(conn, "Test Bank", "schwab")

        created = _ensure_account(
            conn,
            plaid_item_id="item_guard",
            institution_name="Test Bank",
            plaid_account_id="acct_new",
            account_payload={"name": "Checking", "type": "depository", "subtype": "checking"},
        )
        assert created is None
        assert conn.execute("SELECT id FROM accounts WHERE plaid_account_id = 'acct_new'").fetchone() is None

        existing_id = uuid.uuid4().hex
        _insert_account(
            conn,
            account_id=existing_id,
            institution_name="Test Bank",
            source="plaid",
            plaid_account_id="acct_existing",
        )
        conn.execute("UPDATE accounts SET account_name = 'old account name' WHERE id = ?", (existing_id,))

        updated = _ensure_account(
            conn,
            plaid_item_id="item_guard",
            institution_name="Test Bank",
            plaid_account_id="acct_existing",
            account_payload={"name": "new account name", "type": "depository", "subtype": "checking"},
        )
        assert updated is None
        row = conn.execute("SELECT account_name FROM accounts WHERE id = ?", (existing_id,)).fetchone()
        assert row["account_name"] == "old account name"


def test_ensure_account_creates_for_active_provider(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        account_id = _ensure_account(
            conn,
            plaid_item_id="item_ok",
            institution_name="Test Bank",
            plaid_account_id="acct_ok",
            account_payload={"name": "Checking", "type": "depository", "subtype": "checking"},
        )
        assert account_id is not None
        row = conn.execute(
            "SELECT source, plaid_item_id FROM accounts WHERE plaid_account_id = 'acct_ok'"
        ).fetchone()
        assert row["source"] == "plaid"
        assert row["plaid_item_id"] == "item_ok"


def test_refresh_balances_skips_none_accounts(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Client:
        def accounts_balance_get(self, request):
            return _Resp(
                {
                    "accounts": [
                        {
                            "account_id": "acct_refresh_skip",
                            "name": "Checking",
                            "type": "depository",
                            "subtype": "checking",
                            "balances": {"current": 123.45, "available": 120.00, "iso_currency_code": "USD"},
                        }
                    ]
                }
            )

    with connect(db_path) as conn:
        _seed_plaid_item(conn, plaid_item_id="item_refresh_skip", institution_name="Test Bank")
        _route(conn, "Test Bank", "schwab")
        monkeypatch.setattr(
            "finance_cli.plaid_client.config_status",
            lambda: PlaidConfigStatus(configured=True, has_sdk=True, missing_env=[], env="sandbox"),
        )
        monkeypatch.setattr("finance_cli.plaid_client._create_plaid_api_client", lambda: _Client())
        monkeypatch.setattr("finance_cli.plaid_client._get_access_token_for_item", lambda item, region_name=None: "token")

        out = refresh_balances(conn, item_id="item_refresh_skip")
        assert out["items_refreshed"] == 1
        assert out["accounts_updated"] == 0
        assert out["snapshots_updated"] == 0
        assert conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"] == 0


def test_schwab_sync_skips_when_routed_to_plaid(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _route(conn, "Charles Schwab", "plaid")
        out = sync_schwab_balances(conn)
        assert out["accounts_requested"] == 0
        assert out["accounts_synced"] == 0
        assert "plaid" in out.get("skipped_reason", "")


def test_transaction_sync_skips_routed_accounts(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        item = _seed_plaid_item(conn, plaid_item_id="item_sync_skip", institution_name="Test Bank")
        _route(conn, "Test Bank", "schwab")

        counts = apply_sync_updates(
            conn,
            item,
            added=[
                {
                    "transaction_id": "tx_skip_1",
                    "account_id": "acct_sync_skip",
                    "date": "2025-02-10",
                    "amount": 10.00,
                    "name": "Coffee",
                    "merchant_name": "Coffee",
                    "pending": False,
                    "personal_finance_category": {},
                }
            ],
            modified=[],
            removed=[],
            accounts=[
                {
                    "account_id": "acct_sync_skip",
                    "name": "Checking",
                    "type": "depository",
                    "subtype": "checking",
                }
            ],
            next_cursor="cursor_skip",
        )
        conn.commit()

        assert counts["added"] == 0
        assert counts["skipped"] == 1
        assert conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"] == 0
        assert conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"] == 0


def test_provider_switch_deactivates_old_accounts(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _insert_account(conn, account_id="acct_sw_schwab", institution_name="Charles Schwab", source="schwab")
        _insert_account(conn, account_id="acct_sw_plaid", institution_name="Charles Schwab", source="plaid")
        conn.commit()

    code, payload = _run_cli(["provider", "switch", "Charles Schwab", "plaid"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["summary"]["deactivated_count"] == 1

    with connect(db_path) as conn:
        schwab_row = conn.execute("SELECT is_active FROM accounts WHERE id = 'acct_sw_schwab'").fetchone()
        plaid_row = conn.execute("SELECT is_active FROM accounts WHERE id = 'acct_sw_plaid'").fetchone()
        routing_row = conn.execute(
            "SELECT provider FROM provider_routing WHERE institution_name = 'Charles Schwab'"
        ).fetchone()
        assert schwab_row["is_active"] == 0
        assert plaid_row["is_active"] == 1
        assert routing_row["provider"] == "plaid"


def test_provider_switch_preserves_transactions(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _insert_account(conn, account_id="acct_tx_old", institution_name="Charles Schwab", source="schwab")
        conn.execute(
            """
            INSERT INTO transactions (id, account_id, dedupe_key, date, description, amount_cents, source, is_active)
            VALUES (?, ?, ?, '2025-02-01', 'Old txn', -1000, 'manual', 1)
            """,
            (uuid.uuid4().hex, "acct_tx_old", f"dedupe:{uuid.uuid4().hex}"),
        )
        conn.commit()

    code, payload = _run_cli(["provider", "switch", "Charles Schwab", "plaid"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        txn = conn.execute("SELECT is_active FROM transactions WHERE account_id = 'acct_tx_old'").fetchone()
        assert txn["is_active"] == 1


def test_provider_switch_preserves_snapshots(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _insert_account(conn, account_id="acct_snap_old", institution_name="Charles Schwab", source="schwab")
        conn.execute(
            """
            INSERT INTO balance_snapshots (id, account_id, balance_current_cents, source, snapshot_date)
            VALUES (?, 'acct_snap_old', 50000, 'refresh', date('now'))
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    code, payload = _run_cli(["provider", "switch", "Charles Schwab", "plaid"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        snap_count = conn.execute("SELECT COUNT(*) AS n FROM balance_snapshots").fetchone()["n"]
        assert snap_count == 1


def test_provider_switch_preserves_aliases(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _insert_account(conn, account_id="acct_alias_hash", institution_name="Charles Schwab", source="schwab")
        _insert_account(conn, account_id="acct_alias_canon", institution_name="Charles Schwab", source="schwab")
        conn.execute(
            """
            INSERT INTO account_aliases (hash_account_id, canonical_id)
            VALUES ('acct_alias_hash', 'acct_alias_canon')
            """
        )
        conn.commit()

    code, payload = _run_cli(["provider", "switch", "Charles Schwab", "plaid"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT hash_account_id, canonical_id
              FROM account_aliases
             WHERE hash_account_id = 'acct_alias_hash'
            """
        ).fetchone()
        assert row["canonical_id"] == "acct_alias_canon"


def test_provider_switch_preserves_data(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _insert_account(conn, account_id="acct_data_old", institution_name="Charles Schwab", source="schwab")
        conn.commit()

    code, payload = _run_cli(["provider", "switch", "Charles Schwab", "plaid"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n, MAX(is_active) AS is_active FROM accounts WHERE id = 'acct_data_old'"
        ).fetchone()
        assert row["n"] == 1
        assert row["is_active"] == 0


def test_provider_switch_generic(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _insert_account(conn, account_id="acct_generic_a", institution_name="Example Bank", source="plaid")
        _insert_account(conn, account_id="acct_generic_b", institution_name="Example Bank", source="schwab")
        conn.commit()

    code, payload = _run_cli(["provider", "switch", "Example Bank", "custom_provider"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["new_provider"] == "custom_provider"

    with connect(db_path) as conn:
        provider = conn.execute(
            "SELECT provider FROM provider_routing WHERE institution_name = 'Example Bank'"
        ).fetchone()["provider"]
        active_count = conn.execute(
            "SELECT COUNT(*) AS n FROM accounts WHERE institution_name = 'Example Bank' AND is_active = 1"
        ).fetchone()["n"]
        assert provider == "custom_provider"
        assert active_count == 0


def test_provider_status_shows_routing(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _insert_account(conn, account_id="acct_status_sw", institution_name="Charles Schwab", source="schwab")
        _insert_account(conn, account_id="acct_status_local", institution_name="Local CU", source="plaid")
        _route(conn, "Local CU", "schwab")
        conn.commit()

    code, payload = _run_cli(["provider", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "provider.status"

    rows = {row["institution_name"]: row for row in payload["data"]["institutions"]}
    assert rows["Charles Schwab"]["designated_provider"] == "schwab"
    assert rows["Charles Schwab"]["account_counts"]["schwab"] == 1
    assert rows["Local CU"]["designated_provider"] == "schwab"
    assert rows["Local CU"]["account_counts"]["plaid"] == 1
