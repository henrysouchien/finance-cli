from __future__ import annotations

import sqlite3
import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import biz_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_category(conn, name: str, *, is_income: int = 0) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
        VALUES (?, ?, NULL, 0, ?, 0, 0)
        """,
        (category_id, name, is_income),
    )
    return category_id


def _category_id(conn, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ? LIMIT 1", (name,)).fetchone()
    assert row is not None, f"missing category: {name}"
    return str(row["id"])


def _seed_schedule_c_map(
    conn,
    category_name: str,
    *,
    line: str,
    line_number: str,
    deduction_pct: float = 1.0,
    tax_year: int = 2025,
) -> None:
    conn.execute(
        """
        INSERT INTO schedule_c_map
            (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (uuid.uuid4().hex, _category_id(conn, category_name), line, line_number, deduction_pct, tax_year),
    )


def _seed_business_account(conn) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business, source
        ) VALUES (?, 'Tax Test Bank', 'Biz Checking', 'checking', 0, 1, 1, 'manual')
        """,
        (account_id,),
    )
    return account_id


def _seed_txn(
    conn,
    *,
    account_id: str,
    amount_cents: int,
    date_str: str,
    category_name: str,
    use_type: str | None = "Business",
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents,
            category_id, category_source, use_type, is_active, is_payment, source
        ) VALUES (?, ?, ?, ?, ?, ?, 'user', ?, 1, 0, 'manual')
        """,
        (txn_id, account_id, date_str, f"txn-{txn_id[:8]}", amount_cents, _category_id(conn, category_name), use_type),
    )
    return txn_id


def _seed_line9_dataset(conn) -> str:
    account_id = _seed_business_account(conn)
    _seed_category(conn, "Line9 Income", is_income=1)
    _seed_category(conn, "Line9 Transport")
    _seed_schedule_c_map(conn, "Line9 Transport", line="Car and truck expenses", line_number="9", tax_year=2025)
    _seed_txn(conn, account_id=account_id, amount_cents=300_000, date_str="2025-01-10", category_name="Line9 Income")
    _seed_txn(conn, account_id=account_id, amount_cents=-30_000, date_str="2025-01-12", category_name="Line9 Transport")
    return account_id


def _seed_line11_dataset(conn) -> str:
    account_id = _seed_business_account(conn)
    _seed_category(conn, "1099 Income", is_income=1)
    _seed_category(conn, "1099 Contract Labor")
    _seed_schedule_c_map(conn, "1099 Contract Labor", line="Contract labor", line_number="11", tax_year=2025)
    _seed_txn(conn, account_id=account_id, amount_cents=1_000_000, date_str="2025-01-10", category_name="1099 Income")
    return account_id


def _mileage_add_args(
    *,
    date: str = "2025-03-01",
    miles: float = 45.2,
    destination: str = "Client office",
    purpose: str = "Client meeting",
    vehicle: str = "primary",
    round_trip: bool = False,
    notes: str | None = None,
) -> Namespace:
    return Namespace(
        date=date,
        miles=miles,
        destination=destination,
        purpose=purpose,
        vehicle=vehicle,
        round_trip=round_trip,
        notes=notes,
        format="json",
    )


def _mileage_list_args(*, year: str | None = None, vehicle: str | None = None, limit: int = 50) -> Namespace:
    return Namespace(year=year, vehicle=vehicle, limit=limit, format="json")


def _mileage_summary_args(*, year: str | None = None) -> Namespace:
    return Namespace(year=year, format="json")


def _contractor_add_args(
    *,
    name: str = "Jane Doe",
    tin_last4: str | None = None,
    entity_type: str = "individual",
    notes: str | None = None,
) -> Namespace:
    return Namespace(
        name=name,
        tin_last4=tin_last4,
        entity_type=entity_type,
        notes=notes,
        format="json",
    )


def _contractor_list_args(*, year: str | None = None, include_inactive: bool = False) -> Namespace:
    return Namespace(year=year, include_inactive=include_inactive, format="json")


def _contractor_link_args(
    *,
    contractor_id: str,
    transaction_id: str,
    paid_via_card: bool = False,
) -> Namespace:
    return Namespace(
        contractor_id=contractor_id,
        transaction_id=transaction_id,
        paid_via_card=paid_via_card,
        format="json",
    )


def _tax_setup_args(*, year: str = "2025", mileage_method: str | None = None) -> Namespace:
    return Namespace(
        year=year,
        method=None,
        sqft=None,
        total_sqft=None,
        filing_status=None,
        state=None,
        health_insurance_monthly=None,
        w2_wages=None,
        mileage_method=mileage_method,
        format="json",
    )


def _tax_package_args(*, year: str = "2025") -> Namespace:
    return Namespace(year=year, output=None, salary=None, format="json")


def _report_args(*, year: str = "2025") -> Namespace:
    return Namespace(year=year, format="json")


def _line_item(snapshot: dict, line_number: str) -> dict:
    for item in snapshot["line_items"]:
        if str(item["line_number"]) == line_number:
            return item
    raise AssertionError(f"missing line item {line_number}")


def test_mileage_add_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_mileage_add(_mileage_add_args(), conn)

    assert result["data"]["trip_date"] == "2025-03-01"
    assert result["data"]["miles"] == 45.2
    assert result["data"]["deduction_cents"] == 3_164


def test_mileage_add_round_trip(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_mileage_add(_mileage_add_args(round_trip=True), conn)
    assert result["data"]["round_trip"] is True


def test_mileage_add_validation_negative_miles(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="miles must be > 0"):
            biz_cmd.handle_mileage_add(_mileage_add_args(miles=-1), conn)


def test_mileage_add_validation_bad_date(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            biz_cmd.handle_mileage_add(_mileage_add_args(date="03-01-2025"), conn)


def test_mileage_add_validation_empty_purpose(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="purpose is required"):
            biz_cmd.handle_mileage_add(_mileage_add_args(purpose=""), conn)


def test_mileage_list_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01"), conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-05"), conn)
        result = biz_cmd.handle_mileage_list(_mileage_list_args(year="2025"), conn)

    assert result["data"]["trip_count"] == 2
    assert result["data"]["trips"][0]["trip_date"] == "2025-03-05"


def test_mileage_list_filter_by_vehicle(db_path: Path) -> None:
    with connect(db_path) as conn:
        biz_cmd.handle_mileage_add(_mileage_add_args(vehicle="primary"), conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(vehicle="van"), conn)
        result = biz_cmd.handle_mileage_list(_mileage_list_args(year="2025", vehicle="van"), conn)

    assert result["data"]["trip_count"] == 1
    assert result["data"]["trips"][0]["vehicle"] == "van"


def test_mileage_list_filter_by_year(db_path: Path) -> None:
    with connect(db_path) as conn:
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01"), conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2026-03-01"), conn)
        result = biz_cmd.handle_mileage_list(_mileage_list_args(year="2025"), conn)
    assert all(row["trip_date"].startswith("2025-") for row in result["data"]["trips"])


def test_mileage_list_limit(db_path: Path) -> None:
    with connect(db_path) as conn:
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01"), conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-02"), conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-03"), conn)
        result = biz_cmd.handle_mileage_list(_mileage_list_args(year="2025", limit=2), conn)
    assert result["data"]["trip_count"] == 2


def test_mileage_summary_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_line9_dataset(conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01", miles=10), conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-02", miles=15), conn)
        conn.commit()
        result = biz_cmd.handle_mileage_summary(_mileage_summary_args(year="2025"), conn)

    assert result["data"]["trip_count"] == 2
    assert result["data"]["total_miles"] == 25.0
    assert result["data"]["total_deduction_cents"] == 1_750
    assert result["data"]["transaction_based_line_9_cents"] == 30_000


def test_mileage_summary_empty(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_mileage_summary(_mileage_summary_args(year="2025"), conn)
    assert result["data"]["trip_count"] == 0
    assert result["data"]["total_deduction_cents"] == 0


def test_mileage_summary_rate_lookup(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.execute("UPDATE mileage_rates SET rate_cents = 75 WHERE tax_year = 2025")
        conn.commit()
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01", miles=10), conn)
        result = biz_cmd.handle_mileage_summary(_mileage_summary_args(year="2025"), conn)

    assert result["data"]["rate_cents"] == 75
    assert result["data"]["total_deduction_cents"] == 750


def test_schedule_c_standard_mileage_method(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_line9_dataset(conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01", miles=50), conn)
        conn.commit()
        snapshot = biz_cmd._schedule_c_snapshot(
            conn,
            start=biz_cmd.date(2025, 1, 1),
            end=biz_cmd.date(2025, 12, 31),
            tax_year=2025,
            config={"mileage_method": "standard"},
        )

    line_9 = _line_item(snapshot, "9")
    assert line_9["deductible_cents"] == 3_500


def test_schedule_c_actual_method_ignores_mileage(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_line9_dataset(conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01", miles=50), conn)
        conn.commit()
        snapshot = biz_cmd._schedule_c_snapshot(
            conn,
            start=biz_cmd.date(2025, 1, 1),
            end=biz_cmd.date(2025, 12, 31),
            tax_year=2025,
            config={"mileage_method": "actual"},
        )

    line_9 = _line_item(snapshot, "9")
    assert line_9["deductible_cents"] == 30_000


def test_tax_package_includes_mileage(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_line9_dataset(conn)
        biz_cmd.handle_mileage_add(_mileage_add_args(date="2025-03-01", miles=20), conn)
        conn.commit()
        result = biz_cmd.handle_tax_package(_tax_package_args(year="2025"), conn)

    assert "mileage_summary" in result["data"]
    assert result["data"]["mileage_summary"]["trip_count"] == 1


def test_schedule_c_standard_mileage_empty_log_zeros(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_line9_dataset(conn)
        conn.commit()
        snapshot = biz_cmd._schedule_c_snapshot(
            conn,
            start=biz_cmd.date(2025, 1, 1),
            end=biz_cmd.date(2025, 12, 31),
            tax_year=2025,
            config={"mileage_method": "standard"},
        )

    line_9 = _line_item(snapshot, "9")
    assert line_9["deductible_cents"] == 0
    assert any("no trips logged for 2025" in warning.lower() for warning in snapshot["warnings"])


def test_tax_setup_mileage_method(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_tax_setup(_tax_setup_args(year="2025", mileage_method="standard"), conn)
    assert result["data"]["config"]["mileage_method"] == "standard"


def test_contractor_add_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe"), conn)
    assert result["data"]["name"] == "Jane Doe"
    assert result["data"]["entity_type"] == "individual"


def test_contractor_add_with_tin(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe", tin_last4="1234"), conn)
    assert result["data"]["tin_last4"] == "1234"


def test_contractor_add_validation_empty_name(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="name is required"):
            biz_cmd.handle_contractor_add(_contractor_add_args(name=""), conn)


def test_contractor_add_validation_bad_tin(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="exactly 4 digits"):
            biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe", tin_last4="12a4"), conn)


def test_contractor_add_corporation(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_contractor_add(_contractor_add_args(name="Acme Inc", entity_type="corporation"), conn)
    assert result["data"]["entity_type"] == "corporation"


def test_contractor_list_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe"), conn)
        result = biz_cmd.handle_contractor_list(_contractor_list_args(year="2025"), conn)
    assert result["data"]["totals"]["contractor_count"] == 1


def test_contractor_list_with_year_totals(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        contractor = biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe"), conn)
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-70_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(contractor_id=contractor["data"]["id"], transaction_id=txn_id),
            conn,
        )
        conn.commit()
        result = biz_cmd.handle_contractor_list(_contractor_list_args(year="2025"), conn)

    row = result["data"]["contractors"][0]
    assert row["total_paid_cents"] == 70_000
    assert row["payment_count"] == 1


def test_contractor_list_inactive_hidden(db_path: Path) -> None:
    with connect(db_path) as conn:
        contractor = biz_cmd.handle_contractor_add(_contractor_add_args(name="Old Vendor"), conn)
        conn.execute("UPDATE contractors SET is_active = 0 WHERE id = ?", (contractor["data"]["id"],))
        conn.commit()
        result = biz_cmd.handle_contractor_list(_contractor_list_args(year="2025"), conn)
    assert result["data"]["totals"]["contractor_count"] == 0


def test_contractor_link_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        contractor = biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe"), conn)
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-30_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        result = biz_cmd.handle_contractor_link(
            _contractor_link_args(contractor_id=contractor["data"]["id"], transaction_id=txn_id),
            conn,
        )
    assert result["data"]["contractor_id"] == contractor["data"]["id"]
    assert result["data"]["transaction_id"] == txn_id


def test_contractor_link_duplicate_rejected(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        contractor = biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe"), conn)
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-30_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(contractor_id=contractor["data"]["id"], transaction_id=txn_id),
            conn,
        )
        with pytest.raises(ValueError):
            biz_cmd.handle_contractor_link(
                _contractor_link_args(contractor_id=contractor["data"]["id"], transaction_id=txn_id),
                conn,
            )


def test_1099_report_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        contractor = biz_cmd.handle_contractor_add(_contractor_add_args(name="Jane Doe"), conn)
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-70_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(contractor_id=contractor["data"]["id"], transaction_id=txn_id),
            conn,
        )
        conn.commit()
        result = biz_cmd.handle_1099_report(_report_args(year="2025"), conn)

    assert result["data"]["totals"]["payment_count"] == 1
    assert result["data"]["contractors"][0]["non_card_paid_cents"] == 70_000


def test_1099_report_threshold_flagging(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        individual = biz_cmd.handle_contractor_add(_contractor_add_args(name="Person A"), conn)
        corp = biz_cmd.handle_contractor_add(
            _contractor_add_args(name="Corp B", entity_type="corporation"),
            conn,
        )
        txn1 = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-70_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        txn2 = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-80_000,
            date_str="2025-03-01",
            category_name="1099 Contract Labor",
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(contractor_id=individual["data"]["id"], transaction_id=txn1),
            conn,
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(contractor_id=corp["data"]["id"], transaction_id=txn2),
            conn,
        )
        conn.commit()
        result = biz_cmd.handle_1099_report(_report_args(year="2025"), conn)

    by_name = {row["name"]: row for row in result["data"]["contractors"]}
    assert by_name["Person A"]["requires_1099"] is True
    assert by_name["Corp B"]["requires_1099"] is False


def test_1099_report_unlinked_detection(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-20_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        conn.commit()
        result = biz_cmd.handle_1099_report(_report_args(year="2025"), conn)

    assert result["data"]["unlinked_contract_labor"]["total_cents"] == 20_000
    assert result["data"]["unlinked_contract_labor"]["txn_count"] == 1


def test_1099_report_card_paid_excluded(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        contractor = biz_cmd.handle_contractor_add(_contractor_add_args(name="Card Only"), conn)
        txn = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-100_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(
                contractor_id=contractor["data"]["id"],
                transaction_id=txn,
                paid_via_card=True,
            ),
            conn,
        )
        conn.commit()
        result = biz_cmd.handle_1099_report(_report_args(year="2025"), conn)

    row = result["data"]["contractors"][0]
    assert row["non_card_paid_cents"] == 0
    assert row["card_paid_cents"] == 100_000
    assert row["requires_1099"] is False


def test_1099_report_mixed_payment_channels(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        contractor = biz_cmd.handle_contractor_add(_contractor_add_args(name="Mixed Pay"), conn)
        txn_card = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-80_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        txn_non_card = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-50_000,
            date_str="2025-02-15",
            category_name="1099 Contract Labor",
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(
                contractor_id=contractor["data"]["id"],
                transaction_id=txn_card,
                paid_via_card=True,
            ),
            conn,
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(
                contractor_id=contractor["data"]["id"],
                transaction_id=txn_non_card,
                paid_via_card=False,
            ),
            conn,
        )
        conn.commit()
        result = biz_cmd.handle_1099_report(_report_args(year="2025"), conn)

    row = result["data"]["contractors"][0]
    assert row["total_paid_cents"] == 130_000
    assert row["non_card_paid_cents"] == 50_000
    assert row["card_paid_cents"] == 80_000
    assert row["requires_1099"] is False


def test_contractor_link_duplicate_transaction_rejected(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_line11_dataset(conn)
        contractor_a = biz_cmd.handle_contractor_add(_contractor_add_args(name="A"), conn)
        contractor_b = biz_cmd.handle_contractor_add(_contractor_add_args(name="B"), conn)
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            amount_cents=-30_000,
            date_str="2025-02-01",
            category_name="1099 Contract Labor",
        )
        biz_cmd.handle_contractor_link(
            _contractor_link_args(contractor_id=contractor_a["data"]["id"], transaction_id=txn_id),
            conn,
        )
        with pytest.raises(ValueError, match="already linked to a contractor"):
            biz_cmd.handle_contractor_link(
                _contractor_link_args(contractor_id=contractor_b["data"]["id"], transaction_id=txn_id),
                conn,
            )


def test_contractor_add_bad_tin_length(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO contractors (id, name, tin_last4, entity_type, is_active)
                VALUES (?, 'Schema Test', '12345', 'individual', 1)
                """,
                (uuid.uuid4().hex,),
            )
