from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path
import sqlite3
from types import SimpleNamespace

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


def _category_id(conn, category_name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ? LIMIT 1", (category_name,)).fetchone()
    assert row is not None, f"missing category {category_name}"
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
    category_name: str | None,
    use_type: str | None = "Business",
) -> str:
    txn_id = uuid.uuid4().hex
    category_id = _category_id(conn, category_name) if category_name else None
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents,
            category_id, category_source, use_type, is_active, is_payment, source
        ) VALUES (?, ?, ?, ?, ?, ?, 'user', ?, 1, 0, 'manual')
        """,
        (txn_id, account_id, date_str, f"txn-{txn_id[:8]}", amount_cents, category_id, use_type),
    )
    return txn_id


def _seed_basic_tax_data(conn) -> None:
    account_id = _seed_business_account(conn)
    _seed_category(conn, "Tax Income", is_income=1)
    _seed_category(conn, "Tax Expense")
    _seed_category(conn, "Tax Meals")
    _seed_schedule_c_map(conn, "Tax Expense", line="Advertising", line_number="8", deduction_pct=1.0, tax_year=2025)
    _seed_schedule_c_map(conn, "Tax Meals", line="Deductible meals", line_number="24b", deduction_pct=0.5, tax_year=2025)
    _seed_txn(conn, account_id=account_id, amount_cents=400_000, date_str="2025-01-10", category_name="Tax Income")
    _seed_txn(conn, account_id=account_id, amount_cents=-60_000, date_str="2025-01-12", category_name="Tax Expense")
    _seed_txn(conn, account_id=account_id, amount_cents=-10_000, date_str="2025-02-01", category_name="Tax Meals")


def _tax_setup_args(
    *,
    year: str = "2025",
    method: str | None = None,
    sqft: int | None = None,
    total_sqft: int | None = None,
    filing_status: str | None = None,
    state: str | None = None,
    health_insurance_monthly: float | None = None,
    w2_wages: float | None = None,
) -> Namespace:
    return Namespace(
        year=year,
        method=method,
        sqft=sqft,
        total_sqft=total_sqft,
        filing_status=filing_status,
        state=state,
        health_insurance_monthly=health_insurance_monthly,
        w2_wages=w2_wages,
        format="json",
    )


def _tax_args(
    *,
    year: str = "2025",
    month: str | None = None,
    quarter: str | None = None,
    detail: str | None = None,
    salary: float | None = None,
) -> Namespace:
    return Namespace(
        year=year,
        month=month,
        quarter=quarter,
        detail=detail,
        salary=salary,
        format="json",
    )


def _est_args(
    *,
    est_quarter: str = "2025-Q1",
    rate: float | None = None,
    include_se: bool = True,
    salary: float | None = None,
) -> Namespace:
    return Namespace(
        est_quarter=est_quarter,
        rate=rate,
        include_se=include_se,
        salary=salary,
        format="json",
    )


def _package_args(*, year: str = "2025", output: str | None = None, salary: float | None = None) -> Namespace:
    return Namespace(year=year, output=output, salary=salary, format="json")


def _no_split_rules() -> SimpleNamespace:
    return SimpleNamespace(split_rules=[])


def _home_split_rules() -> SimpleNamespace:
    split_rule = SimpleNamespace(match_category="Rent", business_category="Rent")
    return SimpleNamespace(split_rules=[split_rule])


# ---------------------------------------------------------------------------
# Tax setup
# ---------------------------------------------------------------------------


def test_tax_setup_stores_config(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = biz_cmd.handle_tax_setup(
            _tax_setup_args(
                method="simplified",
                sqft=120,
                filing_status="single",
                state="NY-NYC",
                health_insurance_monthly=850.0,
                w2_wages=12_000.0,
            ),
            conn,
        )

    config = result["data"]["config"]
    assert config["home_office_method"] == "simplified"
    assert config["home_office_sqft"] == "120"
    assert config["state"] == "NY-NYC"
    assert config["health_insurance_monthly_cents"] == "85000"
    assert config["w2_wages_cents"] == "1200000"


def test_tax_setup_updates_existing_values(db_path: Path) -> None:
    with connect(db_path) as conn:
        biz_cmd.handle_tax_setup(_tax_setup_args(method="actual", year="2025"), conn)
        result = biz_cmd.handle_tax_setup(_tax_setup_args(method="simplified", year="2025"), conn)

    assert result["data"]["config"]["home_office_method"] == "simplified"
    assert "home_office_method" in result["data"]["updated_keys"]


def test_tax_setup_validates_state(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="state must be NY, NY-NYC, or empty"):
            biz_cmd.handle_tax_setup(_tax_setup_args(state="CA"), conn)


# ---------------------------------------------------------------------------
# Home office / Line 30
# ---------------------------------------------------------------------------


def test_compute_home_office_simplified_basic(monkeypatch) -> None:
    monkeypatch.setattr(biz_cmd, "load_rules", lambda: _no_split_rules())
    result = biz_cmd._compute_home_office({"home_office_method": "simplified", "home_office_sqft": "150"}, 300_000)
    assert result["method"] == "simplified"
    assert result["deduction_cents"] == 75_000


def test_compute_home_office_simplified_cap(monkeypatch) -> None:
    monkeypatch.setattr(biz_cmd, "load_rules", lambda: _no_split_rules())
    result = biz_cmd._compute_home_office({"home_office_method": "simplified", "home_office_sqft": "300"}, 120_000)
    assert result["tentative_deduction_cents"] == 150_000
    assert result["deduction_cents"] == 120_000


def test_compute_home_office_actual(monkeypatch) -> None:
    monkeypatch.setattr(biz_cmd, "load_rules", lambda: _no_split_rules())
    result = biz_cmd._compute_home_office({"home_office_method": "actual", "home_office_sqft": "120"}, 800_000)
    assert result["method"] == "actual"
    assert result["deduction_cents"] == 0
    assert result["display"] == "See Lines 20b, 25"


def test_compute_home_office_not_configured(monkeypatch) -> None:
    monkeypatch.setattr(biz_cmd, "load_rules", lambda: _no_split_rules())
    result = biz_cmd._compute_home_office({}, 800_000)
    assert result["method"] == "not_configured"
    assert result["deduction_cents"] == 0


def test_compute_home_office_error_for_split_rules(monkeypatch) -> None:
    monkeypatch.setattr(biz_cmd, "load_rules", lambda: _home_split_rules())
    with pytest.raises(ValueError, match="Simplified home office cannot be combined"):
        biz_cmd._compute_home_office({"home_office_method": "simplified", "home_office_sqft": "100"}, 500_000)


def test_schedule_snapshot_line30_from_config(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(biz_cmd, "load_rules", lambda: _no_split_rules())
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        snapshot = biz_cmd._schedule_c_snapshot(
            conn,
            start=biz_cmd.date(2025, 1, 1),
            end=biz_cmd.date(2025, 12, 31),
            tax_year=2025,
            config={"home_office_method": "simplified", "home_office_sqft": "100"},
        )

    assert snapshot["line_30_home_office_method"] == "simplified"
    assert snapshot["line_30_home_office_cents"] == 50_000


def test_schedule_snapshot_line31_reduced_by_line30(db_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(biz_cmd, "load_rules", lambda: _no_split_rules())
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        snapshot = biz_cmd._schedule_c_snapshot(
            conn,
            start=biz_cmd.date(2025, 1, 1),
            end=biz_cmd.date(2025, 12, 31),
            tax_year=2025,
            config={"home_office_method": "simplified", "home_office_sqft": "100"},
        )

    assert snapshot["line_31_before_home_office_cents"] > snapshot["line_31_net_profit_cents"]
    assert (
        snapshot["line_31_before_home_office_cents"] - snapshot["line_31_net_profit_cents"]
        == snapshot["line_30_home_office_cents"]
    )


# ---------------------------------------------------------------------------
# Schedule SE
# ---------------------------------------------------------------------------


def test_compute_se_tax_basic() -> None:
    result = biz_cmd._compute_se_tax(100_000, {}, 2025)
    assert result["se_taxable_cents"] == 92_350
    assert result["ss_tax_cents"] == 11_451
    assert result["medicare_tax_cents"] == 2_678
    assert result["total_se_cents"] == 14_129


def test_compute_se_tax_social_security_cap() -> None:
    result = biz_cmd._compute_se_tax(30_000_000, {}, 2025)
    assert result["ss_taxable_cents"] == 17_610_000


def test_compute_se_tax_floor_under_400() -> None:
    result = biz_cmd._compute_se_tax(39_999, {}, 2025)
    assert result["is_below_floor"] is True
    assert result["total_se_cents"] == 0


def test_compute_se_tax_w2_wage_interaction() -> None:
    result = biz_cmd._compute_se_tax(10_000_000, {"w2_wages_cents": "16000000"}, 2025)
    assert result["remaining_ss_base_cents"] == 1_610_000
    assert result["ss_taxable_cents"] == 1_610_000


def test_compute_se_tax_deductible_half_rounding() -> None:
    result = biz_cmd._compute_se_tax(100_000, {}, 2025)
    assert result["deductible_half_cents"] == 7_065


def test_compute_se_tax_additional_medicare() -> None:
    result = biz_cmd._compute_se_tax(30_000_000, {}, 2025)
    assert result["additional_medicare_cents"] > 0


def test_compute_se_tax_uses_2026_parameters() -> None:
    result = biz_cmd._compute_se_tax(50_000_000, {}, 2026)
    assert result["ss_taxable_cents"] == 18_450_000


# ---------------------------------------------------------------------------
# QBI
# ---------------------------------------------------------------------------


def test_compute_qbi_under_threshold() -> None:
    result = biz_cmd._compute_qbi(1_000_000, 0, 0, 5_000_000, 1_500_000, "single", 2025)
    assert result["qbi_deduction_cents"] == 200_000
    assert result["is_above_threshold"] is False


def test_compute_qbi_loss_yields_zero() -> None:
    result = biz_cmd._compute_qbi(-100_000, 0, 0, 500_000, 1_500_000, "single", 2025)
    assert result["qbi_deduction_cents"] == 0


def test_compute_qbi_above_threshold_warns() -> None:
    result = biz_cmd._compute_qbi(2_000_000, 0, 0, 20_000_000, 1_500_000, "single", 2025)
    assert result["is_above_threshold"] is True
    assert result["warnings"]


def test_compute_qbi_mfj_threshold() -> None:
    result = biz_cmd._compute_qbi(2_000_000, 0, 0, 30_000_000, 3_000_000, "mfj", 2025)
    assert result["is_above_threshold"] is False


def test_compute_qbi_taxable_income_cap() -> None:
    result = biz_cmd._compute_qbi(10_000_000, 0, 0, 2_000_000, 1_500_000, "single", 2025)
    assert result["tentative_qbi_deduction_cents"] == 2_000_000
    assert result["qbi_deduction_cents"] == 100_000


# ---------------------------------------------------------------------------
# Health insurance
# ---------------------------------------------------------------------------


def test_compute_health_insurance_basic() -> None:
    result = biz_cmd._compute_health_insurance({"health_insurance_monthly_cents": "50000"}, 2_000_000, 100_000)
    assert result["annual_premiums_cents"] == 600_000
    assert result["deduction_cents"] == 600_000


def test_compute_health_insurance_cap_after_half_se() -> None:
    result = biz_cmd._compute_health_insurance({"health_insurance_monthly_cents": "100000"}, 500_000, 100_000)
    assert result["earned_income_cap_cents"] == 400_000
    assert result["deduction_cents"] == 400_000


def test_compute_health_insurance_zero_no_config() -> None:
    result = biz_cmd._compute_health_insurance({}, 500_000, 10_000)
    assert result["deduction_cents"] == 0


def test_compute_health_insurance_zero_when_cap_negative() -> None:
    result = biz_cmd._compute_health_insurance({"health_insurance_monthly_cents": "100000"}, 100_000, 200_000)
    assert result["earned_income_cap_cents"] == 0
    assert result["deduction_cents"] == 0


# ---------------------------------------------------------------------------
# Federal/State/City
# ---------------------------------------------------------------------------


def test_compute_federal_tax_brackets() -> None:
    result = biz_cmd._compute_federal_tax(5_000_000, 1_500_000, 0, 2025)
    assert result["taxable_income_cents"] == 3_500_000
    assert result["tax_cents"] == 396_800


def test_compute_ny_tax_uses_agi_base() -> None:
    result = biz_cmd._compute_ny_tax(2_000_000, "single", 2025)
    assert result["taxable_income_cents"] == 1_200_000
    assert result["tax_cents"] == 49_975


def test_compute_nyc_resident_tax() -> None:
    result = biz_cmd._compute_nyc_tax(5_000_000, 1_000_000, "single", "NY-NYC", 2025)
    assert result["resident_tax_cents"] > 0
    assert result["ubt_tax_cents"] == 0


def test_compute_nyc_ubt_above_threshold() -> None:
    result = biz_cmd._compute_nyc_tax(5_000_000, 10_000_000, "single", "NY-NYC", 2025)
    assert result["ubt_tax_cents"] == 400_000


def test_full_summary_without_state_config_zeroes_state_taxes() -> None:
    with sqlite3.connect(":memory:") as conn:
        conn.row_factory = sqlite3.Row
        summary = biz_cmd._compute_full_tax_summary(conn, {"line_31_net_profit_cents": 4_000_000}, {}, 2025)
    assert summary["ny_state"]["tax_cents"] == 0
    assert summary["nyc"]["total_nyc_tax_cents"] == 0


def test_full_summary_ny_uses_agi_not_federal_taxable() -> None:
    with sqlite3.connect(":memory:") as conn:
        conn.row_factory = sqlite3.Row
        summary = biz_cmd._compute_full_tax_summary(
            conn,
            {"line_31_net_profit_cents": 3_000_000},
            {"state": "NY", "filing_status": "single"},
            2025,
        )
    assert summary["ny_state"]["taxable_income_cents"] == max(0, summary["agi_cents"] - 800_000)
    assert summary["ny_state"]["taxable_income_cents"] != summary["federal"]["taxable_income_cents"]


# ---------------------------------------------------------------------------
# S-Corp
# ---------------------------------------------------------------------------


def test_compute_s_corp_analysis_savings_positive() -> None:
    result = biz_cmd._compute_s_corp_analysis(10_000_000, 1_000_000)
    assert result["net_savings_cents"] > 0


def test_compute_s_corp_analysis_low_profit_flag() -> None:
    result = biz_cmd._compute_s_corp_analysis(3_000_000, 300_000)
    assert result["low_profit_flag"] is True


def test_full_summary_includes_nyc_s_corp_caveat() -> None:
    with sqlite3.connect(":memory:") as conn:
        conn.row_factory = sqlite3.Row
        summary = biz_cmd._compute_full_tax_summary(
            conn,
            {"line_31_net_profit_cents": 5_000_000},
            {"state": "NY-NYC", "filing_status": "single"},
            2025,
        )
    assert any("GCT vs UBT" in note for note in summary["s_corp"]["notes"])


# ---------------------------------------------------------------------------
# Unified tax report
# ---------------------------------------------------------------------------


def test_handle_tax_unified_summary_contains_sections(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    assert "Tax Summary" in result["cli_report"]
    assert "Self-Employment Tax (Schedule SE)" in result["cli_report"]
    assert result["data"]["tax_summary"]["schedule_se"]["total_se_cents"] >= 0


def test_handle_tax_detail_schedule_se(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025", detail="schedule-se"), conn)

    assert result["data"]["detail_section"] == "schedule-se"
    assert "Detail: schedule-se" in result["cli_report"]


def test_handle_tax_uses_single_full_summary_computation(db_path: Path, monkeypatch) -> None:
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        original = biz_cmd._compute_full_tax_summary
        calls = {"count": 0}

        def _wrapped(*args, **kwargs):
            calls["count"] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(biz_cmd, "_compute_full_tax_summary", _wrapped)
        biz_cmd.handle_tax(_tax_args(year="2025", detail="qbi"), conn)

    assert calls["count"] == 1


# ---------------------------------------------------------------------------
# Estimated tax
# ---------------------------------------------------------------------------


def _seed_estimated_tax_data(conn) -> None:
    account_id = _seed_business_account(conn)
    _seed_category(conn, "Est Income", is_income=1)
    _seed_category(conn, "Est Expense")
    _seed_schedule_c_map(conn, "Est Expense", line="Advertising", line_number="8", deduction_pct=1.0, tax_year=2025)
    _seed_txn(conn, account_id=account_id, amount_cents=2_000_000, date_str="2025-01-05", category_name="Est Income")
    _seed_txn(conn, account_id=account_id, amount_cents=-500_000, date_str="2025-02-05", category_name="Est Expense")


def test_estimated_tax_bracket_method_with_components(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_estimated_tax_data(conn)
        biz_cmd.handle_tax_setup(
            _tax_setup_args(year="2025", filing_status="single", state="NY-NYC", health_insurance_monthly=200.0),
            conn,
        )
        conn.commit()
        result = biz_cmd.handle_estimated_tax(_est_args(est_quarter="2025-Q1", rate=None, include_se=True), conn)

    assert result["data"]["method"] == "bracket"
    assert result["data"]["components_cents"]["federal_tax_cents"] >= 0
    assert result["data"]["components_cents"]["se_tax_cents"] > 0


def test_estimated_tax_no_se_flag_excludes_se_component(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_estimated_tax_data(conn)
        biz_cmd.handle_tax_setup(_tax_setup_args(year="2025", filing_status="single", state="NY"), conn)
        conn.commit()
        with_se = biz_cmd.handle_estimated_tax(_est_args(est_quarter="2025-Q1", rate=None, include_se=True), conn)
        without_se = biz_cmd.handle_estimated_tax(_est_args(est_quarter="2025-Q1", rate=None, include_se=False), conn)

    assert with_se["data"]["components_cents"]["se_tax_cents"] > 0
    assert without_se["data"]["components_cents"]["se_tax_cents"] == 0
    assert with_se["data"]["estimated_annual_tax_cents"] > without_se["data"]["estimated_annual_tax_cents"]


def test_estimated_tax_rate_override_bypasses_brackets(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_estimated_tax_data(conn)
        biz_cmd.handle_tax_setup(_tax_setup_args(year="2025", filing_status="single", state="NY"), conn)
        conn.commit()
        result = biz_cmd.handle_estimated_tax(_est_args(est_quarter="2025-Q1", rate=0.25, include_se=True), conn)

    assert result["data"]["method"] == "flat_rate"
    assert result["data"]["estimated_annual_tax_cents"] == biz_cmd._round_half_up(
        result["data"]["annualized_profit_cents"] * 0.25
    )


# ---------------------------------------------------------------------------
# Tax package
# ---------------------------------------------------------------------------


def test_tax_package_assembles_sections(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        result = biz_cmd.handle_tax_package(_package_args(year="2025"), conn)

    assert "schedule_c" in result["data"]
    assert "tax_summary" in result["data"]
    assert "transaction_groups" in result["data"]


def test_tax_package_handles_missing_config(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        result = biz_cmd.handle_tax_package(_package_args(year="2025"), conn)

    assert result["data"]["tax_config"] == {}
    assert result["data"]["tax_summary"]["federal"]["tax_cents"] >= 0


def test_tax_package_assumptions_and_output_file(db_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "tax_package.md"
    with connect(db_path) as conn:
        _seed_basic_tax_data(conn)
        conn.commit()
        result = biz_cmd.handle_tax_package(_package_args(year="2025", output=str(output_path)), conn)

    assert output_path.exists()
    text = output_path.read_text(encoding="utf-8")
    assert "Assumptions and limitations" in text
    assert "WARNING: Unclassified transactions" in result["cli_report"]
