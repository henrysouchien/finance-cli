from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import sqlite3

from ..advisory.retirement_limits_data import RETIREMENT_LIMITS
from ..commands import biz_cmd
from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from ..user_rules import load_rules
from .context import InterventionContext
from .registry import (
    CFPDomain,
    CFPProcessStep,
    Intervention,
    InterventionAction,
    Move,
    Priority,
    register_pattern,
)


_T1_DUE_SOON_DAYS = 30
_T1_DEFAULT_TAX_RATE = Decimal("0.30")
_T1_TAX_PAYMENT_MIN_CENTS = 10_000
_T1_TAX_PAYMENT_MATCHES = (
    "1040-es",
    "1040 es",
    "estimated tax",
    "eftps",
    "irs direct pay",
    "irs payment",
    "internal revenue",
    "us treasury",
    "u.s. treasury",
    "treasury tax",
    "tax payment",
    "state tax",
    "department of revenue",
    "franchise tax board",
    "nys tax",
)
_T1_TAX_PAYMENT_EXCLUSIONS = ("property tax", "sales tax", "payroll tax")
_T3_MIN_PRIOR_QUARTER_MILES = Decimal("500")
_T3_MIN_CURRENT_QUARTER_DAYS = 30
_T3_DISABLED_VALUES = {"0", "false", "no", "off", "disable", "disabled", "none"}
_T4_DEFAULT_PREVIEW_SQFT = 150
_T4_SIMPLIFIED_RATE_CENTS_PER_SQFT = 500
_T4_SIMPLIFIED_SQFT_CAP = 300
_T4_HOME_SPLIT_CATEGORIES = {"rent", "utilities"}
_T4_UNCONFIGURED_VALUES = {"", "0", "false", "no", "off", "disabled", "none", "not_configured"}
_T5_APPROACHING_1099_THRESHOLD_CENTS = 50_000
_T5_REQUIRES_1099_THRESHOLD_CENTS = 60_000
_T5_FLAG_TYPE = "january_1099_prep"
_T6_SEP_ACCOUNT_TYPE = "sep_ira"
_T6_MIN_Q4_PROFIT_SHARE = Decimal("0.35")
_T6_CONSERVATIVE_SEP_RATE = Decimal("0.18")
_T6_MIN_ROOM_CENTS = 100_000
_T7_LOOKBACK_DAYS = 7
_T7_MIN_TXN_COUNT = 3
_T7_MIN_DEDUCTION_CENTS = 15_000


@dataclass(frozen=True)
class _T1Quarter:
    tax_year: int
    number: int
    due_date: date
    payment_window_start: date

    @property
    def label(self) -> str:
        return f"{self.tax_year}-Q{self.number}"


@dataclass(frozen=True)
class _T1Candidate:
    quarter: _T1Quarter
    days_until_due: int
    ytd_net_profit_cents: int
    annualized_profit_cents: int
    estimated_quarterly_payment_cents: int
    observed_payment_cents: int
    set_aside_gap_cents: int
    rate: Decimal | None
    method: str


@dataclass(frozen=True)
class _Quarter:
    year: int
    number: int
    start: date
    end: date

    @property
    def label(self) -> str:
        return f"{self.year}-Q{self.number}"


@dataclass(frozen=True)
class _T3Candidate:
    current_quarter: _Quarter
    prior_year: int
    prior_quarter_miles: tuple[Decimal, Decimal, Decimal, Decimal]
    avg_miles: Decimal
    rate_cents: int
    marginal_tax_rate: Decimal
    deduction_value_cents: int
    tax_kept_cents: int


@dataclass(frozen=True)
class _T4Candidate:
    tax_year: int
    business_income_cents: int
    monthly_housing_cents: int
    housing_month_count: int
    preview_sqft: int
    deduction_value_cents: int
    tax_kept_cents: int
    marginal_tax_rate: Decimal


@dataclass(frozen=True)
class _T5Candidate:
    contractor_id: str
    contractor_name: str
    tin_on_file: bool
    tax_year: int
    payment_count: int
    non_card_paid_cents: int
    card_paid_cents: int
    requires_1099: bool


@dataclass(frozen=True)
class _T6Candidate:
    tax_year: int
    ytd_net_profit_cents: int
    q4_net_profit_cents: int
    q4_profit_share_pct: int
    annual_limit_cents: int
    contributed_ytd_cents: int
    room_remaining_cents: int
    monthly_target_cents: int
    start_month: str
    end_month: str
    tax_kept_cents: int
    marginal_tax_rate: Decimal


@dataclass(frozen=True)
class _T7Candidate:
    tax_year: int
    start: date
    end: date
    txn_count: int
    deduction_cents: int
    tax_kept_cents: int
    marginal_tax_rate: Decimal
    top_category: str
    top_category_deduction_cents: int


def _round_cents(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _format_miles(value: Decimal) -> str:
    rounded = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"{int(rounded):,}"


def _format_rate_pct(rate: Decimal) -> str:
    pct = (rate * Decimal("100")).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    if pct == pct.to_integral_value():
        return f"{int(pct)}%"
    return f"{pct}%"


def _configured_tax_rate(config: dict[str, str]) -> Decimal | None:
    for key in (
        "marginal_tax_rate",
        "marginal_tax_rate_pct",
        "estimated_tax_rate",
        "estimated_tax_rate_pct",
        "tax_rate",
        "tax_rate_pct",
    ):
        raw = config.get(key)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            value = Decimal(str(raw).strip())
        except InvalidOperation:
            continue
        if value < 0:
            continue
        if value > 1 or key.endswith("_pct"):
            value = value / Decimal("100")
        if Decimal("0") <= value <= Decimal("1"):
            return value
    return None


def _quarter_for(day: date) -> _Quarter:
    quarter_number = ((day.month - 1) // 3) + 1
    start_month = ((quarter_number - 1) * 3) + 1
    start = date(day.year, start_month, 1)
    if quarter_number == 4:
        end = date(day.year, 12, 31)
    else:
        end = date(day.year, start_month + 3, 1) - timedelta(days=1)
    return _Quarter(year=day.year, number=quarter_number, start=start, end=end)


def _quarter_by_number(year: int, quarter_number: int) -> _Quarter:
    if quarter_number < 1 or quarter_number > 4:
        raise ValueError("quarter_number must be 1..4")
    return _quarter_for(date(year, ((quarter_number - 1) * 3) + 1, 1))


def _mileage_miles(
    conn: sqlite3.Connection,
    *,
    tax_year: int,
    start: date,
    end: date,
) -> Decimal:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(miles), 0) AS total_miles
          FROM mileage_log
         WHERE tax_year = ?
           AND date(trip_date) >= date(?)
           AND date(trip_date) <= date(?)
        """,
        (tax_year, start.isoformat(), end.isoformat()),
    ).fetchone()
    return Decimal(str(row["total_miles"] or 0))


def _mileage_rate_cents(conn: sqlite3.Connection, tax_year: int) -> int:
    row = conn.execute(
        """
        SELECT rate_cents
          FROM mileage_rates
         WHERE tax_year <= ?
         ORDER BY tax_year DESC
         LIMIT 1
        """,
        (tax_year,),
    ).fetchone()
    if row is not None and row["rate_cents"] is not None:
        return int(row["rate_cents"])
    return 70


def _tax_config(conn: sqlite3.Connection, tax_year: int) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT config_key, config_value
          FROM tax_config
         WHERE tax_year = ?
        """,
        (tax_year,),
    ).fetchall()
    return {str(row["config_key"]): str(row["config_value"]) for row in rows}


def _mileage_tracking_disabled(config: dict[str, str]) -> bool:
    for key in ("mileage_tracking", "track_mileage", "mileage_log_tracking"):
        value = str(config.get(key, "")).strip().lower()
        if value in _T3_DISABLED_VALUES:
            return True
    return False


def _parse_tax_rate(config: dict[str, str]) -> Decimal:
    return _configured_tax_rate(config) or _T1_DEFAULT_TAX_RATE


def _t1_due_date(tax_year: int, quarter: int) -> date:
    if quarter == 1:
        return date(tax_year, 4, 15)
    if quarter == 2:
        return date(tax_year, 6, 15)
    if quarter == 3:
        return date(tax_year, 9, 15)
    if quarter == 4:
        return date(tax_year + 1, 1, 15)
    raise ValueError("quarter must be 1..4")


def _t1_payment_window_start(tax_year: int, quarter: int) -> date:
    if quarter == 1:
        return date(tax_year, 1, 16)
    if quarter == 2:
        return date(tax_year, 4, 16)
    if quarter == 3:
        return date(tax_year, 6, 16)
    if quarter == 4:
        return date(tax_year, 9, 16)
    raise ValueError("quarter must be 1..4")


def _t1_next_due_quarter(as_of: date) -> _T1Quarter | None:
    candidates: list[_T1Quarter] = []
    for tax_year in (as_of.year - 1, as_of.year):
        for quarter in range(1, 5):
            due_date = _t1_due_date(tax_year, quarter)
            days_until_due = (due_date - as_of).days
            if 0 <= days_until_due <= _T1_DUE_SOON_DAYS:
                candidates.append(
                    _T1Quarter(
                        tax_year=tax_year,
                        number=quarter,
                        due_date=due_date,
                        payment_window_start=_t1_payment_window_start(tax_year, quarter),
                    )
                )
    if not candidates:
        return None
    return min(candidates, key=lambda candidate: candidate.due_date)


def _looks_like_t1_tax_payment(text: str) -> bool:
    normalized = " ".join(text.casefold().replace("_", " ").replace("/", " ").split())
    if any(exclusion in normalized for exclusion in _T1_TAX_PAYMENT_EXCLUSIONS):
        return False
    return any(token in normalized for token in _T1_TAX_PAYMENT_MATCHES)


def _t1_observed_payment_cents(
    conn: sqlite3.Connection,
    *,
    start: date,
    end: date,
) -> int:
    rows = conn.execute(
        """
        SELECT t.description, c.name AS category_name, ABS(t.amount_cents) AS paid_cents
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.amount_cents < 0
           AND date(t.date) >= date(?)
           AND date(t.date) <= date(?)
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    total_cents = 0
    for row in rows:
        paid_cents = int(row["paid_cents"] or 0)
        if paid_cents < _T1_TAX_PAYMENT_MIN_CENTS:
            continue
        text = f"{row['description'] or ''} {row['category_name'] or ''}"
        if _looks_like_t1_tax_payment(text):
            total_cents += paid_cents
    return total_cents


def _t1_estimated_quarterly_payment(
    conn: sqlite3.Connection,
    *,
    annualized_profit_cents: int,
    config: dict[str, str],
    tax_year: int,
) -> tuple[int, Decimal | None, str]:
    rate = _configured_tax_rate(config)
    if rate is not None:
        estimated_annual_tax_cents = _round_cents(Decimal(annualized_profit_cents) * rate)
        return _round_cents(Decimal(estimated_annual_tax_cents) / Decimal(4)), rate, "configured_rate"
    if config:
        tax_summary = biz_cmd._compute_full_tax_summary(
            conn,
            {"line_31_net_profit_cents": annualized_profit_cents},
            config,
            tax_year,
        )
        return int(tax_summary["quarterly_payment_cents"]), None, "bracket"
    estimated_annual_tax_cents = _round_cents(Decimal(annualized_profit_cents) * _T1_DEFAULT_TAX_RATE)
    return _round_cents(Decimal(estimated_annual_tax_cents) / Decimal(4)), _T1_DEFAULT_TAX_RATE, "default_rate"


def _find_t1_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _T1Candidate | None:
    as_of = ctx.now.date()
    quarter = _t1_next_due_quarter(as_of)
    if quarter is None:
        return None

    config = _tax_config(conn, quarter.tax_year)
    ytd_end = min(as_of, date(quarter.tax_year, 12, 31))
    try:
        snapshot = biz_cmd._schedule_c_snapshot(
            conn,
            start=date(quarter.tax_year, 1, 1),
            end=ytd_end,
            tax_year=quarter.tax_year,
            config=config,
            rules_path=ctx.rules_path,
        )
    except Exception:
        return None

    ytd_net_profit_cents = int(snapshot["line_31_net_profit_cents"])
    if ytd_net_profit_cents <= 0:
        return None

    annualized_profit_cents = _round_cents(
        Decimal(ytd_net_profit_cents) * Decimal(4) / Decimal(quarter.number)
    )
    if annualized_profit_cents <= 0:
        return None

    estimated_quarterly_payment_cents, rate, method = _t1_estimated_quarterly_payment(
        conn,
        annualized_profit_cents=annualized_profit_cents,
        config=config,
        tax_year=quarter.tax_year,
    )
    if estimated_quarterly_payment_cents <= 0:
        return None

    observed_payment_cents = _t1_observed_payment_cents(
        conn,
        start=quarter.payment_window_start,
        end=as_of,
    )
    set_aside_gap_cents = max(0, estimated_quarterly_payment_cents - observed_payment_cents)
    if set_aside_gap_cents <= 0:
        return None

    return _T1Candidate(
        quarter=quarter,
        days_until_due=(quarter.due_date - as_of).days,
        ytd_net_profit_cents=ytd_net_profit_cents,
        annualized_profit_cents=annualized_profit_cents,
        estimated_quarterly_payment_cents=estimated_quarterly_payment_cents,
        observed_payment_cents=observed_payment_cents,
        set_aside_gap_cents=set_aside_gap_cents,
        rate=rate,
        method=method,
    )


def _find_t3_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _T3Candidate | None:
    as_of = ctx.now.date()
    current_quarter = _quarter_for(as_of)
    days_elapsed = (as_of - current_quarter.start).days + 1
    if days_elapsed < _T3_MIN_CURRENT_QUARTER_DAYS:
        return None

    config = _tax_config(conn, current_quarter.year)
    if _mileage_tracking_disabled(config):
        return None

    current_miles = _mileage_miles(
        conn,
        tax_year=current_quarter.year,
        start=current_quarter.start,
        end=current_quarter.end,
    )
    if current_miles > 0:
        return None

    prior_year = current_quarter.year - 1
    prior_quarter_miles: list[Decimal] = []
    for quarter_number in range(1, 5):
        prior_quarter = _quarter_by_number(prior_year, quarter_number)
        miles = _mileage_miles(
            conn,
            tax_year=prior_year,
            start=prior_quarter.start,
            end=prior_quarter.end,
        )
        if miles < _T3_MIN_PRIOR_QUARTER_MILES:
            return None
        prior_quarter_miles.append(miles)

    avg_miles = sum(prior_quarter_miles, Decimal("0")) / Decimal(len(prior_quarter_miles))
    rate_cents = _mileage_rate_cents(conn, current_quarter.year)
    deduction_value_cents = _round_cents(avg_miles * Decimal(rate_cents))
    marginal_tax_rate = _parse_tax_rate(config)
    tax_kept_cents = _round_cents(Decimal(deduction_value_cents) * marginal_tax_rate)
    if deduction_value_cents <= 0 or tax_kept_cents <= 0:
        return None

    return _T3Candidate(
        current_quarter=current_quarter,
        prior_year=prior_year,
        prior_quarter_miles=(
            prior_quarter_miles[0],
            prior_quarter_miles[1],
            prior_quarter_miles[2],
            prior_quarter_miles[3],
        ),
        avg_miles=avg_miles,
        rate_cents=rate_cents,
        marginal_tax_rate=marginal_tax_rate,
        deduction_value_cents=deduction_value_cents,
        tax_kept_cents=tax_kept_cents,
    )


def _has_t4_home_office_config(config: dict[str, str]) -> bool:
    for key, raw_value in config.items():
        if key != "home_total_sqft" and not key.startswith("home_office_"):
            continue
        value = str(raw_value or "").strip().lower()
        if value not in _T4_UNCONFIGURED_VALUES:
            return True
    return False


def _has_t4_home_split_rule_conflict(ctx: InterventionContext) -> bool:
    if ctx.rules_path is None:
        return False
    try:
        split_rules = load_rules(path=ctx.rules_path).split_rules
    except Exception:
        return False
    for rule in split_rules:
        match_category = (rule.match_category or "").strip().lower()
        business_category = (rule.business_category or "").strip().lower()
        if match_category in _T4_HOME_SPLIT_CATEGORIES or business_category in _T4_HOME_SPLIT_CATEGORIES:
            return True
    return False


def _t4_business_income_cents(conn: sqlite3.Connection, *, tax_year: int, as_of: date) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND t.use_type = 'Business'
           AND substr(t.date, 1, 4) = ?
           AND date(t.date) <= date(?)
        """,
        (str(tax_year), as_of.isoformat()),
    ).fetchone()
    return int(row["total_cents"] or 0)


def _t4_monthly_housing_cents(conn: sqlite3.Connection, *, tax_year: int, as_of: date) -> tuple[int, int]:
    rows = conn.execute(
        """
        SELECT substr(t.date, 1, 7) AS month_key,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN categories parent ON parent.id = c.parent_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND substr(t.date, 1, 4) = ?
           AND date(t.date) <= date(?)
           AND (
                lower(COALESCE(c.name, '')) LIKE '%rent%'
             OR lower(COALESCE(c.name, '')) LIKE '%mortgage%'
             OR lower(COALESCE(parent.name, '')) LIKE '%rent%'
             OR lower(COALESCE(parent.name, '')) LIKE '%mortgage%'
           )
         GROUP BY month_key
        """,
        (str(tax_year), as_of.isoformat()),
    ).fetchall()
    if not rows:
        return 0, 0
    total_cents = sum(int(row["total_cents"] or 0) for row in rows)
    month_count = len(rows)
    if total_cents <= 0 or month_count <= 0:
        return 0, 0
    return _round_cents(Decimal(total_cents) / Decimal(month_count)), month_count


def _find_t4_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _T4Candidate | None:
    tax_year = ctx.now.year
    as_of = ctx.now.date()
    config = _tax_config(conn, tax_year)
    if _has_t4_home_office_config(config) or _has_t4_home_split_rule_conflict(ctx):
        return None

    business_income_cents = _t4_business_income_cents(conn, tax_year=tax_year, as_of=as_of)
    if business_income_cents <= 0:
        return None

    monthly_housing_cents, housing_month_count = _t4_monthly_housing_cents(
        conn,
        tax_year=tax_year,
        as_of=as_of,
    )
    if monthly_housing_cents <= 0:
        return None

    preview_sqft = _T4_DEFAULT_PREVIEW_SQFT
    deduction_value_cents = min(
        min(preview_sqft, _T4_SIMPLIFIED_SQFT_CAP) * _T4_SIMPLIFIED_RATE_CENTS_PER_SQFT,
        business_income_cents,
    )
    marginal_tax_rate = _parse_tax_rate(config)
    tax_kept_cents = _round_cents(Decimal(deduction_value_cents) * marginal_tax_rate)
    if deduction_value_cents <= 0 or tax_kept_cents <= 0:
        return None

    return _T4Candidate(
        tax_year=tax_year,
        business_income_cents=business_income_cents,
        monthly_housing_cents=monthly_housing_cents,
        housing_month_count=housing_month_count,
        preview_sqft=preview_sqft,
        deduction_value_cents=deduction_value_cents,
        tax_kept_cents=tax_kept_cents,
        marginal_tax_rate=marginal_tax_rate,
    )


def _find_t5_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _T5Candidate | None:
    tax_year = ctx.now.year
    rows = conn.execute(
        """
        SELECT c.id AS contractor_id,
               c.name AS contractor_name,
               c.tin_last4,
               c.entity_type,
               COUNT(
                   CASE
                       WHEN t.is_active = 1 AND cp.paid_via_card = 0
                       THEN 1
                   END
               ) AS payment_count,
               COALESCE(
                   SUM(
                       CASE
                           WHEN t.is_active = 1 AND cp.paid_via_card = 0
                           THEN ABS(t.amount_cents)
                           ELSE 0
                       END
                   ),
                   0
               ) AS non_card_paid_cents,
               COALESCE(
                   SUM(
                       CASE
                           WHEN t.is_active = 1 AND cp.paid_via_card = 1
                           THEN ABS(t.amount_cents)
                           ELSE 0
                       END
                   ),
                   0
               ) AS card_paid_cents
          FROM contractors c
          JOIN contractor_payments cp ON cp.contractor_id = c.id
          JOIN transactions t ON t.id = cp.transaction_id
         WHERE c.is_active = 1
           AND c.entity_type != 'corporation'
           AND cp.tax_year = ?
           AND NOT EXISTS (
                SELECT 1
                  FROM contractor_tax_prep_flags f
                 WHERE f.contractor_id = c.id
                   AND f.tax_year = ?
                   AND f.flag_type = ?
                   AND f.status = 'active'
           )
         GROUP BY c.id, c.name, c.tin_last4, c.entity_type
        """,
        (tax_year, tax_year, _T5_FLAG_TYPE),
    ).fetchall()

    candidates: list[_T5Candidate] = []
    for row in rows:
        non_card_paid_cents = int(row["non_card_paid_cents"] or 0)
        if non_card_paid_cents < _T5_APPROACHING_1099_THRESHOLD_CENTS:
            continue
        candidates.append(
            _T5Candidate(
                contractor_id=str(row["contractor_id"]),
                contractor_name=str(row["contractor_name"] or ""),
                tin_on_file=bool(row["tin_last4"]),
                tax_year=tax_year,
                payment_count=int(row["payment_count"] or 0),
                non_card_paid_cents=non_card_paid_cents,
                card_paid_cents=int(row["card_paid_cents"] or 0),
                requires_1099=non_card_paid_cents >= _T5_REQUIRES_1099_THRESHOLD_CENTS,
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: (
            candidate.non_card_paid_cents,
            candidate.payment_count,
            candidate.contractor_name.casefold(),
        ),
    )


def _t6_schedule_c_net_profit_cents(
    conn: sqlite3.Connection,
    *,
    start: date,
    end: date,
    tax_year: int,
    config: dict[str, str],
    ctx: InterventionContext,
) -> int | None:
    try:
        snapshot = biz_cmd._schedule_c_snapshot(
            conn,
            start=start,
            end=end,
            tax_year=tax_year,
            config=config,
            rules_path=ctx.rules_path,
        )
    except Exception:
        return None
    return int(snapshot["line_31_net_profit_cents"])


def _active_t6_target_exists(conn: sqlite3.Connection, *, tax_year: int) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM retirement_contribution_targets
         WHERE tax_year = ?
           AND account_type = ?
           AND status = 'active'
         LIMIT 1
        """,
        (tax_year, _T6_SEP_ACCOUNT_TYPE),
    ).fetchone()
    return row is not None


def _known_t6_contributed_ytd_cents(conn: sqlite3.Connection, *, tax_year: int) -> int:
    row = conn.execute(
        """
        SELECT MAX(COALESCE(contributed_ytd_cents, 0)) AS contributed_ytd_cents
          FROM retirement_contribution_targets
         WHERE tax_year = ?
           AND account_type = ?
        """,
        (tax_year, _T6_SEP_ACCOUNT_TYPE),
    ).fetchone()
    return int(row["contributed_ytd_cents"] or 0)


def _find_t6_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _T6Candidate | None:
    as_of = ctx.now.date()
    if as_of.month < 10:
        return None

    tax_year = as_of.year
    limits = RETIREMENT_LIMITS.get(tax_year)
    if limits is None:
        return None
    if _active_t6_target_exists(conn, tax_year=tax_year):
        return None

    config = _tax_config(conn, tax_year)
    ytd_net_profit_cents = _t6_schedule_c_net_profit_cents(
        conn,
        start=date(tax_year, 1, 1),
        end=as_of,
        tax_year=tax_year,
        config=config,
        ctx=ctx,
    )
    q4_net_profit_cents = _t6_schedule_c_net_profit_cents(
        conn,
        start=date(tax_year, 10, 1),
        end=as_of,
        tax_year=tax_year,
        config=config,
        ctx=ctx,
    )
    if ytd_net_profit_cents is None or q4_net_profit_cents is None:
        return None
    if ytd_net_profit_cents <= 0 or q4_net_profit_cents <= 0:
        return None

    q4_profit_share = Decimal(q4_net_profit_cents) / Decimal(ytd_net_profit_cents)
    if q4_profit_share < _T6_MIN_Q4_PROFIT_SHARE:
        return None

    annual_limit_cents = min(
        int(limits["401k_total_limit_cents"]),
        _round_cents(Decimal(ytd_net_profit_cents) * _T6_CONSERVATIVE_SEP_RATE),
    )
    contributed_ytd_cents = _known_t6_contributed_ytd_cents(conn, tax_year=tax_year)
    room_remaining_cents = annual_limit_cents - contributed_ytd_cents
    if room_remaining_cents < _T6_MIN_ROOM_CENTS:
        return None

    months_remaining = 12 - as_of.month + 1
    monthly_target_cents = room_remaining_cents // months_remaining
    if monthly_target_cents <= 0:
        return None

    marginal_tax_rate = _parse_tax_rate(config)
    tax_kept_cents = _round_cents(Decimal(room_remaining_cents) * marginal_tax_rate)
    if tax_kept_cents <= 0:
        return None

    return _T6Candidate(
        tax_year=tax_year,
        ytd_net_profit_cents=ytd_net_profit_cents,
        q4_net_profit_cents=q4_net_profit_cents,
        q4_profit_share_pct=_round_cents(q4_profit_share * Decimal(100)),
        annual_limit_cents=annual_limit_cents,
        contributed_ytd_cents=contributed_ytd_cents,
        room_remaining_cents=room_remaining_cents,
        monthly_target_cents=monthly_target_cents,
        start_month=f"{tax_year}-{as_of.month:02d}",
        end_month=f"{tax_year}-12",
        tax_kept_cents=tax_kept_cents,
        marginal_tax_rate=marginal_tax_rate,
    )


def _find_t7_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _T7Candidate | None:
    as_of = ctx.now.date()
    tax_year = as_of.year
    start = max(date(tax_year, 1, 1), as_of - timedelta(days=_T7_LOOKBACK_DAYS - 1))
    rows = conn.execute(
        """
        SELECT c.name AS category_name,
               COUNT(*) AS txn_count,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS gross_cents,
               COALESCE(
                   SUM(CAST(ROUND(ABS(t.amount_cents) * scm.deduction_pct) AS INTEGER)),
                   0
               ) AS deduction_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN schedule_c_map scm ON scm.id = (
                SELECT scm2.id
                  FROM schedule_c_map scm2
                 WHERE scm2.category_id = t.category_id
                   AND scm2.tax_year <= ?
                 ORDER BY scm2.tax_year DESC, scm2.id DESC
                 LIMIT 1
          )
         WHERE t.use_type = 'Business'
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.is_reviewed = 1
           AND t.amount_cents < 0
           AND date(t.date) >= date(?)
           AND date(t.date) <= date(?)
         GROUP BY c.id, c.name
        HAVING deduction_cents > 0
         ORDER BY deduction_cents DESC, txn_count DESC, c.name
        """,
        (tax_year, start.isoformat(), as_of.isoformat()),
    ).fetchall()
    if not rows:
        return None

    txn_count = sum(int(row["txn_count"] or 0) for row in rows)
    deduction_cents = sum(int(row["deduction_cents"] or 0) for row in rows)
    if txn_count < _T7_MIN_TXN_COUNT or deduction_cents < _T7_MIN_DEDUCTION_CENTS:
        return None

    config = _tax_config(conn, tax_year)
    marginal_tax_rate = _parse_tax_rate(config)
    tax_kept_cents = _round_cents(Decimal(deduction_cents) * marginal_tax_rate)
    if tax_kept_cents <= 0:
        return None

    top_row = rows[0]
    return _T7Candidate(
        tax_year=tax_year,
        start=start,
        end=as_of,
        txn_count=txn_count,
        deduction_cents=deduction_cents,
        tax_kept_cents=tax_kept_cents,
        marginal_tax_rate=marginal_tax_rate,
        top_category=str(top_row["category_name"]),
        top_category_deduction_cents=int(top_row["deduction_cents"] or 0),
    )


@register_pattern(
    id="T-1",
    move=Move.WARN,
    tiers=(2,),
    priority=Priority.HIGH,
    cooldown=timedelta(days=90),
    tool="biz_estimated_tax",
    cfp_domains=(CFPDomain.TAX,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
)
def evaluate_t1_quarterly_estimated_tax_warning(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_t1_candidate(conn, ctx)
    if candidate is None:
        return None

    estimated = fmt_dollars(cents_to_dollars(candidate.estimated_quarterly_payment_cents))
    gap = fmt_dollars(cents_to_dollars(candidate.set_aside_gap_cents))
    ytd_profit = fmt_dollars(cents_to_dollars(candidate.ytd_net_profit_cents))
    annualized_profit = fmt_dollars(cents_to_dollars(candidate.annualized_profit_cents))
    payment_detail = (
        f"Tax-like payments observed since {candidate.quarter.payment_window_start.isoformat()}: "
        f"{fmt_dollars(cents_to_dollars(candidate.observed_payment_cents))}."
    )
    if candidate.method == "bracket":
        method_detail = "Estimate method: bracket/configured tax profile."
    elif candidate.method == "configured_rate" and candidate.rate is not None:
        method_detail = f"Estimate method: {_format_rate_pct(candidate.rate)} configured rate."
    else:
        method_detail = f"Estimate method: {_format_rate_pct(_T1_DEFAULT_TAX_RATE)} default rate."
    action_params: dict[str, object] = {
        "est_quarter": candidate.quarter.label,
        "include_se": True,
    }
    if candidate.rate is not None:
        action_params["rate"] = float(candidate.rate)
    return Intervention(
        pattern_id="T-1",
        move=Move.WARN,
        tiers=(2,),
        priority=Priority.HIGH,
        headline=(
            f"{candidate.quarter.label} estimated tax due in {candidate.days_until_due} days, "
            f"~{estimated} based on YTD business profit. Set aside {gap} now to avoid "
            "the underpayment penalty."
        ),
        detail_bullets=(
            f"Due date: {candidate.quarter.due_date.isoformat()}.",
            f"YTD Schedule C net profit through {ctx.now.date().isoformat()}: {ytd_profit}.",
            f"Annualized profit estimate: {annualized_profit}.",
            payment_detail,
            method_detail,
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Run estimated tax breakdown",
            tool="biz_estimated_tax",
            params=action_params,
            build_stub=False,
        ),
        dollar_impact_cents=candidate.set_aside_gap_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("T-1"),
    )


@register_pattern(
    id="T-2",
    move=Move.PATTERN_CATCH,
    tiers=(2,),
    cooldown=timedelta(days=30),
    tool="build:bulk_reclassify_business",
    cfp_domains=(CFPDomain.TAX,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
)
def evaluate_t2_untagged_business_deductions(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    rows = conn.execute(
        """
        SELECT c.name AS category_name,
               scm.schedule_c_line,
               COUNT(*) AS txn_count,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN schedule_c_map scm ON t.category_id = scm.category_id
         WHERE (t.use_type IS NULL OR t.use_type = 'Personal')
           AND t.is_active = 1
           AND t.amount_cents < 0
           AND substr(t.date, 1, 4) = ?
         GROUP BY c.id, c.name, scm.schedule_c_line
         ORDER BY total_cents DESC, c.name
        """,
        (str(ctx.now.year),),
    ).fetchall()
    if not rows:
        return None

    top_row = rows[0]
    total_cents = int(top_row["total_cents"] or 0)
    if total_cents <= 0:
        return None

    category_name = str(top_row["category_name"])
    schedule_c_line = str(top_row["schedule_c_line"])
    return Intervention(
        pattern_id="T-2",
        move=Move.PATTERN_CATCH,
        tiers=(2,),
        priority=Priority.MEDIUM,
        headline=(
            f"{fmt_dollars(cents_to_dollars(total_cents))} in {category_name} this year "
            "that look business-related but aren't tagged. Want me to flag them as "
            "Schedule C deductions?"
        ),
        detail_bullets=(
            f"{int(top_row['txn_count'])} transactions matched {schedule_c_line}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Tell me more",
            tool="build:bulk_reclassify_business",
            params={"category": category_name, "schedule_c_line": schedule_c_line},
            build_stub=True,
        ),
        dollar_impact_cents=total_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("T-2"),
    )


@register_pattern(
    id="T-3",
    move=Move.WARN,
    tiers=(2,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=90),
    tool="biz_mileage_add",
    cfp_domains=(CFPDomain.TAX,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
)
def evaluate_t3_mileage_gap_warning(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_t3_candidate(conn, ctx)
    if candidate is None:
        return None

    deduction_value = fmt_dollars(cents_to_dollars(candidate.deduction_value_cents))
    tax_kept = fmt_dollars(cents_to_dollars(candidate.tax_kept_cents))
    prior_quarters = ", ".join(
        f"Q{index}: {_format_miles(miles)} mi"
        for index, miles in enumerate(candidate.prior_quarter_miles, start=1)
    )
    return Intervention(
        pattern_id="T-3",
        move=Move.WARN,
        tiers=(2,),
        priority=Priority.MEDIUM,
        headline=(
            "You logged 0 miles this quarter. Last year you averaged "
            f"{_format_miles(candidate.avg_miles)} mi/quarter = {deduction_value} "
            f"in deductions = {tax_kept} kept at your rate. Worth checking if "
            "you forgot to log."
        ),
        detail_bullets=(
            f"Current quarter checked: {candidate.current_quarter.label} "
            f"through {ctx.now.date().isoformat()}.",
            f"{candidate.prior_year} quarterly mileage: {prior_quarters}.",
            f"Mileage rate: {candidate.rate_cents} cents/mi; tax-rate assumption: "
            f"{_format_rate_pct(candidate.marginal_tax_rate)}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Log missing mileage",
            tool="biz_mileage_add",
            params={
                "date": ctx.now.date().isoformat(),
                "vehicle": "primary",
                "round_trip": False,
                "notes": f"Prompted by T-3 mileage gap warning for {candidate.current_quarter.label}.",
            },
            build_stub=True,
        ),
        dollar_impact_cents=candidate.tax_kept_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("T-3"),
    )


@register_pattern(
    id="T-4",
    move=Move.DIAGNOSE,
    tiers=(2,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=90),
    tool="setup_home_office_tracking",
    cfp_domains=(CFPDomain.TAX,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_t4_home_office_deduction_unclaimed(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_t4_candidate(conn, ctx)
    if candidate is None:
        return None

    monthly_housing = fmt_dollars(cents_to_dollars(candidate.monthly_housing_cents))
    deduction_value = fmt_dollars(cents_to_dollars(candidate.deduction_value_cents))
    tax_kept = fmt_dollars(cents_to_dollars(candidate.tax_kept_cents))
    business_income = fmt_dollars(cents_to_dollars(candidate.business_income_cents))
    return Intervention(
        pattern_id="T-4",
        move=Move.DIAGNOSE,
        tiers=(2,),
        priority=Priority.MEDIUM,
        headline=(
            "Home office isn't claimed. Based on your rent/mortgage "
            f"({monthly_housing}/mo) and business income, a {candidate.preview_sqft} sqft "
            f"simplified-method preview is ~{deduction_value}/yr you're not deducting = "
            f"{tax_kept} kept at your rate."
        ),
        detail_bullets=(
            f"Current tax year: {candidate.tax_year}.",
            f"Business income observed: {business_income}.",
            f"Personal rent/mortgage average: {monthly_housing}/mo across "
            f"{candidate.housing_month_count} observed months.",
            f"Preview uses {candidate.preview_sqft} sqft at "
            f"{fmt_dollars(cents_to_dollars(_T4_SIMPLIFIED_RATE_CENTS_PER_SQFT))}/sqft, "
            f"capped at {_T4_SIMPLIFIED_SQFT_CAP} sqft and by business income; tax-rate "
            f"assumption: {_format_rate_pct(candidate.marginal_tax_rate)}.",
            "Confirm dedicated office square footage before saving home-office tax config.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Preview home office setup",
            tool="setup_home_office_tracking",
            params={
                "year": str(candidate.tax_year),
                "sqft": candidate.preview_sqft,
                "method": "simplified",
                "dry_run": True,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.tax_kept_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("T-4"),
    )


@register_pattern(
    id="T-5",
    move=Move.WARN,
    tiers=(2,),
    priority=Priority.HIGH,
    cooldown=timedelta(days=30),
    tool="flag_contractor_january_prep",
    cfp_domains=(CFPDomain.TAX,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
)
def evaluate_t5_1099_contractor_threshold_warning(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_t5_candidate(conn, ctx)
    if candidate is None:
        return None

    paid = fmt_dollars(cents_to_dollars(candidate.non_card_paid_cents))
    if candidate.requires_1099:
        threshold_sentence = (
            "That crosses the $600 1099-NEC threshold, so you'll need to file for them "
            "with the IRS in January and send them their copy."
        )
        reason = "Contractor has crossed the non-card 1099-NEC reporting threshold."
    else:
        threshold_sentence = (
            "One more invoice crosses $600 -- at that point you'll need to file a 1099-NEC "
            "for them with the IRS in January and send them their copy."
        )
        reason = "Contractor is approaching the non-card 1099-NEC reporting threshold."

    w9_sentence = (
        "Worth collecting their W-9 now if you don't have it."
        if not candidate.tin_on_file
        else "TIN is already on file; flag this for January prep now."
    )
    return Intervention(
        pattern_id="T-5",
        move=Move.WARN,
        tiers=(2,),
        priority=Priority.HIGH,
        headline=(
            f"You've paid {candidate.contractor_name} {paid} this year. "
            f"{threshold_sentence} {w9_sentence}"
        ),
        detail_bullets=(
            f"Current tax year: {candidate.tax_year}.",
            f"Non-card contractor payments: {paid} across {candidate.payment_count} linked transactions.",
            f"Card/processor payments excluded from 1099-NEC threshold: "
            f"{fmt_dollars(cents_to_dollars(candidate.card_paid_cents))}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Flag January 1099 prep",
            tool="flag_contractor_january_prep",
            params={
                "contractor_id": candidate.contractor_id,
                "tax_year": str(candidate.tax_year),
                "reason": reason,
                "source": "agent",
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.non_card_paid_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("T-5"),
    )


@register_pattern(
    id="T-6",
    move=Move.PRESCRIBE,
    tiers=(2,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=90),
    tool="set_monthly_retirement_target",
    cfp_domains=(CFPDomain.TAX, CFPDomain.RETIREMENT),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_t6_end_of_year_tax_acceleration(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_t6_candidate(conn, ctx)
    if candidate is None:
        return None

    room_remaining = fmt_dollars(cents_to_dollars(candidate.room_remaining_cents))
    tax_kept = fmt_dollars(cents_to_dollars(candidate.tax_kept_cents))
    return Intervention(
        pattern_id="T-6",
        move=Move.PRESCRIBE,
        tiers=(2,),
        priority=Priority.MEDIUM,
        headline=(
            "Year-end tax move: you have "
            f"{room_remaining} of SEP IRA room left. Maxing it before Dec 31 "
            f"saves you {tax_kept} this year."
        ),
        detail_bullets=(
            f"YTD Schedule C net profit: "
            f"{fmt_dollars(cents_to_dollars(candidate.ytd_net_profit_cents))}.",
            (
                f"Q4 net profit through {ctx.now.date().isoformat()}: "
                f"{fmt_dollars(cents_to_dollars(candidate.q4_net_profit_cents))} "
                f"({candidate.q4_profit_share_pct}% of YTD profit)."
            ),
            (
                "SEP room estimate uses a conservative 18% of Schedule C net profit, "
                f"capped by the {candidate.tax_year} retirement plan limit."
            ),
            f"Known SEP contributions this year: "
            f"{fmt_dollars(cents_to_dollars(candidate.contributed_ytd_cents))}.",
            f"Tax-rate assumption: {_format_rate_pct(candidate.marginal_tax_rate)}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Set monthly SEP target",
            tool="set_monthly_retirement_target",
            params={
                "tax_year": str(candidate.tax_year),
                "account_type": _T6_SEP_ACCOUNT_TYPE,
                "monthly_target_cents": candidate.monthly_target_cents,
                "start_month": candidate.start_month,
                "end_month": candidate.end_month,
                "room_remaining_cents": candidate.room_remaining_cents,
                "annual_limit_cents": candidate.annual_limit_cents,
                "contributed_ytd_cents": candidate.contributed_ytd_cents,
                "estimated_tax_savings_cents": candidate.tax_kept_cents,
                "deadline": f"{candidate.tax_year}-12-31",
                "reason": "Q4 Schedule C profit creates year-end SEP IRA deduction room.",
                "update_monthly_plans": True,
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.tax_kept_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("T-6"),
    )


@register_pattern(
    id="T-7",
    move=Move.COACH,
    tiers=(2,),
    priority=Priority.LOW,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.TAX, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT, CFPProcessStep.MONITOR),
)
def evaluate_t7_business_deduction_streak(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_t7_candidate(conn, ctx)
    if candidate is None:
        return None

    deduction = fmt_dollars(cents_to_dollars(candidate.deduction_cents))
    tax_kept = fmt_dollars(cents_to_dollars(candidate.tax_kept_cents))
    top_category = fmt_dollars(cents_to_dollars(candidate.top_category_deduction_cents))
    return Intervention(
        pattern_id="T-7",
        move=Move.COACH,
        tiers=(2,),
        priority=Priority.LOW,
        headline=(
            f"{candidate.txn_count} reviewed Schedule C expenses this week. "
            f"That's {deduction} documented for {candidate.tax_year}, about {tax_kept} "
            "kept at your tax-rate assumption."
        ),
        detail_bullets=(
            f"Window checked: {candidate.start.isoformat()} through {candidate.end.isoformat()}.",
            f"Top reviewed category: {candidate.top_category} ({top_category} deductible).",
            f"Tax-rate assumption: {_format_rate_pct(candidate.marginal_tax_rate)}.",
            "Observation only; no action queued because the expenses are already classified and reviewed.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=candidate.tax_kept_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("T-7"),
    )
