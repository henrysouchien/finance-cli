#!/usr/bin/env python3
"""Finance CLI MCP Server wrapping existing CLI handlers.

Register with Claude Code:
    claude mcp add finance-cli -- python -m finance_cli.mcp_server
"""

# stdout redirect (required for MCP JSON-RPC over stdio — handler print()
# must not corrupt the transport).
import functools
import inspect
import json
import sys
import uuid

_real_stdout = sys.stdout
sys.stdout = sys.stderr

from argparse import Namespace  # noqa: E402
from datetime import datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Optional  # noqa: E402

from finance_cli.config import load_dotenv  # noqa: E402
from finance_cli.db import connect, initialize_database  # noqa: E402
from finance_cli.commands import (  # noqa: E402
    account_cmd,
    balance_cmd,
    biz_cmd,
    budget,
    cat,
    daily,
    debt_cmd,
    db_cmd,
    dedup_cmd,
    export as export_cmd,
    goal_cmd,
    ingest,
    liability_cmd,
    liquidity_cmd,
    monthly_cmd,
    notify_cmd,
    plaid_cmd,
    plan,
    projection_cmd,
    provider_cmd,
    rules,
    schwab_cmd,
    setup_cmd,
    spending_cmd,
    stripe_cmd,
    subs,
    summary_cmd,
    txn,
    weekly,
)

sys.stdout = _real_stdout
from fastmcp import FastMCP  # noqa: E402

load_dotenv()
initialize_database()

mcp = FastMCP(
    "finance-cli",
    instructions=(
        "Personal finance tools: transaction search, categorization, "
        "spending reports, budget tracking, bank sync, and statement import."
    ),
)


# ---------------------------------------------------------------------------
# Auto-coerce string params → int/bool (MCP-001 fix)
# ---------------------------------------------------------------------------
# FastMCP validates JSON Schema strictly — e.g. "20" fails for int params.
# Patch mcp.tool() so every registered tool auto-coerces string values.

def _coerce_params(fn):
    """Broaden int/bool params to also accept strings, with auto-coercion."""
    import typing

    sig = inspect.signature(fn)
    try:
        hints = typing.get_type_hints(fn)
    except Exception:
        hints = {}
    coercions: dict[str, type] = {}
    new_params = []
    for name, param in sig.parameters.items():
        ann = hints.get(name, param.annotation)
        if ann is int:
            coercions[name] = int
            new_params.append(param.replace(annotation=int | str))
        elif ann is bool:
            coercions[name] = bool
            new_params.append(param.replace(annotation=bool | str))
        else:
            new_params.append(param)
    if not coercions:
        return fn

    @functools.wraps(fn)
    def wrapper(**kwargs):
        for name, target in coercions.items():
            val = kwargs.get(name)
            if val is None or isinstance(val, target):
                continue
            if target is bool and isinstance(val, str):
                kwargs[name] = val.lower() in ("true", "1", "yes")
            elif target is int and isinstance(val, str):
                kwargs[name] = int(val)
        return fn(**kwargs)

    wrapper.__signature__ = sig.replace(parameters=new_params)
    return wrapper


_orig_mcp_tool = mcp.tool


def _tool_with_coercion(*args, **kwargs):
    orig_decorator = _orig_mcp_tool(*args, **kwargs)

    def new_decorator(fn):
        return orig_decorator(_coerce_params(fn))

    return new_decorator


mcp.tool = _tool_with_coercion  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ns(**kwargs) -> Namespace:
    """Build an argparse Namespace with sensible defaults."""
    defaults = {"format": "json", "verbose": False}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _call(handler, ns_kwargs: dict) -> dict:
    """Open a DB connection, call *handler*, return {data, summary}."""
    with connect() as conn:
        result = handler(_ns(**ns_kwargs), conn)
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


_TXN_STRIP_FIELDS = {
    "raw_plaid_json",
    "dedupe_key",
    "split_group_id",
    "parent_transaction_id",
    "split_pct",
    "split_note",
    "removed_at",
    "created_at",
    "updated_at",
}


def _write_cache(tool_name: str, data: Any) -> str:
    """Write full MCP tool data to exports/mcp_cache and return file path."""
    cache_dir = Path(__file__).resolve().parent.parent / "exports" / "mcp_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    short_id = uuid.uuid4().hex[:8]
    file_path = cache_dir / f"{tool_name}_{timestamp}_{short_id}.json"
    file_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    return str(file_path)


def _export_output_path(prefix: str) -> str:
    """Build a timestamped CSV output path under exports/."""
    export_dir = Path(__file__).resolve().parent.parent / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return str(export_dir / f"{prefix}_{timestamp}.csv")


def _strip_txn_fields(txn_dict: dict[str, Any]) -> dict[str, Any]:
    """Return a new transaction dict with verbose/large fields removed."""
    return {key: value for key, value in txn_dict.items() if key not in _TXN_STRIP_FIELDS}


_SIMULATION_CAP_CAVEAT = (
    "Baseline minimum-payment simulation did not fully pay off within the 360-month cap; "
    "time/interest savings estimates are approximate."
)


def _attach_simulation_cap_caveat(result: dict[str, Any]) -> dict[str, Any]:
    data = dict(result.get("data", {}))
    baseline = data.get("baseline")
    if isinstance(baseline, dict) and baseline.get("fully_paid_off") is False:
        data["caveat"] = _SIMULATION_CAP_CAVEAT
    return {"data": data, "summary": result.get("summary", {})}


# ===================================================================
# 1. Status & Overview (3 tools, read-only)
# ===================================================================

@mcp.tool()
def db_status() -> dict:
    """Database overview: transaction counts, date range, accounts, uncategorized count, top categories.

    Returns:
        Dict with transaction_counts, date_range, active_account_count,
        uncategorized_count, category_source_distribution, top_categories,
        and last_import_at.

    Examples:
        db_status()
    """
    result = _call(db_cmd.handle_status, {})
    with connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE is_active = 1 AND is_reviewed = 0"
        ).fetchone()
    data = dict(result.get("data", {}))
    data["unreviewed_count"] = int(row["cnt"] or 0)
    return {"data": data, "summary": result.get("summary", {})}


@mcp.tool()
def setup_check() -> dict:
    """Environment readiness check: .env, Plaid SDK, DB, rules file, AWS.

    Returns:
        Dict with ready (bool), checks list, counts, and next_steps.

    Examples:
        setup_check()
    """
    return _call(setup_cmd.handle_check, {})


@mcp.tool()
def setup_status() -> dict:
    """Full setup dashboard: env checks, DB stats, Plaid status, category coverage, next steps.

    Returns:
        Dict with environment, database, plaid, category_coverage, and next_steps.

    Examples:
        setup_status()
    """
    result = _call(setup_cmd.handle_status, {})
    data = dict(result.get("data", {}))
    plaid = data.get("plaid")
    if isinstance(plaid, dict):
        items = plaid.get("items") or []
        sanitized_items: list[Any] = []
        for item in items:
            if isinstance(item, dict):
                sanitized_items.append(
                    {
                        key: value
                        for key, value in item.items()
                        if key not in {"access_token_ref", "sync_cursor"}
                    }
                )
            else:
                sanitized_items.append(item)
        sanitized_plaid = dict(plaid)
        sanitized_plaid["items"] = sanitized_items
        data["plaid"] = sanitized_plaid
    return {"data": data, "summary": result.get("summary", {})}


# ===================================================================
# 2. Account Management (6 tools, read+write)
# ===================================================================

@mcp.tool()
def account_list(
    status: str = "active",
    account_type: Optional[str] = None,
    institution: Optional[str] = None,
    source: Optional[str] = None,
    is_business: Optional[bool] = None,
) -> dict:
    """List accounts with optional filters.

    Args:
        status: Account status filter: 'active', 'inactive', or 'all'. Defaults to 'active'.
        account_type: Optional account type filter.
        institution: Optional free-text institution filter.
        source: Optional free-text source filter (plaid/csv_import/pdf_import/manual/schwab/etc.).
        is_business: Optional business-account filter.

    Returns:
        Dict with accounts list and filter echo.

    Examples:
        account_list()
        account_list(status="all", institution="Bank of America")
        account_list(source="schwab")
        account_list(is_business=True)
    """
    return _call(account_cmd.handle_list, {
        "status": status,
        "account_type": account_type,
        "institution": institution,
        "source": source,
        "is_business": is_business,
    })


@mcp.tool()
def account_show(id: str) -> dict:
    """Show full details for a single account.

    Args:
        id: Account ID.

    Returns:
        Dict with account details and transaction stats.

    Examples:
        account_show(id="abc123")
    """
    return _call(account_cmd.handle_show, {"id": id})


@mcp.tool()
def account_set_type(id: str, account_type: str) -> dict:
    """Set account_type and account_type_override for an account.

    Args:
        id: Account ID.
        account_type: New type ('checking', 'savings', 'credit_card', 'investment', 'loan').

    Returns:
        Dict with previous/new type values.

    Examples:
        account_set_type(id="abc123", account_type="investment")
    """
    with connect() as conn:
        result = account_cmd.handle_set_type(_ns(id=id, account_type=account_type), conn)
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def account_set_business(account_id: str, is_business: bool, backfill: bool = False) -> dict:
    """Set account-level business flag and optionally backfill linked active transactions.

    Args:
        account_id: Account ID.
        is_business: True to mark business, False to mark personal.
        backfill: If True, update active transaction use_type for this account:
            - True mode: NULL -> 'Business'
            - False mode: 'Business' -> NULL

    Returns:
        Dict with prior/new business flag values and backfill counts.

    Examples:
        account_set_business(account_id="abc123", is_business=True)
        account_set_business(account_id="abc123", is_business=False, backfill=True)
    """
    with connect() as conn:
        result = account_cmd.handle_set_business(
            _ns(id=account_id, business=bool(is_business), personal=not bool(is_business), backfill=backfill),
            conn,
        )
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def account_deactivate(id: str, cascade: bool = False, force: bool = False) -> dict:
    """Deactivate an account, optionally cascading to linked transactions/subscriptions.

    Args:
        id: Account ID.
        cascade: If True, deactivate linked transactions and auto-detected subscriptions.
        force: If True, allow deactivation even when the account is an alias canonical target.

    Returns:
        Dict with deactivation and cascade results.

    Examples:
        account_deactivate(id="abc123")
        account_deactivate(id="abc123", cascade=True, force=True)
    """
    with connect() as conn:
        result = account_cmd.handle_deactivate(_ns(id=id, cascade=cascade, force=force), conn)
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def account_activate(id: str) -> dict:
    """Activate a deactivated account.

    Args:
        id: Account ID.

    Returns:
        Dict with activation results.

    Examples:
        account_activate(id="abc123")
    """
    with connect() as conn:
        result = account_cmd.handle_activate(_ns(id=id), conn)
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


# ===================================================================
# 3. Financial Reports (16 tools, read-only)
# ===================================================================

@mcp.tool()
def daily_summary(date: Optional[str] = None, view: str = "all") -> dict:
    """Show transactions and spending for a specific date.

    Args:
        date: ISO date string (YYYY-MM-DD). Defaults to today.
        view: Use-type view filter: 'personal', 'business', or 'all' (default).

    Returns:
        Dict with date, transactions list, unreviewed_count, and data_range.

    Examples:
        daily_summary()
        daily_summary(date="2026-02-15")
    """
    full_result = _call(daily.handle_daily, {"date": date, "pending": False, "view": view})
    _write_cache("daily_summary", full_result)

    data = dict(full_result.get("data", {}))
    transactions = data.get("transactions", [])
    data["transactions"] = [
        _strip_txn_fields(txn_data) if isinstance(txn_data, dict) else txn_data
        for txn_data in transactions
    ]
    return {"data": data, "summary": full_result.get("summary", {})}


@mcp.tool()
def weekly_summary(week: Optional[str] = None, compare: bool = False, view: str = "all") -> dict:
    """Weekly spending by category, optionally compared to the prior week.

    Args:
        week: ISO week string like '2026-W07'. Defaults to current week.
        compare: If True, include prior week comparison with deltas.
        view: Use-type view filter: 'personal', 'business', or 'all' (default).

    Returns:
        Dict with week_start, week_end, categories, and optional comparison data.

    Examples:
        weekly_summary()
        weekly_summary(week="2026-W07", compare=True)
    """
    return _call(weekly.handle_weekly, {"week": week, "compare": compare, "view": view})


@mcp.tool()
def balance_net_worth(exclude_investments: bool = False, view: str = "all") -> dict:
    """Compute net worth from current account balances.

    Args:
        exclude_investments: Exclude investment accounts from the calculation.
        view: Use-type view filter: 'personal', 'business', or 'all' (default).

    Returns:
        Dict with assets, liabilities, net_worth (dollars and cents), and breakdown by account type.

    Examples:
        balance_net_worth()
        balance_net_worth(exclude_investments=True)
    """
    return _call(balance_cmd.handle_net_worth, {"exclude_investments": exclude_investments, "view": view})


@mcp.tool()
def balance_show(
    account_type: Optional[str] = None,
    show_all: bool = False,
    view: str = "all",
) -> dict:
    """Show current balances with optional account type filtering."""
    return _call(balance_cmd.handle_show, {"type": account_type, "show_all": show_all, "view": view})


@mcp.tool()
def balance_history(account: str, days: int = 90) -> dict:
    """Show daily balance history for one account."""
    return _call(balance_cmd.handle_history, {"account": account, "days": days, "view": "all"})


@mcp.tool()
def liquidity(view: str = "all", include_investments: bool = True) -> dict:
    """Liquidity snapshot: liquid balance, credit owed, 90-day income/expense, subscription burn, projected net.

    Returns:
        Dict with liquid_balance, credit_owed, income/expense_90d, subscription burn, projected_net.

    Examples:
        liquidity()
    """
    return _call(liquidity_cmd.handle_liquidity, {
        "forecast": 90,
        "include_investments": include_investments,
        "view": view,
    })


@mcp.tool()
def debt_dashboard(include_zero_balance: bool = False, sort: str = "balance") -> dict:
    """Per-card debt breakdown: balances, APRs, minimums, monthly interest, and totals.

    Args:
        include_zero_balance: Include zero-balance credit cards.
        sort: Sort key for cards: 'balance', 'apr', or 'interest'.

    Returns:
        Dict with debt dashboard data and summary counts.

    Examples:
        debt_dashboard()
        debt_dashboard(sort="apr")
    """
    return _call(debt_cmd.handle_dashboard, {
        "include_zero_balance": include_zero_balance,
        "sort": sort,
    })


@mcp.tool()
def debt_interest(months: int = 12, summary_only: bool = True) -> dict:
    """Project minimum-payment interest over N months.

    Args:
        months: Number of months to project (>= 1).
        summary_only: If True (default), omit per-card monthly rows to reduce payload size.

    Returns:
        Dict with projection schedule and aggregate interest totals.

    Examples:
        debt_interest()
        debt_interest(months=24)
    """
    return _call(debt_cmd.handle_interest, {"months": months, "summary_only": summary_only})


@mcp.tool()
def debt_simulate(
    extra_dollars: float = 500,
    strategy: str = "compare",
    summary_only: bool = True,
    lump_sum: float = 0,
    lump_sum_month: int = 1,
) -> dict:
    """Simulate debt paydown using avalanche, snowball, or side-by-side comparison.

    Args:
        extra_dollars: Extra monthly payment in dollars.
        strategy: 'avalanche', 'snowball', or 'compare'.
        summary_only: If True (default), omit per-card monthly rows to reduce payload size.
        lump_sum: One-time lump sum payment in dollars (e.g. tax refund, bonus).
        lump_sum_month: Month number (1-based) when the lump sum is applied (default 1).

    Returns:
        Dict with simulation outputs and strategy summary.

    Examples:
        debt_simulate()
        debt_simulate(extra_dollars=750, strategy="avalanche")
        debt_simulate(extra_dollars=500, lump_sum=5000, lump_sum_month=3)
    """
    return _call(
        debt_cmd.handle_simulate,
        {
            "extra": extra_dollars,
            "strategy": strategy,
            "summary_only": summary_only,
            "lump_sum": lump_sum,
            "lump_month": lump_sum_month,
        },
    )


@mcp.tool()
def subs_list(show_all: bool = False) -> dict:
    """List tracked subscriptions, sorted by monthly cost.

    Args:
        show_all: Include cancelled/inactive subscriptions.

    Returns:
        Dict with subscriptions list and summary counts.

    Examples:
        subs_list()
        subs_list(show_all=True)
    """
    full_result = _call(subs.handle_list, {"show_all": show_all})
    _write_cache("subs_list", full_result)

    data = dict(full_result.get("data", {}))
    all_subscriptions_raw = data.get("subscriptions", [])
    all_subscriptions: list[Any] = []
    for item in all_subscriptions_raw:
        if not isinstance(item, dict):
            all_subscriptions.append(item)
            continue
        sub = dict(item)
        monthly_amount = sub.get("monthly_amount")
        if monthly_amount is not None:
            sub["monthly_amount"] = round(float(monthly_amount), 2)
        vendor = str(sub.get("vendor_name") or "")
        sub["short_name"] = vendor[:30]
        all_subscriptions.append(sub)
    active_subscriptions = [
        item
        for item in all_subscriptions
        if isinstance(item, dict) and bool(int(item.get("is_active") or 0))
    ]
    inactive_subscriptions = [
        item
        for item in all_subscriptions
        if isinstance(item, dict) and not bool(int(item.get("is_active") or 0))
    ]

    data["subscriptions"] = all_subscriptions if show_all else active_subscriptions
    summary = dict(full_result.get("summary", {}))
    summary.update({
        "active_subscriptions": len(active_subscriptions),
        "inactive_subscriptions": len(inactive_subscriptions),
        "total_subscriptions": len(all_subscriptions),
    })
    return {"data": data, "summary": summary}


@mcp.tool()
def subs_total() -> dict:
    """Total monthly subscription burn rate.

    Returns:
        Dict with monthly_burn, yearly_burn, and active_subscriptions count.

    Examples:
        subs_total()
    """
    return _call(subs.handle_total, {})


@mcp.tool()
def subs_detect() -> dict:
    """Detect recurring subscriptions from transactions."""
    with connect() as conn:
        result = subs.handle_detect(_ns(), conn)
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def subs_recurring() -> dict:
    """List detected recurring spending patterns (pre-subscription candidates)."""
    return _call(subs.handle_recurring, {})


@mcp.tool()
def subs_add(
    vendor: str,
    amount: float,
    frequency: str,
    category: Optional[str] = None,
    use_type: Optional[str] = None,
) -> dict:
    """Add a subscription."""
    return _call(subs.handle_add, {
        "vendor": vendor,
        "amount": amount,
        "frequency": frequency,
        "category": category,
        "use_type": use_type,
    })


@mcp.tool()
def subs_cancel(id: str) -> dict:
    """Cancel a subscription."""
    return _call(subs.handle_cancel, {"id": id})


@mcp.tool()
def subs_audit() -> dict:
    """Audit subscriptions vs debt: classify essential/discretionary,
    model debt payoff impact of cutting discretionary subs."""
    return _attach_simulation_cap_caveat(_call(subs.handle_audit, {}))


@mcp.tool()
def debt_impact(months: int = 3, cut_pct: int = 50) -> dict:
    """Model discretionary spending cuts -> debt payoff impact.

    Computes N-month average spending per category, classifies as
    essential/discretionary, then models how cutting discretionary
    spending would accelerate debt payoff via avalanche simulation.

    Args:
        months: Lookback months for average spending (default: 3).
        cut_pct: Discretionary cut percentage 1-100 (default: 50).
    """
    return _attach_simulation_cap_caveat(_call(debt_cmd.handle_impact, {"months": months, "cut_pct": cut_pct}))


@mcp.tool()
def biz_pl(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
    compare: bool = False,
) -> dict:
    """Business income statement (P&L) for a period."""
    return _call(biz_cmd.handle_pl, {
        "month": month,
        "quarter": quarter,
        "year": year,
        "compare": compare,
    })


@mcp.tool()
def biz_cashflow(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
) -> dict:
    """Business cash flow statement for a period."""
    return _call(biz_cmd.handle_cashflow, {
        "month": month,
        "quarter": quarter,
        "year": year,
    })


@mcp.tool()
def biz_tax(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
    detail: Optional[str] = None,
    salary: Optional[float] = None,
) -> dict:
    """Schedule C tax report for a period."""
    return _call(biz_cmd.handle_tax, {
        "month": month,
        "quarter": quarter,
        "year": year,
        "detail": detail,
        "salary": salary,
    })


@mcp.tool()
def biz_tax_detail(
    detail: str,
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
    salary: Optional[float] = None,
) -> dict:
    """Schedule C tax report detail section."""
    return _call(biz_cmd.handle_tax, {
        "month": month,
        "quarter": quarter,
        "year": year,
        "detail": detail,
        "salary": salary,
    })


@mcp.tool()
def biz_tax_setup(
    year: str,
    method: Optional[str] = None,
    sqft: Optional[int] = None,
    total_sqft: Optional[int] = None,
    filing_status: Optional[str] = None,
    state: Optional[str] = None,
    health_insurance_monthly: Optional[float] = None,
    w2_wages: Optional[float] = None,
    mileage_method: Optional[str] = None,
) -> dict:
    """Configure tax assumptions for a tax year."""
    with connect() as conn:
        result = biz_cmd.handle_tax_setup(
            _ns(
                year=year,
                method=method,
                sqft=sqft,
                total_sqft=total_sqft,
                filing_status=filing_status,
                state=state,
                health_insurance_monthly=health_insurance_monthly,
                w2_wages=w2_wages,
                mileage_method=mileage_method,
            ),
            conn,
        )
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def biz_tax_package(year: str, output: Optional[str] = None, salary: Optional[float] = None) -> dict:
    """Generate full tax package output for a tax year."""
    return _call(biz_cmd.handle_tax_package, {"year": year, "output": output, "salary": salary})


@mcp.tool()
def biz_estimated_tax(
    est_quarter: Optional[str] = None,
    year: Optional[int] = None,
    rate: Optional[float] = None,
    include_se: bool = True,
    salary: Optional[float] = None,
) -> dict:
    """Quarterly estimated tax calculation."""
    return _call(
        biz_cmd.handle_estimated_tax,
        {"est_quarter": est_quarter, "year": year, "rate": rate, "include_se": include_se, "salary": salary},
    )


@mcp.tool()
def biz_mileage_add(
    date: str,
    miles: float,
    destination: str,
    purpose: str,
    vehicle: str = "primary",
    round_trip: bool = False,
    notes: Optional[str] = None,
) -> dict:
    """Add a mileage log trip for Schedule C Line 9 standard mileage tracking."""
    with connect() as conn:
        result = biz_cmd.handle_mileage_add(
            _ns(
                date=date,
                miles=miles,
                destination=destination,
                purpose=purpose,
                vehicle=vehicle,
                round_trip=round_trip,
                notes=notes,
            ),
            conn,
        )
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def biz_mileage_list(
    year: Optional[str] = None,
    vehicle: Optional[str] = None,
    limit: int = 50,
) -> dict:
    """List mileage log entries for a year (and optional vehicle filter)."""
    return _call(
        biz_cmd.handle_mileage_list,
        {"year": year, "vehicle": vehicle, "limit": limit},
    )


@mcp.tool()
def biz_mileage_summary(year: Optional[str] = None) -> dict:
    """Mileage deduction summary: total miles, rate, deduction vs transaction-based Line 9."""
    return _call(biz_cmd.handle_mileage_summary, {"year": year})


@mcp.tool()
def biz_contractor_add(
    name: str,
    tin_last4: Optional[str] = None,
    entity_type: str = "individual",
    notes: Optional[str] = None,
) -> dict:
    """Add a contractor for 1099-NEC tracking."""
    with connect() as conn:
        result = biz_cmd.handle_contractor_add(
            _ns(
                name=name,
                tin_last4=tin_last4,
                entity_type=entity_type,
                notes=notes,
            ),
            conn,
        )
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def biz_contractor_list(year: Optional[int] = None, include_inactive: bool = False) -> dict:
    """List contractors with payment totals for 1099-NEC tracking."""
    return _call(biz_cmd.handle_contractor_list, {"year": year, "include_inactive": include_inactive})


@mcp.tool()
def biz_contractor_link(
    contractor_id: str,
    transaction_id: str,
    paid_via_card: bool = False,
) -> dict:
    """Link a business transaction to a contractor payment record."""
    with connect() as conn:
        result = biz_cmd.handle_contractor_link(
            _ns(
                contractor_id=contractor_id,
                transaction_id=transaction_id,
                paid_via_card=paid_via_card,
            ),
            conn,
        )
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def biz_1099_report(year: str) -> dict:
    """Build a contractor 1099-NEC threshold report for a tax year."""
    return _call(biz_cmd.handle_1099_report, {"year": year})


@mcp.tool()
def biz_forecast(months: int = 6, streams: bool = False) -> dict:
    """Revenue projections by stream with trend analysis."""
    return _call(biz_cmd.handle_forecast, {"months": months, "streams": streams})


@mcp.tool()
def biz_runway(months: int = 3) -> dict:
    """Business burn rate and cash runway estimate."""
    result = _call(biz_cmd.handle_runway, {"months": months})
    data = dict(result.get("data", {}))
    if int(data.get("monthly_net_burn_cents", 0)) < 0:
        data["is_profitable"] = True
        data["note"] = "Monthly net burn is negative; business is profitable and runway is effectively uncapped."
    return {"data": data, "summary": result.get("summary", {})}


@mcp.tool()
def biz_seasonal() -> dict:
    """Month-of-year seasonal revenue averages with confidence levels."""
    return _call(biz_cmd.handle_seasonal, {})


@mcp.tool()
def biz_budget_set(
    section: str,
    amount: float,
    period: str = "monthly",
    effective_from: Optional[str] = None,
) -> dict:
    """Set a business budget for an expense P&L section."""
    with connect() as conn:
        result = biz_cmd.handle_biz_budget_set(
            _ns(section=section, amount=amount, period=period, effective_from=effective_from),
            conn,
        )
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def biz_budget_status(month: Optional[str] = None) -> dict:
    """Show business budget vs actual spend per P&L section."""
    return _call(biz_cmd.handle_biz_budget_status, {"month": month})


@mcp.tool()
def cat_tree() -> dict:
    """Category hierarchy tree with transaction counts per category.

    Returns:
        Dict with tree (nested parent/child categories with txn_count) and total_categories.

    Examples:
        cat_tree()
    """
    return _call(cat.handle_tree, {})


@mcp.tool()
def cat_list() -> dict:
    """List all categories with hierarchy and transaction counts."""
    return _call(cat.handle_list, {})


@mcp.tool()
def cat_add(name: str, parent: Optional[str] = None) -> dict:
    """Add a new category, optionally under a parent."""
    return _call(cat.handle_add, {"name": name, "parent": parent})


@mcp.tool()
def cat_normalize(dry_run: bool = True) -> dict:
    """Backfill source_category, seed mappings, remap non-canonical names."""
    return _call(cat.handle_normalize, {"dry_run": dry_run})


@mcp.tool()
def budget_set(category: str, amount: float, period: str = "monthly", view: str = "personal") -> dict:
    """Set a budget for a category."""
    with connect() as conn:
        result = budget.handle_set(_ns(category=category, amount=amount, period=period, view=view), conn)
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def budget_update(category: str, amount: float, period: str = "monthly", view: str = "personal") -> dict:
    """Update an existing active budget amount for a category."""
    return _call(
        budget.handle_update,
        {"category": category, "amount": amount, "period": period, "view": view},
    )


@mcp.tool()
def budget_delete(category: str, period: str = "monthly", view: str = "personal") -> dict:
    """Delete an existing active budget for a category."""
    return _call(
        budget.handle_delete,
        {"category": category, "period": period, "view": view},
    )


@mcp.tool()
def budget_list(view: str = "all") -> dict:
    """List configured budgets."""
    return _call(budget.handle_list, {"view": view})


@mcp.tool()
def budget_status(month: Optional[str] = None, view: str = "all") -> dict:
    """Show monthly budget vs actual status."""
    return _call(budget.handle_status, {"month": month, "view": view})


@mcp.tool()
def budget_forecast(month: Optional[str] = None, view: str = "all") -> dict:
    """Forecast month-end spending vs budget."""
    return _call(budget.handle_forecast, {"month": month, "view": view})


@mcp.tool()
def budget_alerts(month: Optional[str] = None, view: str = "all") -> dict:
    """Check which budgets are at risk based on current spending run rate."""
    return _call(budget.handle_alerts, {"month": month, "view": view})


@mcp.tool()
def budget_suggest(goal: str = "savings", target: float = 500, view: str = "all") -> dict:
    """Suggest budget cuts to hit a target."""
    return _call(budget.handle_suggest, {"goal": goal, "target": target, "view": view})


@mcp.tool()
def notify_budget_alerts(channel: str = "telegram", view: str = "all", dry_run: bool = False) -> dict:
    """Send or preview budget alert notifications."""
    return _call(
        notify_cmd.handle_budget_alerts,
        {"channel": channel, "view": view, "month": None, "dry_run": dry_run},
    )


@mcp.tool()
def notify_test(channel: str = "telegram", dry_run: bool = False) -> dict:
    """Send or preview a test notification."""
    return _call(notify_cmd.handle_test, {"channel": channel, "dry_run": dry_run})


@mcp.tool()
def liability_show(liability_type: Optional[str] = None, include_inactive: bool = False) -> dict:
    """Show liabilities with optional type filtering."""
    return _call(liability_cmd.handle_show, {"type": liability_type, "include_inactive": include_inactive})


@mcp.tool()
def liability_upcoming(days: int = 30, liability_type: Optional[str] = None) -> dict:
    """Show upcoming liability payments."""
    return _call(liability_cmd.handle_upcoming, {"days": days, "type": liability_type})


@mcp.tool()
def plan_create(month: Optional[str] = None) -> dict:
    """Create or refresh a monthly plan."""
    return _call(plan.handle_create, {"month": month or datetime.now().strftime("%Y-%m")})


@mcp.tool()
def plan_show(month: Optional[str] = None) -> dict:
    """Show a monthly plan."""
    return _call(plan.handle_show, {"month": month})


@mcp.tool()
def plan_review() -> dict:
    """Review the current month's plan against actuals."""
    return _call(plan.handle_review, {})


# ===================================================================
# 4. Transaction Tools (5 tools, read-only)
# ===================================================================

@mcp.tool()
def txn_list(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    category: Optional[str] = None,
    account_id: Optional[str] = None,
    use_type: Optional[str] = None,
    uncategorized: bool = False,
    unreviewed: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List transactions with optional filters and pagination.

    Args:
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).
        category: Filter by category name (exact match).
        uncategorized: Only show uncategorized transactions.
        unreviewed: Only show unreviewed transactions.
        limit: Max rows to return (default 50).
        offset: Skip this many rows for pagination (default 0).

    Returns:
        Dict with transactions list, total_count, and pagination info.

    Examples:
        txn_list()
        txn_list(date_from="2026-02-01", limit=20)
        txn_list(uncategorized=True)
        txn_list(category="Groceries", limit=10)
    """
    return _call(txn.handle_list, {
        "date_from": date_from,
        "date_to": date_to,
        "category": category,
        "account_id": account_id,
        "use_type": use_type,
        "uncategorized": uncategorized,
        "unreviewed": unreviewed,
        "limit": limit,
        "offset": offset,
        "project": None,
        "verbose": False,
    })


@mcp.tool()
def txn_search(query: str, limit: int = 20) -> dict:
    """Full-text search across transaction descriptions (FTS5, falls back to LIKE).

    Args:
        query: Search term or FTS5 query string.

    Returns:
        Dict with matching transactions and query echo.

    Examples:
        txn_search(query="STARBUCKS")
        txn_search(query="amazon prime")
    """
    full_result = _call(txn.handle_search, {"query": query})
    _write_cache("txn_search", full_result)

    data = dict(full_result.get("data", {}))
    transactions = data.get("transactions", [])
    data["transactions"] = [
        _strip_txn_fields(txn_data) if isinstance(txn_data, dict) else txn_data
        for txn_data in transactions
    ][:limit]
    return {"data": data, "summary": full_result.get("summary", {})}


@mcp.tool()
def txn_show(id: str) -> dict:
    """Full details for a single transaction.

    Args:
        id: Transaction ID (hex UUID).

    Returns:
        Dict with all transaction fields including category, account, and notes.

    Examples:
        txn_show(id="a1b2c3d4...")
    """
    full_result = _call(txn.handle_show, {"id": id})
    _write_cache("txn_show", full_result)

    data = dict(full_result.get("data", {}))
    transaction = data.get("transaction")
    if isinstance(transaction, dict):
        data["transaction"] = _strip_txn_fields(transaction)
    return {"data": data, "summary": full_result.get("summary", {})}


@mcp.tool()
def txn_explain(id: str) -> dict:
    """Explain how a transaction was categorized: source, rule, reasoning.

    Args:
        id: Transaction ID (hex UUID).

    Returns:
        Dict with categorization source, matched rule, and confidence.

    Examples:
        txn_explain(id="a1b2c3d4...")
    """
    return _call(txn.handle_explain, {"id": id})


@mcp.tool()
def txn_coverage(date_from: Optional[str] = None) -> dict:
    """Date coverage per account with gap detection.

    Args:
        date_from: Reference start date for gap detection (defaults to earliest transaction).

    Returns:
        Dict with per-account date ranges, transaction counts, and detected gaps.

    Examples:
        txn_coverage()
        txn_coverage(date_from="2026-01-01")
    """
    full_result = _call(txn.handle_coverage, {"date_from": date_from, "date_to": None})
    _write_cache("txn_coverage", full_result)
    return full_result


@mcp.tool()
def txn_add(
    amount: float,
    date: str,
    description: str,
    account_id: Optional[str] = None,
    category: Optional[str] = None,
) -> dict:
    """Add a manual transaction."""
    return _call(txn.handle_add, {
        "amount": amount,
        "date": date,
        "description": description,
        "account_id": account_id,
        "category": category,
    })


@mcp.tool()
def txn_edit(
    id: str,
    amount: Optional[float] = None,
    date: Optional[str] = None,
    description: Optional[str] = None,
    notes: Optional[str] = None,
) -> dict:
    """Edit one transaction."""
    return _call(txn.handle_edit, {
        "id": id,
        "amount": amount,
        "date": date,
        "description": description,
        "notes": notes,
    })


@mcp.tool()
def txn_tag(id: str, project: str) -> dict:
    """Tag one transaction with a project."""
    return _call(txn.handle_tag, {"id": id, "project": project})


# ===================================================================
# 5. Rules & Categorization (9 tools, read+write)
# ===================================================================

@mcp.tool()
def rules_test(description: str, category: Optional[str] = None, source: str = "plaid") -> dict:
    """Test which categorization rules would match a transaction description.

    Args:
        description: Transaction description to test against rules.
        category: Optional category name to test overrides against.
        source: Category source context (default 'plaid').

    Returns:
        Dict with keyword match, split rule, and category override results.

    Examples:
        rules_test(description="VENMO PAYMENT")
        rules_test(description="STARBUCKS", category="Coffee")
    """
    return _call(rules.handle_test, {
        "description": description,
        "category": category,
        "source": source,
    })


@mcp.tool()
def rules_show() -> dict:
    """Show loaded rules.yaml contents."""
    return _call(rules.handle_show, {})


@mcp.tool()
def rules_validate() -> dict:
    """Validate rules.yaml against known categories."""
    return _call(rules.handle_validate, {})


@mcp.tool()
def rules_add_keyword(
    keyword: str,
    category: str,
    use_type: Optional[str] = None,
    priority: int = 0,
) -> dict:
    """Add a keyword to categorization rules."""
    return _call(rules.handle_add_keyword, {
        "keyword": keyword,
        "category": category,
        "use_type": use_type,
        "priority": priority,
    })


@mcp.tool()
def rules_remove_keyword(keyword: str) -> dict:
    """Remove a keyword from categorization rules."""
    return _call(rules.handle_remove_keyword, {"keyword": keyword})


@mcp.tool()
def rules_list() -> dict:
    """List all keyword rules in structured format.

    Returns:
        Dict with list of rules, each containing rule_index, category,
        keywords, use_type, and priority.

    Examples:
        rules_list()
    """
    return _call(rules.handle_list, {})


@mcp.tool()
def rules_update_priority(
    rule_index: int,
    priority: int,
) -> dict:
    """Change the priority on a keyword rule. Use rules_list() first to
    find the rule_index. Higher priority wins ties among equal-length
    keyword matches.

    Args:
        rule_index: Index of the rule to update (from rules_list output).
        priority: New priority value.

    Returns:
        Dict with category, old_priority, new_priority, and rule_index.

    Examples:
        rules_update_priority(rule_index=0, priority=5)
        rules_update_priority(rule_index=3, priority=10)
    """
    return _call(rules.handle_update_priority, {
        "rule_index": rule_index,
        "priority": priority,
    })


@mcp.tool()
def cat_memory_list(
    unconfirmed: bool = False,
    limit: int = 50,
    search: Optional[str] = None,
) -> dict:
    """List vendor-memory rules."""
    return _call(cat.handle_memory_list, {"unconfirmed": unconfirmed, "limit": limit, "search": search})


@mcp.tool()
def cat_memory_add(pattern: str, category: str, use_type: str = "Any") -> dict:
    """Add a vendor-memory rule."""
    return _call(cat.handle_memory_add, {"pattern": pattern, "category": category, "use_type": use_type})


@mcp.tool()
def cat_memory_disable(id: str) -> dict:
    """Disable a vendor-memory rule."""
    return _call(cat.handle_memory_disable, {"id": id})


@mcp.tool()
def cat_memory_confirm(id: str) -> dict:
    """Confirm a vendor memory rule (sets is_confirmed=1)."""
    return _call(cat.handle_memory_confirm, {"id": id})


@mcp.tool()
def cat_memory_delete(id: str) -> dict:
    """Delete a vendor memory rule permanently."""
    return _call(cat.handle_memory_delete, {"id": id})


@mcp.tool()
def cat_memory_undo(txn_id: str) -> dict:
    """Undo vendor memory for a transaction (revert to uncategorized)."""
    return _call(cat.handle_memory_undo, {"txn_id": txn_id})


@mcp.tool()
def cat_auto_categorize(dry_run: bool = True, ai: bool = False) -> dict:
    """Run categorization pipeline on uncategorized transactions.

    Args:
        dry_run: If True (default), preview matches without saving. Set False to commit.
        ai: If True, also run AI categorization pass on remaining unmatched.

    Returns:
        Dict with updated count, by_source breakdown, and ambiguous count.

    Examples:
        cat_auto_categorize()
        cat_auto_categorize(dry_run=False)
        cat_auto_categorize(dry_run=False, ai=True)
    """
    # Cannot use _call helper: needs manual commit when dry_run=False.
    with connect() as conn:
        result = cat.handle_auto_categorize(
            _ns(dry_run=dry_run, ai=ai, provider=None, batch_size=None),
            conn,
        )
        if not dry_run:
            conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def cat_apply_splits(commit: bool = False, backfill: bool = False) -> dict:
    """Apply split rules to matching unsplit transactions.

    Args:
        commit: If True, create split children and deactivate parent rows.
        backfill: If True, scan all active unsplit transactions (not just unreviewed).

    Returns:
        Dict with candidate count, split count, created children, and match details.

    Examples:
        cat_apply_splits()
        cat_apply_splits(commit=True)
        cat_apply_splits(commit=True, backfill=True)
    """
    return _call(cat.handle_apply_splits, {"commit": commit, "backfill": backfill})


@mcp.tool()
def cat_classify_use_type(commit: bool = False) -> dict:
    """Classify NULL transaction use_type values from keyword rules and category overrides.

    Args:
        commit: If True, persist updates. Default False runs as dry-run.

    Returns:
        Dict with scanned count, candidate updates, applied updates, and reason breakdown.

    Examples:
        cat_classify_use_type()
        cat_classify_use_type(commit=True)
    """
    return _call(cat.handle_classify_use_type, {"commit": commit})


@mcp.tool()
def txn_categorize(txn_id: str, category: str, remember: bool = False) -> dict:
    """Categorize a single transaction (and optionally save as vendor memory rule).

    Args:
        txn_id: Transaction ID to categorize.
        category: Category name to assign (must exist in DB).
        remember: If True, save a vendor memory rule for future auto-matching.

    Returns:
        Dict with transaction_id, category, previous/updated state, and remembered flag.

    Examples:
        txn_categorize(txn_id="abc123", category="Dining")
        txn_categorize(txn_id="abc123", category="Coffee", remember=True)
    """
    return _call(txn.handle_categorize, {
        "txn_id": txn_id,
        "category": category,
        "remember": remember,
        "bulk": False,
        "ids": None,
        "date_from": None,
        "date_to": None,
        "query": None,
    })


@mcp.tool()
def txn_bulk_categorize(
    category: str,
    query: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    remember: bool = False,
) -> dict:
    """Categorize multiple transactions matching filters."""
    return _call(txn.handle_categorize, {
        "category": category,
        "bulk": True,
        "query": query,
        "date_from": date_from,
        "date_to": date_to,
        "remember": remember,
        "txn_id": None,
        "ids": None,
    })


@mcp.tool()
def txn_review(
    txn_id: Optional[str] = None,
    all_today: bool = False,
    before: Optional[str] = None,
) -> dict:
    """Mark transactions as reviewed.

    Args:
        txn_id: Single transaction ID to mark reviewed.
        all_today: If True, mark all of today's transactions reviewed.
        before: Mark all transactions before this date (YYYY-MM-DD) reviewed.

    Returns:
        Dict with updated count or transaction_id confirmation.

    Examples:
        txn_review(txn_id="abc123")
        txn_review(all_today=True)
        txn_review(before="2026-02-01")
    """
    return _call(txn.handle_review, {
        "txn_id": txn_id,
        "all_today": all_today,
        "before": before,
    })


# ===================================================================
# 6. Setup & Import (7 tools, write)
# ===================================================================

@mcp.tool()
def setup_init(dry_run: bool = True) -> dict:
    """Initialize environment: seed categories, create .env template, bootstrap rules.yaml.

    Args:
        dry_run: If True (default), preview changes without applying.

    Returns:
        Dict with env_template, categories, and rules_file status.

    Examples:
        setup_init()
        setup_init(dry_run=False)
    """
    return _call(setup_cmd.handle_init, {"dry_run": dry_run})


@mcp.tool()
def setup_connect(
    user_id: Optional[str] = None,
    include_liabilities: bool = False,
    timeout: int = 300,
    skip_sync: bool = False,
    open_browser: bool = False,
) -> dict:
    """Link a bank institution via Plaid Hosted Link and optionally sync.

    Args:
        user_id: Client user ID for Plaid (default 'default').
        include_liabilities: Request liabilities product during link.
        timeout: Seconds to wait for user to complete link flow (default 300).
        skip_sync: If True, skip initial transaction sync after linking.
        open_browser: If True, automatically open the hosted link URL.

    Returns:
        Dict with linked item details, sync results, and hosted link URL.

    Examples:
        setup_connect(open_browser=True)
        setup_connect(include_liabilities=True, timeout=600)
    """
    return _call(setup_cmd.handle_connect, {
        "user_id": user_id or "default",
        "include_liabilities": include_liabilities,
        "timeout": timeout,
        "skip_sync": skip_sync,
        "open_browser": open_browser,
    })


@mcp.tool()
def plaid_sync(days: Optional[int] = None, item: Optional[str] = None, force: bool = False) -> dict:
    """Sync transactions from Plaid for all linked items (or a specific one).

    Args:
        days: Limit sync to last N days.
        item: Specific Plaid item ID to sync.
        force: Force refresh even if recently synced.

    Returns:
        Dict with items_synced, added, modified, removed counts.

    Examples:
        plaid_sync()
        plaid_sync(days=7, force=True)
    """
    return _call(plaid_cmd.handle_sync, {"days": days, "item": item, "force": force})


@mcp.tool()
def plaid_status() -> dict:
    """Plaid configuration status and linked item registry.

    Returns:
        Dict with configured flag, items list, active/error counts.

    Examples:
        plaid_status()
    """
    return _call(plaid_cmd.handle_status, {})


@mcp.tool()
def plaid_link(
    user_id: Optional[str] = None,
    wait: bool = False,
    timeout: int = 300,
    open_browser: bool = False,
    update: bool = False,
    item: Optional[str] = None,
    product: Optional[str] = None,
    include_balance: bool = False,
    include_liabilities: bool = False,
    allow_duplicate: bool = False,
) -> dict:
    """Create a Plaid Hosted Link session to connect a bank account.

    Generates a hosted link URL for the user to complete bank authentication.
    Returns the URL immediately by default — pass wait=True to block until
    the user finishes and exchange the token.

    Args:
        user_id: Client user ID for Plaid (default 'finance-cli-user').
        wait: If True, poll until user completes link flow (default False).
        timeout: Seconds to wait for completion when wait=True (default 300).
        open_browser: Open the hosted link URL in the user's browser (default False).
        update: Use update mode for an existing item (requires item param).
        item: Plaid item ID for update-mode re-authentication.
        product: Comma-separated Plaid products to request (e.g. 'transactions,investments').
        include_balance: Request balance product (default False).
        include_liabilities: Request liabilities product (default False).
        allow_duplicate: Allow linking same institution twice (default False).

    Returns:
        Dict with hosted_link_url, session details, and linked_item (if wait=True).

    Examples:
        plaid_link()
        plaid_link(wait=True, open_browser=True, include_liabilities=True)
        plaid_link(update=True, item="item_abc123")
    """
    products = [p.strip() for p in product.split(",")] if product else None
    return _call(plaid_cmd.handle_link, {
        "user_id": user_id or "finance-cli-user",
        "wait": wait,
        "timeout": timeout,
        "poll_seconds": 10,
        "open_browser": open_browser,
        "update": update,
        "item": item,
        "product": products,
        "include_balance": include_balance,
        "include_liabilities": include_liabilities,
        "allow_duplicate": allow_duplicate,
    })


@mcp.tool()
def plaid_unlink(item: str) -> dict:
    """Disconnect a Plaid item (bank connection).

    Deactivates the local item, its accounts, and all linked transactions.
    Creates a database backup before unlinking.

    Args:
        item: Plaid item ID to disconnect (from plaid_status output).

    Returns:
        Dict with item_id, status, and backup_path.

    Examples:
        plaid_unlink(item="item_abc123")
    """
    return _call(plaid_cmd.handle_unlink, {"item": item})


@mcp.tool()
def plaid_balance_refresh(item: Optional[str] = None, force: bool = False) -> dict:
    """Refresh real-time balances from Plaid for all items or a specific one.

    Args:
        item: Specific Plaid item ID to refresh.
        force: Force refresh even if recently updated.

    Returns:
        Dict with items_refreshed, accounts_updated, snapshots_updated.

    Examples:
        plaid_balance_refresh()
        plaid_balance_refresh(force=True)
    """
    return _call(plaid_cmd.handle_balance_refresh, {"item": item, "force": force})


@mcp.tool()
def stripe_link() -> dict:
    """Link Stripe using STRIPE_API_KEY and create/update local Stripe connection metadata.

    Returns:
        Dict with linked Stripe account metadata.

    Examples:
        stripe_link()
    """
    return _call(stripe_cmd.handle_link, {})


@mcp.tool()
def stripe_sync(days: Optional[int] = None, force: bool = False, backfill: bool = False) -> dict:
    """Sync Stripe balance transactions and payout dedup.

    Args:
        days: Optional lookback window (overrides stored cursor).
        force: If True, bypass sync cooldown.
        backfill: If True, ignore stored cursor and pull full history.

    Returns:
        Dict with sync counters and payout dedup outcomes.

    Examples:
        stripe_sync()
        stripe_sync(days=30, force=True)
        stripe_sync(backfill=True)
    """
    return _call(stripe_cmd.handle_sync, {"days": days, "force": force, "backfill": backfill})


@mcp.tool()
def stripe_status() -> dict:
    """Stripe configuration and connection status.

    Returns:
        Dict with readiness flags, connection metadata, and Stripe transaction counts.

    Examples:
        stripe_status()
    """
    return _call(stripe_cmd.handle_status, {})


@mcp.tool()
def stripe_revenue(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
) -> dict:
    """Stripe revenue summary grouped by month.

    Args:
        month: Optional month filter (YYYY-MM).
        quarter: Optional quarter filter (YYYY-QN).
        year: Optional year filter (YYYY).

    Returns:
        Dict with monthly gross, fees, refunds, and net totals.

    Examples:
        stripe_revenue()
        stripe_revenue(month="2026-02")
        stripe_revenue(year="2026")
    """
    return _call(stripe_cmd.handle_revenue, {"month": month, "quarter": quarter, "year": year})


@mcp.tool()
def stripe_unlink() -> dict:
    """Disconnect Stripe by marking the local connection as disconnected.

    Returns:
        Dict with unlink status.

    Examples:
        stripe_unlink()
    """
    return _call(stripe_cmd.handle_unlink, {})


@mcp.tool()
def ingest_csv(file: str, institution: str, commit: bool = False) -> dict:
    """Import a CSV bank statement.

    Args:
        file: Path to the CSV file.
        institution: Institution name (e.g. 'chase', 'amex', 'schwab').
        commit: If True, save imported transactions. Default is dry-run.

    Returns:
        Dict with file info, row counts, inserted/skipped/error counts.

    Examples:
        ingest_csv(file="/path/to/chase.csv", institution="chase")
        ingest_csv(file="/path/to/chase.csv", institution="chase", commit=True)
    """
    return _call(ingest.handle_ingest_csv, {
        "file": file,
        "institution": institution,
        "commit": commit,
    })


@mcp.tool()
def ingest_statement(
    file: str,
    commit: bool = False,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    institution: Optional[str] = None,
    card_ending: Optional[str] = None,
    account_id: Optional[str] = None,
    replace: bool = False,
    allow_partial: bool = False,
) -> dict:
    """Import a PDF bank statement via AI parser.

    Args:
        file: Path to the PDF statement file.
        commit: If True, save to DB. Default is dry-run preview.
        provider: AI provider ('claude' or 'openai'). Uses rules.yaml default if omitted.
        model: Model name override.
        institution: Institution hint (e.g. 'chase', 'amex').
        card_ending: Card ending hint (e.g. '1234').
        account_id: Existing account ID to tag transactions with.
        replace: Replace previously imported data for same file hash.
        allow_partial: Import unblocked rows when some are confidence-blocked.

    Returns:
        Dict with transaction_count, inserted, skipped_duplicates, reconcile_status.

    Examples:
        ingest_statement(file="/path/to/statement.pdf")
        ingest_statement(file="/path/to/statement.pdf", commit=True, institution="amex")
    """
    return _call(ingest.handle_ingest_statement, {
        "file": file,
        "dir": None,
        "commit": commit,
        "backend": None,
        "provider": provider,
        "model": model,
        "max_tokens": None,
        "institution": institution,
        "card_ending": card_ending,
        "account_id": account_id,
        "replace": replace,
        "allow_partial": allow_partial,
        "require_reconciled": False,
    })


@mcp.tool()
def ingest_batch(
    dir: str,
    commit: bool = False,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    institution: Optional[str] = None,
    card_ending: Optional[str] = None,
    allow_partial: bool = False,
) -> dict:
    """Batch import PDF and CSV files from a directory.

    Args:
        dir: Directory containing PDF/CSV statement files.
        commit: If True, save to DB. Default is dry-run preview.
        provider: AI provider for PDF parsing ('claude' or 'openai').
        model: Model name override for PDF parsing.
        institution: Institution hint for account matching.
        card_ending: Card ending hint for account matching.
        allow_partial: For PDFs, import unblocked rows when confidence-blocked.

    Returns:
        Dict with per-file reports, total inserted/skipped/error counts.

    Examples:
        ingest_batch(dir="/path/to/statements/")
        ingest_batch(dir="/path/to/statements/", commit=True)
    """
    return _call(ingest.handle_ingest_batch, {
        "dir": dir,
        "commit": commit,
        "backend": None,
        "provider": provider,
        "model": model,
        "max_tokens": None,
        "institution": institution,
        "card_ending": card_ending,
        "allow_partial": allow_partial,
    })


@mcp.tool()
def export_sheets(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    year: Optional[str] = None,
    new: bool = False,
    spreadsheet_id: Optional[str] = None,
) -> dict:
    """Export transactions/reports to Google Sheets.

    Args:
        date_from: Optional start date (YYYY-MM-DD) for transactions/spending tabs.
        date_to: Optional end date (YYYY-MM-DD) for transactions/spending tabs.
        year: Optional business year (YYYY).
        new: If True, create a new spreadsheet and save it as default.
        spreadsheet_id: Optional existing spreadsheet id to update.

    Returns:
        Dict with spreadsheet metadata and per-tab export stats.

    Examples:
        export_sheets()
        export_sheets(year="2025")
        export_sheets(date_from="2025-01-01", date_to="2025-03-31", spreadsheet_id="abc123")
    """
    return _call(
        export_cmd.handle_sheets,
        {
            "date_from": date_from,
            "date_to": date_to,
            "year": year,
            "auth": False,
            "new": new,
            "spreadsheet_id": spreadsheet_id,
            "interactive": False,
        },
    )


@mcp.tool()
def export_csv(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    category: Optional[str] = None,
) -> dict:
    """Export filtered transactions to CSV."""
    return _call(
        export_cmd.handle_csv,
        {
            "date_from": date_from,
            "date_to": date_to,
            "category": category,
            "output": _export_output_path("transactions"),
        },
    )


@mcp.tool()
def export_summary(month: Optional[str] = None) -> dict:
    """Export monthly summary to CSV."""
    resolved_month = month or datetime.now().strftime("%Y-%m")
    return _call(
        export_cmd.handle_summary,
        {
            "month": resolved_month,
            "output": _export_output_path(f"summary_{resolved_month.replace('-', '')}"),
        },
    )


@mcp.tool()
def export_wave(month: str, output: str = "exports") -> dict:
    """Export transactions as Wave accounting CSVs for a given month."""
    return _call(export_cmd.handle_wave, {"month": month, "output": output})


@mcp.tool()
def dedup_review_key_only(
    account_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict:
    """Review risky key-only dedup matches for manual verification."""
    return _call(
        dedup_cmd.handle_review_key_only,
        {"account_id": account_id, "date_from": date_from, "date_to": date_to},
    )


@mcp.tool()
def dedup_backfill_aliases(commit: bool = False) -> dict:
    """Backfill account aliases linking hash accounts to canonical Plaid accounts."""
    return _call(dedup_cmd.handle_backfill_aliases, {"commit": commit})


@mcp.tool()
def dedup_create_alias(from_id: str, to_id: str, commit: bool = False) -> dict:
    """Create an explicit account alias from one account to another."""
    return _call(dedup_cmd.handle_create_alias, {"from_id": from_id, "to_id": to_id, "commit": commit})


@mcp.tool()
def dedup_suggest_aliases() -> dict:
    """Suggest potential account aliases based on institution/card matching."""
    return _call(dedup_cmd.handle_suggest_aliases, {})


@mcp.tool()
def dedup_detect_equivalences(min_overlap: int = 3) -> dict:
    """Detect institution naming equivalences across import sources."""
    return _call(dedup_cmd.handle_detect_equivalences, {"min_overlap": min_overlap})


@mcp.tool()
def dedup_cross_format(dry_run: bool = True, account_id: Optional[str] = None) -> dict:
    """Run cross-format duplicate detection (optional commit)."""
    return _call(
        dedup_cmd.handle_cross_format,
        {
            "account_id": account_id,
            "date_from": None,
            "date_to": None,
            "commit": not dry_run,
            "include_key_only": False,
        },
    )


@mcp.tool()
def dedup_audit_names() -> dict:
    """Audit institution names and aliasing gaps."""
    return _call(dedup_cmd.handle_audit_names, {})


@mcp.tool()
def provider_status() -> dict:
    """Show institution provider routing status."""
    return _call(provider_cmd.handle_status, {})


@mcp.tool()
def provider_switch(institution: str, provider: str) -> dict:
    """Switch institution routing provider."""
    with connect() as conn:
        result = provider_cmd.handle_switch(_ns(institution=institution, provider=provider), conn)
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def schwab_sync() -> dict:
    """Sync Schwab balances."""
    return _call(schwab_cmd.handle_sync, {})


@mcp.tool()
def schwab_status() -> dict:
    """Show Schwab integration status."""
    return _call(schwab_cmd.handle_status, {})


# ===================================================================
# 7. Pipeline (1 tool)
# ===================================================================

@mcp.tool()
def monthly_run(
    month: Optional[str] = None,
    sync: bool = False,
    ai: bool = False,
    dry_run: bool = True,
    skip: Optional[list[str]] = None,
) -> dict:
    """Run the monthly pipeline: sync, dedup, categorize, detect subscriptions.

    Args:
        month: Target month (YYYY-MM). Defaults to current month.
        sync: If True, run Plaid sync + balance refresh first.
        ai: If True, include AI categorization pass.
        dry_run: If True (default), preview without committing changes.
        skip: List of steps to skip. Valid: 'dedup', 'categorize', 'detect'.

    Returns:
        Dict with per-step results, health checks, and timing.

    Examples:
        monthly_run()
        monthly_run(month="2026-02", dry_run=False)
        monthly_run(sync=True, ai=True, dry_run=False)
        monthly_run(skip=["dedup", "detect"], dry_run=True)
    """
    # monthly_run manages its own commits/rollbacks internally per step.
    with connect() as conn:
        result = monthly_cmd.handle_run(
            _ns(
                month=month or datetime.now().strftime("%Y-%m"),
                sync=sync,
                ai=ai,
                dry_run=dry_run,
                skip=skip or [],
                export_dir=None,
            ),
            conn,
        )
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


# ===================================================================
# 8. Database (1 tool)
# ===================================================================

@mcp.tool()
def db_backup() -> dict:
    """Create a timestamped backup of the SQLite database.

    Returns:
        Dict with db_path, backup_path, and size_bytes.

    Examples:
        db_backup()
    """
    return _call(db_cmd.handle_backup, {"output": None})


# ===================================================================
# 9. Reporting (3 tools, read-only)
# ===================================================================

@mcp.tool()
def financial_summary(view: str = "all") -> dict:
    """Financial health dashboard: net worth, cash flow, savings rate, obligations, data health.

    Args:
        view: Filter transactions by use_type ('personal', 'business', 'all').

    Returns:
        Dict with balances, cash flow, risk metrics, obligations, and data health.

    Examples:
        financial_summary()
        financial_summary(view="personal")
    """
    return _call(summary_cmd.handle_summary, {"view": view})


@mcp.tool()
def spending_trends(months: int = 6, view: str = "all") -> dict:
    """Monthly spending trends by category with trend indicators.

    Args:
        months: Number of months to include (default 6).
        view: Filter transactions by use_type ('personal', 'business', 'all').

    Returns:
        Dict with per-category monthly spending pivot and trend arrows.

    Examples:
        spending_trends()
        spending_trends(months=3, view="business")
    """
    return _call(spending_cmd.handle_trends, {"months": months, "view": view})


@mcp.tool()
def liability_obligations() -> dict:
    """Consolidated view of all fixed monthly obligations: recurring flows, debt minimums, subscriptions.

    Returns:
        Dict with three obligation sections (recurring, debt, subscriptions) and grand total.

    Examples:
        liability_obligations()
    """
    return _call(liability_cmd.handle_obligations, {})


# ===================================================================
# 10. Planning & Goals (4 tools)
# ===================================================================

@mcp.tool()
def net_worth_projection(months: int = 12) -> dict:
    """Project net worth forward using current trends: income, expenses, debt paydown, investment growth.

    Args:
        months: Projection horizon in months (default 12).

    Returns:
        Dict with current balances, monthly averages, and milestone projections.

    Examples:
        net_worth_projection()
        net_worth_projection(months=24)
    """
    return _call(projection_cmd.handle_projection, {"months": months})


@mcp.tool()
def goal_set(
    name: str,
    target: float,
    metric: str = "net_worth",
    direction: str = "up",
    deadline: Optional[str] = None,
) -> dict:
    """Set or update a financial goal.

    Args:
        name: Goal name (unique; re-using a name updates the existing goal).
        target: Target value in dollars (or percentage for savings_rate).
        metric: One of 'net_worth', 'liquid_cash', 'total_debt', 'investments', 'savings_rate'.
        direction: 'up' (target above current) or 'down' (target below current, e.g. debt).
        deadline: Optional ISO date deadline (YYYY-MM-DD).

    Returns:
        Dict with created/updated goal details.

    Examples:
        goal_set(name="Emergency Fund", target=25000, metric="liquid_cash")
        goal_set(name="Debt Free", target=0, metric="total_debt", direction="down")
    """
    with connect() as conn:
        result = goal_cmd.handle_set(
            _ns(name=name, target=target, metric=metric, direction=direction, deadline=deadline),
            conn,
        )
        conn.commit()
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


@mcp.tool()
def goal_list() -> dict:
    """List all active financial goals.

    Returns:
        Dict with list of active goals and their current values.

    Examples:
        goal_list()
    """
    return _call(goal_cmd.handle_list, {})


@mcp.tool()
def goal_status() -> dict:
    """Show progress on all active goals with progress bars and time estimates.

    Returns:
        Dict with per-goal progress percentage, current value, and estimated months to target.

    Examples:
        goal_status()
    """
    return _call(goal_cmd.handle_status, {})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
