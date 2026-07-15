"""Telegram approval helpers for tool-gated gateway calls."""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from finance_cli.gateway.tools import (
    APPROVAL_REQUIRED_TOOLS as _APPROVAL_REQUIRED_TOOLS,
    READ_ONLY_TOOLS as _READ_ONLY_TOOLS,
    needs_approval,
)

__all__ = [
    "_APPROVAL_REQUIRED_TOOLS",
    "_READ_ONLY_TOOLS",
    "needs_approval",
]


def _is_truthy(value: Any) -> bool:
    """Check truthiness handling string booleans from MCP layer."""
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no"}
    return bool(value)


def _fmt_amount(value: Any) -> str:
    """Format a numeric value as $X,XXX or $X,XXX.XX. Negative -> -$X."""
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return str(value)
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount == amount.to_integral_value():
        return f"{sign}${int(amount):,}"
    return f"{sign}${amount:,.2f}"


def _sanitize_summary(text: str, max_len: int = 60) -> str:
    """Single-line, length-capped summary for the approval header."""
    text = " ".join(text.splitlines()).strip()
    if len(text) > max_len:
        return text[: max_len - 1].rstrip() + "…"
    return text


def _fmt_budget_set(inp: dict[str, Any]) -> str:
    category = inp.get("category", "?")
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else "?"
    period = inp.get("period", "monthly")
    short_period = {"monthly": "mo", "weekly": "wk", "yearly": "yr"}.get(period, period)
    return f"Set budget: {category} → {amount}/{short_period}"


def _fmt_budget_update(inp: dict[str, Any]) -> str:
    category = inp.get("category", "?")
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else "?"
    return f"Update budget: {category} → {amount}"


def _fmt_budget_reallocate(inp: dict[str, Any]) -> str:
    from_category = inp.get("from_category", "?")
    to_category = inp.get("to_category", "?")
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else "?"
    return f"Move budget: {amount} {from_category} → {to_category}"


def _fmt_budget_delete(inp: dict[str, Any]) -> str:
    return f"Delete budget: {inp.get('category', '?')}"


def _fmt_txn_categorize(inp: dict[str, Any]) -> str:
    category = inp.get("category", "?")
    remember = " (+ remember)" if _is_truthy(inp.get("remember")) else ""
    return f"Categorize → {category}{remember}"


def _fmt_txn_bulk_categorize(inp: dict[str, Any]) -> str:
    category = inp.get("category", "?")
    query = inp.get("query", "")
    date_from = inp.get("date_from", "")
    date_to = inp.get("date_to", "")
    ids = inp.get("ids")
    if isinstance(ids, list) and ids:
        target = f"{len(ids)} selected txn(s)"
    elif isinstance(ids, str) and ids.strip():
        count = len([value for value in ids.split(",") if value.strip()])
        target = f"{count} selected txn(s)"
    elif query:
        target = f'"{query}"'
    elif date_from or date_to:
        target = f"{date_from or '...'} to {date_to or '...'}"
    else:
        target = "matching txns"
    remember = " (+ remember)" if _is_truthy(inp.get("remember")) else ""
    return f"Bulk categorize {target} → {category}{remember}"


def _fmt_txn_edit(inp: dict[str, Any]) -> str:
    parts: list[str] = []
    if "amount" in inp:
        parts.append(f"amount={_fmt_amount(inp['amount'])}")
    if "description" in inp:
        parts.append("desc")
    if "date" in inp:
        parts.append(f"date={inp['date']}")
    if "notes" in inp:
        parts.append("notes")
    return f"Edit transaction: {', '.join(parts) or 'fields'}"


def _fmt_txn_deactivate(inp: dict[str, Any]) -> str:
    txn_id = str(inp.get("id", "?"))[:12]
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Deactivate transaction: {txn_id} ({mode})"


def _fmt_txn_add(inp: dict[str, Any]) -> str:
    description = str(inp.get("description", "?"))[:40]
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else ""
    return f"Add transaction: {description} {amount}".strip()


def _fmt_txn_review(inp: dict[str, Any]) -> str:
    if _is_truthy(inp.get("all_today")):
        return "Review all today's transactions"
    if inp.get("before"):
        return f"Review transactions before {inp['before']}"
    return "Review transaction"


def _fmt_txn_tag(inp: dict[str, Any]) -> str:
    return f"Tag transaction: project={inp.get('project', '?')}"


def _fmt_txn_bulk_tag(inp: dict[str, Any]) -> str:
    items = inp.get("items") or []
    count = len(items) if isinstance(items, list) else 0
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Tag {count} transaction(s) ({mode})"


def _fmt_txn_dispute_workflow(inp: dict[str, Any]) -> str:
    transaction_id = str(inp.get("transaction_id", "?"))[:12]
    reason = inp.get("dispute_reason", "duplicate_charge")
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Prepare dispute workflow: {transaction_id}, {reason} ({mode})"


def _fmt_bulk_tag_billable(inp: dict[str, Any]) -> str:
    ids = inp.get("ids") or []
    count = len(ids) if isinstance(ids, list) else 0
    project = inp.get("project", "?")
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Tag {count} billable expense(s): {project} ({mode})"


def _fmt_loan_add(inp: dict[str, Any]) -> str:
    creditor = inp.get("creditor", "?")
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else "?"
    return f"Add loan: {creditor} {amount}"


def _fmt_loan_payment(inp: dict[str, Any]) -> str:
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else "?"
    return f"Record loan payment: {amount}"


def _fmt_loan_disburse(inp: dict[str, Any]) -> str:
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else "?"
    return f"Record loan disbursement: {amount}"


def _fmt_loan_adjust(inp: dict[str, Any]) -> str:
    loan_id = str(inp.get("loan_id", "?"))[:12]
    return f"Adjust loan terms: {loan_id}"


def _fmt_loan_close(inp: dict[str, Any]) -> str:
    loan_id = str(inp.get("loan_id", "?"))[:12]
    forgiven = " (forgiven)" if inp.get("forgiven") else ""
    return f"Close loan: {loan_id}{forgiven}"


def _fmt_subs_cancel(inp: dict[str, Any]) -> str:
    return f"Cancel subscription: {inp.get('id', '?')}"


def _fmt_subs_add(inp: dict[str, Any]) -> str:
    vendor = inp.get("vendor", "?")
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else ""
    return f"Add subscription: {vendor} {amount}".strip()


def _fmt_subs_update(inp: dict[str, Any]) -> str:
    subscription_id = str(inp.get("id", "?"))[:12]
    fields = [
        name
        for name in ("vendor", "amount", "frequency", "category", "use_type")
        if inp.get(name) is not None
    ]
    if inp.get("clear_category"):
        fields.append("category")
    if inp.get("clear_use_type"):
        fields.append("use_type")
    suffix = f" ({', '.join(fields)})" if fields else ""
    return f"Update subscription: {subscription_id}{suffix}"


def _fmt_subs_detect(inp: dict[str, Any]) -> str:
    del inp
    return "Detect subscriptions from transactions"


def _fmt_rules_add(inp: dict[str, Any]) -> str:
    keyword = inp.get("keyword", "?")
    category = inp.get("category", "?")
    return f'Add rule: "{keyword}" → {category}'


def _fmt_rules_add_bulk(inp: dict[str, Any]) -> str:
    items = inp.get("items") or []
    count = len(items) if isinstance(items, list) else 0
    return f"Add {count} keyword rule(s)"


def _fmt_rules_add_split(inp: dict[str, Any]) -> str:
    pct = inp.get("business_pct", "?")
    if inp.get("match_category"):
        target = str(inp.get("match_category"))
    else:
        keywords = inp.get("match_keywords") or []
        target = ", ".join(str(value) for value in keywords[:3]) if isinstance(keywords, list) else "keywords"
    return f"Add split rule: {target} → {pct}% business"


def _fmt_rules_remove(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f'Remove rule: "{inp.get("keyword", "?")}"{mode}'


def _fmt_rules_show(inp: dict[str, Any]) -> str:
    del inp
    return "Show rules"


def _fmt_rules_update_priority(inp: dict[str, Any]) -> str:
    return f"Update rule priority: {inp.get('rule_index', '?')}"


def _fmt_sync(label: str) -> Callable[[dict[str, Any]], str]:
    def _inner(inp: dict[str, Any]) -> str:
        item = inp.get("item") or inp.get("account_id") or ""
        extra = f" ({str(item)[:20]})" if item else ""
        return f"Sync {label}{extra}"

    return _inner


def _fmt_dedup(inp: dict[str, Any]) -> str:
    mode = "preview" if _is_truthy(inp.get("dry_run", True)) else "apply"
    scope = " + key-only" if _is_truthy(inp.get("include_key_only")) else ""
    return f"Remove duplicate transactions ({mode}{scope})"


def _fmt_dedup_backfill(inp: dict[str, Any]) -> str:
    mode = "apply" if _is_truthy(inp.get("commit")) else "preview"
    return f"Link matching accounts ({mode})"


def _fmt_dedup_create_alias(inp: dict[str, Any]) -> str:
    from_id = str(inp.get("from_id", "?"))[:12]
    to_id = str(inp.get("to_id", "?"))[:12]
    mode = "apply" if _is_truthy(inp.get("commit")) else "preview"
    return f"Link accounts: {from_id} → {to_id} ({mode})"


def _fmt_dedup_same_source_apply(inp: dict[str, Any]) -> str:
    ids = str(inp.get("ids", ""))
    count = len([i for i in ids.split(",") if i.strip()]) if ids else 0
    return f"Remove {count} duplicate transaction(s)"


def _fmt_cat_auto(inp: dict[str, Any]) -> str:
    mode = "preview" if _is_truthy(inp.get("dry_run", True)) else "apply"
    ai = " + AI" if _is_truthy(inp.get("ai")) else ""
    return f"Categorize transactions automatically ({mode}{ai})"


def _fmt_cat_memory_add(inp: dict[str, Any]) -> str:
    pattern = str(inp.get("pattern", "?"))[:30]
    category = inp.get("category", "?")
    return f'Remember "{pattern}" as {category}'


def _fmt_cat_memory_add_bulk(inp: dict[str, Any]) -> str:
    rules = inp.get("rules") or []
    count = len(rules) if isinstance(rules, list) else 0
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Save {count} vendor memory rule(s) ({mode})"


def _fmt_cat_review_new_merchants(inp: dict[str, Any]) -> str:
    items = inp.get("items") or []
    count = len(items) if isinstance(items, list) else 0
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Review {count} new merchant(s) ({mode})"


def _fmt_cat_add(inp: dict[str, Any]) -> str:
    return f"Add category: {inp.get('name', '?')}"


def _fmt_cat_apply_splits(inp: dict[str, Any]) -> str:
    mode = "apply" if _is_truthy(inp.get("commit")) else "preview"
    return f"Apply split rules ({mode})"


def _fmt_cat_classify_use_type(inp: dict[str, Any]) -> str:
    mode = "apply" if _is_truthy(inp.get("commit")) else "preview"
    return f"Tag business vs personal ({mode})"


def _fmt_bulk_reclassify_business(inp: dict[str, Any]) -> str:
    ids = inp.get("ids") or []
    count = len(ids) if isinstance(ids, list) else 0
    category = inp.get("category")
    suffix = f" -> {category}" if category else ""
    remember = " (+ remember)" if _is_truthy(inp.get("remember")) else ""
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Reclassify {count} business expense(s){suffix}{remember} ({mode})"


def _fmt_cat_normalize(inp: dict[str, Any]) -> str:
    mode = "preview" if _is_truthy(inp.get("dry_run", True)) else "apply"
    return f"Clean up category names ({mode})"


def _fmt_cat_memory_confirm(inp: dict[str, Any]) -> str:
    return f"Confirm vendor memory: {inp.get('id', '?')}"


def _fmt_cat_memory_delete(inp: dict[str, Any]) -> str:
    return f"Delete vendor memory: {inp.get('id', '?')}"


def _fmt_cat_memory_delete_bulk(inp: dict[str, Any]) -> str:
    ids = inp.get("ids") or []
    count = len(ids) if isinstance(ids, list) else 0
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Delete {count} vendor memory rule(s) ({mode})"


def _fmt_cat_memory_disable(inp: dict[str, Any]) -> str:
    return f"Disable vendor memory: {inp.get('id', '?')}"


def _fmt_cat_memory_disable_bulk(inp: dict[str, Any]) -> str:
    ids = inp.get("ids") or []
    count = len(ids) if isinstance(ids, list) else 0
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Disable {count} vendor memory rule(s) ({mode})"


def _fmt_cat_memory_restore(inp: dict[str, Any]) -> str:
    token = str(inp.get("restore_token", "?"))
    return f"Restore vendor memory: token {token[:12]}"


def _fmt_cat_memory_undo(inp: dict[str, Any]) -> str:
    return f"Undo vendor memory: txn {inp.get('txn_id', '?')}"


def _fmt_goal_set(inp: dict[str, Any]) -> str:
    name = inp.get("name", "?")
    target = _fmt_amount(inp["target"]) if "target" in inp else "?"
    return f"Set goal: {name} → {target}"


def _fmt_account_activate(inp: dict[str, Any]) -> str:
    return f"Activate account: {str(inp.get('id', '?'))[:12]}"


def _fmt_account_deactivate(inp: dict[str, Any]) -> str:
    suffix = " and its transactions" if _is_truthy(inp.get("cascade")) else ""
    return f"Deactivate account: {str(inp.get('id', '?'))[:12]}{suffix}"


def _fmt_account_business(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("id", "?"))[:12]
    mode = "business" if _is_truthy(inp.get("is_business")) else "personal"
    backfill = " and update past transactions" if _is_truthy(inp.get("backfill")) else ""
    return f"Mark {account_id} as {mode}{backfill}"


def _fmt_account_set_type(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("id", "?"))[:12]
    return f"Set account {account_id} type: {inp.get('account_type', '?')}"


def _fmt_balance_update(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("account", "?"))[:12]
    fields: list[str] = []
    if inp.get("current") is not None:
        fields.append(f"current={_fmt_amount(inp['current'])}")
    if inp.get("available") is not None:
        fields.append(f"available={_fmt_amount(inp['available'])}")
    if inp.get("balance_limit") is not None:
        fields.append(f"limit={_fmt_amount(inp['balance_limit'])}")
    mode = "preview" if _is_truthy(inp.get("dry_run")) else "apply"
    return f"Update balance: {account_id} ({', '.join(fields) or 'fields'}, {mode})"


def _fmt_biz_budget(inp: dict[str, Any]) -> str:
    section = inp.get("section", "?")
    amount = _fmt_amount(inp["amount"]) if "amount" in inp else "?"
    return f"Set biz budget: {section} → {amount}"


def _fmt_biz_mileage(inp: dict[str, Any]) -> str:
    miles = inp.get("miles", "?")
    date = inp.get("date", "")
    return f"Log mileage: {miles} mi" + (f" on {date}" if date else "")


def _fmt_biz_tax_setup(inp: dict[str, Any]) -> str:
    del inp
    return "Configure business tax settings"


def _fmt_home_office_tracking(inp: dict[str, Any]) -> str:
    year = inp.get("year", "?")
    sqft = inp.get("sqft", "?")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set up home-office tracking: {year}, {sqft} sqft{mode}"


def _fmt_retirement_target(inp: dict[str, Any]) -> str:
    year = inp.get("tax_year", "?")
    account_type = inp.get("account_type", "?")
    start_month = inp.get("start_month", "?")
    end_month = inp.get("end_month", "?")
    target_cents = inp.get("monthly_target_cents")
    target = (
        _fmt_amount(Decimal(str(target_cents)) / Decimal("100"))
        if target_cents is not None
        else "?"
    )
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set retirement target: {account_type} {year}, {target}/mo {start_month}-{end_month}{mode}"


def _fmt_monthly_transfer_goal(inp: dict[str, Any]) -> str:
    year = inp.get("tax_year", "?")
    account_type = inp.get("account_type", "roth_ira")
    start_month = inp.get("start_month", "?")
    end_month = inp.get("end_month", "?")
    transfer_cents = inp.get("monthly_transfer_cents")
    transfer = (
        _fmt_amount(Decimal(str(transfer_cents)) / Decimal("100"))
        if transfer_cents is not None
        else "?"
    )
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set transfer goal: {account_type} {year}, {transfer}/mo {start_month}-{end_month}{mode}"


def _fmt_biz_contractor_add(inp: dict[str, Any]) -> str:
    return f"Add contractor: {inp.get('name', '?')}"


def _fmt_biz_contractor_link(inp: dict[str, Any]) -> str:
    transaction_id = str(inp.get("transaction_id", "?"))[:12]
    contractor_id = str(inp.get("contractor_id", "?"))[:12]
    return f"Link txn {transaction_id} to contractor {contractor_id}"


def _fmt_contractor_january_prep(inp: dict[str, Any]) -> str:
    contractor_id = str(inp.get("contractor_id", "?"))[:12]
    tax_year = inp.get("tax_year") or "current year"
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Flag contractor {contractor_id} for {tax_year} 1099 prep{mode}"


def _fmt_spending_freeze_set(inp: dict[str, Any]) -> str:
    scope = inp.get("scope", "discretionary")
    hold_until = inp.get("hold_until") or inp.get("due_date") or "soon"
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set spending freeze: {scope} until {hold_until}{mode}"


def _fmt_spending_freeze_clear(inp: dict[str, Any]) -> str:
    flag_id = str(inp.get("flag_id", "?"))[:12]
    status = inp.get("status", "resolved")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Clear spending freeze {flag_id}: {status}{mode}"


def _fmt_late_month_buffer(inp: dict[str, Any]) -> str:
    category = inp.get("category_name", "Late-Month Buffer")
    amount = _fmt_amount(inp["amount_cents"] / 100) if "amount_cents" in inp else "?"
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set late-month buffer: {category} -> {amount}/mo{mode}"


def _fmt_card_paydown_flag(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("account_id", "?"))[:12]
    amount = (
        f" for {_fmt_amount(Decimal(str(inp['suggested_payment_cents'])) / Decimal('100'))}"
        if "suggested_payment_cents" in inp and inp.get("suggested_payment_cents")
        else ""
    )
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Flag card {account_id} for paydown{amount}{mode}"


def _fmt_card_paydown_clear(inp: dict[str, Any]) -> str:
    flag_id = str(inp.get("flag_id", "?"))[:12]
    status = inp.get("status", "resolved")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Clear card paydown flag {flag_id}: {status}{mode}"


def _fmt_notify(inp: dict[str, Any]) -> str:
    channel = inp.get("channel", "telegram")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Send budget alerts via {channel}{mode}"


def _fmt_notify_test(inp: dict[str, Any]) -> str:
    channel = inp.get("channel", "telegram")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Test notification: {channel}{mode}"


def _fmt_notify_channel_set(inp: dict[str, Any]) -> str:
    return f"Configure {inp.get('channel', '?')} notifications"


def _fmt_notify_channel_remove(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Remove {inp.get('channel', '?')} notification config{mode}"


def _fmt_card_rotation_reminder(inp: dict[str, Any]) -> str:
    end_date = inp.get("intro_apr_end_date", "?")
    return f"Set 0% APR card rotation reminder before {end_date}"


def _fmt_balance_transfer_reminder(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("account_id", "?"))[:12]
    remind_on = inp.get("remind_on", "?")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set balance-transfer reminder: {account_id} on {remind_on}{mode}"


def _fmt_hysa_transfer_flag(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("account_id", "?"))[:12]
    transfer_cents = inp.get("suggested_transfer_cents")
    transfer = (
        _fmt_amount(Decimal(str(transfer_cents)) / Decimal("100"))
        if transfer_cents is not None
        else "?"
    )
    hysa_apy_bps = inp.get("hysa_apy_bps", "?")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Flag HYSA transfer: {account_id}, {transfer} at {hysa_apy_bps} bps{mode}"


def _fmt_savings_automation(inp: dict[str, Any]) -> str:
    goal_id = str(inp.get("goal_id", "?"))[:12]
    amount_cents = inp.get("amount_cents")
    amount = (
        _fmt_amount(Decimal(str(amount_cents)) / Decimal("100"))
        if amount_cents is not None
        else "?"
    )
    cadence = inp.get("cadence", "monthly")
    start_date = inp.get("start_date", "?")
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set savings automation: {goal_id}, {amount} {cadence} from {start_date}{mode}"


def _fmt_low_balance_alert_set(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("account_id", "?"))[:12]
    threshold_cents = inp.get("threshold_cents")
    threshold = (
        _fmt_amount(Decimal(str(threshold_cents)) / Decimal("100"))
        if threshold_cents is not None
        else "?"
    )
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set low-balance alert: {account_id} below {threshold}{mode}"


def _fmt_low_balance_alert_check(inp: dict[str, Any]) -> str:
    mode = "preview" if _is_truthy(inp.get("dry_run", True)) else "send"
    return f"Check low-balance alerts ({mode})"


def _fmt_debt_set_apr(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("account_id", inp.get("account", "?")))[:12]
    apr = inp.get("apr_pct", inp.get("apr", "?"))
    try:
        apr_value = Decimal(str(apr))
        apr_text = f"{apr_value:.2f}".rstrip("0").rstrip(".")
    except (InvalidOperation, TypeError, ValueError):
        apr_text = str(apr)
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Set debt APR: {account_id} to {apr_text}%{mode}"


def _fmt_debt_balance_portion_add(inp: dict[str, Any]) -> str:
    account_id = str(inp.get("account_id", inp.get("account", "?")))[:8]
    label = _sanitize_summary(str(inp.get("label", "portion")), max_len=9)
    principal = inp.get("principal_dollars", inp.get("principal", "?"))
    amount = _fmt_amount(principal) if principal != "?" else "?"
    apr = inp.get("apr_pct", inp.get("apr", "?"))
    try:
        apr_value = Decimal(str(apr))
        apr_text = f"{apr_value:.2f}".rstrip("0").rstrip(".")
    except (InvalidOperation, TypeError, ValueError):
        apr_text = str(apr)
    payment = inp.get("monthly_payment_dollars", inp.get("monthly_payment"))
    payment_text = ""
    if payment is not None:
        payment_text = f" {_fmt_amount(payment)}/mo"
    mode = " preview" if _is_truthy(inp.get("dry_run")) else " apply"
    return f"Add portion: {account_id} {label} {amount}@{apr_text}%{payment_text}{mode}"


def _fmt_debt_balance_portion_update(inp: dict[str, Any]) -> str:
    portion_id = str(inp.get("portion_id", "?"))[:12]
    fields = []
    field_labels = {
        "label": "label",
        "principal_dollars": "principal",
        "principal": "principal",
        "apr_pct": "apr",
        "apr": "apr",
        "monthly_payment_dollars": "payment",
        "monthly_payment": "payment",
        "portion_type": "type",
        "promo_end_date": "end_date",
        "notes": "notes",
    }
    for key, label in field_labels.items():
        if inp.get(key) is not None:
            fields.append(label)
    clear_labels = {
        "clear_monthly_payment": "clear_payment",
        "clear_promo_end_date": "clear_end",
        "clear_notes": "clear_notes",
    }
    for key, label in clear_labels.items():
        if _is_truthy(inp.get(key)):
            fields.append(label)
    field_text = ", ".join(dict.fromkeys(fields)) or "fields"
    mode = " preview" if _is_truthy(inp.get("dry_run")) else " apply"
    return f"Update portion: {portion_id} ({field_text}){mode}"


def _fmt_debt_balance_portion_deactivate(inp: dict[str, Any]) -> str:
    portion_id = str(inp.get("portion_id", "?"))[:12]
    mode = " preview" if _is_truthy(inp.get("dry_run")) else " apply"
    return f"Deactivate portion: {portion_id}{mode}"


def _fmt_coach_debt_payoff_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save debt payoff action plan{mode}"


def _fmt_coach_emergency_fund_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save emergency fund plan{mode}"


def _fmt_coach_savings_goal_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save savings goal plan{mode}"


def _fmt_coach_spending_plan_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save spending plan{mode}"


def _fmt_coach_tax_readiness_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save tax readiness plan{mode}"


def _fmt_coach_homebuying_readiness_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save homebuying readiness plan{mode}"


def _fmt_coach_retirement_contribution_readiness_artifact_save(
    inp: dict[str, Any],
) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save retirement contribution readiness plan{mode}"


def _fmt_coach_retirement_income_readiness_artifact_save(
    inp: dict[str, Any],
) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save retirement income readiness plan{mode}"


def _fmt_coach_investment_readiness_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save investment readiness plan{mode}"


def _fmt_coach_estate_document_readiness_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save estate document readiness checklist{mode}"


def _fmt_coach_financial_plan_intake_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save financial planning snapshot{mode}"


def _fmt_coach_risk_insurance_readiness_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save risk insurance readiness plan{mode}"


def _fmt_coach_advisor_handoff_readiness_artifact_save(inp: dict[str, Any]) -> str:
    mode = " (preview)" if _is_truthy(inp.get("dry_run")) else ""
    return f"Save advisor handoff readiness packet{mode}"


def _fmt_monthly(inp: dict[str, Any]) -> str:
    mode = "dry run" if _is_truthy(inp.get("dry_run", True)) else "apply"
    return f"Monthly cleanup ({mode})"


def _fmt_export_sheets(inp: dict[str, Any]) -> str:
    del inp
    return "Export to Google Sheets"


def _fmt_plan_create(inp: dict[str, Any]) -> str:
    del inp
    return "Create monthly plan"


def _fmt_provider_switch(inp: dict[str, Any]) -> str:
    return f"Switch {inp.get('institution', '?')} → {inp.get('provider', '?')}"


def _fmt_plaid_unlink(inp: dict[str, Any]) -> str:
    return f"Unlink Plaid: {str(inp.get('item', '?'))[:20]}"


def _fmt_setup_init(inp: dict[str, Any]) -> str:
    return f"Initialize categories (dry_run={'yes' if _is_truthy(inp.get('dry_run', True)) else 'no'})"


def _fmt_setup_connect(inp: dict[str, Any]) -> str:
    wait = "yes" if _is_truthy(inp.get("wait")) else "no"
    open_browser = "yes" if _is_truthy(inp.get("open_browser")) else "no"
    return f"Set up Plaid connection (wait={wait}, open_browser={open_browser})"


def _fmt_plaid_link(inp: dict[str, Any]) -> str:
    wait = "yes" if _is_truthy(inp.get("wait")) else "no"
    return f"Connect bank via Plaid (wait={wait})"


def _fmt_plaid_exchange(inp: dict[str, Any]) -> str:
    del inp
    return "Complete Plaid bank link"


def _fmt_stripe_link(inp: dict[str, Any]) -> str:
    del inp
    return "Link Stripe account"


def _fmt_stripe_unlink(inp: dict[str, Any]) -> str:
    del inp
    return "Unlink Stripe account"


def _fmt_agent_memory_update(inp: dict[str, Any]) -> str:
    del inp
    return "Update agent memory"


def _fmt_agent_session_write(inp: dict[str, Any]) -> str:
    del inp
    return "Write session notes"



def _fmt_log_issue(inp: dict[str, Any]) -> str:
    return f"Log issue: {str(inp.get('title', '?'))[:40]}"


def _fmt_error_update(inp: dict[str, Any]) -> str:
    error_id = str(inp.get("error_id", "?"))[:12]
    status = inp.get("status", "?")
    return f"Update error {error_id} → {status}"


def _fmt_issue_update(inp: dict[str, Any]) -> str:
    issue_id = str(inp.get("issue_id", "?"))[:12]
    status = inp.get("status", "?")
    return f"Update issue {issue_id} → {status}"


def _fmt_cost_limit_set(inp: dict[str, Any]) -> str:
    provider = inp.get("provider", "?")
    period = inp.get("period", "?")
    action = inp.get("action", "?")
    try:
        dollars = Decimal(str(inp.get("limit_usd6", "0"))) / Decimal("1000000")
    except (InvalidOperation, TypeError, ValueError):
        dollars = Decimal("0")
    return f"Set cost limit: {provider} {period} → {_fmt_amount(dollars)} ({action})"


def _fmt_db_backup_prune(inp: dict[str, Any]) -> str:
    mode = "preview" if _is_truthy(inp.get("dry_run", True)) else "apply"
    return f"Prune old backups ({mode})"


def _fmt_db_export_preferences(inp: dict[str, Any]) -> str:
    del inp
    return "Export preferences bundle"


def _fmt_db_import_preferences(inp: dict[str, Any]) -> str:
    mode = str(inp.get("mode", "merge") or "merge")
    state = "preview" if _is_truthy(inp.get("dry_run", True)) else "apply"
    return f"Import preferences ({mode}, {state})"


_TOOL_SUMMARIES: dict[str, Callable[[dict[str, Any]], str]] = {
    "budget_set": _fmt_budget_set,
    "budget_update": _fmt_budget_update,
    "budget_reallocate": _fmt_budget_reallocate,
    "budget_delete": _fmt_budget_delete,
    "txn_categorize": _fmt_txn_categorize,
    "txn_bulk_categorize": _fmt_txn_bulk_categorize,
    "txn_edit": _fmt_txn_edit,
    "txn_deactivate": _fmt_txn_deactivate,
    "txn_add": _fmt_txn_add,
    "txn_review": _fmt_txn_review,
    "txn_tag": _fmt_txn_tag,
    "txn_bulk_tag": _fmt_txn_bulk_tag,
    "txn_dispute_workflow": _fmt_txn_dispute_workflow,
    "bulk_tag_billable_expenses": _fmt_bulk_tag_billable,
    "loan_add": _fmt_loan_add,
    "loan_payment": _fmt_loan_payment,
    "loan_disburse": _fmt_loan_disburse,
    "loan_adjust": _fmt_loan_adjust,
    "loan_close": _fmt_loan_close,
    "subs_cancel": _fmt_subs_cancel,
    "subs_add": _fmt_subs_add,
    "subs_update": _fmt_subs_update,
    "subs_detect": _fmt_subs_detect,
    "rules_add_keyword": _fmt_rules_add,
    "rules_add_keywords": _fmt_rules_add_bulk,
    "rules_add_split": _fmt_rules_add_split,
    "rules_remove_keyword": _fmt_rules_remove,
    "rules_update_priority": _fmt_rules_update_priority,
    "plaid_sync": _fmt_sync("Plaid"),
    "plaid_balance_refresh": _fmt_sync("Plaid balances"),
    "schwab_sync": _fmt_sync("Schwab"),
    "stripe_sync": _fmt_sync("Stripe"),
    "dedup_cross_format": _fmt_dedup,
    "dedup_backfill_aliases": _fmt_dedup_backfill,
    "dedup_create_alias": _fmt_dedup_create_alias,
    "dedup_same_source_apply": _fmt_dedup_same_source_apply,
    "cat_auto_categorize": _fmt_cat_auto,
    "cat_memory_add": _fmt_cat_memory_add,
    "cat_memory_add_bulk": _fmt_cat_memory_add_bulk,
    "cat_review_new_merchants": _fmt_cat_review_new_merchants,
    "cat_add": _fmt_cat_add,
    "cat_apply_splits": _fmt_cat_apply_splits,
    "cat_classify_use_type": _fmt_cat_classify_use_type,
    "bulk_reclassify_business": _fmt_bulk_reclassify_business,
    "cat_normalize": _fmt_cat_normalize,
    "cat_memory_confirm": _fmt_cat_memory_confirm,
    "cat_memory_delete": _fmt_cat_memory_delete,
    "cat_memory_delete_bulk": _fmt_cat_memory_delete_bulk,
    "cat_memory_disable": _fmt_cat_memory_disable,
    "cat_memory_disable_bulk": _fmt_cat_memory_disable_bulk,
    "cat_memory_restore": _fmt_cat_memory_restore,
    "cat_memory_undo": _fmt_cat_memory_undo,
    "goal_set": _fmt_goal_set,
    "bank_account_activate": _fmt_account_activate,
    "bank_account_deactivate": _fmt_account_deactivate,
    "account_set_business": _fmt_account_business,
    "account_set_type": _fmt_account_set_type,
    "balance_update": _fmt_balance_update,
    "biz_budget_set": _fmt_biz_budget,
    "biz_mileage_add": _fmt_biz_mileage,
    "biz_tax_setup": _fmt_biz_tax_setup,
    "setup_home_office_tracking": _fmt_home_office_tracking,
    "set_monthly_retirement_target": _fmt_retirement_target,
    "setup_monthly_transfer_goal": _fmt_monthly_transfer_goal,
    "biz_contractor_add": _fmt_biz_contractor_add,
    "biz_contractor_link": _fmt_biz_contractor_link,
    "flag_contractor_january_prep": _fmt_contractor_january_prep,
    "set_spending_freeze_flag": _fmt_spending_freeze_set,
    "clear_spending_freeze_flag": _fmt_spending_freeze_clear,
    "add_late_month_buffer_budget": _fmt_late_month_buffer,
    "flag_card_for_paydown": _fmt_card_paydown_flag,
    "clear_card_paydown_flag": _fmt_card_paydown_clear,
    "notify_budget_alerts": _fmt_notify,
    "notify_channel_remove": _fmt_notify_channel_remove,
    "notify_channel_set": _fmt_notify_channel_set,
    "notify_test": _fmt_notify_test,
    "card_rotation_reminder_set": _fmt_card_rotation_reminder,
    "set_balance_transfer_reminder": _fmt_balance_transfer_reminder,
    "flag_account_for_hysa_transfer": _fmt_hysa_transfer_flag,
    "setup_savings_automation": _fmt_savings_automation,
    "set_low_balance_alert": _fmt_low_balance_alert_set,
    "low_balance_alerts_check": _fmt_low_balance_alert_check,
    "debt_set_apr": _fmt_debt_set_apr,
    "debt_balance_portion_add": _fmt_debt_balance_portion_add,
    "debt_balance_portion_update": _fmt_debt_balance_portion_update,
    "debt_balance_portion_deactivate": _fmt_debt_balance_portion_deactivate,
    "monthly_run": _fmt_monthly,
    "export_sheets": _fmt_export_sheets,
    "plan_create": _fmt_plan_create,
    "provider_switch": _fmt_provider_switch,
    "statement_normalizer_stage": lambda inp: f"Stage normalizer: {inp.get('key', '?')}",
    "statement_normalizer_activate": lambda inp: f"Activate normalizer: {inp.get('key', '?')}",
    "normalizer_update": lambda inp: f"Update normalizer: {inp.get('key', '?')}",
    "normalizer_register_institution": (
        lambda inp: f"Register institution: {inp.get('canonical_name', '?')}"
    ),
    "setup_init": _fmt_setup_init,
    "setup_connect": _fmt_setup_connect,
    "plaid_link": _fmt_plaid_link,
    "plaid_exchange": _fmt_plaid_exchange,
    "plaid_unlink": _fmt_plaid_unlink,
    "stripe_link": _fmt_stripe_link,
    "stripe_unlink": _fmt_stripe_unlink,
    "agent_memory_update": _fmt_agent_memory_update,
    "agent_session_write": _fmt_agent_session_write,
    "skill_state_set": lambda inp: f"Set skill state: {inp.get('name', '?')}",
    "skill_state_clear": lambda inp: f"Clear skill state: {inp.get('name', '?')}",
    "skip_onboarding": lambda _: "Skip optional onboarding setup",
    "strategy_preference_set": (
        lambda inp: f"Set strategy preference: {inp.get('domain', '?')} -> {inp.get('strategy', '?')}"
    ),
    "strategy_preference_clear": (
        lambda inp: f"Clear strategy preference: {inp.get('domain', '?')}"
    ),
    "error_update": _fmt_error_update,
    "issue_update": _fmt_issue_update,
    "finance_log_issue": _fmt_log_issue,
    "interventions_act": lambda inp: f"Mark intervention acted: {inp.get('log_id', '?')}",
    "interventions_dismiss": lambda inp: f"Dismiss intervention: {inp.get('log_id', '?')}",
    "interventions_mute": lambda inp: f"Mute intervention pattern: {inp.get('pattern_id', '?')}",
    "interventions_unmute": lambda inp: f"Unmute intervention pattern: {inp.get('pattern_id', '?')}",
    "coach_debt_payoff_artifact_save": _fmt_coach_debt_payoff_artifact_save,
    "coach_emergency_fund_artifact_save": _fmt_coach_emergency_fund_artifact_save,
    "coach_savings_goal_artifact_save": _fmt_coach_savings_goal_artifact_save,
    "coach_spending_plan_artifact_save": _fmt_coach_spending_plan_artifact_save,
    "coach_tax_readiness_artifact_save": _fmt_coach_tax_readiness_artifact_save,
    "coach_homebuying_readiness_artifact_save": (
        _fmt_coach_homebuying_readiness_artifact_save
    ),
    "coach_retirement_contribution_readiness_artifact_save": (
        _fmt_coach_retirement_contribution_readiness_artifact_save
    ),
    "coach_retirement_income_readiness_artifact_save": (
        _fmt_coach_retirement_income_readiness_artifact_save
    ),
    "coach_investment_readiness_artifact_save": (
        _fmt_coach_investment_readiness_artifact_save
    ),
    "coach_estate_document_readiness_artifact_save": (
        _fmt_coach_estate_document_readiness_artifact_save
    ),
    "coach_financial_plan_intake_artifact_save": (
        _fmt_coach_financial_plan_intake_artifact_save
    ),
    "coach_risk_insurance_readiness_artifact_save": (
        _fmt_coach_risk_insurance_readiness_artifact_save
    ),
    "coach_advisor_handoff_readiness_artifact_save": (
        _fmt_coach_advisor_handoff_readiness_artifact_save
    ),
    "cost_limits_set": _fmt_cost_limit_set,
    "db_backup": lambda _: "Create backup bundle",
    "db_backup_prune": _fmt_db_backup_prune,
    "db_export_preferences": _fmt_db_export_preferences,
    "db_import_preferences": _fmt_db_import_preferences,
}


def _summarize_tool(tool_name: str, tool_input: dict[str, Any]) -> str:
    formatter = _TOOL_SUMMARIES.get(tool_name)
    if formatter:
        try:
            return _sanitize_summary(formatter(tool_input))
        except Exception:
            pass
    return _sanitize_summary(tool_name.replace("_", " ").capitalize())


def format_approval_message(event: dict[str, Any]) -> str:
    """Format a tool approval prompt for Telegram."""
    tool_name = str(event.get("tool_name", "unknown"))
    raw_input = event.get("tool_input", {})
    tool_input = raw_input if isinstance(raw_input, dict) else {}
    reason = tool_input.pop("_approval_reason", None) if isinstance(raw_input, dict) else None
    summary = _summarize_tool(tool_name, tool_input)
    lines = [f"🔒 {summary}"]
    if isinstance(reason, str) and reason.strip():
        sanitized = " ".join(reason.strip().splitlines())
        if len(sanitized) > 120:
            sanitized = sanitized[:117].rstrip() + "…"
        lines.append(f'"{sanitized}"')
    lines.extend(["", f"Tool: {tool_name}"])
    for key, value in list(tool_input.items())[:4]:
        lines.append(f"  {key}: {_format_value(value)}")
    return "\n".join(lines)


def build_approval_keyboard(nonce: str, tool_name: str) -> list[list[dict[str, str]]]:
    """Build the inline keyboard for an approval request."""
    del tool_name
    return [
        [
            {
                "text": "✅ Approve",
                "callback_data": json.dumps({"n": nonce, "a": "y"}, separators=(",", ":")),
            },
            {
                "text": "❌ Deny",
                "callback_data": json.dumps({"n": nonce, "a": "n"}, separators=(",", ":")),
            },
        ]
    ]


def parse_callback_data(data: str) -> tuple[str, str] | None:
    """Parse Telegram callback data into a nonce/action pair."""
    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    nonce = payload.get("n")
    action = payload.get("a")
    if not isinstance(nonce, str) or not isinstance(action, str):
        return None
    if action not in {"y", "n"}:
        return None
    return nonce, action


def _format_value(value: Any) -> str:
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, default=str)
    text = " ".join(text.splitlines())
    if len(text) > 80:
        return f"{text[:77]}..."
    return text
