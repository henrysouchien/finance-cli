"""Manual loan tracking commands."""

from __future__ import annotations

import calendar
import json
import sqlite3
import uuid
from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Any

from finance_cli.exceptions import ConflictError, NotFoundError, ValidationError

from ..models import cents_to_dollars
from .common import fmt_dollars, today_iso

_INTEREST_TYPES = {"simple", "compound", "none"}
_USE_TYPES = {"Personal", "Business"}


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("loan", parents=[format_parent], help="Manual loan tracking")
    loan_sub = parser.add_subparsers(dest="loan_command", required=True)

    p_add = loan_sub.add_parser("add", parents=[format_parent], help="Create a manual loan")
    p_add.add_argument("--creditor", required=True, help="Creditor name")
    p_add.add_argument("--amount", required=True, type=float, help="Initial disbursement amount in dollars")
    p_add.add_argument("--start-date", required=True, help="Loan start date (YYYY-MM-DD)")
    p_add.add_argument("--rate", type=float, default=0.0, help="Annual interest rate percent")
    p_add.add_argument("--interest-type", choices=sorted(_INTEREST_TYPES), help="Interest calculation type")
    p_add.add_argument("--monthly-payment", type=float, help="Monthly payment amount in dollars")
    p_add.add_argument("--due-day", type=int, help="Payment due day of month (1-31)")
    p_add.add_argument("--expected-payoff", help="Target payoff date (YYYY-MM-DD)")
    p_add.add_argument("--use-type", default="Personal", choices=sorted(_USE_TYPES), help="Personal or Business")
    p_add.add_argument("--description", help="Optional notes")
    p_add.set_defaults(func=handle_add, command_name="loan.add")

    p_list = loan_sub.add_parser("list", parents=[format_parent], help="List manual loans")
    p_list.add_argument("--include-inactive", action="store_true")
    p_list.set_defaults(func=handle_list, command_name="loan.list")

    p_show = loan_sub.add_parser("show", parents=[format_parent], help="Show manual loan details")
    p_show.add_argument("loan_id", help="Loan ID")
    p_show.set_defaults(func=handle_show, command_name="loan.show")

    p_payment = loan_sub.add_parser("payment", parents=[format_parent], help="Record a repayment")
    p_payment.add_argument("loan_id", help="Loan ID")
    p_payment.add_argument("--amount", required=True, type=float, help="Payment amount in dollars")
    p_payment.add_argument("--date", help="Payment date (YYYY-MM-DD)")
    p_payment.add_argument("--transaction", dest="transaction_id", help="Optional linked transaction ID")
    p_payment.add_argument("--notes", help="Optional payment notes")
    p_payment.set_defaults(func=handle_payment, command_name="loan.payment")

    p_disburse = loan_sub.add_parser("disburse", parents=[format_parent], help="Record additional borrowing")
    p_disburse.add_argument("loan_id", help="Loan ID")
    p_disburse.add_argument("--amount", required=True, type=float, help="Additional disbursement amount in dollars")
    p_disburse.add_argument("--date", help="Disbursement date (YYYY-MM-DD)")
    p_disburse.add_argument("--notes", help="Optional notes")
    p_disburse.set_defaults(func=handle_disburse, command_name="loan.disburse")

    p_adjust = loan_sub.add_parser("adjust", parents=[format_parent], help="Adjust loan terms")
    p_adjust.add_argument("loan_id", help="Loan ID")
    p_adjust.add_argument("--rate", type=float, help="New annual interest rate percent")
    p_adjust.add_argument("--interest-type", choices=sorted(_INTEREST_TYPES), help="New interest type")
    p_adjust.add_argument("--monthly-payment", type=float, help="New monthly payment amount in dollars")
    p_adjust.add_argument("--due-day", type=int, help="New due day of month (1-31)")
    p_adjust.add_argument("--expected-payoff", help="New target payoff date (YYYY-MM-DD)")
    p_adjust.add_argument("--balance", type=float, help="Override balance in dollars")
    p_adjust.add_argument("--description", help="New description")
    p_adjust.set_defaults(func=handle_adjust, command_name="loan.adjust")

    p_close = loan_sub.add_parser("close", parents=[format_parent], help="Close a manual loan")
    p_close.add_argument("loan_id", help="Loan ID")
    p_close.add_argument("--forgiven", action="store_true", help="Forgive remaining balance")
    p_close.set_defaults(func=handle_close, command_name="loan.close")

    p_schedule = loan_sub.add_parser("schedule", parents=[format_parent], help="Repayment schedule (Phase 2)")
    p_schedule.add_argument("loan_id", help="Loan ID")
    p_schedule.add_argument("--months", type=int, default=0, help="Projection horizon (max 120)")
    p_schedule.set_defaults(func=handle_schedule, command_name="loan.schedule")


def _fmt(args, fmt: str | None) -> str:
    if fmt:
        return str(fmt)
    return str(getattr(args, "format", "json") or "json")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _require_loan_id(args) -> str:
    loan_id = _clean_text(getattr(args, "loan_id", None))
    if not loan_id:
        raise ValidationError("loan_id is required")
    return loan_id


def _dollars_to_cents(value: float) -> int:
    return int(round(float(value) * 100))


def _require_positive_dollars(value: Any, field_name: str) -> int:
    if value is None:
        raise ValidationError(f"{field_name} is required")
    amount = float(value)
    if amount <= 0:
        raise ValidationError(f"{field_name} must be > 0")
    return _dollars_to_cents(amount)


def _optional_positive_dollars(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    amount = float(value)
    if amount <= 0:
        raise ValidationError(f"{field_name} must be > 0")
    return _dollars_to_cents(amount)


def _optional_nonnegative_dollars(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    amount = float(value)
    if amount < 0:
        raise ValidationError(f"{field_name} must be >= 0")
    return _dollars_to_cents(amount)


def _require_iso_date(value: Any, field_name: str) -> str:
    text = _clean_text(value)
    if not text:
        raise ValidationError(f"{field_name} is required")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be a valid YYYY-MM-DD date") from exc
    return parsed.isoformat()


def _optional_iso_date(value: Any, field_name: str) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return _require_iso_date(text, field_name)


def _validate_due_day(value: Any) -> int | None:
    if value is None:
        return None
    due_day = int(value)
    if due_day < 1 or due_day > 31:
        raise ValidationError("due_day must be between 1 and 31")
    return due_day


def _validate_rate(value: Any) -> float:
    rate = float(value if value is not None else 0.0)
    if rate < 0:
        raise ValidationError("rate must be >= 0")
    return rate


def _validate_use_type(value: Any) -> str:
    use_type = _clean_text(value) or "Personal"
    if use_type not in _USE_TYPES:
        raise ValidationError("use_type must be 'Personal' or 'Business'")
    return use_type


def _validate_interest_type(value: Any) -> str:
    interest_type = _clean_text(value)
    if interest_type is None:
        raise ValidationError("interest_type is required")
    if interest_type not in _INTEREST_TYPES:
        raise ValidationError("interest_type must be one of: simple, compound, none")
    return interest_type


def _validate_rate_type_consistency(rate: float, interest_type: str) -> None:
    if interest_type == "none" and rate != 0.0:
        raise ValidationError("interest_type 'none' requires rate 0")
    if interest_type != "none" and rate <= 0.0:
        raise ValidationError("non-'none' interest_type requires a positive rate")


def _resolve_new_interest(rate: float, interest_type: Any) -> str:
    if interest_type is None:
        return "none" if rate == 0.0 else "simple"
    resolved = _validate_interest_type(interest_type)
    _validate_rate_type_consistency(rate, resolved)
    return resolved


def _resolve_adjusted_interest(current_row: sqlite3.Row, args) -> tuple[float, str]:
    has_rate = getattr(args, "rate", None) is not None
    has_interest_type = getattr(args, "interest_type", None) is not None

    new_rate = _validate_rate(args.rate) if has_rate else float(current_row["interest_rate_pct"])
    new_interest_type = (
        _validate_interest_type(args.interest_type)
        if has_interest_type
        else str(current_row["interest_type"])
    )

    if has_rate and not has_interest_type:
        if new_rate == 0.0:
            new_interest_type = "none"
        elif new_interest_type == "none":
            new_interest_type = "simple"
    elif has_interest_type and not has_rate:
        if new_interest_type == "none":
            new_rate = 0.0

    _validate_rate_type_consistency(new_rate, new_interest_type)
    return new_rate, new_interest_type


def _optional_transaction_id(args) -> str | None:
    return _clean_text(getattr(args, "transaction_id", None) or getattr(args, "transaction", None))


def _parse_event_details(value: Any) -> Any:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _loan_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["balance"] = cents_to_dollars(int(item["current_balance_cents"]))
    item["total_disbursed"] = cents_to_dollars(int(item["total_disbursed_cents"]))
    monthly_payment = item.get("monthly_payment_cents")
    item["monthly_payment"] = cents_to_dollars(int(monthly_payment)) if monthly_payment is not None else None
    item["is_active"] = bool(int(item.get("is_active", 0)))
    return item


def _disbursement_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["amount"] = cents_to_dollars(int(item["amount_cents"]))
    return item


def _payment_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["amount"] = cents_to_dollars(int(item["amount_cents"]))
    return item


def _event_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["details"] = _parse_event_details(item.get("details"))
    return item


def _fetch_loan_row(conn: sqlite3.Connection, loan_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT *
          FROM manual_loans
         WHERE id = ?
         LIMIT 1
        """,
        (loan_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError("loan not found")
    return row


def _fetch_active_loan_row(conn: sqlite3.Connection, loan_id: str) -> sqlite3.Row:
    row = _fetch_loan_row(conn, loan_id)
    if int(row["is_active"] or 0) != 1:
        raise ValidationError("loan must be active")
    return row


def _fetch_transaction_row(conn: sqlite3.Connection, transaction_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT id, date, description, amount_cents, is_active
          FROM transactions
         WHERE id = ?
         LIMIT 1
        """,
        (transaction_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError("transaction not found")
    return row


def _insert_event(
    conn: sqlite3.Connection,
    loan_id: str,
    event_type: str,
    details: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO loan_events (id, loan_id, event_type, details)
        VALUES (?, ?, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            loan_id,
            event_type,
            json.dumps(details, sort_keys=True) if details is not None else None,
        ),
    )


def _fetch_loan_snapshot(conn: sqlite3.Connection, loan_id: str) -> dict[str, Any]:
    return _loan_row_to_dict(_fetch_loan_row(conn, loan_id))


def _loan_preview(row: sqlite3.Row | dict[str, Any], **updates: Any) -> dict[str, Any]:
    preview = dict(row)
    preview.update(updates)
    return _loan_row_to_dict(preview)


def _build_list_cli_report(loans: list[dict[str, Any]], total_balance_cents: int) -> str:
    if not loans:
        return "No manual loans"

    lines = [
        f"{'Creditor':<24} {'Balance':>12} {'Disbursed':>12} {'Rate':>8} {'Monthly':>12} {'Active':>8}",
        "-" * 82,
    ]
    for loan in loans:
        monthly_payment = loan.get("monthly_payment")
        monthly_text = fmt_dollars(float(monthly_payment)) if monthly_payment is not None else "Flexible"
        lines.append(
            f"{str(loan['creditor_name'])[:24]:<24} "
            f"{fmt_dollars(float(loan['balance'])):>12} "
            f"{fmt_dollars(float(loan['total_disbursed'])):>12} "
            f"{float(loan['interest_rate_pct']):>7.2f}% "
            f"{monthly_text:>12} "
            f"{('yes' if loan['is_active'] else 'no'):>8}"
        )
    lines.append("-" * 82)
    lines.append(f"{'Total Outstanding':<24} {fmt_dollars(cents_to_dollars(total_balance_cents)):>12}")
    return "\n".join(lines)


def _build_show_cli_report(
    loan: dict[str, Any],
    disbursements: list[dict[str, Any]],
    payments: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> str:
    lines = [
        f"Loan {loan['id']}",
        f"Creditor: {loan['creditor_name']}",
        f"Balance: {fmt_dollars(float(loan['balance']))}",
        f"Total Disbursed: {fmt_dollars(float(loan['total_disbursed']))}",
        f"Rate: {float(loan['interest_rate_pct']):.2f}% ({loan['interest_type']})",
        f"Monthly Payment: {fmt_dollars(float(loan['monthly_payment'])) if loan['monthly_payment'] is not None else 'Flexible'}",
        f"Due Day: {loan['payment_due_day'] if loan['payment_due_day'] is not None else '—'}",
        f"Active: {'yes' if loan['is_active'] else 'no'}",
        "",
        f"Disbursements: {len(disbursements)}",
    ]
    for item in disbursements:
        lines.append(f"  {item['disbursement_date']} {fmt_dollars(float(item['amount']))} {item.get('notes') or ''}".rstrip())
    lines.append("")
    lines.append(f"Payments: {len(payments)}")
    for item in payments:
        suffix = f" txn={item['transaction_id']}" if item.get("transaction_id") else ""
        lines.append(f"  {item['payment_date']} {fmt_dollars(float(item['amount']))}{suffix}")
    lines.append("")
    lines.append(f"Events: {len(events)}")
    for item in events:
        lines.append(f"  {item['created_at']} {item['event_type']}")
    return "\n".join(lines)


def _monthly_interest_cents(balance_cents: int, rate_pct: float) -> int:
    if int(balance_cents) <= 0 or float(rate_pct) <= 0:
        return 0
    digits = len(str(abs(int(balance_cents)))) if balance_cents else 1
    with localcontext() as ctx:
        ctx.prec = max(28, digits + 16)
        return int(
            (
                Decimal(balance_cents)
                * Decimal(str(rate_pct))
                / Decimal("100")
                / Decimal("12")
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )


def _add_months(base_date: date, months: int, preferred_day: int | None = None) -> date:
    total_months = (base_date.year * 12) + (base_date.month - 1) + int(months)
    year = total_months // 12
    month = (total_months % 12) + 1
    day = preferred_day or base_date.day
    return date(year, month, min(int(day), calendar.monthrange(year, month)[1]))


def _first_payment_date(anchor_date: date, due_day: int | None) -> date:
    preferred_day = int(due_day or anchor_date.day)
    current_month_due = date(
        anchor_date.year,
        anchor_date.month,
        min(preferred_day, calendar.monthrange(anchor_date.year, anchor_date.month)[1]),
    )
    if current_month_due >= anchor_date:
        return current_month_due
    return _add_months(current_month_due, 1, preferred_day)


def _display_schedule_rows(
    schedule: list[dict[str, Any]],
    *,
    summary_only: bool,
) -> list[dict[str, Any] | str]:
    if not summary_only or len(schedule) <= 12:
        return list(schedule)
    return list(schedule[:6]) + ["..."] + list(schedule[-6:])


def _build_schedule_cli_report(
    loan_id: str,
    schedule: list[dict[str, Any] | str],
    schedule_summary: dict[str, Any],
    warnings: list[str],
    message: str | None = None,
) -> str:
    if message:
        return message

    lines = [
        f"Loan Schedule {loan_id}",
        f"{'Month':>5} {'Payment':>12} {'Principal':>12} {'Interest':>12} {'Balance':>12}",
        "-" * 61,
    ]
    for row in schedule:
        if row == "...":
            lines.append(f"{'...':>5} {'...':>12} {'...':>12} {'...':>12} {'...':>12}")
            continue
        lines.append(
            f"{int(row['month']):>5} "
            f"{fmt_dollars(cents_to_dollars(int(row['payment_cents']))):>12} "
            f"{fmt_dollars(cents_to_dollars(int(row['principal_cents']))):>12} "
            f"{fmt_dollars(cents_to_dollars(int(row['interest_cents']))):>12} "
            f"{fmt_dollars(cents_to_dollars(int(row['remaining_balance_cents']))):>12}"
        )

    lines.append("-" * 61)
    lines.append(
        f"Total Payments: {fmt_dollars(cents_to_dollars(int(schedule_summary['total_payments_cents'])))}"
    )
    lines.append(
        f"Total Principal: {fmt_dollars(cents_to_dollars(int(schedule_summary['total_principal_cents'])))}"
    )
    lines.append(
        f"Total Interest: {fmt_dollars(cents_to_dollars(int(schedule_summary['total_interest_cents'])))}"
    )
    lines.append(f"Months Projected: {int(schedule_summary['months_to_payoff'])}")
    if schedule_summary.get("payoff_date"):
        lines.append(f"Estimated Payoff Date: {schedule_summary['payoff_date']}")
    if int(schedule_summary.get("balance_remaining_cents", 0)) > 0:
        lines.append(
            "Balance Remaining: "
            f"{fmt_dollars(cents_to_dollars(int(schedule_summary['balance_remaining_cents'])))}"
        )

    if warnings:
        lines.append("")
        lines.extend(warnings)

    return "\n".join(lines)


def _project_schedule(
    loan_row: sqlite3.Row,
    *,
    months: int,
    summary_only: bool,
) -> tuple[list[dict[str, Any] | str], dict[str, Any], list[str]]:
    monthly_payment_cents = int(loan_row["monthly_payment_cents"])
    interest_type = str(loan_row["interest_type"])
    rate_pct = float(loan_row["interest_rate_pct"])
    current_balance_cents = int(loan_row["current_balance_cents"])
    original_principal_cents = current_balance_cents

    warnings: list[str] = []
    full_schedule: list[dict[str, Any]] = []
    total_payments_cents = 0
    total_interest_cents = 0
    total_principal_cents = 0

    auto_months = int(months) == 0
    horizon = 120 if auto_months else int(months)

    first_month_interest_cents = 0
    if interest_type == "simple":
        first_month_interest_cents = _monthly_interest_cents(original_principal_cents, rate_pct)
    elif interest_type == "compound":
        first_month_interest_cents = _monthly_interest_cents(current_balance_cents, rate_pct)
    if interest_type in {"simple", "compound"} and monthly_payment_cents <= first_month_interest_cents:
        warnings.append(
            "Warning: monthly payment "
            f"({fmt_dollars(cents_to_dollars(monthly_payment_cents))}) is less than monthly interest "
            f"({fmt_dollars(cents_to_dollars(first_month_interest_cents))}). Balance will grow."
        )

    remaining_balance_cents = current_balance_cents
    fully_paid_off = False

    for month in range(1, horizon + 1):
        if remaining_balance_cents <= 0:
            fully_paid_off = True
            break

        if interest_type == "none":
            interest_cents = 0
            payment_cents = min(monthly_payment_cents, remaining_balance_cents)
            principal_cents = payment_cents
        elif interest_type == "simple":
            interest_cents = _monthly_interest_cents(original_principal_cents, rate_pct)
            payment_cents = min(monthly_payment_cents, remaining_balance_cents + interest_cents)
            principal_cents = payment_cents - interest_cents
        elif interest_type == "compound":
            interest_cents = _monthly_interest_cents(remaining_balance_cents, rate_pct)
            payment_cents = min(monthly_payment_cents, remaining_balance_cents + interest_cents)
            principal_cents = payment_cents - interest_cents
        else:
            raise ValidationError(f"unsupported interest_type: {interest_type}")

        remaining_balance_cents = max(0, remaining_balance_cents - principal_cents)
        total_payments_cents += payment_cents
        total_interest_cents += interest_cents
        total_principal_cents += principal_cents

        full_schedule.append(
            {
                "month": month,
                "payment_cents": payment_cents,
                "principal_cents": principal_cents,
                "interest_cents": interest_cents,
                "remaining_balance_cents": remaining_balance_cents,
            }
        )

        if remaining_balance_cents <= 0:
            fully_paid_off = True
            break

    if auto_months and not fully_paid_off and remaining_balance_cents > 0:
        warnings.append(
            "Projection capped at 120 months; balance remaining: "
            f"{fmt_dollars(cents_to_dollars(remaining_balance_cents))}"
        )

    start_date_value = date.fromisoformat(str(loan_row["start_date"]))
    anchor_date = max(start_date_value, date.fromisoformat(today_iso()))
    due_day = loan_row["payment_due_day"]
    payoff_date = None
    if full_schedule and fully_paid_off:
        first_payment_date = _first_payment_date(anchor_date, int(due_day) if due_day is not None else None)
        payoff_date = _add_months(
            first_payment_date,
            len(full_schedule) - 1,
            int(due_day) if due_day is not None else first_payment_date.day,
        ).isoformat()

    schedule_summary = {
        "total_payments_cents": total_payments_cents,
        "total_interest_cents": total_interest_cents,
        "total_principal_cents": total_principal_cents,
        "months_to_payoff": len(full_schedule),
        "payoff_date": payoff_date,
        "fully_paid_off": fully_paid_off,
        "balance_remaining_cents": remaining_balance_cents,
    }

    return _display_schedule_rows(full_schedule, summary_only=summary_only), schedule_summary, warnings


def handle_add(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    dry_run = bool(getattr(args, "dry_run", False))
    idempotency_key = getattr(args, "idempotency_key", None)
    creditor = _clean_text(getattr(args, "creditor", None))
    if not creditor:
        raise ValidationError("creditor is required")

    amount_cents = _require_positive_dollars(getattr(args, "amount", None), "amount")
    start_date = _require_iso_date(getattr(args, "start_date", None), "start_date")
    expected_payoff = _optional_iso_date(getattr(args, "expected_payoff", None), "expected_payoff")
    if expected_payoff and expected_payoff < start_date:
        raise ValidationError("expected_payoff must be on or after start_date")

    rate = _validate_rate(getattr(args, "rate", 0.0))
    interest_type = _resolve_new_interest(rate, getattr(args, "interest_type", None))
    monthly_payment_cents = _optional_positive_dollars(getattr(args, "monthly_payment", None), "monthly_payment")
    due_day = _validate_due_day(getattr(args, "due_day", None))
    use_type = _validate_use_type(getattr(args, "use_type", None))
    description = _clean_text(getattr(args, "description", None))

    loan_id = uuid.uuid4().hex
    disbursement_id = uuid.uuid4().hex

    try:
        conn.execute(
            """
            INSERT INTO manual_loans (
                id,
                creditor_name,
                description,
                total_disbursed_cents,
                current_balance_cents,
                interest_rate_pct,
                interest_type,
                monthly_payment_cents,
                payment_due_day,
                start_date,
                expected_payoff_date,
                use_type,
                idempotency_key,
                is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                loan_id,
                creditor,
                description,
                amount_cents,
                amount_cents,
                rate,
                interest_type,
                monthly_payment_cents,
                due_day,
                start_date,
                expected_payoff,
                use_type,
                idempotency_key,
            ),
        )
        conn.execute(
            """
            INSERT INTO loan_disbursements (id, loan_id, amount_cents, disbursement_date, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (disbursement_id, loan_id, amount_cents, start_date, description),
        )
        disbursement = {
            "id": disbursement_id,
            "loan_id": loan_id,
            "amount_cents": amount_cents,
            "amount": cents_to_dollars(amount_cents),
            "disbursement_date": start_date,
            "notes": description,
        }
        if dry_run:
            loan = _loan_preview(
                {
                    "id": loan_id,
                    "creditor_name": creditor,
                    "description": description,
                    "total_disbursed_cents": amount_cents,
                    "current_balance_cents": amount_cents,
                    "interest_rate_pct": rate,
                    "interest_type": interest_type,
                    "monthly_payment_cents": monthly_payment_cents,
                    "payment_due_day": due_day,
                    "start_date": start_date,
                    "expected_payoff_date": expected_payoff,
                    "use_type": use_type,
                    "is_active": 1,
                }
            )
            result = {
                "data": {
                    "loan": loan,
                    "disbursement": disbursement,
                    "dry_run": True,
                },
                "summary": {
                    "loan_id": loan_id,
                    "is_active": True,
                },
                "cli_report": (
                    f"[DRY RUN] Would add loan {loan_id} from {creditor} "
                    f"for {fmt_dollars(cents_to_dollars(amount_cents))}"
                ),
            }
            conn.rollback()
            return result
        conn.commit()
    except sqlite3.IntegrityError as exc:
        if idempotency_key and "idempotency_key" in str(exc):
            conn.rollback()
            existing = conn.execute(
                "SELECT id FROM manual_loans WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is None:
                raise
            loan = _fetch_loan_snapshot(conn, existing["id"])
            first_disbursement = conn.execute(
                """
                SELECT *
                  FROM loan_disbursements
                 WHERE loan_id = ?
                 ORDER BY disbursement_date ASC, rowid ASC
                 LIMIT 1
                """,
                (existing["id"],),
            ).fetchone()
            data: dict[str, Any] = {
                "loan": loan,
                "already_existed": True,
                **({"dry_run": True} if dry_run else {}),
            }
            if first_disbursement is not None:
                data["disbursement"] = _disbursement_row_to_dict(first_disbursement)
            return {
                "data": data,
                "summary": {
                    "loan_id": existing["id"],
                    "is_active": bool(loan["is_active"]),
                },
                "cli_report": f"Loan {existing['id']} already exists (idempotent retry)",
            }
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise

    loan = _fetch_loan_snapshot(conn, loan_id)
    return {
        "data": {
            "loan": loan,
            "disbursement": disbursement,
        },
        "summary": {
            "loan_id": loan_id,
            "is_active": True,
        },
        "cli_report": (
            f"Added loan {loan_id} from {creditor} "
            f"for {fmt_dollars(cents_to_dollars(amount_cents))}"
        ),
    }


def handle_list(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    include_inactive = bool(getattr(args, "include_inactive", False))
    limit = int(getattr(args, "limit", 100))
    offset = int(getattr(args, "offset", 0))
    if limit < 1:
        raise ValidationError("limit must be >= 1")
    if offset < 0:
        raise ValidationError("offset must be >= 0")

    where_sql = ""
    params: tuple[Any, ...] = ()
    if not include_inactive:
        where_sql = "WHERE is_active = 1"

    total_count = int(
        conn.execute(
            f"""
            SELECT COUNT(*) AS total_count
              FROM manual_loans
              {where_sql}
            """,
            params,
        ).fetchone()["total_count"]
        or 0
    )
    total_balance_cents = int(
        conn.execute(
            f"""
            SELECT COALESCE(SUM(current_balance_cents), 0) AS total_balance_cents
              FROM manual_loans
              {where_sql}
            """,
            params,
        ).fetchone()["total_balance_cents"]
        or 0
    )
    active_count = total_count if not include_inactive else int(
        conn.execute(
            """
            SELECT COUNT(*) AS active_count
              FROM manual_loans
             WHERE is_active = 1
            """
        ).fetchone()["active_count"]
        or 0
    )

    rows = conn.execute(
        f"""
        SELECT *
          FROM manual_loans
          {where_sql}
         ORDER BY is_active DESC, updated_at DESC, creditor_name ASC
         LIMIT ? OFFSET ?
        """,
        (*params, limit, offset),
    ).fetchall()
    loans = [_loan_row_to_dict(row) for row in rows]

    return {
        "data": {
            "loans": loans,
            "total_balance_cents": total_balance_cents,
            "total_balance": cents_to_dollars(total_balance_cents),
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
        },
        "summary": {
            "total_loans": total_count,
            "active_loans": active_count,
        },
        "cli_report": _build_list_cli_report(loans, total_balance_cents),
    }


def handle_show(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    loan_id = _require_loan_id(args)
    loan = _fetch_loan_snapshot(conn, loan_id)

    disbursements = [
        _disbursement_row_to_dict(row)
        for row in conn.execute(
            """
            SELECT *
              FROM loan_disbursements
             WHERE loan_id = ?
             ORDER BY disbursement_date ASC, rowid ASC
            """,
            (loan_id,),
        ).fetchall()
    ]
    payments = [
        _payment_row_to_dict(row)
        for row in conn.execute(
            """
            SELECT *
              FROM loan_payments
             WHERE loan_id = ?
             ORDER BY payment_date ASC, rowid ASC
            """,
            (loan_id,),
        ).fetchall()
    ]
    events = [
        _event_row_to_dict(row)
        for row in conn.execute(
            """
            SELECT *
              FROM loan_events
             WHERE loan_id = ?
             ORDER BY rowid ASC
            """,
            (loan_id,),
        ).fetchall()
    ]

    return {
        "data": {
            "loan": loan,
            "disbursements": disbursements,
            "payments": payments,
            "events": events,
        },
        "summary": {
            "loan_id": loan_id,
            "payment_count": len(payments),
            "disbursement_count": len(disbursements),
            "event_count": len(events),
        },
        "cli_report": _build_show_cli_report(loan, disbursements, payments, events),
    }


def handle_payment(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    dry_run = bool(getattr(args, "dry_run", False))
    loan_id = _require_loan_id(args)
    requested_amount_cents = _require_positive_dollars(getattr(args, "amount", None), "amount")
    transaction_id = _optional_transaction_id(args)
    notes = _clean_text(getattr(args, "notes", None))

    payment_date = _clean_text(getattr(args, "date", None))
    if transaction_id:
        txn = _fetch_transaction_row(conn, transaction_id)
        payment_date = payment_date or str(txn["date"])
    payment_date = _require_iso_date(payment_date or today_iso(), "date")

    payment_id = uuid.uuid4().hex

    conn.execute("BEGIN IMMEDIATE")
    try:
        loan_before = _fetch_active_loan_row(conn, loan_id)
        current_balance_cents = int(loan_before["current_balance_cents"])
        if current_balance_cents <= 0:
            raise ValidationError("loan balance must be greater than 0")

        try:
            cursor = conn.execute(
                """
                INSERT INTO loan_payments (id, loan_id, amount_cents, payment_date, transaction_id, notes)
                SELECT ?, id, MIN(?, current_balance_cents), ?, ?, ?
                  FROM manual_loans
                 WHERE id = ? AND is_active = 1
                """,
                (payment_id, requested_amount_cents, payment_date, transaction_id, notes, loan_id),
            )
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if "loan_payments.transaction_id" in message:
                raise ConflictError("transaction is already linked to a loan payment") from exc
            raise
        if int(cursor.rowcount or 0) != 1:
            raise ValidationError("loan must be active")

        conn.execute(
            """
            UPDATE manual_loans
               SET current_balance_cents = MAX(current_balance_cents - ?, 0),
                   is_active = CASE
                       WHEN MAX(current_balance_cents - ?, 0) = 0 THEN 0
                       ELSE is_active
                   END,
                   updated_at = datetime('now')
             WHERE id = ?
               AND is_active = 1
            """,
            (requested_amount_cents, requested_amount_cents, loan_id),
        )

        payment_row = conn.execute(
            """
            SELECT *
              FROM loan_payments
             WHERE id = ?
             LIMIT 1
            """,
            (payment_id,),
        ).fetchone()
        if payment_row is None:
            raise ValidationError("failed to record payment")

        loan_after = _fetch_loan_row(conn, loan_id)
        if int(loan_after["current_balance_cents"]) == 0 and int(loan_before["is_active"]) == 1:
            _insert_event(conn, loan_id, "close", {"reason": "paid_off", "payment_id": payment_id})

        payment = _payment_row_to_dict(payment_row)
        loan = _loan_row_to_dict(loan_after)
        result = {
            "data": {
                "payment": payment,
                "loan": loan,
                "requested_amount_cents": requested_amount_cents,
                "requested_amount": cents_to_dollars(requested_amount_cents),
                **({"dry_run": True} if dry_run else {}),
            },
            "summary": {
                "loan_id": loan_id,
                "is_active": loan["is_active"],
            },
            "cli_report": (
                f"{'[DRY RUN] Would record' if dry_run else 'Recorded'} payment "
                f"{fmt_dollars(float(payment['amount']))} on loan {loan_id}; "
                f"balance={fmt_dollars(float(loan['balance']))}"
            ),
        }
        if dry_run:
            conn.rollback()
            return result
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return result


def handle_disburse(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    dry_run = bool(getattr(args, "dry_run", False))
    loan_id = _require_loan_id(args)
    amount_cents = _require_positive_dollars(getattr(args, "amount", None), "amount")
    disbursement_date = _require_iso_date(getattr(args, "date", None) or today_iso(), "date")
    notes = _clean_text(getattr(args, "notes", None))
    disbursement_id = uuid.uuid4().hex

    conn.execute("BEGIN IMMEDIATE")
    try:
        loan_before = _fetch_loan_row(conn, loan_id)
        conn.execute(
            """
            INSERT INTO loan_disbursements (id, loan_id, amount_cents, disbursement_date, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (disbursement_id, loan_id, amount_cents, disbursement_date, notes),
        )
        conn.execute(
            """
            UPDATE manual_loans
               SET current_balance_cents = current_balance_cents + ?,
                   total_disbursed_cents = total_disbursed_cents + ?,
                   is_active = 1,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (amount_cents, amount_cents, loan_id),
        )
        if int(loan_before["is_active"] or 0) != 1:
            _insert_event(
                conn,
                loan_id,
                "reopen",
                {"reason": "disbursement", "disbursement_id": disbursement_id},
            )
        if dry_run:
            loan = _loan_preview(
                loan_before,
                current_balance_cents=int(loan_before["current_balance_cents"]) + amount_cents,
                total_disbursed_cents=int(loan_before["total_disbursed_cents"]) + amount_cents,
                is_active=1,
            )
            result = {
                "data": {
                    "disbursement": {
                        "id": disbursement_id,
                        "loan_id": loan_id,
                        "amount_cents": amount_cents,
                        "amount": cents_to_dollars(amount_cents),
                        "disbursement_date": disbursement_date,
                        "notes": notes,
                    },
                    "loan": loan,
                    "dry_run": True,
                },
                "summary": {
                    "loan_id": loan_id,
                    "is_active": loan["is_active"],
                },
                "cli_report": (
                    f"[DRY RUN] Would record disbursement {fmt_dollars(cents_to_dollars(amount_cents))} "
                    f"on loan {loan_id}; balance={fmt_dollars(float(loan['balance']))}"
                ),
            }
            conn.rollback()
            return result
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    loan = _fetch_loan_snapshot(conn, loan_id)
    return {
        "data": {
            "disbursement": {
                "id": disbursement_id,
                "loan_id": loan_id,
                "amount_cents": amount_cents,
                "amount": cents_to_dollars(amount_cents),
                "disbursement_date": disbursement_date,
                "notes": notes,
            },
            "loan": loan,
        },
        "summary": {
            "loan_id": loan_id,
            "is_active": loan["is_active"],
        },
        "cli_report": (
            f"Recorded disbursement {fmt_dollars(cents_to_dollars(amount_cents))} on loan {loan_id}; "
            f"balance={fmt_dollars(float(loan['balance']))}"
        ),
    }


def handle_adjust(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    dry_run = bool(getattr(args, "dry_run", False))
    loan_id = _require_loan_id(args)
    loan_before = _fetch_loan_row(conn, loan_id)

    changed_before: dict[str, Any] = {}
    changed_after: dict[str, Any] = {}
    update_fields: list[tuple[str, Any]] = []

    has_adjustment = any(
        getattr(args, name, None) is not None
        for name in (
            "rate",
            "interest_type",
            "monthly_payment",
            "due_day",
            "expected_payoff",
            "balance",
            "description",
        )
    )
    if not has_adjustment:
        raise ValidationError("at least one adjustment is required")

    if getattr(args, "rate", None) is not None or getattr(args, "interest_type", None) is not None:
        new_rate, new_interest_type = _resolve_adjusted_interest(loan_before, args)
        if float(loan_before["interest_rate_pct"]) != new_rate:
            changed_before["interest_rate_pct"] = float(loan_before["interest_rate_pct"])
            changed_after["interest_rate_pct"] = new_rate
            update_fields.append(("interest_rate_pct = ?", new_rate))
        if str(loan_before["interest_type"]) != new_interest_type:
            changed_before["interest_type"] = str(loan_before["interest_type"])
            changed_after["interest_type"] = new_interest_type
            update_fields.append(("interest_type = ?", new_interest_type))

    if getattr(args, "monthly_payment", None) is not None:
        monthly_payment_cents = _optional_positive_dollars(args.monthly_payment, "monthly_payment")
        if loan_before["monthly_payment_cents"] != monthly_payment_cents:
            changed_before["monthly_payment_cents"] = loan_before["monthly_payment_cents"]
            changed_after["monthly_payment_cents"] = monthly_payment_cents
            update_fields.append(("monthly_payment_cents = ?", monthly_payment_cents))

    if getattr(args, "due_day", None) is not None:
        due_day = _validate_due_day(args.due_day)
        if loan_before["payment_due_day"] != due_day:
            changed_before["payment_due_day"] = loan_before["payment_due_day"]
            changed_after["payment_due_day"] = due_day
            update_fields.append(("payment_due_day = ?", due_day))

    if getattr(args, "expected_payoff", None) is not None:
        expected_payoff = _optional_iso_date(args.expected_payoff, "expected_payoff")
        if expected_payoff and expected_payoff < str(loan_before["start_date"]):
            raise ValidationError("expected_payoff must be on or after start_date")
        if loan_before["expected_payoff_date"] != expected_payoff:
            changed_before["expected_payoff_date"] = loan_before["expected_payoff_date"]
            changed_after["expected_payoff_date"] = expected_payoff
            update_fields.append(("expected_payoff_date = ?", expected_payoff))

    if getattr(args, "balance", None) is not None:
        balance_cents = _optional_nonnegative_dollars(args.balance, "balance")
        if loan_before["current_balance_cents"] != balance_cents:
            changed_before["current_balance_cents"] = int(loan_before["current_balance_cents"])
            changed_after["current_balance_cents"] = balance_cents
            update_fields.append(("current_balance_cents = ?", balance_cents))

    if getattr(args, "description", None) is not None:
        description = _clean_text(args.description)
        if loan_before["description"] != description:
            changed_before["description"] = loan_before["description"]
            changed_after["description"] = description
            update_fields.append(("description = ?", description))

    new_is_active = int(loan_before["is_active"] or 0)
    if getattr(args, "balance", None) is not None:
        balance_cents = _optional_nonnegative_dollars(args.balance, "balance")
        if balance_cents == 0:
            new_is_active = 0
        elif balance_cents > 0:
            new_is_active = 1
    if int(loan_before["is_active"] or 0) != new_is_active:
        changed_before["is_active"] = bool(int(loan_before["is_active"] or 0))
        changed_after["is_active"] = bool(new_is_active)
        update_fields.append(("is_active = ?", new_is_active))

    if not update_fields:
        loan = _loan_row_to_dict(loan_before)
        return {
            "data": {"loan": loan, "changed_fields": [], **({"dry_run": True} if dry_run else {})},
            "summary": {"loan_id": loan_id, "changed_fields": 0},
            "cli_report": (
                f"[DRY RUN] No changes would be applied to loan {loan_id}"
                if dry_run
                else f"No changes applied to loan {loan_id}"
            ),
        }

    assignments = ", ".join(field for field, _ in update_fields)
    values = [value for _, value in update_fields]
    values.append(loan_id)

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            f"""
            UPDATE manual_loans
               SET {assignments},
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            tuple(values),
        )
        _insert_event(
            conn,
            loan_id,
            "adjust",
            {"before": changed_before, "after": changed_after},
        )
        if int(loan_before["is_active"] or 0) == 0 and new_is_active == 1:
            _insert_event(conn, loan_id, "reopen", {"reason": "balance_adjustment"})
        if int(loan_before["is_active"] or 0) == 1 and new_is_active == 0:
            _insert_event(conn, loan_id, "close", {"reason": "balance_adjustment"})
        if dry_run:
            loan = _loan_preview(loan_before, **changed_after)
            result = {
                "data": {
                    "loan": loan,
                    "changed_fields": sorted(changed_after.keys()),
                    "event": {"before": changed_before, "after": changed_after},
                    "dry_run": True,
                },
                "summary": {
                    "loan_id": loan_id,
                    "changed_fields": len(changed_after),
                    "is_active": loan["is_active"],
                },
                "cli_report": f"[DRY RUN] Would adjust loan {loan_id}",
            }
            conn.rollback()
            return result
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    loan = _fetch_loan_snapshot(conn, loan_id)
    return {
        "data": {
            "loan": loan,
            "changed_fields": sorted(changed_after.keys()),
            "event": {"before": changed_before, "after": changed_after},
        },
        "summary": {
            "loan_id": loan_id,
            "changed_fields": len(changed_after),
            "is_active": loan["is_active"],
        },
        "cli_report": f"Adjusted loan {loan_id}",
    }


def handle_close(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    dry_run = bool(getattr(args, "dry_run", False))
    loan_id = _require_loan_id(args)
    forgiven = bool(getattr(args, "forgiven", False))

    conn.execute("BEGIN IMMEDIATE")
    try:
        loan_before = _fetch_active_loan_row(conn, loan_id)
        prior_balance_cents = int(loan_before["current_balance_cents"])
        if not forgiven and prior_balance_cents != 0:
            raise ValidationError(
                "loan balance must be 0 to close "
                f"(remaining {fmt_dollars(cents_to_dollars(prior_balance_cents))}); use --forgiven to close it"
            )

        if forgiven:
            conn.execute(
                """
                UPDATE manual_loans
                   SET current_balance_cents = 0,
                       is_active = 0,
                       updated_at = datetime('now')
                 WHERE id = ?
                """,
                (loan_id,),
            )
            _insert_event(conn, loan_id, "forgive", {"forgiven_amount_cents": prior_balance_cents})
        else:
            conn.execute(
                """
                UPDATE manual_loans
                   SET is_active = 0,
                       updated_at = datetime('now')
                 WHERE id = ?
                """,
                (loan_id,),
            )

        _insert_event(
            conn,
            loan_id,
            "close",
            {"reason": "manual_close", "forgiven": forgiven},
        )
        if dry_run:
            loan = _loan_preview(
                loan_before,
                current_balance_cents=0 if forgiven else prior_balance_cents,
                is_active=0,
            )
            result = {
                "data": {
                    "loan": loan,
                    "forgiven": forgiven,
                    "forgiven_amount_cents": prior_balance_cents if forgiven else 0,
                    "forgiven_amount": cents_to_dollars(prior_balance_cents) if forgiven else 0.0,
                    "dry_run": True,
                },
                "summary": {
                    "loan_id": loan_id,
                    "is_active": False,
                    "forgiven": forgiven,
                },
                "cli_report": f"[DRY RUN] Would close loan {loan_id}",
            }
            conn.rollback()
            return result
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    loan = _fetch_loan_snapshot(conn, loan_id)
    return {
        "data": {
            "loan": loan,
            "forgiven": forgiven,
            "forgiven_amount_cents": prior_balance_cents if forgiven else 0,
            "forgiven_amount": cents_to_dollars(prior_balance_cents) if forgiven else 0.0,
        },
        "summary": {
            "loan_id": loan_id,
            "is_active": False,
            "forgiven": forgiven,
        },
        "cli_report": f"Closed loan {loan_id}",
    }


def handle_schedule(args, conn: sqlite3.Connection, fmt: str | None = None) -> dict[str, Any]:
    _fmt(args, fmt)
    loan_id = _require_loan_id(args)
    months = int(getattr(args, "months", 0) or 0)
    if months < 0 or months > 120:
        raise ValidationError("months must be between 0 and 120")
    summary_only = bool(getattr(args, "summary_only", False))

    loan_row = _fetch_loan_row(conn, loan_id)
    current_balance_cents = int(loan_row["current_balance_cents"])
    monthly_payment_cents = loan_row["monthly_payment_cents"]

    if current_balance_cents <= 0:
        message = "Loan is fully paid off."
        schedule_summary = {
            "total_payments_cents": 0,
            "total_interest_cents": 0,
            "total_principal_cents": 0,
            "months_to_payoff": 0,
            "payoff_date": None,
            "fully_paid_off": True,
            "balance_remaining_cents": 0,
        }
        return {
            "data": {
                "loan_id": loan_id,
                "schedule": [],
                "summary": schedule_summary,
                "warnings": [],
                "message": message,
            },
            "summary": {
                "loan_id": loan_id,
                "months_to_payoff": 0,
                "fully_paid_off": True,
                "warning_count": 0,
            },
            "cli_report": message,
        }

    if monthly_payment_cents is None:
        message = "No fixed payment schedule. Record payments as they occur."
        schedule_summary = {
            "total_payments_cents": 0,
            "total_interest_cents": 0,
            "total_principal_cents": 0,
            "months_to_payoff": 0,
            "payoff_date": None,
            "fully_paid_off": False,
            "balance_remaining_cents": current_balance_cents,
        }
        return {
            "data": {
                "loan_id": loan_id,
                "schedule": [],
                "summary": schedule_summary,
                "warnings": [],
                "message": message,
            },
            "summary": {
                "loan_id": loan_id,
                "months_to_payoff": 0,
                "fully_paid_off": False,
                "warning_count": 0,
            },
            "cli_report": message,
        }

    schedule, schedule_summary, warnings = _project_schedule(
        loan_row,
        months=months,
        summary_only=summary_only,
    )
    return {
        "data": {
            "loan_id": loan_id,
            "schedule": schedule,
            "summary": schedule_summary,
            "warnings": warnings,
        },
        "summary": {
            "loan_id": loan_id,
            "months_to_payoff": int(schedule_summary["months_to_payoff"]),
            "fully_paid_off": bool(schedule_summary["fully_paid_off"]),
            "warning_count": len(warnings),
        },
        "cli_report": _build_schedule_cli_report(
            loan_id,
            schedule,
            schedule_summary,
            warnings,
        ),
    }
