"""Legacy per-bank PDF regex extractors.

Archived from pdf.py during CQ-006 cleanup (2026-02-20). These are NOT used in
production — all PDF imports go through ai_statement_parser.py. Retained here in
case the regex patterns are useful as a starting point for future work.

To use any of these, import the helpers they depend on from pdf.py:
    from .pdf import (
        ExtractResult, _extract_pdf_text, _finalize_result,
        _extract_statement_total_cents, _infer_statement_year,
        _normalize_credit_sign, _parse_amount_to_cents, _to_iso_date,
    )
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from .pdf import (
    ExtractResult,
    _extract_pdf_text,
    _extract_statement_total_cents,
    _finalize_result,
    _infer_statement_year,
    _normalize_credit_sign,
    _parse_amount_to_cents,
    _to_iso_date,
)


def _extract_chase_checking(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)
    year = _infer_statement_year(text)

    transactions: list[dict[str, object]] = []
    line_re = re.compile(r"^(\d{2}/\d{2})\s+(.+?)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)$")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        match = line_re.match(line)
        if not match:
            continue

        date_token, description, amount_token = match.groups()
        iso_date = _to_iso_date(date_token, year)
        if not iso_date:
            continue

        amount_cents = _parse_amount_to_cents(amount_token)
        if amount_cents == 0:
            continue

        if amount_cents > 0:
            lowered = description.lower()
            if any(token in lowered for token in ["to ", "transfer", "payment", "withdraw", "debit"]):
                amount_cents = -abs(amount_cents)

        transactions.append(
            {
                "date": iso_date,
                "description": description.strip(),
                "amount_cents": amount_cents,
                "card_ending": None,
                "source": "Chase Checking",
            }
        )

    return _finalize_result(transactions, _extract_statement_total_cents(text), [])


def _extract_bofa_checking(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)

    transactions: list[dict[str, object]] = []
    section: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        lower = line.lower()
        if "deposits and other additions" in lower:
            section = "deposit"
            continue
        if "withdrawals and other subtractions" in lower:
            section = "withdrawal"
            continue
        if lower.startswith("checks"):
            section = "check"
            continue
        if "service fees" in lower or "total checks" in lower:
            section = None
            continue

        match = re.match(r"^(\d{2}/\d{2}/\d{2})\s+(.+?)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)$", line)
        if match and section in {"deposit", "withdrawal"}:
            date_token, description, amount_token = match.groups()
            iso_date = _to_iso_date(date_token, datetime.utcnow().year)
            if not iso_date:
                continue
            amount_cents = _parse_amount_to_cents(amount_token)
            if section == "withdrawal":
                amount_cents = -abs(amount_cents)
            else:
                amount_cents = abs(amount_cents)
            transactions.append(
                {
                    "date": iso_date,
                    "description": description.strip(),
                    "amount_cents": amount_cents,
                    "card_ending": None,
                    "source": "BofA Checking",
                }
            )
            continue

        check_match = re.match(r"^(\d{2}/\d{2}/\d{2})\s+(\d+)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)$", line)
        if check_match and section == "check":
            date_token, check_num, amount_token = check_match.groups()
            iso_date = _to_iso_date(date_token, datetime.utcnow().year)
            if not iso_date:
                continue
            amount_cents = -abs(_parse_amount_to_cents(amount_token))
            transactions.append(
                {
                    "date": iso_date,
                    "description": f"Check #{check_num}",
                    "amount_cents": amount_cents,
                    "card_ending": None,
                    "source": "BofA Checking",
                }
            )

    return _finalize_result(transactions, _extract_statement_total_cents(text), [])


def _extract_bofa_credit(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)
    year = _infer_statement_year(text)

    transactions: list[dict[str, object]] = []
    in_txn_section = False

    line_re = re.compile(r"^(\d{2}/\d{2})\s+\d{2}/\d{2}\s+(.+?)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)$")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "transactions" in line.lower():
            in_txn_section = True
            continue
        if not in_txn_section:
            continue

        match = line_re.match(line)
        if not match:
            continue

        date_token, description, amount_token = match.groups()
        iso_date = _to_iso_date(date_token, year)
        if not iso_date:
            continue

        amount_cents = _normalize_credit_sign(_parse_amount_to_cents(amount_token), description)
        transactions.append(
            {
                "date": iso_date,
                "description": description.strip(),
                "amount_cents": amount_cents,
                "card_ending": None,
                "source": "BofA Credit",
            }
        )

    return _finalize_result(transactions, _extract_statement_total_cents(text), [])


def _extract_schwab_checking(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)
    year = _infer_statement_year(text)

    transactions: list[dict[str, object]] = []
    line_re = re.compile(r"^(\d{2}/\d{2})\s+(.+?)\s+\$([\d,]+\.\d{2})\s+\$([\d,]+\.\d{2})$")

    lines = [line.strip() for line in text.splitlines()]
    i = 0
    while i < len(lines):
        line = lines[i]
        match = line_re.match(line)
        if not match:
            i += 1
            continue

        date_token, description, amount_token, _balance_token = match.groups()
        iso_date = _to_iso_date(date_token, year)
        if not iso_date:
            i += 1
            continue

        amount_cents = _parse_amount_to_cents(amount_token)
        if any(token in description.lower() for token in ["interest", "credit", "funds transfer", "deposit"]):
            amount_cents = abs(amount_cents)
        else:
            amount_cents = -abs(amount_cents)

        if i + 1 < len(lines) and not re.match(r"^\d{2}/\d{2}", lines[i + 1]):
            description = f"{description} {lines[i + 1].strip()}"
            i += 1

        transactions.append(
            {
                "date": iso_date,
                "description": description.strip(),
                "amount_cents": amount_cents,
                "card_ending": None,
                "source": "Schwab Checking",
            }
        )
        i += 1

    return _finalize_result(transactions, _extract_statement_total_cents(text), [])


def _extract_barclays(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)

    transactions: list[dict[str, object]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("DDAATTEE") or "Card ending" in line:
            continue

        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue

        body, amount_token = parts
        body_tokens = body.split(" ", 3)
        if len(body_tokens) < 4:
            continue

        date_token = " ".join(body_tokens[:3])
        description = body_tokens[3].strip()

        try:
            iso_date = datetime.strptime(date_token, "%b %d, %Y").date().isoformat()
        except ValueError:
            continue

        amount_cents = _normalize_credit_sign(_parse_amount_to_cents(amount_token), description)
        transactions.append(
            {
                "date": iso_date,
                "description": description,
                "amount_cents": amount_cents,
                "card_ending": None,
                "source": "Barclays",
            }
        )

    return _finalize_result(transactions, _extract_statement_total_cents(text), [])


def _extract_citi(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)
    year = _infer_statement_year(text)

    transactions: list[dict[str, object]] = []
    # Common statement rows: MM/DD Description Debit Credit
    line_re = re.compile(
        r"^(\d{2}/\d{2})(?:/\d{2,4})?\s+(.+?)\s+(-?\$?[\d,]*\d?\.\d{2}|-)\s+(-?\$?[\d,]*\d?\.\d{2}|-)$"
    )

    for raw_line in text.splitlines():
        line = raw_line.strip()
        match = line_re.match(line)
        if not match:
            continue

        date_token, description, debit_token, credit_token = match.groups()
        iso_date = _to_iso_date(date_token, year)
        if not iso_date:
            continue

        debit_cents = 0 if debit_token == "-" else abs(_parse_amount_to_cents(debit_token))
        credit_cents = 0 if credit_token == "-" else abs(_parse_amount_to_cents(credit_token))
        amount_cents = -debit_cents + credit_cents
        if amount_cents == 0:
            continue

        transactions.append(
            {
                "date": iso_date,
                "description": description.strip(),
                "amount_cents": amount_cents,
                "card_ending": None,
                "source": "Citi",
            }
        )

    return _finalize_result(transactions, _extract_statement_total_cents(text), [])
