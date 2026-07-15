"""Deterministic starter setup proposals for onboarding."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class BudgetProposal:
    id: str
    category: str
    amount: float
    amount_cents: int
    period: str
    view: str
    historical_monthly_average_cents: int
    percent_under_average: int
    months_considered: int
    rationale: str
    tool_name: str = "budget_set"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tool_input"] = {
            "category": self.category,
            "amount": self.amount,
            "period": self.period,
            "view": self.view,
        }
        return payload


@dataclass(frozen=True)
class GoalProposal:
    id: str
    name: str
    target: float
    target_cents: int
    metric: str
    direction: str
    deadline: str | None
    rationale: str
    source: str
    tool_name: str = "goal_set"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tool_input"] = {
            "name": self.name,
            "target": self.target,
            "metric": self.metric,
            "direction": self.direction,
            **({"deadline": self.deadline} if self.deadline else {}),
        }
        return payload


@dataclass(frozen=True)
class RuleProposal:
    id: str
    business_pct: float
    business_category: str
    personal_category: str
    match_category: str | None
    match_keywords: tuple[str, ...]
    note: str
    rationale: str
    sample_descriptions: tuple[str, ...]
    tool_name: str = "rules_add_split"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["match_keywords"] = list(self.match_keywords)
        payload["sample_descriptions"] = list(self.sample_descriptions)
        payload["tool_input"] = {
            "business_pct": self.business_pct,
            "business_category": self.business_category,
            "personal_category": self.personal_category,
            "match_category": self.match_category,
            "match_keywords": list(self.match_keywords),
            "note": self.note,
        }
        return payload


_EXCLUDED_BUDGET_CATEGORIES = {
    "credit card payment",
    "loan payment",
    "payments",
    "payments & transfers",
    "transfer",
    "transfers",
}
_MIXED_USE_CATEGORY_MAP = {
    "software & subscriptions": ("Office Expense", "Software & Subscriptions"),
    "subscriptions": ("Office Expense", "Subscriptions"),
    "internet": ("Office Expense", "Internet"),
    "phone": ("Office Expense", "Phone"),
    "telecom": ("Office Expense", "Telecom"),
    "rent": ("Office Expense", "Rent"),
    "travel": ("Travel", "Travel"),
    "meals": ("Meals", "Meals"),
}
_BUSINESS_VENDOR_KEYWORDS = (
    "adobe",
    "amazon",
    "apple",
    "canva",
    "cowork",
    "figma",
    "github",
    "google",
    "intuit",
    "microsoft",
    "notion",
    "office",
    "quickbooks",
    "shopify",
    "slack",
    "stripe",
    "upwork",
    "zoom",
)


def _stable_id(kind: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps({"kind": kind, **payload}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"{kind}_{digest[:16]}"


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        return default


def _query(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
    try:
        return list(conn.execute(sql, params).fetchall())
    except sqlite3.OperationalError:
        return []


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = (), default: Any = 0) -> Any:
    try:
        row = conn.execute(sql, params).fetchone()
    except sqlite3.OperationalError:
        return default
    if row is None:
        return default
    try:
        return row[0]
    except Exception:
        return default


def _round_budget_cents(value: float) -> int:
    rounded = int(round(value / 500.0) * 500)
    return max(rounded, 1000)


def _dollars(cents: int) -> float:
    return round(cents / 100, 2)


def _clean_text(value: str | None) -> str:
    return " ".join(str(value or "").strip().split())


def _liquid_cash_cents(conn: sqlite3.Connection) -> int:
    value = _scalar(
        conn,
        """
        SELECT COALESCE(SUM(balance_current_cents), 0)
          FROM accounts
         WHERE is_active = 1
           AND account_type IN ('checking', 'savings')
        """,
        default=0,
    )
    return int(value or 0)


def _avg_monthly_expense_cents(conn: sqlite3.Connection, months: int) -> int:
    total = int(
        _scalar(
            conn,
            """
            SELECT COALESCE(SUM(ABS(amount_cents)), 0)
              FROM transactions
             WHERE is_active = 1
               AND amount_cents < 0
               AND COALESCE(is_payment, 0) = 0
            """,
            default=0,
        )
        or 0
    )
    return int(total / max(months, 1))


def _uncategorized_count(conn: sqlite3.Connection) -> int:
    return int(
        _scalar(
            conn,
            """
            SELECT COUNT(*)
              FROM transactions
             WHERE is_active = 1
               AND category_id IS NULL
            """,
            default=0,
        )
        or 0
    )


def _category_names(conn: sqlite3.Connection) -> set[str]:
    rows = _query(conn, "SELECT name FROM categories")
    return {_clean_text(_row_get(row, "name")).lower() for row in rows if _clean_text(_row_get(row, "name"))}


def _best_existing_category(candidates: tuple[str, ...], existing_lower: set[str]) -> str | None:
    by_lower = {name.lower(): name for name in candidates}
    for lower_name, original in by_lower.items():
        if lower_name in existing_lower:
            return original
    return None


def starter_budget_propose(
    conn: sqlite3.Connection,
    user_type: str | None,
    priority: str | None,
    months_of_history: int,
) -> list[BudgetProposal]:
    """Propose small starter budgets from categorized spending history."""
    del user_type
    months = max(int(months_of_history or 0), 1)
    limit = 2 if months <= 1 else 5
    rows = _query(
        conn,
        """
        SELECT c.name AS category,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents,
               COUNT(*) AS txn_count,
               COUNT(DISTINCT substr(t.date, 1, 7)) AS active_months
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.amount_cents < 0
           AND COALESCE(t.is_payment, 0) = 0
           AND COALESCE(c.is_income, 0) = 0
         GROUP BY c.name
        HAVING total_cents > 0
         ORDER BY total_cents DESC, txn_count DESC, lower(c.name)
         LIMIT 12
        """,
    )

    proposals: list[BudgetProposal] = []
    for row in rows:
        category = _clean_text(_row_get(row, "category"))
        if not category or category.lower() in _EXCLUDED_BUDGET_CATEGORIES:
            continue
        total_cents = int(_row_get(row, "total_cents", 0) or 0)
        active_months = int(_row_get(row, "active_months", 0) or 0)
        denominator = max(min(months, active_months or months), 1)
        monthly_average = int(total_cents / denominator)
        if monthly_average < 5000:
            continue
        target_pct = 0.90 if priority == "save_more" else 0.92
        amount_cents = min(_round_budget_cents(monthly_average * target_pct), monthly_average - 100)
        if amount_cents <= 0:
            continue
        percent_under = max(round((monthly_average - amount_cents) / monthly_average * 100), 1)
        payload = {
            "category": category,
            "amount_cents": amount_cents,
            "period": "monthly",
            "view": "personal",
        }
        proposals.append(
            BudgetProposal(
                id=_stable_id("budget", payload),
                category=category,
                amount=_dollars(amount_cents),
                amount_cents=amount_cents,
                period="monthly",
                view="personal",
                historical_monthly_average_cents=monthly_average,
                percent_under_average=percent_under,
                months_considered=denominator,
                rationale=(
                    f"Your {category} spending averaged ${_dollars(monthly_average):.2f}/mo; "
                    f"this starts about {percent_under}% lower."
                ),
            )
        )
        if len(proposals) >= limit:
            break
    return proposals


def starter_goal_propose(conn: sqlite3.Connection, priority: str | None, user_type: str | None) -> list[GoalProposal]:
    """Propose at most one starter goal aligned to the selected priority."""
    normalized_priority = _clean_text(priority).lower()
    normalized_user_type = _clean_text(user_type).lower()
    months = max(
        int(
            _scalar(
                conn,
                "SELECT COUNT(DISTINCT substr(date, 1, 7)) FROM transactions WHERE is_active = 1",
                default=1,
            )
            or 1
        ),
        1,
    )
    avg_expense_cents = _avg_monthly_expense_cents(conn, months)

    if normalized_priority == "save_more":
        current_liquid = _liquid_cash_cents(conn)
        target_cents = max(avg_expense_cents * 3, current_liquid + 100_000, 300_000)
        payload = {
            "name": "3-month emergency fund",
            "target_cents": target_cents,
            "metric": "liquid_cash",
            "direction": "up",
        }
        return [
            GoalProposal(
                id=_stable_id("goal", payload),
                name="3-month emergency fund",
                target=_dollars(target_cents),
                target_cents=target_cents,
                metric="liquid_cash",
                direction="up",
                deadline=None,
                source="priority_save_more",
                rationale="A 3-month emergency fund gives the coach a concrete savings target to track.",
            )
        ]

    if normalized_priority == "pay_down_debt":
        row = _highest_apr_debt(conn)
        if row is None:
            return []
        account_name = _clean_text(_row_get(row, "account_name")) or "highest-APR debt"
        apr = float(_row_get(row, "apr", 0.0) or 0.0)
        balance_cents = abs(int(_row_get(row, "balance_cents", 0) or 0))
        payload = {
            "name": f"Pay off {account_name}",
            "target_cents": 0,
            "metric": "total_debt",
            "direction": "down",
        }
        return [
            GoalProposal(
                id=_stable_id("goal", payload),
                name=f"Pay off {account_name}",
                target=0.0,
                target_cents=0,
                metric="total_debt",
                direction="down",
                deadline=None,
                source="priority_pay_down_debt",
                rationale=(
                    f"{account_name} has the highest detected APR"
                    f"{f' at {apr:.2f}%' if apr else ''}"
                    f" and about ${_dollars(balance_cents):.2f} outstanding."
                ),
            )
        ]

    if normalized_priority == "taxes" or normalized_user_type in {"side_hustle", "self_employed"}:
        income_cents = int(
            _scalar(
                conn,
                """
                SELECT COALESCE(SUM(amount_cents), 0)
                  FROM transactions
                 WHERE is_active = 1
                   AND amount_cents > 0
                """,
                default=0,
            )
            or 0
        )
        monthly_income = int(income_cents / months)
        target_cents = max(int(monthly_income * 0.25), 100_000)
        payload = {
            "name": "Quarterly tax buffer",
            "target_cents": target_cents,
            "metric": "liquid_cash",
            "direction": "up",
        }
        return [
            GoalProposal(
                id=_stable_id("goal", payload),
                name="Quarterly tax buffer",
                target=_dollars(target_cents),
                target_cents=target_cents,
                metric="liquid_cash",
                direction="up",
                deadline=None,
                source="priority_taxes",
                rationale="A tax buffer keeps variable or business income from creating a surprise bill.",
            )
        ]

    return []


def _highest_apr_debt(conn: sqlite3.Connection) -> Any | None:
    rows = _query(
        conn,
        """
        SELECT l.id,
               COALESCE(a.account_name, a.institution_name, l.loan_name, 'Debt') AS account_name,
               COALESCE(l.apr_purchase, l.interest_rate_pct, l.apr_balance_transfer, l.apr_cash_advance, 0) AS apr,
               COALESCE(ABS(a.balance_current_cents), l.last_statement_balance_cents,
                        l.origination_principal_cents, 0) AS balance_cents
          FROM liabilities l
          LEFT JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND COALESCE(ABS(a.balance_current_cents), l.last_statement_balance_cents,
                        l.origination_principal_cents, 0) > 0
         ORDER BY apr DESC, balance_cents DESC, lower(account_name)
         LIMIT 1
        """,
    )
    return rows[0] if rows else None


def starter_rule_propose(
    conn: sqlite3.Connection,
    user_type: str | None,
    transaction_sample: list[dict[str, Any]] | None = None,
) -> list[RuleProposal]:
    """Propose mixed-use split rules for self-employed or side-hustle users."""
    normalized_user_type = _clean_text(user_type).lower()
    if normalized_user_type not in {"side_hustle", "self_employed"}:
        return []

    existing_categories = _category_names(conn)
    rows = _query(
        conn,
        """
        SELECT t.description, c.name AS category, COUNT(*) AS txn_count
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.amount_cents < 0
           AND COALESCE(t.is_payment, 0) = 0
         GROUP BY lower(t.description), c.name
         ORDER BY txn_count DESC, lower(t.description)
         LIMIT 40
        """,
    )
    if transaction_sample:
        rows = [
            {"description": item.get("description"), "category": item.get("category"), "txn_count": 1}
            for item in transaction_sample
        ] + rows

    proposals: list[RuleProposal] = []
    seen_signatures: set[tuple[str | None, tuple[str, ...]]] = set()
    for row in rows:
        description = _clean_text(_row_get(row, "description"))
        category = _clean_text(_row_get(row, "category"))
        if not description or not category:
            continue
        category_key = category.lower()
        keyword = _business_keyword(description)
        mapped = _MIXED_USE_CATEGORY_MAP.get(category_key)
        if mapped is None and keyword is None:
            continue
        business_candidates = (mapped[0], "Office Expense", "Professional Fees", "Supplies") if mapped else (
            "Office Expense",
            "Professional Fees",
            "Supplies",
        )
        personal_candidates = (mapped[1], category) if mapped else (category,)
        business_category = _best_existing_category(business_candidates, existing_categories)
        personal_category = _best_existing_category(personal_candidates, existing_categories)
        if not business_category or not personal_category or business_category.lower() == personal_category.lower():
            continue
        match_keywords = (keyword,) if keyword else ()
        match_category = category if mapped and not match_keywords else None
        signature = (match_category, match_keywords)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        payload = {
            "business_pct": 50.0,
            "business_category": business_category,
            "personal_category": personal_category,
            "match_category": match_category,
            "match_keywords": list(match_keywords),
        }
        proposals.append(
            RuleProposal(
                id=_stable_id("rule", payload),
                business_pct=50.0,
                business_category=business_category,
                personal_category=personal_category,
                match_category=match_category,
                match_keywords=match_keywords,
                note="Starter onboarding split. Adjust the percentage after reviewing real usage.",
                rationale=(
                    f"{description} looks like it may mix business and personal use; start with a 50/50 split."
                ),
                sample_descriptions=(description,),
            )
        )
        if len(proposals) >= 2:
            break
    return proposals


def _business_keyword(description: str) -> str | None:
    normalized = description.lower()
    for keyword in _BUSINESS_VENDOR_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", normalized):
            return keyword.upper()
    return None


def build_starter_setup_batch(
    conn: sqlite3.Connection,
    *,
    user_type: str | None,
    priority: str | None,
    months_of_history: int,
) -> dict[str, Any]:
    budgets = starter_budget_propose(conn, user_type, priority, months_of_history)
    goals = starter_goal_propose(conn, priority, user_type)
    rules = starter_rule_propose(conn, user_type)
    debt_goal = next((goal for goal in goals if goal.metric == "total_debt"), None)
    return {
        "summary": {
            "budget_proposals": len(budgets),
            "goal_proposals": len(goals),
            "rule_proposals": len(rules),
        },
        "categorization_pending_count": _uncategorized_count(conn),
        "budget_proposals": [proposal.to_dict() for proposal in budgets],
        "goal_proposals": [proposal.to_dict() for proposal in goals],
        "rule_proposals": [proposal.to_dict() for proposal in rules],
        "debt_nudge": debt_goal.rationale if debt_goal is not None else None,
    }
