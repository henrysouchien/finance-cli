"""Shared contracts for recommendation-enriched page surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from typing import Any, Literal, Mapping, NotRequired, Sequence, TypedDict


RecommendationTone = Literal["coach", "warn", "pattern", "diagnose"]
LivingMarginTone = Literal["neutral", "positive", "concern"]
SurfaceFieldState = Literal["missing", "null", "valid", "invalid"]

_RECOMMENDATION_TONES: set[str] = {"coach", "warn", "pattern", "diagnose"}
_LIVING_MARGIN_TONES: set[str] = {"neutral", "positive", "concern"}
_MISSING = object()
_BUDGET_ALERTS_SOURCE = "budget_alerts"
_BUDGET_STATUS_SOURCE = "budget_status"
_DEBT_SOURCE = "debt_dashboard"
_GOALS_SOURCE = "goals_status"
_NET_WORTH_SOURCE = "net_worth"
_ONBOARDING_SOURCE = "onboarding_state"
_PLAID_ITEMS_SOURCE = "plaid_items"
_PLAID_CONSENT_NOTICE_WINDOW_DAYS = 14
_SPENDING_SOURCE = "spending_trends"
_SUMMARY_SOURCE = "summary"
_SUBS_AUDIT_SOURCE = "subs_audit"
_SUBS_LIST_SOURCE = "subs_list"
_NET_WORTH_LIABILITY_ACCOUNT_TYPES = {
    "auto_loan",
    "credit",
    "credit_card",
    "loan",
    "manual_loans",
    "mortgage",
    "student_loan",
}


class RecommendationAnnotation(TypedDict):
    source: str
    text: str
    tone: RecommendationTone


class RecommendationInsight(RecommendationAnnotation):
    meta: str | None
    bullets: list[str]


class LivingMargin(TypedDict):
    source: str
    text: str
    tone: LivingMarginTone


class NormalizedSurfacePayload(TypedDict):
    source: str
    text: str
    tone: str
    meta: NotRequired[str | None]
    bullets: NotRequired[list[str]]


@dataclass(frozen=True)
class SurfaceField:
    """Normalized interpretation of a recommendation surface field."""

    state: SurfaceFieldState
    payload: NormalizedSurfacePayload | None = None

    @property
    def should_render(self) -> bool:
        return self.state == "valid"

    @property
    def should_use_legacy_fallback(self) -> bool:
        return self.state in {"missing", "invalid"}


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _required_text(value: Any, *, field_name: str) -> str:
    text = _clean_text(value)
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def _recommendation_tone(
    value: Any, *, default: RecommendationTone = "pattern"
) -> RecommendationTone:
    raw = str(value or "").strip()
    if raw in _RECOMMENDATION_TONES:
        return raw  # type: ignore[return-value]
    return default


def _living_margin_tone(
    value: Any, *, default: LivingMarginTone = "neutral"
) -> LivingMarginTone:
    raw = str(value or "").strip()
    if raw in _LIVING_MARGIN_TONES:
        return raw  # type: ignore[return-value]
    return default


def recommendation_annotation(
    *,
    source: Any,
    text: Any,
    tone: Any,
    default_tone: RecommendationTone = "pattern",
) -> RecommendationAnnotation:
    return {
        "source": _required_text(source, field_name="source"),
        "text": _required_text(text, field_name="text"),
        "tone": _recommendation_tone(tone, default=default_tone),
    }


def recommendation_insight(
    *,
    source: Any,
    text: Any,
    tone: Any,
    meta: Any = None,
    bullets: Sequence[Any] | None = None,
    default_tone: RecommendationTone = "diagnose",
) -> RecommendationInsight:
    cleaned_bullets = [_clean_text(bullet) for bullet in bullets or ()]
    return {
        **recommendation_annotation(
            source=source,
            text=text,
            tone=tone,
            default_tone=default_tone,
        ),
        "meta": _clean_text(meta) or None,
        "bullets": [bullet for bullet in cleaned_bullets if bullet],
    }


def living_margin(
    *,
    source: Any,
    text: Any,
    tone: Any,
    default_tone: LivingMarginTone = "neutral",
) -> LivingMargin:
    return {
        "source": _required_text(source, field_name="source"),
        "text": _required_text(text, field_name="text"),
        "tone": _living_margin_tone(tone, default=default_tone),
    }


def interpret_surface_field(
    value: Any = _MISSING,
    *,
    default_tone: RecommendationTone = "pattern",
) -> SurfaceField:
    """Apply the shared missing/null/blank contract to a surface payload.

    Missing means legacy producer and should fall back. Explicit null means the
    producer deliberately has no recommendation for that slot. Invalid payloads
    are dropped and may use compatibility fallback.
    """

    if value is _MISSING:
        return SurfaceField(state="missing")
    if value is None:
        return SurfaceField(state="null")
    if not isinstance(value, Mapping):
        return SurfaceField(state="invalid")

    text = _clean_text(value.get("text"))
    if not text:
        return SurfaceField(state="invalid")

    payload: NormalizedSurfacePayload = {
        "source": _clean_text(value.get("source")) or "unknown",
        "text": text,
        "tone": _recommendation_tone(value.get("tone"), default=default_tone),
    }
    meta = _clean_text(value.get("meta"))
    if meta:
        payload["meta"] = meta

    raw_bullets = value.get("bullets")
    if isinstance(raw_bullets, Sequence) and not isinstance(raw_bullets, (str, bytes)):
        bullets = [_clean_text(bullet) for bullet in raw_bullets]
        payload["bullets"] = [bullet for bullet in bullets if bullet]

    return SurfaceField(state="valid", payload=payload)


def format_cents(value: Any, *, places: int = 0, absolute: bool = True) -> str:
    """Format a cents-denominated amount for recommendation copy."""

    try:
        cents = float(value)
    except (TypeError, ValueError):
        cents = 0.0
    if not isfinite(cents):
        cents = 0.0
    if absolute:
        cents = abs(cents)

    amount = cents / 100
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    return f"{sign}${amount:,.{places}f}"


def _as_int(value: Any) -> int:
    try:
        return int(round(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _as_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if isfinite(parsed) else default


def _format_percent(value: float) -> str:
    return f"{int(value)}" if float(value).is_integer() else f"{value:.1f}"


def _pluralize(count: int, singular: str, plural: str | None = None) -> str:
    return singular if count == 1 else plural or f"{singular}s"


def _budget_format_dollars(value: Any) -> str:
    dollars = _safe_float(value)
    return f"${abs(dollars):,.0f}"


def _budget_use_type_label(value: Any) -> str:
    text = str(value or "personal").replace("_", " ").strip()
    return text.title() if text else "Personal"


_BUDGET_ALERT_SEVERITY_RANK = {"over": 0, "alert": 1, "warn": 2}


def _budget_alert_sort_key(row: Mapping[str, Any]) -> tuple[int, float, str, str]:
    severity_rank = _BUDGET_ALERT_SEVERITY_RANK.get(
        str(row.get("severity") or ""), 99
    )
    forecast_utilization = _safe_float(row.get("forecast_utilization"))
    category = str(row.get("category_name") or "")
    use_type = str(row.get("use_type") or "")
    return (severity_rank, -forecast_utilization, category, use_type)


def _budget_alert_sentence(row: Mapping[str, Any]) -> str:
    use_type_label = _budget_use_type_label(row.get("use_type"))
    category_label = f"{row.get('category_name') or 'This category'} ({use_type_label})"
    forecast_pct = int(_safe_float(row.get("forecast_utilization")) * 100 + 0.5)
    daily_room = max(0.0, _safe_float(row.get("remaining_daily_budget")))
    daily_action = (
        f"keep new spending near {_budget_format_dollars(daily_room)}/day"
        if daily_room > 0
        else "hold new spending at $0/day"
    )
    sentence_daily_action = f"{daily_action[0].upper()}{daily_action[1:]}"

    if row.get("severity") == "over":
        actual_cents = abs(_as_int(row.get("actual_cents")))
        budget_cents = abs(_as_int(row.get("budget_cents")))
        over_cents = max(0, actual_cents - budget_cents)
        over_text = (
            f"{format_cents(over_cents)} over budget"
            if over_cents > 0
            else "over budget"
        )
        return f"Pause {category_label}: it is {over_text}. {sentence_daily_action} until next month."

    if row.get("severity") == "alert":
        return f"Steer {category_label}: {daily_action} to finish near {forecast_pct}% of budget."

    return f"Keep {category_label} steady: {daily_action}; current pace lands near {forecast_pct}% of budget."


def _budget_alert_insight(alerts: Any) -> dict[str, Any] | None:
    if not isinstance(alerts, list):
        return None

    alert_rows = [row for row in alerts if isinstance(row, dict)]
    sorted_alerts = sorted(alert_rows, key=_budget_alert_sort_key)

    if not sorted_alerts:
        return None

    top_alerts = sorted_alerts[:2]
    bullets: list[str] = []
    for row in top_alerts:
        use_type_label = _budget_use_type_label(row.get("use_type"))
        category_label = f"{row.get('category_name') or 'This category'} ({use_type_label})"

        if row.get("severity") == "over":
            actual_cents = abs(_as_int(row.get("actual_cents")))
            budget_cents = abs(_as_int(row.get("budget_cents")))
            over_cents = max(0, actual_cents - budget_cents)
            bullets.append(
                f"{category_label} is {format_cents(over_cents)} over; pause new spending here first."
            )
            continue

        daily_room = max(0.0, _safe_float(row.get("remaining_daily_budget")))
        bullets.append(
            f"{category_label} has {_budget_format_dollars(daily_room)}/day left to land near plan."
        )

    return {
        **recommendation_annotation(
            source=_BUDGET_ALERTS_SOURCE,
            text=" ".join(_budget_alert_sentence(row) for row in top_alerts),
            tone=(
                "warn"
                if any(row.get("severity") == "over" for row in sorted_alerts)
                else "diagnose"
            ),
        ),
        "bullets": bullets,
    }


def _budget_status_living_margin(row: Mapping[str, Any]) -> LivingMargin | None:
    budget_cents = _as_int(row.get("budget_cents"))
    actual_cents = abs(_as_int(row.get("actual_cents")))
    remaining_cents = _as_int(row.get("remaining_cents") or budget_cents - actual_cents)
    utilization = _safe_float(row.get("utilization"))
    category = str(row.get("category_name") or "this category")

    if budget_cents <= 0:
        return None

    if actual_cents > budget_cents:
        return living_margin(
            source=_BUDGET_STATUS_SOURCE,
            text=f"{format_cents(actual_cents - budget_cents)} over plan. Pause new spending before expanding this guardrail.",
            tone="concern",
        )

    if utilization >= 0.8:
        return living_margin(
            source=_BUDGET_STATUS_SOURCE,
            text=f"{format_cents(max(0, remaining_cents))} left. Keep {category} close to plan for the rest of the month.",
            tone="concern",
        )

    if utilization <= 0.5:
        return living_margin(
            source=_BUDGET_STATUS_SOURCE,
            text=f"{format_cents(max(0, remaining_cents))} room left. {category} is not crowding the month right now.",
            tone="positive",
        )

    return living_margin(
        source=_BUDGET_STATUS_SOURCE,
        text=f"{format_cents(max(0, remaining_cents))} left. Stay near this pace to keep the month balanced.",
        tone="neutral",
    )


def _budget_status_annotation(text: str, tone: Any) -> RecommendationAnnotation:
    return recommendation_annotation(source=_BUDGET_STATUS_SOURCE, text=text, tone=tone)


def _budget_status_stat_annotations(
    status_rows: list[dict[str, Any]],
) -> dict[str, RecommendationAnnotation]:
    total_budget_cents = sum(_as_int(row.get("budget_cents")) for row in status_rows)
    total_spent_cents = sum(abs(_as_int(row.get("actual_cents"))) for row in status_rows)
    remaining_cents = total_budget_cents - total_spent_cents

    if total_budget_cents <= 0:
        return {
            "committed": _budget_status_annotation(
                "$0 still uncommitted; keep new spending attached to daily room.",
                "coach",
            )
        }

    overall_utilization = (total_spent_cents / total_budget_cents) * 100
    room_tone = (
        "warn"
        if overall_utilization >= 100
        else "pattern"
        if overall_utilization >= 80
        else "coach"
    )

    return {
        "planned": _budget_status_annotation(
            (
                f"{format_cents(total_budget_cents)} planned across {len(status_rows)} "
                f"budget{'' if len(status_rows) == 1 else 's'}; move dollars before categories crowd the month."
            ),
            "pattern",
        ),
        "committed": (
            _budget_status_annotation(
                f"{format_cents(abs(remaining_cents))} over plan; hold new spending in crowded categories.",
                "warn",
            )
            if total_spent_cents > total_budget_cents
            else _budget_status_annotation(
                f"{format_cents(max(0, remaining_cents))} still uncommitted; "
                "keep new spending attached to daily room.",
                "coach",
            )
        ),
        "room_used": _budget_status_annotation(
            (
                f"{format_cents(max(0, remaining_cents))} left; "
                f"{_format_percent(overall_utilization)}% of the monthly plan is used."
            ),
            room_tone,
        ),
    }


def enrich_budget_status_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation fields to budget status data."""

    enriched = dict(data)
    raw_status = enriched.get("status", [])
    status_rows = (
        [dict(row) for row in raw_status if isinstance(row, dict)]
        if isinstance(raw_status, list)
        else []
    )

    for row in status_rows:
        row["living_margin"] = _budget_status_living_margin(row)

    enriched["status"] = status_rows
    enriched["stat_annotations"] = _budget_status_stat_annotations(status_rows)
    return enriched


def enrich_budget_alerts_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation fields to budget alert data."""

    enriched = dict(data)
    enriched["budget_insight"] = _budget_alert_insight(enriched.get("alerts", []))
    return enriched


def _summary_format_dollars(value: Any) -> str:
    return f"${abs(_safe_float(value)):,.0f}"


def _summary_format_minutes(minutes: Any) -> str:
    rounded = max(1, round(_safe_float(minutes)))
    if rounded < 60:
        return f"{rounded} min"
    hours = rounded / 60
    return f"{hours:.0f} hrs" if float(hours).is_integer() else f"{hours:.1f} hrs"


def _summary_annotation(text: str, tone: Any) -> RecommendationAnnotation:
    return recommendation_annotation(source=_SUMMARY_SOURCE, text=text, tone=tone)


def _summary_money_position_annotation(
    data: Mapping[str, Any],
) -> RecommendationAnnotation | None:
    raw_goals = data.get("goals", [])
    goals = raw_goals if isinstance(raw_goals, list) else []
    for goal in goals:
        if not isinstance(goal, Mapping):
            continue
        if _safe_float(goal.get("progress_pct")) >= 100:
            continue
        target_cents = goal.get("target_cents")
        current_cents = goal.get("current_cents")
        if target_cents is not None and current_cents is not None:
            gap = max(0.0, abs(_as_int(target_cents) - _as_int(current_cents)) / 100)
            if gap > 0:
                name = str(goal.get("name") or "your goal")
                months = _as_float_or_none(goal.get("estimated_months"))
                time_text = f"; ~{max(1, round(months))} mos left" if months is not None else ""
                return _summary_annotation(
                    f"{_summary_format_dollars(gap)} from {name}{time_text}.",
                    "coach",
                )

    net_worth = _safe_float(data.get("net_worth"))
    if net_worth != 0:
        return _summary_annotation(
            f"{_summary_format_dollars(net_worth)} current position; add a goal to turn it into a target.",
            "pattern",
        )
    return None


def _summary_alert_currency_value(
    alert: Mapping[str, Any],
    cents_key: str,
    value_key: str,
) -> float:
    if alert.get(cents_key) is not None:
        return _safe_float(alert.get(cents_key)) / 100
    return _safe_float(alert.get(value_key))


def _summary_spend_to_steer_annotation(
    alerts: list[dict[str, Any]],
) -> RecommendationAnnotation | None:
    if not alerts:
        return None
    top = alerts[0]
    category = str(top.get("category_name") or "this category")
    remaining_daily = _summary_alert_currency_value(
        top, "remaining_daily_budget_cents", "remaining_daily_budget"
    )
    if str(top.get("severity") or "") == "over":
        actual = abs(_summary_alert_currency_value(top, "actual_cents", "actual"))
        budget_value = abs(_summary_alert_currency_value(top, "budget_cents", "budget"))
        over_by = max(0.0, actual - budget_value)
        if over_by > 0:
            return _summary_annotation(
                f"{_summary_format_dollars(over_by)} over {category}; hold new spending at {_summary_format_dollars(max(0.0, remaining_daily))}/day this month.",
                "warn",
            )

    return _summary_annotation(
        f"{_summary_format_dollars(max(0.0, remaining_daily))}/day keeps {category} near plan this month.",
        "warn" if str(top.get("severity") or "") == "alert" else "pattern",
    )


def _summary_budget_alert_living_margin(alert: Mapping[str, Any]) -> LivingMargin:
    category = str(alert.get("category_name") or "this category")
    severity = str(alert.get("severity") or "")
    remaining_daily = _summary_alert_currency_value(
        alert, "remaining_daily_budget_cents", "remaining_daily_budget"
    )

    if severity == "over":
        actual = abs(_summary_alert_currency_value(alert, "actual_cents", "actual"))
        budget_value = abs(_summary_alert_currency_value(alert, "budget_cents", "budget"))
        over_by = max(0.0, actual - budget_value)

        return living_margin(
            source=_SUMMARY_SOURCE,
            text=(
                f"{_summary_format_dollars(over_by)} over plan. The next move is a pause, not a bigger budget."
                if over_by > 0
                else f"{_summary_format_dollars(max(0.0, remaining_daily))}/day left. The next move is a pause, not a bigger budget."
            ),
            tone="concern",
        )

    return living_margin(
        source=_SUMMARY_SOURCE,
        text=f"{_summary_format_dollars(max(0.0, remaining_daily))}/day left. Steer {category} now while there is still room.",
        tone="concern" if severity == "alert" else "neutral",
    )


def _summary_budget_alerts_with_living_margins(
    alerts: Any,
) -> list[dict[str, Any]]:
    alert_rows = (
        [dict(alert) for alert in alerts if isinstance(alert, Mapping)]
        if isinstance(alerts, list)
        else []
    )
    for alert in alert_rows:
        alert["living_margin"] = _summary_budget_alert_living_margin(alert)
    return alert_rows


def _summary_handled_annotation(
    transaction_count: int,
    categorization_rate: float,
) -> RecommendationAnnotation | None:
    handled = round(transaction_count * categorization_rate)
    if handled <= 0:
        return None
    return _summary_annotation(
        f"About {_summary_format_minutes(handled * 0.5)} of sorting handled across {handled} transactions.",
        "coach" if categorization_rate >= 0.8 else "pattern",
    )


def _summary_subscription_annotation(
    data: Mapping[str, Any],
) -> RecommendationAnnotation | None:
    subscriptions = data.get("subscriptions")
    if subscriptions is None and data.get("subscriptions_cents") is not None:
        subscriptions = _safe_float(data.get("subscriptions_cents")) / 100
    subscriptions = _safe_float(subscriptions)
    if subscriptions <= 0:
        return None
    return _summary_annotation(
        f"{_summary_format_dollars(subscriptions)}/mo in recurring spend; review fixed charges before they disappear into the baseline.",
        "pattern",
    )


def _summary_transaction_insight(
    data: Mapping[str, Any],
    *,
    transaction_count: int,
) -> RecommendationAnnotation | None:
    if transaction_count <= 0:
        return None

    uncategorized = _as_int(data.get("uncategorized"))
    unreviewed = _as_int(data.get("unreviewed"))
    total_checks = uncategorized + unreviewed
    uncategorized_label = _pluralize(uncategorized, "transaction")
    unreviewed_label = _pluralize(unreviewed, "transaction")

    if uncategorized > 0 and unreviewed > 0:
        return _summary_annotation(
            (
                f"Clear {total_checks} transaction checks: categorize {uncategorized} "
                f"and review {unreviewed} so budgets and recommendations stay actionable."
            ),
            "warn",
        )
    if uncategorized > 0:
        return _summary_annotation(
            f"Assign categories to {uncategorized} {uncategorized_label} so spending moves land in the right budgets.",
            "warn",
        )
    if unreviewed > 0:
        return _summary_annotation(
            f"Confirm {unreviewed} {unreviewed_label} so CashNerd can keep applying the right money rules.",
            "pattern",
        )
    return _summary_annotation(
        "Transactions are categorized and reviewed. Your budgets and recommendations have clean inputs.",
        "coach",
    )


def _summary_transaction_annotations(
    data: Mapping[str, Any],
    *,
    transaction_count: int,
) -> dict[str, object]:
    uncategorized = _as_int(data.get("uncategorized"))
    unreviewed = _as_int(data.get("unreviewed"))
    expense_30d = _safe_float(data.get("expense_30d"))
    uncategorized_label = _pluralize(uncategorized, "transaction")
    unreviewed_label = _pluralize(unreviewed, "transaction")

    spent_annotation = (
        _summary_annotation(
            f"{_summary_format_dollars(expense_30d)} spent in 30 days; keep new imports categorized before this becomes budget drift.",
            "pattern",
        )
        if expense_30d > 0
        else _summary_annotation(
            "No 30-day spending yet; import fresh transactions before budgeting off this view.",
            "coach",
        )
    )
    needs_category = (
        _summary_annotation(
            f"Categorize {uncategorized} {uncategorized_label} before trusting category-level recommendations.",
            "warn",
        )
        if uncategorized > 0
        else _summary_annotation(
            "Every active transaction has a category. Budgets can use clean inputs.",
            "coach",
        )
    )
    needs_review = (
        _summary_annotation(
            f"Review {unreviewed} {unreviewed_label} so money rules do not learn from stale labels.",
            "pattern",
        )
        if unreviewed > 0
        else _summary_annotation(
            "Reviewed transactions are ready for rules and recurring-charge detection.",
            "coach",
        )
    )

    return {
        "insight": _summary_transaction_insight(
            data, transaction_count=transaction_count
        ),
        "stat_annotations": {
            "spent_30d": spent_annotation,
            "needs_category": needs_category,
            "needs_review": needs_review,
        },
    }


def _summary_stat_annotations(
    data: Mapping[str, Any],
    *,
    transaction_count: int,
    categorization_rate: float,
) -> dict[str, RecommendationAnnotation | None]:
    raw_alerts = data.get("budget_alerts", [])
    alerts = (
        [alert for alert in raw_alerts if isinstance(alert, dict)]
        if isinstance(raw_alerts, list)
        else []
    )
    return {
        "handled_automatically": _summary_handled_annotation(
            transaction_count, categorization_rate
        ),
        "money_position": _summary_money_position_annotation(data),
        "spend_to_steer": _summary_spend_to_steer_annotation(alerts),
        "subscription_drag": _summary_subscription_annotation(data),
    }


def enrich_summary_surface(
    data: Mapping[str, Any],
    *,
    transaction_count: int,
    categorization_rate: float,
) -> dict[str, Any]:
    """Attach shared recommendation fields to dashboard summary data."""

    enriched = dict(data)
    enriched["budget_alerts"] = _summary_budget_alerts_with_living_margins(
        enriched.get("budget_alerts", [])
    )
    enriched["stat_annotations"] = _summary_stat_annotations(
        enriched,
        transaction_count=transaction_count,
        categorization_rate=categorization_rate,
    )
    enriched["transaction_annotations"] = _summary_transaction_annotations(
        enriched,
        transaction_count=transaction_count,
    )
    return enriched


def _subscription_monthly_amount_cents(row: Mapping[str, Any]) -> int:
    return int(round(_safe_float(row.get("monthly_amount")) * 100))


def _subscription_audit_annotation(text: str, tone: Any) -> RecommendationAnnotation:
    return recommendation_annotation(source=_SUBS_AUDIT_SOURCE, text=text, tone=tone)


def _subscription_stat_annotations(
    data: Mapping[str, Any],
) -> dict[str, RecommendationAnnotation | None]:
    essential_count = _as_int(data.get("essential_count"))
    discretionary_count = _as_int(data.get("discretionary_count"))
    essential_monthly_cents = _as_int(data.get("essential_monthly_cents"))
    discretionary_monthly_cents = _as_int(data.get("discretionary_monthly_cents"))
    active_count = essential_count + discretionary_count
    total_monthly_cents = essential_monthly_cents + discretionary_monthly_cents

    return {
        "discretionary": (
            _subscription_audit_annotation(
                (
                    f"{format_cents(discretionary_monthly_cents)}/mo cuttable across "
                    f"{discretionary_count} {_pluralize(discretionary_count, 'subscription')}."
                ),
                "warn",
            )
            if discretionary_monthly_cents > 0
            else _subscription_audit_annotation(
                "No discretionary subscriptions tagged yet. Review classifications before cutting.",
                "pattern",
            )
        ),
        "essential": (
            _subscription_audit_annotation(
                (
                    f"{format_cents(essential_monthly_cents)}/mo marked essential. "
                    "Check for duplicate coverage before cutting."
                ),
                "coach",
            )
            if essential_monthly_cents > 0
            else None
        ),
        "recurring": (
            _subscription_audit_annotation(
                (
                    f"{format_cents(total_monthly_cents)}/mo recurring baseline across "
                    f"{active_count} {_pluralize(active_count, 'subscription')}."
                ),
                "pattern",
            )
            if total_monthly_cents > 0
            else None
        ),
    }


def _subscription_insight(
    data: Mapping[str, Any],
    scenarios: list[dict[str, Any]],
) -> RecommendationInsight | None:
    essential_count = _as_int(data.get("essential_count"))
    discretionary_count = _as_int(data.get("discretionary_count"))
    essential_monthly_cents = _as_int(data.get("essential_monthly_cents"))
    discretionary_monthly_cents = _as_int(data.get("discretionary_monthly_cents"))
    active_count = essential_count + discretionary_count

    if active_count <= 0:
        return None

    meta = f"{active_count} active {_pluralize(active_count, 'subscription')} tracked"
    top_three_scenario = next(
        (
            scenario
            for scenario in scenarios
            if isinstance(scenario.get("subs_affected"), list)
            and len(scenario["subs_affected"]) == 3
        ),
        None,
    )

    if top_three_scenario is not None:
        monthly_savings_cents = _as_int(top_three_scenario.get("monthly_savings_cents"))
        interest_saved_cents = _as_int(top_three_scenario.get("interest_saved_cents"))

        if monthly_savings_cents > 0 and interest_saved_cents > 0:
            return recommendation_insight(
                source=_SUBS_AUDIT_SOURCE,
                text=(
                    "Cut the top 3 discretionary subscriptions to free "
                    f"{format_cents(monthly_savings_cents)}/mo and save "
                    f"{format_cents(interest_saved_cents)} in interest."
                ),
                meta=meta,
                bullets=[
                    f"{format_cents(monthly_savings_cents * 12)}/yr of discretionary spend is available to review.",
                    "Tie the cash freed to debt paydown before it disappears into the baseline.",
                ],
                tone="warn",
            )

        if monthly_savings_cents > 0:
            return recommendation_insight(
                source=_SUBS_AUDIT_SOURCE,
                text=(
                    "Review the top 3 discretionary subscriptions, "
                    f"{format_cents(monthly_savings_cents)}/mo or "
                    f"{format_cents(monthly_savings_cents * 12)}/yr in recurring spend."
                ),
                meta=meta,
                bullets=[
                    (
                        f"{format_cents(monthly_savings_cents * 12)}/yr can be redirected "
                        "if the top 3 do not still earn their keep."
                    )
                ],
                tone="warn",
            )

    total_monthly_cents = essential_monthly_cents + discretionary_monthly_cents

    if (
        total_monthly_cents > 0
        and discretionary_monthly_cents > 0
        and discretionary_count > 0
    ):
        discretionary_share = round(
            (discretionary_monthly_cents / total_monthly_cents) * 100
        )
        return recommendation_insight(
            source=_SUBS_AUDIT_SOURCE,
            text=(
                "Start with discretionary subscriptions, "
                f"{format_cents(discretionary_monthly_cents)}/mo across "
                f"{discretionary_count} {_pluralize(discretionary_count, 'service')} and "
                f"{discretionary_share}% of recurring spend."
            ),
            meta=meta,
            bullets=[
                f"{format_cents(discretionary_monthly_cents * 12)}/yr is tagged discretionary."
            ],
            tone="warn",
        )

    if total_monthly_cents > 0:
        return recommendation_insight(
            source=_SUBS_AUDIT_SOURCE,
            text=(
                f"Use the {format_cents(total_monthly_cents)}/mo recurring-spend baseline "
                "to choose what stays and what gets cut."
            ),
            meta=meta,
            bullets=[
                "Recurring charges are classified as must-keep right now; watch for duplicate coverage."
            ],
            tone="coach",
        )

    return None


def _subscription_scenario_living_margin(
    scenario: Mapping[str, Any],
) -> LivingMargin | None:
    monthly_savings_cents = _as_int(scenario.get("monthly_savings_cents"))
    if monthly_savings_cents <= 0:
        return None

    return living_margin(
        source=_SUBS_AUDIT_SOURCE,
        text=(
            f"{format_cents(monthly_savings_cents)}/mo can be redirected immediately; "
            f"that is {format_cents(monthly_savings_cents * 12)}/yr before interest effects."
        ),
        tone="positive",
    )


def _subscription_row_living_margin(
    subscription: Mapping[str, Any],
    *,
    essential_categories: frozenset[str],
) -> LivingMargin | None:
    monthly_cents = _subscription_monthly_amount_cents(subscription)
    if monthly_cents <= 0:
        return None

    amount = format_cents(monthly_cents)
    category_name = str(subscription.get("category_name") or "")

    normalized_category = category_name.strip().casefold()
    if any(normalized_category == category.casefold() for category in essential_categories):
        return living_margin(
            source=_SUBS_LIST_SOURCE,
            text=f"{amount}/mo essential baseline. Keep unless duplicate coverage exists.",
            tone="positive",
        )

    if category_name.strip():
        return living_margin(
            source=_SUBS_LIST_SOURCE,
            text=f"{amount}/mo cut candidate. Confirm use before it stays in the baseline.",
            tone="concern",
        )

    return living_margin(
        source=_SUBS_LIST_SOURCE,
        text=f"{amount}/mo recurring charge. Classify keep or cut before the next review.",
        tone="neutral",
    )


def enrich_subscription_list_surface(
    data: Mapping[str, Any],
    *,
    essential_categories: frozenset[str],
) -> dict[str, Any]:
    """Attach shared recommendation fields to subscription list data."""

    enriched = dict(data)
    raw_subscriptions = enriched.get("subscriptions", [])
    subscriptions = (
        [
            dict(subscription)
            for subscription in raw_subscriptions
            if isinstance(subscription, Mapping)
        ]
        if isinstance(raw_subscriptions, list)
        else []
    )

    for subscription in subscriptions:
        subscription["living_margin"] = _subscription_row_living_margin(
            subscription,
            essential_categories=essential_categories,
        )

    enriched["subscriptions"] = subscriptions
    return enriched


def enrich_subscription_audit_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation fields to subscription audit data."""

    enriched = dict(data)
    raw_scenarios = enriched.get("scenarios", [])
    scenarios = (
        [dict(scenario) for scenario in raw_scenarios if isinstance(scenario, Mapping)]
        if isinstance(raw_scenarios, list)
        else []
    )

    for scenario in scenarios:
        scenario["living_margin"] = _subscription_scenario_living_margin(scenario)

    enriched["scenarios"] = scenarios
    enriched["stat_annotations"] = _subscription_stat_annotations(enriched)
    enriched["subscription_insight"] = _subscription_insight(enriched, scenarios)
    return enriched


def _net_worth_is_liability_account_type(account_type: str) -> bool:
    return account_type.casefold() in _NET_WORTH_LIABILITY_ACCOUNT_TYPES


def _net_worth_format_signed_cents(cents: int) -> str:
    return f"-{format_cents(cents)}" if cents < 0 else format_cents(cents)


def _net_worth_format_ratio(value: float) -> str:
    return f"{int(value)}" if float(value).is_integer() else f"{value:.1f}"


def _net_worth_annotation(text: str, tone: Any) -> RecommendationAnnotation:
    return recommendation_annotation(source=_NET_WORTH_SOURCE, text=text, tone=tone)


def _net_worth_current_stat_annotations(
    data: Mapping[str, Any],
) -> dict[str, RecommendationAnnotation | None]:
    net_worth_cents = _as_int(
        data.get(
            "net_worth_cents",
            round(_safe_float(data.get("net_worth")) * 100),
        )
    )
    assets_cents = _as_int(
        data.get("assets_cents", round(_safe_float(data.get("assets")) * 100))
    )
    liabilities_cents = _as_int(
        data.get(
            "liabilities_cents",
            round(_safe_float(data.get("liabilities")) * 100),
        )
    )

    return {
        "assets": (
            _net_worth_annotation(
                f"{format_cents(assets_cents)} available on the asset side of the plan.",
                "pattern",
            )
            if assets_cents > 0
            else None
        ),
        "liabilities": (
            _net_worth_annotation(
                f"{format_cents(liabilities_cents)} in debt drag to route through a payoff order.",
                "pattern",
            )
            if liabilities_cents > 0
            else None
        ),
        "net_worth": (
            _net_worth_annotation(
                f"{_net_worth_format_signed_cents(net_worth_cents)} current position; use account mix to choose the next move.",
                "pattern",
            )
            if net_worth_cents != 0
            else None
        ),
    }


def _net_worth_asset_delta_annotation(
    delta_cents: int,
) -> RecommendationAnnotation | None:
    if delta_cents > 0:
        return _net_worth_annotation(
            f"{format_cents(delta_cents)} more assets than prior snapshot; keep new cash assigned.",
            "coach",
        )
    if delta_cents < 0:
        return _net_worth_annotation(
            f"{format_cents(delta_cents)} asset drawdown since prior snapshot; verify it was planned.",
            "warn",
        )
    return None


def _net_worth_liability_delta_annotation(
    delta_cents: int,
) -> RecommendationAnnotation | None:
    if delta_cents > 0:
        return _net_worth_annotation(
            f"{format_cents(delta_cents)} less debt than prior snapshot; keep paydown pointed at high-rate balances.",
            "coach",
        )
    if delta_cents < 0:
        return _net_worth_annotation(
            f"{format_cents(delta_cents)} more debt than prior snapshot; check which account drove it.",
            "warn",
        )
    return None


def _net_worth_delta_annotation(delta_cents: int) -> RecommendationAnnotation | None:
    if delta_cents > 0:
        return _net_worth_annotation(
            f"{format_cents(delta_cents)} higher than prior snapshot; keep the surplus attached to a goal.",
            "coach",
        )
    if delta_cents < 0:
        return _net_worth_annotation(
            f"{format_cents(delta_cents)} lower than prior snapshot; check spending and debt movement first.",
            "warn",
        )
    return None


def _net_worth_delta_stat_annotations(
    points: list[dict[str, Any]],
) -> dict[str, RecommendationAnnotation | None]:
    if len(points) < 2:
        return {"assets": None, "liabilities": None, "net_worth": None}

    previous = points[-2]
    latest = points[-1]
    net_worth_delta = _as_int(latest.get("net_worth_cents")) - _as_int(
        previous.get("net_worth_cents")
    )
    assets_delta = _as_int(latest.get("assets_cents")) - _as_int(
        previous.get("assets_cents")
    )
    liabilities_delta = _as_int(previous.get("liabilities_cents")) - _as_int(
        latest.get("liabilities_cents")
    )

    return {
        "assets": _net_worth_asset_delta_annotation(assets_delta),
        "liabilities": _net_worth_liability_delta_annotation(liabilities_delta),
        "net_worth": _net_worth_delta_annotation(net_worth_delta),
    }


def _net_worth_account_living_margin(
    entry: Mapping[str, Any],
    *,
    assets_cents: int,
    liabilities_cents: int,
) -> LivingMargin | None:
    account_type = str(entry.get("account_type") or "")
    balance_cents = _as_int(entry.get("balance_cents"))
    is_asset = not _net_worth_is_liability_account_type(account_type)
    total_cents = assets_cents if is_asset else liabilities_cents

    if total_cents == 0:
        return None

    proportion = round(abs(balance_cents) / abs(total_cents) * 100)
    formatted_balance = format_cents(balance_cents)

    if not is_asset:
        return living_margin(
            source=_NET_WORTH_SOURCE,
            text=(
                f"{formatted_balance} debt here is {proportion}% of liabilities. "
                "Prioritize the highest-rate balance first."
            ),
            tone="concern",
        )

    if proportion >= 60:
        return living_margin(
            source=_NET_WORTH_SOURCE,
            text=f"{formatted_balance} is {proportion}% of assets. Keep this account tied to a job or goal.",
            tone="neutral",
        )

    return living_margin(
        source=_NET_WORTH_SOURCE,
        text=f"{formatted_balance} supports the asset side of the plan.",
        tone="positive",
    )


def _net_worth_tracked_months(points: list[dict[str, Any]]) -> int:
    if len(points) < 2:
        return 0

    try:
        first_date = date.fromisoformat(str(points[0].get("snapshot_date")))
        last_date = date.fromisoformat(str(points[-1].get("snapshot_date")))
    except (TypeError, ValueError):
        return max(1, round(len(points) / 30))

    elapsed_days = max(0, (last_date - first_date).days)
    return max(1, round(elapsed_days / 30))


def _net_worth_history_insight(
    points: list[dict[str, Any]],
) -> RecommendationInsight | None:
    if len(points) < 2:
        return None

    first = points[0]
    latest = points[-1]
    net_worth_change = _as_int(latest.get("net_worth_cents")) - _as_int(
        first.get("net_worth_cents")
    )
    tracked_months = _net_worth_tracked_months(points)
    month_label = _pluralize(tracked_months, "month")
    meta = f"{len(points)} balance snapshots tracked"

    if net_worth_change > 0:
        return recommendation_insight(
            source=_NET_WORTH_SOURCE,
            text=(
                f"Protect the momentum: net worth is up {format_cents(net_worth_change)} "
                f"over the last {tracked_months} {month_label}. Keep surplus aimed at the next goal."
            ),
            meta=meta,
            bullets=[
                f"{format_cents(net_worth_change)} higher than the starting snapshot.",
                "Keep surplus assigned before it drifts back into unplanned spending.",
            ],
            tone="coach",
        )

    if net_worth_change < 0:
        return recommendation_insight(
            source=_NET_WORTH_SOURCE,
            text=(
                f"Review the drivers: net worth is down {format_cents(net_worth_change)} "
                f"over the last {tracked_months} {month_label}. Check spending and debt changes first."
            ),
            meta=meta,
            bullets=[
                f"{format_cents(net_worth_change)} lower than the starting snapshot.",
                "Check spending, debt balances, and asset transfers before assuming it is market movement.",
            ],
            tone="warn",
        )

    return None


def _net_worth_current_insight(
    data: Mapping[str, Any],
    breakdown: list[dict[str, Any]],
) -> RecommendationInsight | None:
    assets_cents = _as_int(
        data.get("assets_cents", round(_safe_float(data.get("assets")) * 100))
    )
    liabilities_cents = _as_int(
        data.get(
            "liabilities_cents",
            round(_safe_float(data.get("liabilities")) * 100),
        )
    )
    meta = f"Net worth snapshot across {len(breakdown)} {_pluralize(len(breakdown), 'account')}"

    if assets_cents > 0 and liabilities_cents > 0:
        liabilities_pct_of_assets = (liabilities_cents / assets_cents) * 100

        if liabilities_pct_of_assets > 50:
            return recommendation_insight(
                source=_NET_WORTH_SOURCE,
                text=(
                    f"Liabilities are {round(liabilities_pct_of_assets)}% of total assets. "
                    "Prioritize paydown to speed up net worth growth."
                ),
                meta=meta,
                bullets=[
                    f"{format_cents(liabilities_cents)} in liabilities is slowing the asset base.",
                    "Start with high-rate balances before optimizing smaller asset moves.",
                ],
                tone="warn",
            )

        assets_to_liabilities_ratio = assets_cents / liabilities_cents

        if assets_to_liabilities_ratio > 5:
            return recommendation_insight(
                source=_NET_WORTH_SOURCE,
                text=(
                    "Assets outweigh liabilities "
                    f"{_net_worth_format_ratio(assets_to_liabilities_ratio)}-to-1. "
                    "Keep new surplus working toward the next goal."
                ),
                meta=meta,
                bullets=[
                    "The balance sheet has room to aim new surplus at the next milestone."
                ],
                tone="coach",
            )

    return None


def enrich_net_worth_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation fields to current net-worth data."""

    enriched = dict(data)
    assets_cents = _as_int(
        enriched.get("assets_cents", round(_safe_float(enriched.get("assets")) * 100))
    )
    liabilities_cents = _as_int(
        enriched.get(
            "liabilities_cents",
            round(_safe_float(enriched.get("liabilities")) * 100),
        )
    )
    raw_breakdown = enriched.get("breakdown", [])
    breakdown = (
        [dict(entry) for entry in raw_breakdown if isinstance(entry, Mapping)]
        if isinstance(raw_breakdown, list)
        else []
    )

    for entry in breakdown:
        entry["living_margin"] = _net_worth_account_living_margin(
            entry,
            assets_cents=assets_cents,
            liabilities_cents=liabilities_cents,
        )

    enriched["breakdown"] = breakdown
    enriched["stat_annotations"] = _net_worth_current_stat_annotations(enriched)
    enriched["net_worth_insight"] = _net_worth_current_insight(enriched, breakdown)
    return enriched


def enrich_net_worth_history_surface(
    data: Mapping[str, Any],
    *,
    suppress_recommendations: bool = False,
) -> dict[str, Any]:
    """Attach shared recommendation fields to net-worth history data."""

    enriched = dict(data)
    raw_points = enriched.get("points", [])
    points = (
        [dict(point) for point in raw_points if isinstance(point, Mapping)]
        if isinstance(raw_points, list)
        else []
    )
    enriched["points"] = points

    if suppress_recommendations:
        enriched["stat_annotations"] = {
            "assets": None,
            "liabilities": None,
            "net_worth": None,
        }
        enriched["net_worth_insight"] = None
        return enriched

    enriched["stat_annotations"] = _net_worth_delta_stat_annotations(points)
    enriched["net_worth_insight"] = _net_worth_history_insight(points)
    return enriched


def _plaid_settings_note(text: str) -> dict[str, str]:
    return {"source": _PLAID_ITEMS_SOURCE, "text": text}


def _parse_plaid_consent_expiration(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    normalized = f"{text[:-1]}+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _plaid_consent_reauthorization_due(item: Mapping[str, Any]) -> bool:
    expires_at = _parse_plaid_consent_expiration(item.get("consent_expiration_time"))
    if expires_at is None:
        return False
    notice_at = datetime.now(timezone.utc) + timedelta(days=_PLAID_CONSENT_NOTICE_WINDOW_DAYS)
    return expires_at <= notice_at


def _plaid_settings_annotations_for_items(
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    reconnect = 0
    reauthorize = 0
    pending = 0
    ok = 0

    for item in items:
        status = str(item.get("status") or "")
        if item.get("needs_reauth") or _plaid_consent_reauthorization_due(item):
            reauthorize += 1
            continue
        if status not in {"active", "pending"}:
            reconnect += 1
            continue
        if status == "pending":
            pending += 1
            continue
        ok += 1

    attention = reconnect + reauthorize
    total = attention + pending + ok
    insight: dict[str, str] | None = None
    if total > 0:
        meta = f"Recommendation data health across {total} institution{'s' if total != 1 else ''}"
        if reauthorize > 0 and reconnect > 0:
            insight = {
                "source": _PLAID_ITEMS_SOURCE,
                "text": f"Reauthorize or reconnect {attention} connections so recommendations use current data.",
                "meta": meta,
            }
        elif reauthorize > 0:
            insight = {
                "source": _PLAID_ITEMS_SOURCE,
                "text": f"Reauthorize {reauthorize} connection{'s' if reauthorize != 1 else ''} so recommendations use current data.",
                "meta": meta,
            }
        elif reconnect > 0:
            insight = {
                "source": _PLAID_ITEMS_SOURCE,
                "text": f"Reconnect {reconnect} connection{'s' if reconnect != 1 else ''} so recommendations use current data.",
                "meta": meta,
            }
        elif pending > 0:
            connection_label = "connections are" if pending != 1 else "connection is"
            pronoun = "they can" if pending != 1 else "it can"
            insight = {
                "source": _PLAID_ITEMS_SOURCE,
                "text": f"{pending} {connection_label} still initializing before {pronoun} power recommendations.",
                "meta": meta,
            }
        else:
            connection_label = "connections are" if ok != 1 else "connection is"
            insight = {
                "source": _PLAID_ITEMS_SOURCE,
                "text": f"All {ok} {connection_label} ready for recommendations.",
                "meta": meta,
            }

    item_count = len(items)
    ready_note = (
        f"{reauthorize} need{'s' if reauthorize == 1 else ''} reauthorization"
        if reauthorize > 0 and reconnect == 0
        else f"{attention} need{'s' if attention == 1 else ''} attention"
        if attention > 0
        else f"{item_count} total"
    )
    latest_note = (
        f"{item_count} connection{'s' if item_count != 1 else ''} powering recommendations"
        if item_count > 0
        else "Connect a bank to start"
    )
    return {
        "connection_insight": insight,
        "stat_notes": {
            "ready_connections": _plaid_settings_note(ready_note),
            "latest_data": _plaid_settings_note(latest_note),
        },
    }


def enrich_plaid_items_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation fields to Plaid item settings data."""

    enriched = dict(data)
    raw_items = enriched.get("items", [])
    items = (
        [dict(item) for item in raw_items if isinstance(item, Mapping)]
        if isinstance(raw_items, list)
        else []
    )
    enriched["items"] = items
    enriched["settings_annotations"] = _plaid_settings_annotations_for_items(items)
    return enriched


def _onboarding_insight_payload(
    *,
    text: str,
    bullets: Sequence[str],
    meta: str,
    tone: Any,
    phase: str,
) -> dict[str, Any]:
    payload = recommendation_insight(
        source=_ONBOARDING_SOURCE,
        text=text,
        meta=meta,
        bullets=bullets,
        tone=tone,
    )
    payload["phase"] = phase
    return payload


def _onboarding_insight(
    *,
    current_phase: str,
    phases: list[dict[str, Any]],
    required_done: int,
    required_total: int,
    gate_open: bool,
    fully_onboarded: bool,
    is_demo_mode: bool,
) -> dict[str, Any]:
    phase_map = {str(phase.get("id")): phase for phase in phases if isinstance(phase, dict)}
    current = phase_map.get(current_phase, {})
    missing = current.get("missing") if isinstance(current, dict) else []
    missing_fields = [str(item) for item in missing] if isinstance(missing, list) else []
    meta = f"Step {min(required_done + 1, required_total)} of {required_total}" if required_total else "Setup"

    if gate_open:
        if not fully_onboarded:
            return _onboarding_insight_payload(
                text=(
                    "Sample data opened the dashboard; finish setup before relying on real recommendations."
                    if is_demo_mode
                    else "The dashboard is open; finish setup to tune the next recommendations."
                ),
                bullets=["CashNerd can keep coaching from here without blocking dashboard access."],
                meta="Dashboard open",
                tone="coach",
                phase=current_phase,
            )
        return _onboarding_insight_payload(
            text="Setup has enough data to start making dashboard recommendations.",
            bullets=(
                ["Sample data is active; swap in real accounts when you are ready."]
                if is_demo_mode
                else ["Open the dashboard to see your first money move."]
            ),
            meta="Setup ready",
            tone="coach",
            phase=current_phase,
        )

    if current_phase == "connect":
        if "one_month_history_or_acknowledgment" in missing_fields:
            text = "Add a month of transactions or acknowledge limited history before CashNerd starts steering moves."
            bullets = ["The chat can help import a CSV or continue with a thinner first recommendation."]
        else:
            text = "Connect one account or preview sample data so CashNerd can make the first recommendation."
            bullets = [
                "No balances or transactions are visible yet.",
                "Sample preview keeps real financial data out.",
            ]
        tone = "diagnose"
    elif current_phase == "profile":
        text = "Tell CashNerd how your income behaves so the next recommendations fit your money rhythm."
        bullets = ["Profile answers stay scoped to setup and recommendation context."]
        tone = "coach"
    elif current_phase == "focus":
        text = "Pick the first money area to steer before CashNerd builds your starting plan."
        bullets = ["Debt, spending, savings, and taxes route to different coaching moves."]
        tone = "coach"
    elif current_phase == "setup":
        text = "Confirm the starter setup once the recommended first move looks right."
        bullets = ["The dashboard opens after the setup guardrails are acknowledged."]
        tone = "coach"
    else:
        text = "Finish setup so CashNerd can turn your money data into the next action."
        bullets = []
        tone = "diagnose"

    return _onboarding_insight_payload(
        text=text,
        bullets=bullets,
        meta=meta,
        tone=tone,
        phase=current_phase,
    )


def enrich_onboarding_state_surface(
    data: Mapping[str, Any],
    *,
    insight_phases: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Attach shared recommendation fields to onboarding state data."""

    enriched = dict(data)
    phase_source = insight_phases if insight_phases is not None else enriched.get("phases", [])
    phases = (
        [dict(phase) for phase in phase_source if isinstance(phase, Mapping)]
        if isinstance(phase_source, (list, tuple))
        else []
    )
    progress = enriched.get("progress")
    progress_map = progress if isinstance(progress, Mapping) else {}
    required_done = _as_int(progress_map.get("required_done"))
    required_total = _as_int(progress_map.get("required_total", len(phases))) or len(phases)
    current_phase = str(enriched.get("current_phase") or "connect")
    enriched["onboarding_insight"] = _onboarding_insight(
        current_phase=current_phase,
        phases=phases,
        required_done=required_done,
        required_total=required_total,
        gate_open=bool(enriched.get("is_gate_open")),
        fully_onboarded=bool(enriched.get("is_fully_onboarded")),
        is_demo_mode=bool(enriched.get("is_demo_mode")),
    )
    return enriched


_GOAL_CURRENCY_METRICS = {"net_worth", "liquid_cash", "total_debt", "investments"}
_GOAL_RATE_METRICS = {"savings_rate"}
_GOAL_DEFAULT_DIRECTIONS = {
    "net_worth": "up",
    "liquid_cash": "up",
    "savings_rate": "up",
    "total_debt": "down",
    "investments": "up",
}


def _goal_format_number(value: float) -> str:
    rounded = round(value, 1)
    if rounded == int(rounded):
        return f"{int(rounded):,}"
    return f"{rounded:,.1f}"


def _goal_format_currency(value: float) -> str:
    return f"${abs(value):,.0f}"


def _goal_format_percent(value: float) -> str:
    return f"{_goal_format_number(value)}%"


def _goal_format_percentage_points(value: float) -> str:
    rounded = round(abs(value), 1)
    return f"{_goal_format_number(value)} percentage {_pluralize(1 if rounded == 1 else 2, 'point')}"


def _goal_annotation(text: str, tone: Any) -> RecommendationAnnotation:
    return recommendation_annotation(source=_GOALS_SOURCE, text=text, tone=tone)


def _goal_metric(goal: Mapping[str, Any]) -> str:
    metric = goal.get("metric")
    return str(metric) if metric else "net_worth"


def _goal_direction(goal: Mapping[str, Any]) -> str:
    direction = goal.get("direction")
    if isinstance(direction, str) and direction:
        return direction
    return _GOAL_DEFAULT_DIRECTIONS.get(_goal_metric(goal), "up")


def _goal_numeric_value(goal: Mapping[str, Any], kind: str) -> float:
    metric = _goal_metric(goal)

    if metric == "savings_rate":
        pct_key = f"{kind}_pct"
        return _safe_float(
            goal.get(pct_key) if goal.get(pct_key) is not None else goal.get(kind)
        )

    if metric in _GOAL_CURRENCY_METRICS:
        cents_key = f"{kind}_cents"
        cents = goal.get(cents_key)
        return _safe_float(cents) / 100 if cents is not None else _safe_float(goal.get(kind))

    return _safe_float(goal.get(kind))


def _goal_clamped_progress(goal: Mapping[str, Any]) -> float:
    return max(0.0, min(100.0, _safe_float(goal.get("progress_pct"))))


def _goal_on_track(goal: Mapping[str, Any]) -> bool:
    progress = _goal_clamped_progress(goal)
    if progress >= 100:
        return True

    if _goal_metric(goal) in _GOAL_RATE_METRICS:
        return progress > 0

    return goal.get("estimated_months") is not None


def _goal_remaining_to_target(goal: Mapping[str, Any]) -> float:
    current = _goal_numeric_value(goal, "current")
    target = _goal_numeric_value(goal, "target")
    if _goal_direction(goal) == "down":
        return max(0.0, current - target)
    return max(0.0, target - current)


def _goal_format_delta(goal: Mapping[str, Any], value: float) -> str:
    metric = _goal_metric(goal)
    if metric == "savings_rate":
        return _goal_format_percentage_points(value)
    if metric in _GOAL_CURRENCY_METRICS:
        return _goal_format_currency(value)
    return _goal_format_number(value)


def _goal_living_margin(goal: Mapping[str, Any]) -> LivingMargin:
    remaining = _goal_remaining_to_target(goal)
    name = str(goal.get("name") or "Goal")

    if _goal_clamped_progress(goal) >= 100:
        return living_margin(
            source=_GOALS_SOURCE,
            text=f"{name} is complete. Point the next surplus dollar at the follow-up target.",
            tone="positive",
        )

    if _goal_metric(goal) in _GOAL_RATE_METRICS and _goal_on_track(goal):
        return living_margin(
            source=_GOALS_SOURCE,
            text=(
                f"{_goal_format_delta(goal, remaining)} left. "
                "Keep the next surplus move assigned so the savings-rate gain keeps compounding."
            ),
            tone="positive",
        )

    estimated_months = goal.get("estimated_months")
    if estimated_months is not None:
        return living_margin(
            source=_GOALS_SOURCE,
            text=(
                f"{_goal_format_delta(goal, remaining)} left. "
                f"At this pace, target is about {estimated_months} months away."
            ),
            tone="positive",
        )

    return living_margin(
        source=_GOALS_SOURCE,
        text=(
            f"{_goal_format_delta(goal, remaining)} left. "
            "Add a monthly move so CashNerd can project the finish line."
        ),
        tone="concern",
    )


def _goal_stat_annotations(
    goals: list[dict[str, Any]],
) -> dict[str, RecommendationAnnotation | None]:
    goals_on_track = sum(1 for goal in goals if _goal_on_track(goal))
    off_track_count = len(goals) - goals_on_track
    average_progress = (
        sum(_goal_clamped_progress(goal) for goal in goals) / len(goals)
        if goals
        else 0.0
    )
    incomplete_goals = [goal for goal in goals if _goal_clamped_progress(goal) < 100]
    nearest_goal = (
        sorted(incomplete_goals, key=_goal_clamped_progress, reverse=True)[0]
        if incomplete_goals
        else None
    )

    goals_moving = None
    if goals:
        goals_moving = (
            _goal_annotation(
                (
                    f"{off_track_count} goal{'' if off_track_count == 1 else 's'} "
                    f"{'needs' if off_track_count == 1 else 'need'} a new monthly move before "
                    f"{'it' if off_track_count == 1 else 'they'} can get back on track."
                ),
                "warn",
            )
            if off_track_count > 0
            else _goal_annotation("Every active goal is moving. Keep surplus assigned.", "coach")
        )

    closest_win = None
    if nearest_goal is not None:
        closest_win = _goal_annotation(
            (
                f"{_goal_format_delta(nearest_goal, _goal_remaining_to_target(nearest_goal))} left for "
                f"{str(nearest_goal.get('name') or 'Goal')} at the current pace."
            ),
            "coach" if _goal_on_track(nearest_goal) else "warn",
        )
    elif goals:
        closest_win = _goal_annotation(
            "All tracked goals are complete. Add the next target.", "coach"
        )

    return {
        "closest_win": closest_win,
        "goals_moving": goals_moving,
        "progress_banked": (
            _goal_annotation(
                (
                    f"{_goal_format_percent(average_progress)} average progress across {len(goals)} active "
                    f"goal{'' if len(goals) == 1 else 's'}."
                ),
                "pattern",
            )
            if goals
            else None
        ),
    }


def _goal_insight(goals: list[dict[str, Any]]) -> RecommendationInsight | None:
    if not goals:
        return None

    goals_on_track = sum(1 for goal in goals if _goal_on_track(goal))
    off_track_count = len(goals) - goals_on_track
    metrics = {_goal_metric(goal) for goal in goals}
    incomplete_goals = [goal for goal in goals if _goal_clamped_progress(goal) < 100]
    best = (
        sorted(incomplete_goals, key=_goal_clamped_progress, reverse=True)[0]
        if incomplete_goals
        else None
    )
    total = len(goals)
    text = (
        f"Keep momentum on {goals_on_track} of {total} active "
        f"{_pluralize(total, 'goal')}."
    )

    if best is not None:
        name = str(best.get("name") or "Goal")
        text += f" Push surplus toward {name}: {_goal_clamped_progress(best):.0f}% complete"
        estimated_months = best.get("estimated_months")
        text += f" with about {estimated_months} months to go." if estimated_months is not None else "."

    return recommendation_insight(
        source=_GOALS_SOURCE,
        text=text,
        meta=(
            f"{total} active {_pluralize(total, 'goal')} across "
            f"{len(metrics)} {_pluralize(len(metrics), 'metric')}"
        ),
        bullets=(
            [
                f"{_goal_format_delta(best, _goal_remaining_to_target(best))} left for {str(best.get('name') or 'Goal')}.",
                (
                    f"{off_track_count} {_pluralize(off_track_count, 'goal')} "
                    f"{'needs' if off_track_count == 1 else 'need'} a new monthly move before "
                    "CashNerd can project the finish line."
                    if off_track_count > 0
                    else "Keep surplus pointed at the closest win before adding a new target."
                ),
            ]
            if best is not None
            else ["All active goals are complete. Pick the next target before surplus drifts."]
        ),
        tone="warn" if off_track_count > 0 else "coach",
    )


def enrich_goal_status_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation fields to goal status data."""

    enriched = dict(data)
    raw_goals = enriched.get("goals", [])
    goals = (
        [dict(goal) for goal in raw_goals if isinstance(goal, dict)]
        if isinstance(raw_goals, list)
        else []
    )

    for goal in goals:
        goal["living_margin"] = _goal_living_margin(goal)

    enriched["goals"] = goals
    enriched["stat_annotations"] = _goal_stat_annotations(goals)
    enriched["goal_insight"] = _goal_insight(goals)
    return enriched


def _format_intro_apr_date(value: Any) -> str:
    raw = str(value or "")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        return raw
    return f"{parsed:%b} {parsed.day}, {parsed.year}"


def _debt_annotation(text: str, tone: Any) -> RecommendationAnnotation:
    return recommendation_annotation(source=_DEBT_SOURCE, text=text, tone=tone)


def _debt_card_apr(card: Mapping[str, Any]) -> float | None:
    return _as_float_or_none(card.get("apr"))


def _highest_apr_debt_card(cards: list[dict[str, Any]]) -> dict[str, Any] | None:
    known_apr_cards = [card for card in cards if _debt_card_apr(card) is not None]
    if not known_apr_cards:
        return None
    return max(known_apr_cards, key=lambda card: _debt_card_apr(card) or 0)


def _debt_card_living_margin(
    card: dict[str, Any],
    highest_apr_card: dict[str, Any] | None,
) -> LivingMargin | None:
    monthly_interest_cents = _as_int(card.get("monthly_interest_cents"))
    apr = _debt_card_apr(card)
    utilization_pct = _as_float_or_none(card.get("utilization_pct"))

    if (
        highest_apr_card is not None
        and highest_apr_card.get("card_id") == card.get("card_id")
        and monthly_interest_cents > 0
    ):
        return living_margin(
            source=_DEBT_SOURCE,
            text=(
                f"{format_cents(monthly_interest_cents)}/mo interest here. "
                "Put extra dollars here before lower-rate balances."
            ),
            tone="concern",
        )

    if apr == 0 and card.get("intro_apr_end_date"):
        return living_margin(
            source=_DEBT_SOURCE,
            text=(
                f"0% APR through {_format_intro_apr_date(card.get('intro_apr_end_date'))}. "
                "Keep payoff scheduled before the promo ends."
            ),
            tone="positive",
        )

    if utilization_pct is not None and utilization_pct > 50:
        return living_margin(
            source=_DEBT_SOURCE,
            text=(
                f"{_format_percent(utilization_pct)}% utilization. "
                "Paydown here can also relieve credit pressure."
            ),
            tone="concern",
        )

    if monthly_interest_cents > 0:
        return living_margin(
            source=_DEBT_SOURCE,
            text=f"{format_cents(monthly_interest_cents)}/mo interest leak to include in the payoff comparison.",
            tone="neutral",
        )

    return None


def _build_debt_stat_annotations(
    data: Mapping[str, Any],
) -> dict[str, RecommendationAnnotation | None]:
    total_balance_cents = _as_int(data.get("total_balance_cents"))
    total_monthly_interest_cents = _as_int(data.get("total_monthly_interest_cents"))
    weighted_avg_apr = _as_float_or_none(data.get("weighted_avg_apr"))

    if weighted_avg_apr is None:
        apr = _debt_annotation(
            "APR data is missing; add rates before trusting the payoff order.",
            "pattern",
        )
    elif weighted_avg_apr >= 20:
        apr = _debt_annotation(
            f"{_format_percent(weighted_avg_apr)}% weighted APR; every extra dollar needs a target.",
            "warn",
        )
    elif weighted_avg_apr > 0:
        apr = _debt_annotation(
            f"{_format_percent(weighted_avg_apr)}% weighted APR; compare avalanche vs snowball before splitting extra cash.",
            "pattern",
        )
    else:
        apr = _debt_annotation(
            "0% weighted APR; keep promo dates current before the rate resets.", "coach"
        )

    return {
        "apr": apr,
        "balance": (
            _debt_annotation(
                f"{format_cents(total_balance_cents)} principal to route through a payoff order.",
                "pattern",
            )
            if total_balance_cents > 0
            else None
        ),
        "interest": (
            _debt_annotation(
                f"{format_cents(total_monthly_interest_cents)}/mo disappears before principal; "
                "attack the highest APR balance first.",
                "warn",
            )
            if total_monthly_interest_cents > 0
            else _debt_annotation(
                "$0/mo interest right now; protect the promo or grace window.", "coach"
            )
        ),
    }


def _debt_insight(
    cards: list[dict[str, Any]], data: Mapping[str, Any]
) -> RecommendationInsight:
    liability_count = len(cards)
    meta = (
        f"{liability_count} active {_pluralize(liability_count, 'liability', 'liabilities')} "
        f"across {liability_count} {_pluralize(liability_count, 'account')}"
    )
    total_monthly_interest_cents = _as_int(data.get("total_monthly_interest_cents"))
    weighted_avg_apr = _as_float_or_none(data.get("weighted_avg_apr"))
    known_apr_cards = sorted(
        [card for card in cards if _debt_card_apr(card) is not None],
        key=lambda card: _debt_card_apr(card) or 0,
        reverse=True,
    )
    highest_apr_card = known_apr_cards[0] if known_apr_cards else None
    lowest_apr_card = known_apr_cards[-1] if known_apr_cards else None

    if total_monthly_interest_cents <= 0:
        return recommendation_insight(
            source=_DEBT_SOURCE,
            text=(
                "Protect the payoff window: this debt is not leaking interest right now. "
                "Keep payoff dates visible before APRs reset."
            ),
            meta=meta,
            bullets=[
                "Keep promo dates current so the plan does not miss a reset.",
                "Use the simulator before adding new debt or changing payment order.",
            ],
            tone="coach",
        )

    base_text = f"Cut the interest leak first: this debt costs {format_cents(total_monthly_interest_cents)}/mo in interest."
    missing_apr = recommendation_insight(
        source=_DEBT_SOURCE,
        text=base_text,
        meta=meta,
        bullets=["Add APRs so the payoff order can stop guessing."],
        tone="diagnose",
    )

    if weighted_avg_apr is None or highest_apr_card is None:
        return missing_apr

    highest_apr = _debt_card_apr(highest_apr_card)
    if highest_apr is None:
        return missing_apr

    highest_label = str(highest_apr_card.get("label") or "the highest APR card")
    lowest_label = str((lowest_apr_card or {}).get("label") or "the lowest APR card")
    lowest_apr = _debt_card_apr(lowest_apr_card or {}) or 0
    apr_spread = highest_apr - lowest_apr

    return recommendation_insight(
        source=_DEBT_SOURCE,
        text=f"{base_text} Start with {highest_label} at {highest_apr:.2f}% APR.",
        meta=meta,
        bullets=[
            f"{highest_label} carries the highest APR pressure.",
            (
                f"{_format_percent(apr_spread)} point spread vs {lowest_label}; avalanche math should lead."
                if apr_spread >= 3
                else "APR spread is tight enough to compare avalanche and snowball before choosing."
            ),
        ],
        tone="warn" if highest_apr >= 15 else "diagnose",
    )


def enrich_debt_dashboard_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation surface fields to debt dashboard data."""

    enriched = dict(data)
    raw_cards = enriched.get("cards", [])
    cards = (
        [dict(card) for card in raw_cards if isinstance(card, dict)]
        if isinstance(raw_cards, list)
        else []
    )
    highest_apr_card = _highest_apr_debt_card(cards)

    for card in cards:
        card["living_margin"] = _debt_card_living_margin(card, highest_apr_card)

    enriched["cards"] = cards
    enriched["stat_annotations"] = _build_debt_stat_annotations(enriched)
    enriched["debt_insight"] = _debt_insight(cards, enriched)
    return enriched


def _spending_annotation(text: str, tone: Any) -> RecommendationAnnotation:
    return recommendation_annotation(source=_SPENDING_SOURCE, text=text, tone=tone)


def _spending_category_month_cents(category: Mapping[str, Any], month: str) -> int:
    months_cents = category.get("months_cents")
    if isinstance(months_cents, dict) and month in months_cents:
        return int(round(_safe_float(months_cents.get(month))))

    months = category.get("months")
    if isinstance(months, dict):
        return int(round(_safe_float(months.get(month)) * 100))

    return 0


def _spending_category_average_cents(category: Mapping[str, Any]) -> int:
    if "average_cents" in category:
        return int(round(_safe_float(category.get("average_cents"))))
    return int(round(_safe_float(category.get("average")) * 100))


def _spending_trend_label(trend: Any) -> str:
    if trend == "\u2191":
        return "rising"
    if trend == "\u2193":
        return "falling"
    return "steady"


def _spending_category_living_margin(
    category: dict[str, Any],
    latest_month: str | None,
) -> LivingMargin | None:
    if not latest_month:
        return None

    current_cents = _spending_category_month_cents(category, latest_month)
    average_cents = _spending_category_average_cents(category)
    delta_cents = current_cents - average_cents
    abs_delta = format_cents(delta_cents)
    category_name = str(category.get("category") or "This category")
    trend = category.get("trend")

    if trend == "\u2191" and delta_cents > 0:
        return living_margin(
            source=_SPENDING_SOURCE,
            text=f"{abs_delta} above typical. Check {category_name} before it becomes the baseline.",
            tone="concern",
        )

    if trend == "\u2193" and delta_cents < 0:
        return living_margin(
            source=_SPENDING_SOURCE,
            text=f"{abs_delta} below typical. Keep what changed working this month.",
            tone="positive",
        )

    if average_cents > 0:
        within_typical = abs(delta_cents) <= max(500, average_cents * 0.1)
        return living_margin(
            source=_SPENDING_SOURCE,
            text=(
                f"{format_cents(current_cents)} is close to typical. No immediate spending move here."
                if within_typical
                else f"{abs_delta} from typical. Worth checking if this is planned."
            ),
            tone="neutral" if within_typical else "concern",
        )

    return None


def _build_spending_stat_annotations(
    data: Mapping[str, Any],
    categories: list[dict[str, Any]],
) -> dict[str, RecommendationAnnotation | None]:
    months = [str(month) for month in data.get("months", [])]
    latest_month = months[-1] if months else None
    prior_month = months[-2] if len(months) >= 2 else None
    totals_cents = (
        data.get("totals_cents") if isinstance(data.get("totals_cents"), dict) else {}
    )

    latest_total_cents = (
        int(round(_safe_float(totals_cents.get(latest_month)))) if latest_month else 0
    )
    prior_total_cents = (
        int(round(_safe_float(totals_cents.get(prior_month)))) if prior_month else 0
    )
    mom_delta_cents = latest_total_cents - prior_total_cents

    total_values = [
        int(round(_safe_float(totals_cents.get(month)))) for month in months
    ]
    average_cents = (
        int(round(sum(total_values) / len(total_values)))
        if total_values
        else int(round(_safe_float(data.get("grand_average")) * 100))
    )
    top_category = (
        max(
            categories,
            key=lambda category: _spending_category_month_cents(category, latest_month),
        )
        if latest_month and categories
        else None
    )

    current_spend = None
    if mom_delta_cents > 0:
        current_spend = _spending_annotation(
            f"{format_cents(mom_delta_cents)} more than last month; start with the biggest category driver.",
            "warn",
        )
    elif mom_delta_cents < 0:
        current_spend = _spending_annotation(
            f"{format_cents(mom_delta_cents)} less than last month; protect the lower run rate.",
            "coach",
        )

    top_category_annotation = None
    if top_category and latest_month:
        current_cents = _spending_category_month_cents(top_category, latest_month)
        trend_label = _spending_trend_label(top_category.get("trend"))
        if trend_label == "rising":
            tone = "warn"
        elif trend_label == "falling":
            tone = "coach"
        else:
            tone = "pattern"
        top_category_annotation = _spending_annotation(
            f"{format_cents(current_cents)} this month; {trend_label} category to steer first.",
            tone,
        )
    elif categories:
        top_category_annotation = _spending_annotation(
            f"{len(categories)} categories available for pattern checks.",
            "pattern",
        )

    return {
        "current_spend": current_spend,
        "typical_month": (
            _spending_annotation(
                f"{format_cents(average_cents)}/mo is the baseline for spotting unusual changes.",
                "pattern",
            )
            if average_cents > 0
            else None
        ),
        "top_category": top_category_annotation,
    }


def _spending_history_meta(months: list[str]) -> str:
    return (
        f"{len(months)} {'month' if len(months) == 1 else 'months'} of spending history"
    )


def _spending_insight(
    categories: list[dict[str, Any]], months: list[str]
) -> dict[str, Any]:
    latest_month = months[-1] if months else None
    meta = _spending_history_meta(months)

    if not latest_month:
        return {
            "source": _SPENDING_SOURCE,
            "text": "Add transactions to see which spending categories need steering.",
            "meta": meta,
            "tone": "diagnose",
        }

    rising_categories = [
        category for category in categories if category.get("trend") == "\u2191"
    ]
    highest_rising = (
        max(
            rising_categories,
            key=lambda category: _spending_category_month_cents(category, latest_month),
        )
        if rising_categories
        else None
    )
    if highest_rising:
        latest_cents = _spending_category_month_cents(highest_rising, latest_month)
        average_cents = _spending_category_average_cents(highest_rising)
        above_average_cents = max(0, latest_cents - average_cents)
        category_name = str(highest_rising.get("category") or "this category")
        bullets = (
            [
                f"{category_name} is {format_cents(above_average_cents)} above its tracked average.",
                "Start there before the higher run rate becomes the new baseline.",
            ]
            if above_average_cents > 0
            else [
                "This is the highest rising category in the current spending history."
            ]
        )
        return recommendation_insight(
            source=_SPENDING_SOURCE,
            text=(
                f"Review {category_name}: it is trending up and averaging "
                f"{format_cents(average_cents)}/mo across tracked months."
            ),
            meta=meta,
            bullets=bullets,
            tone="warn",
        )

    declining_categories = [
        category for category in categories if category.get("trend") == "\u2193"
    ]
    highest_declining = (
        max(declining_categories, key=_spending_category_average_cents)
        if declining_categories
        else None
    )
    if highest_declining:
        category_name = str(highest_declining.get("category") or "this category")
        average_cents = _spending_category_average_cents(highest_declining)
        return recommendation_insight(
            source=_SPENDING_SOURCE,
            text=(
                f"Protect the savings in {category_name}: it is trending down and averaging "
                f"{format_cents(average_cents)}/mo."
            ),
            meta=meta,
            bullets=[
                f"{category_name} is the clearest lower-spend category in this window.",
                "Keep the change deliberate so the savings do not become a one-off dip.",
            ],
            tone="coach",
        )

    return recommendation_insight(
        source=_SPENDING_SOURCE,
        text="No spending category needs action right now.",
        meta=meta,
        bullets=[
            "Use the category table to spot new merchant spikes before they become recurring habits."
        ],
        tone="diagnose",
    )


def enrich_spending_trends_surface(data: Mapping[str, Any]) -> dict[str, Any]:
    """Attach shared recommendation surface fields to spending trend data."""

    enriched = dict(data)
    raw_categories = enriched.get("categories", [])
    categories = (
        [dict(category) for category in raw_categories if isinstance(category, Mapping)]
        if isinstance(raw_categories, list)
        else []
    )
    months = [str(month) for month in enriched.get("months", [])]
    latest_month = months[-1] if months else None

    for category in categories:
        category["living_margin"] = _spending_category_living_margin(
            category, latest_month
        )

    enriched["categories"] = categories
    enriched["stat_annotations"] = _build_spending_stat_annotations(
        enriched, categories
    )
    enriched["spending_insight"] = _spending_insight(categories, months)
    return enriched
