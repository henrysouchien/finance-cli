from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from ..categorizer import normalize_description
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

_AUTO_CATEGORY_SOURCES = frozenset(
    {
        "ai",
        "auto_prefix",
        "category_mapping",
        "keyword_rule",
        "plaid",
        "vendor_memory",
    }
)
_K1_MIN_UNCATEGORIZED_COUNT = 10
_K1_IMPORT_SETTLE_WINDOW = timedelta(hours=1)
_K2_MIN_RECAT_COUNT = 3
_K2_MIN_CATEGORY_SHARE_PERCENT = 80
_K3_MIN_MANUAL_COUNT = 5
_K3_LOOKBACK_DAYS = 7
_K3_K2_SUPPRESSION_DAYS = 14
_K4_MIN_MERCHANT_COUNT = 5
_K4_LOOKBACK_DAYS = 7
_K4_AUTO_CATEGORY_SOURCES = frozenset(
    {"ai", "category_mapping", "keyword_rule", "plaid"}
)
_K5_MIN_OVERRIDE_DATES = 2


@dataclass(frozen=True)
class _K1Candidate:
    count: int
    oldest_date: date
    historical_auto_coverage_pct: int | None

    def days_old(self, as_of: date) -> int:
        return max(0, (as_of - self.oldest_date).days)


@dataclass(frozen=True)
class _K2Candidate:
    pattern: str
    display_description: str
    category_id: str
    category_name: str
    use_type: str
    total_count: int
    target_count: int
    first_date: date
    latest_date: date

    @property
    def touches_per_month(self) -> int:
        month_span = (self.latest_date.year - self.first_date.year) * 12
        month_span += self.latest_date.month - self.first_date.month + 1
        return max(1, self.total_count // max(month_span, 1))


@dataclass(frozen=True)
class _K3Rule:
    pattern: str
    display_description: str
    category_id: str
    category_name: str
    use_type: str
    count: int
    latest_date: date

    def action_params(self) -> dict[str, str]:
        return {
            "pattern": self.pattern,
            "category": self.category_name,
            "use_type": self.use_type,
        }


@dataclass(frozen=True)
class _K3Candidate:
    rules: tuple[_K3Rule, ...]
    transaction_count: int

    @property
    def merchant_count(self) -> int:
        return len(self.rules)


@dataclass(frozen=True)
class _K4Merchant:
    pattern: str
    display_description: str
    category_id: str
    category_name: str
    use_type: str
    category_sources: tuple[str, ...]
    transaction_ids: tuple[str, ...]
    latest_date: date

    def action_item(self) -> dict[str, object]:
        return {
            "decision": "confirm",
            "merchant": self.display_description,
            "pattern": self.pattern,
            "category": self.category_name,
            "use_type": self.use_type,
            "txn_ids": list(self.transaction_ids),
        }


@dataclass(frozen=True)
class _K4Candidate:
    merchants: tuple[_K4Merchant, ...]

    @property
    def merchant_count(self) -> int:
        return len(self.merchants)


@dataclass(frozen=True)
class _VendorMemoryRule:
    id: str
    pattern: str
    category_id: str
    category_name: str
    use_type: str


@dataclass(frozen=True)
class _K5Override:
    rule: _VendorMemoryRule
    description: str
    new_category_id: str
    new_category_name: str
    txn_date: date


@dataclass(frozen=True)
class _K5Candidate:
    rule: _VendorMemoryRule
    display_description: str
    new_category_id: str
    new_category_name: str
    override_count: int
    distinct_dates: int
    latest_date: date


@dataclass(frozen=True)
class _K2Txn:
    description: str
    category_id: str
    category_name: str
    use_type: str | None
    txn_date: date


@dataclass(frozen=True)
class _K4Txn:
    id: str
    description: str
    category_id: str
    category_name: str
    category_source: str
    use_type: str | None
    txn_date: date


def _parse_txn_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _parse_db_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None)


def _recent_import_batch_exists(conn: sqlite3.Connection, now: datetime) -> bool:
    row = conn.execute(
        "SELECT MAX(created_at) AS last_import_at FROM import_batches"
    ).fetchone()
    last_import_at = _parse_db_datetime(row["last_import_at"] if row else None)
    if last_import_at is None:
        return False
    return last_import_at >= now.replace(tzinfo=None) - _K1_IMPORT_SETTLE_WINDOW


def _historical_auto_coverage_pct(conn: sqlite3.Connection) -> int | None:
    rows = conn.execute(
        """
        SELECT category_source, COUNT(*) AS count
          FROM transactions
         WHERE is_active = 1
           AND category_id IS NOT NULL
           AND category_source IS NOT NULL
         GROUP BY category_source
        """
    ).fetchall()
    total = sum(int(row["count"] or 0) for row in rows)
    if total <= 0:
        return None
    auto_total = sum(
        int(row["count"] or 0)
        for row in rows
        if str(row["category_source"]) in _AUTO_CATEGORY_SOURCES
    )
    return round(auto_total * 100 / total)


def _find_k1_candidate(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> _K1Candidate | None:
    if _recent_import_batch_exists(conn, now):
        return None

    row = conn.execute(
        """
        SELECT COUNT(*) AS count,
               MIN(date) AS oldest_date
          FROM transactions
         WHERE is_active = 1
           AND is_reviewed = 0
           AND category_id IS NULL
        """
    ).fetchone()
    count = int(row["count"] or 0) if row else 0
    if count < _K1_MIN_UNCATEGORIZED_COUNT:
        return None

    oldest_date = _parse_txn_date(row["oldest_date"])
    if oldest_date is None:
        return None

    return _K1Candidate(
        count=count,
        oldest_date=oldest_date,
        historical_auto_coverage_pct=_historical_auto_coverage_pct(conn),
    )


def _candidate_use_type(rows: list[_K2Txn]) -> str:
    normalized = [
        row.use_type if row.use_type in {"Business", "Personal"} else "Any"
        for row in rows
    ]
    unique = set(normalized)
    if len(unique) == 1:
        return str(next(iter(unique)))
    return "Any"


def _has_applicable_vendor_memory(
    conn: sqlite3.Connection,
    *,
    pattern: str,
    use_type: str,
) -> bool:
    if use_type in {"Business", "Personal"}:
        rows = conn.execute(
            """
            SELECT 1
              FROM vendor_memory
             WHERE (description_pattern = ? OR ? LIKE description_pattern || '%')
               AND use_type IN (?, 'Any')
             LIMIT 1
            """,
            (pattern, pattern, use_type),
        ).fetchone()
    else:
        rows = conn.execute(
            """
            SELECT 1
              FROM vendor_memory
             WHERE (description_pattern = ? OR ? LIKE description_pattern || '%')
               AND use_type = 'Any'
             LIMIT 1
            """,
            (pattern, pattern),
        ).fetchone()
    return rows is not None


def _best_matching_vendor_memory_rule(
    conn: sqlite3.Connection,
    *,
    pattern: str,
    use_type: str | None,
) -> _VendorMemoryRule | None:
    exact_rows = _fetch_vendor_memory_rule_rows(
        conn,
        pattern=pattern,
        use_type=use_type,
        exact_only=True,
    )
    if exact_rows:
        winner = sorted(
            exact_rows,
            key=lambda row: (
                0 if row["use_type"] == use_type else 1,
                -int(row["priority"] or 0),
                -float(row["confidence"] or 0.0),
                -int(row["match_count"] or 0),
                str(row["id"]),
            ),
        )[0]
        return _rule_from_row(winner)

    prefix_rows = _fetch_vendor_memory_rule_rows(
        conn,
        pattern=pattern,
        use_type=use_type,
        exact_only=False,
    )
    if not prefix_rows:
        return None

    max_len = max(len(str(row["description_pattern"] or "")) for row in prefix_rows)
    scoped = [
        row
        for row in prefix_rows
        if len(str(row["description_pattern"] or "")) == max_len
        and pattern.startswith(str(row["description_pattern"] or ""))
    ]
    if not scoped:
        return None

    max_priority = max(int(row["priority"] or 0) for row in scoped)
    scoped = [row for row in scoped if int(row["priority"] or 0) == max_priority]
    max_conf = max(float(row["confidence"] or 0.0) for row in scoped)
    scoped = [row for row in scoped if float(row["confidence"] or 0.0) == max_conf]
    max_match_count = max(int(row["match_count"] or 0) for row in scoped)
    scoped = [
        row for row in scoped if int(row["match_count"] or 0) == max_match_count
    ]
    if len(scoped) != 1:
        return None
    return _rule_from_row(scoped[0])


def _fetch_vendor_memory_rule_rows(
    conn: sqlite3.Connection,
    *,
    pattern: str,
    use_type: str | None,
    exact_only: bool,
) -> list[sqlite3.Row]:
    params: list[object] = [pattern]
    where = ["vm.is_enabled = 1", "vm.category_id IS NOT NULL"]
    if exact_only:
        where.append("vm.description_pattern = ?")
    else:
        where.append("? LIKE vm.description_pattern || '%'")

    if use_type in {"Business", "Personal"}:
        where.append("vm.use_type IN (?, 'Any')")
        params.append(use_type)
    else:
        where.append("vm.use_type = 'Any'")

    rows = conn.execute(
        f"""
        SELECT vm.id,
               vm.description_pattern,
               vm.category_id,
               vm.use_type,
               vm.priority,
               vm.confidence,
               vm.match_count,
               c.name AS category_name
          FROM vendor_memory vm
          JOIN categories c ON c.id = vm.category_id
         WHERE {' AND '.join(where)}
        """,
        tuple(params),
    ).fetchall()

    if use_type in {"Business", "Personal"}:
        exact_use = [row for row in rows if row["use_type"] == use_type]
        if exact_use:
            return exact_use
    return rows


def _rule_from_row(row: sqlite3.Row) -> _VendorMemoryRule:
    return _VendorMemoryRule(
        id=str(row["id"]),
        pattern=str(row["description_pattern"]),
        category_id=str(row["category_id"]),
        category_name=str(row["category_name"]),
        use_type=str(row["use_type"]),
    )


def _logged_k2_memory_patterns(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> set[str]:
    cutoff = now.replace(tzinfo=None) - timedelta(days=_K3_K2_SUPPRESSION_DAYS)
    rows = conn.execute(
        """
        SELECT payload
          FROM intervention_log
         WHERE pattern_id = 'K-2'
           AND surface <> 'cli'
           AND fired_at >= ?
        """,
        (cutoff.strftime("%Y-%m-%d %H:%M:%S"),),
    ).fetchall()

    patterns: set[str] = set()
    for row in rows:
        try:
            payload = json.loads(str(row["payload"] or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        action = payload.get("action")
        if not isinstance(action, dict):
            continue
        params = action.get("params")
        if not isinstance(params, dict):
            continue
        pattern = params.get("pattern")
        if not isinstance(pattern, str):
            continue
        normalized = normalize_description(pattern)
        if normalized:
            patterns.add(normalized)
    return patterns


def _format_k3_rule_list(rules: tuple[_K3Rule, ...]) -> str:
    preview = [
        f"{rule.display_description} as {rule.category_name}" for rule in rules[:5]
    ]
    remaining = len(rules) - len(preview)
    if remaining > 0:
        preview.append(f"and {remaining} more")
    return "; ".join(preview)


def _format_k4_merchant_list(merchants: tuple[_K4Merchant, ...]) -> str:
    preview = [
        f"{merchant.display_description} as {merchant.category_name}"
        for merchant in merchants[:5]
    ]
    remaining = len(merchants) - len(preview)
    if remaining > 0:
        preview.append(f"and {remaining} more")
    return "; ".join(preview)


def _looks_like_k2_recat_group(txns: list[_K2Txn]) -> bool:
    if len(txns) < _K2_MIN_RECAT_COUNT:
        return False
    category_counts = Counter(txn.category_id for txn in txns)
    _category_id, target_count = category_counts.most_common(1)[0]
    return target_count * 100 >= len(txns) * _K2_MIN_CATEGORY_SHARE_PERCENT


def _prior_merchant_patterns(conn: sqlite3.Connection, *, before_date: date) -> set[str]:
    rows = conn.execute(
        """
        SELECT description
          FROM transactions
         WHERE is_active = 1
           AND description IS NOT NULL
           AND date < ?
        """,
        (before_date.isoformat(),),
    ).fetchall()
    return {
        pattern
        for row in rows
        if (pattern := normalize_description(str(row["description"] or "")))
    }


@register_pattern(
    id="K-1",
    move=Move.PRESCRIBE,
    tiers=(1, 3),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=7),
    tool="cat_auto_categorize",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.IMPLEMENT,
        CFPProcessStep.MONITOR,
    ),
)
def evaluate_k1_uncategorized_pileup(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_k1_candidate(conn, now=ctx.now)
    if candidate is None:
        return None

    days_old = candidate.days_old(ctx.now.date())
    history_phrase = (
        "Historically, "
        f"{candidate.historical_auto_coverage_pct}% of categorized transactions "
        "came from automatic sources."
        if candidate.historical_auto_coverage_pct is not None
        else (
            "No historical automatic categorization coverage yet; the dry-run "
            "preview will show what can be handled."
        )
    )
    return Intervention(
        pattern_id="K-1",
        move=Move.PRESCRIBE,
        tiers=(1, 3),
        priority=Priority.MEDIUM,
        headline=(
            f"{candidate.count} uncategorized transactions piling up - oldest is "
            f"{days_old} days ago. Want me to run auto-categorize?"
        ),
        detail_bullets=(
            "Only active, unreviewed uncategorized transactions are counted.",
            history_phrase,
            (
                "The action opens a dry-run preview first; committing requires "
                "explicit approval."
            ),
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Preview auto-categorize",
            tool="cat_auto_categorize",
            params={"dry_run": True, "ai": False},
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("K-1"),
    )


def _find_k5_candidate(conn: sqlite3.Connection) -> _K5Candidate | None:
    rows = conn.execute(
        """
        SELECT t.date,
               t.description,
               t.category_id,
               t.use_type,
               c.name AS category_name
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.category_source = 'user'
           AND t.category_id IS NOT NULL
           AND t.description IS NOT NULL
         ORDER BY t.date ASC, t.id ASC
        """
    ).fetchall()

    grouped: dict[tuple[str, str], list[_K5Override]] = defaultdict(list)
    for row in rows:
        pattern = normalize_description(str(row["description"] or ""))
        if not pattern:
            continue
        txn_date = _parse_txn_date(row["date"])
        if txn_date is None:
            continue
        use_type = (
            row["use_type"]
            if row["use_type"] in {"Business", "Personal"}
            else None
        )
        rule = _best_matching_vendor_memory_rule(
            conn,
            pattern=pattern,
            use_type=use_type,
        )
        if rule is None:
            continue
        new_category_id = str(row["category_id"])
        if new_category_id == rule.category_id:
            continue
        grouped[(rule.id, new_category_id)].append(
            _K5Override(
                rule=rule,
                description=str(row["description"]),
                new_category_id=new_category_id,
                new_category_name=str(row["category_name"]),
                txn_date=txn_date,
            )
        )

    candidates: list[_K5Candidate] = []
    for overrides in grouped.values():
        dates = {override.txn_date for override in overrides}
        if len(dates) < _K5_MIN_OVERRIDE_DATES:
            continue
        latest = max(overrides, key=lambda override: override.txn_date)
        candidates.append(
            _K5Candidate(
                rule=latest.rule,
                display_description=latest.description,
                new_category_id=latest.new_category_id,
                new_category_name=latest.new_category_name,
                override_count=len(overrides),
                distinct_dates=len(dates),
                latest_date=latest.txn_date,
            )
        )

    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda candidate: (
            -candidate.distinct_dates,
            -candidate.override_count,
            -candidate.latest_date.toordinal(),
            candidate.rule.pattern,
        ),
    )[0]


@register_pattern(
    id="K-5",
    move=Move.WARN,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    tool="cat_memory_add",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.IMPLEMENT,
        CFPProcessStep.MONITOR,
    ),
)
def evaluate_k5_stale_rule_override(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_k5_candidate(conn)
    if candidate is None:
        return None

    return Intervention(
        pattern_id="K-5",
        move=Move.WARN,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Your rule for '{candidate.rule.pattern}' says "
            f"{candidate.rule.category_name}, but you keep changing it to "
            f"{candidate.new_category_name}. The rule looks stale."
        ),
        detail_bullets=(
            (
                f"{candidate.override_count} overrides across "
                f"{candidate.distinct_dates} different dates."
            ),
            f"Latest matching transaction: {candidate.display_description}.",
            "Updating the rule will change future matches, not past transactions.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Update vendor rule",
            tool="cat_memory_add",
            params={
                "pattern": candidate.rule.pattern,
                "category": candidate.new_category_name,
                "use_type": candidate.rule.use_type,
            },
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("K-5"),
    )


def _find_k2_candidate(conn: sqlite3.Connection) -> _K2Candidate | None:
    rows = conn.execute(
        """
        SELECT t.date,
               t.description,
               t.category_id,
               t.use_type,
               c.name AS category_name
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.category_source = 'user'
           AND t.category_id IS NOT NULL
           AND t.description IS NOT NULL
         ORDER BY t.date ASC, t.id ASC
        """
    ).fetchall()

    grouped: dict[str, list[_K2Txn]] = defaultdict(list)
    for row in rows:
        pattern = normalize_description(str(row["description"] or ""))
        if not pattern:
            continue
        txn_date = _parse_txn_date(row["date"])
        if txn_date is None:
            continue
        grouped[pattern].append(
            _K2Txn(
                description=str(row["description"]),
                category_id=str(row["category_id"]),
                category_name=str(row["category_name"]),
                use_type=None if row["use_type"] is None else str(row["use_type"]),
                txn_date=txn_date,
            )
        )

    candidates: list[_K2Candidate] = []
    for pattern, txns in grouped.items():
        total_count = len(txns)
        if total_count < _K2_MIN_RECAT_COUNT:
            continue

        category_counts = Counter(txn.category_id for txn in txns)
        category_id, target_count = category_counts.most_common(1)[0]
        if target_count * 100 < total_count * _K2_MIN_CATEGORY_SHARE_PERCENT:
            continue

        target_rows = [txn for txn in txns if txn.category_id == category_id]
        latest_target = max(target_rows, key=lambda txn: txn.txn_date)
        use_type = _candidate_use_type(target_rows)
        if _has_applicable_vendor_memory(conn, pattern=pattern, use_type=use_type):
            continue

        dates = [txn.txn_date for txn in txns]
        candidates.append(
            _K2Candidate(
                pattern=pattern,
                display_description=latest_target.description,
                category_id=category_id,
                category_name=latest_target.category_name,
                use_type=use_type,
                total_count=total_count,
                target_count=target_count,
                first_date=min(dates),
                latest_date=max(dates),
            )
        )

    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda candidate: (
            -candidate.target_count,
            -candidate.total_count,
            -candidate.latest_date.toordinal(),
            candidate.pattern,
        ),
    )[0]


@register_pattern(
    id="K-2",
    move=Move.PATTERN_CATCH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=14),
    tool="cat_memory_add",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.DEVELOP,
        CFPProcessStep.IMPLEMENT,
    ),
)
def evaluate_k2_repeated_recategorization(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_k2_candidate(conn)
    if candidate is None:
        return None

    target_share_pct = round(candidate.target_count * 100 / candidate.total_count)
    return Intervention(
        pattern_id="K-2",
        move=Move.PATTERN_CATCH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"You've recategorized '{candidate.display_description}' "
            f"{candidate.total_count} times - {target_share_pct}% to "
            f"{candidate.category_name}. Want me to remember that?"
        ),
        detail_bullets=(
            f"Normalized vendor pattern: {candidate.pattern}",
            (
                f"Most common category: {candidate.category_name} "
                f"({candidate.target_count}/{candidate.total_count} fixes)."
            ),
            (
                f"Estimated avoided fixes: about "
                f"{candidate.touches_per_month}/month."
            ),
            "No applicable vendor-memory rule currently covers this pattern.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Remember this category",
            tool="cat_memory_add",
            params={
                "pattern": candidate.pattern,
                "category": candidate.category_name,
                "use_type": candidate.use_type,
            },
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("K-2"),
    )


def _find_k3_candidate(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> _K3Candidate | None:
    as_of = now.date()
    lookback_start = as_of - timedelta(days=_K3_LOOKBACK_DAYS - 1)
    suppressed_patterns = _logged_k2_memory_patterns(conn, now=now)
    rows = conn.execute(
        """
        SELECT t.date,
               t.description,
               t.category_id,
               t.use_type,
               c.name AS category_name
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.category_source = 'user'
           AND t.category_id IS NOT NULL
           AND t.description IS NOT NULL
           AND t.date >= ?
           AND t.date <= ?
         ORDER BY t.date ASC, t.id ASC
        """,
        (lookback_start.isoformat(), as_of.isoformat()),
    ).fetchall()

    grouped: dict[str, list[_K2Txn]] = defaultdict(list)
    for row in rows:
        pattern = normalize_description(str(row["description"] or ""))
        if not pattern or pattern in suppressed_patterns:
            continue
        txn_date = _parse_txn_date(row["date"])
        if txn_date is None:
            continue
        grouped[pattern].append(
            _K2Txn(
                description=str(row["description"]),
                category_id=str(row["category_id"]),
                category_name=str(row["category_name"]),
                use_type=None if row["use_type"] is None else str(row["use_type"]),
                txn_date=txn_date,
            )
        )

    rules: list[_K3Rule] = []
    transaction_count = 0
    for pattern, txns in grouped.items():
        if _looks_like_k2_recat_group(txns):
            continue
        category_ids = {txn.category_id for txn in txns}
        if len(category_ids) != 1:
            continue
        use_type = _candidate_use_type(txns)
        if _has_applicable_vendor_memory(conn, pattern=pattern, use_type=use_type):
            continue
        latest = max(txns, key=lambda txn: txn.txn_date)
        rules.append(
            _K3Rule(
                pattern=pattern,
                display_description=latest.description,
                category_id=latest.category_id,
                category_name=latest.category_name,
                use_type=use_type,
                count=len(txns),
                latest_date=latest.txn_date,
            )
        )
        transaction_count += len(txns)

    if transaction_count < _K3_MIN_MANUAL_COUNT:
        return None

    ordered_rules = tuple(
        sorted(
            rules,
            key=lambda rule: (
                -rule.count,
                -rule.latest_date.toordinal(),
                rule.pattern,
            ),
        )
    )
    return _K3Candidate(rules=ordered_rules, transaction_count=transaction_count)


@register_pattern(
    id="K-3",
    move=Move.PRESCRIBE,
    tiers=(1, 3),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=7),
    tool="cat_memory_add_bulk",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.IMPLEMENT,
        CFPProcessStep.MONITOR,
    ),
)
def evaluate_k3_bulk_memory_offer(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_k3_candidate(conn, now=ctx.now)
    if candidate is None:
        return None

    return Intervention(
        pattern_id="K-3",
        move=Move.PRESCRIBE,
        tiers=(1, 3),
        priority=Priority.MEDIUM,
        headline=(
            f"You manually categorized {candidate.transaction_count} transactions "
            f"this week across {candidate.merchant_count} merchants. Want me to "
            "learn from all of them?"
        ),
        detail_bullets=(
            "Next time I'll handle them automatically.",
            f"Ready to remember: {_format_k3_rule_list(candidate.rules)}.",
            (
                "Merchants already covered by vendor memory, K-2 repeated-merchant "
                "handling, or a recent K-2 suggestion are skipped."
            ),
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Remember these merchants",
            tool="cat_memory_add_bulk",
            params={
                "rules": [rule.action_params() for rule in candidate.rules],
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("K-3"),
    )


def _find_k4_candidate(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> _K4Candidate | None:
    as_of = now.date()
    lookback_start = as_of - timedelta(days=_K4_LOOKBACK_DAYS - 1)
    prior_patterns = _prior_merchant_patterns(conn, before_date=lookback_start)
    placeholders = ",".join("?" for _ in _K4_AUTO_CATEGORY_SOURCES)
    rows = conn.execute(
        f"""
        SELECT t.id,
               t.date,
               t.description,
               t.category_id,
               t.category_source,
               t.use_type,
               c.name AS category_name
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.category_id IS NOT NULL
           AND t.category_source IN ({placeholders})
           AND t.description IS NOT NULL
           AND t.date >= ?
           AND t.date <= ?
         ORDER BY t.date ASC, t.id ASC
        """,
        (
            *sorted(_K4_AUTO_CATEGORY_SOURCES),
            lookback_start.isoformat(),
            as_of.isoformat(),
        ),
    ).fetchall()

    grouped: dict[str, list[_K4Txn]] = defaultdict(list)
    for row in rows:
        pattern = normalize_description(str(row["description"] or ""))
        if not pattern or pattern in prior_patterns:
            continue
        txn_date = _parse_txn_date(row["date"])
        if txn_date is None:
            continue
        grouped[pattern].append(
            _K4Txn(
                id=str(row["id"]),
                description=str(row["description"]),
                category_id=str(row["category_id"]),
                category_name=str(row["category_name"]),
                category_source=str(row["category_source"]),
                use_type=None if row["use_type"] is None else str(row["use_type"]),
                txn_date=txn_date,
            )
        )

    merchants: list[_K4Merchant] = []
    for pattern, txns in grouped.items():
        category_ids = {txn.category_id for txn in txns}
        if len(category_ids) != 1:
            continue
        use_type = _candidate_use_type(
            [
                _K2Txn(
                    description=txn.description,
                    category_id=txn.category_id,
                    category_name=txn.category_name,
                    use_type=txn.use_type,
                    txn_date=txn.txn_date,
                )
                for txn in txns
            ]
        )
        if _has_applicable_vendor_memory(conn, pattern=pattern, use_type=use_type):
            continue
        latest = max(txns, key=lambda txn: txn.txn_date)
        merchants.append(
            _K4Merchant(
                pattern=pattern,
                display_description=latest.description,
                category_id=latest.category_id,
                category_name=latest.category_name,
                use_type=use_type,
                category_sources=tuple(sorted({txn.category_source for txn in txns})),
                transaction_ids=tuple(sorted(txn.id for txn in txns)),
                latest_date=latest.txn_date,
            )
        )

    if len(merchants) < _K4_MIN_MERCHANT_COUNT:
        return None

    return _K4Candidate(
        merchants=tuple(
            sorted(
                merchants,
                key=lambda merchant: (
                    -merchant.latest_date.toordinal(),
                    merchant.pattern,
                ),
            )
        )
    )


@register_pattern(
    id="K-4",
    move=Move.COACH,
    tiers=(1,),
    priority=Priority.LOW,
    cooldown=timedelta(days=14),
    tool="cat_review_new_merchants",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.PRESENT,
        CFPProcessStep.MONITOR,
    ),
)
def evaluate_k4_new_merchant_confidence_check(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_k4_candidate(conn, now=ctx.now)
    if candidate is None:
        return None

    merchant_list = _format_k4_merchant_list(candidate.merchants)
    return Intervention(
        pattern_id="K-4",
        move=Move.COACH,
        tiers=(1,),
        priority=Priority.LOW,
        headline=(
            f"{candidate.merchant_count} new merchants this week, all handled "
            f"automatically. Quick sanity check - any of these look wrong? "
            f"{merchant_list}"
        ),
        detail_bullets=(
            (
                "Only first-time merchants with stable auto-assigned categories "
                "are included."
            ),
            "Known vendor-memory matches and prior merchant history are skipped.",
            (
                "Confirm saves vendor memory; fix recategorizes the listed "
                "transactions and saves the corrected rule."
            ),
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Review new merchants",
            tool="cat_review_new_merchants",
            params={
                "items": [merchant.action_item() for merchant in candidate.merchants],
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("K-4"),
    )
