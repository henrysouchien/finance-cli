from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sqlite3

from ..db import _connected_main_db_path
from .helpers import trailing_avg_expenses_cents


@dataclass(frozen=True)
class Goal:
    id: str
    name: str
    metric: str
    target_cents: int | None
    target_pct: float | None
    starting_cents: int | None
    starting_pct: float | None
    direction: str
    deadline: str | None


@dataclass(frozen=True)
class FallbackGoal:
    label: str
    target_cents: int
    is_fallback: bool


@dataclass(frozen=True)
class StrategyPrefs:
    debt_strategy: str | None = None
    debt_rationale: str | None = None
    debt_source: str | None = None

    def is_empty(self) -> bool:
        return not any((self.debt_strategy, self.debt_rationale, self.debt_source))


@dataclass(frozen=True)
class InterventionContext:
    now: datetime
    data_dir: Path | None
    rules_path: Path | None
    goals: tuple[Goal, ...]
    fallback_goal: FallbackGoal
    strategy_prefs: StrategyPrefs
    trailing_3mo_avg_expense_cents: int
    trailing_6mo_avg_expense_cents: int
    recent_fires: dict[str, datetime]
    recent_dismissals: dict[str, datetime]
    muted_patterns: frozenset[str]


def build_fallback_goal(trailing_3mo_avg_expense_cents: int) -> FallbackGoal:
    return FallbackGoal(
        label="3-month emergency fund",
        target_cents=max(int(trailing_3mo_avg_expense_cents), 0) * 3,
        is_fallback=True,
    )


def _parse_db_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value))


def _load_recent_fires(conn: sqlite3.Connection) -> dict[str, datetime]:
    # CLI rows are diagnostic/audit records from `interventions list`; they
    # should not consume cooldown for dashboard, agent, chat, email, etc.
    rows = conn.execute(
        """
        SELECT pattern_id, MAX(fired_at) AS fired_at
          FROM intervention_log
         WHERE surface <> 'cli'
         GROUP BY pattern_id
        """
    ).fetchall()
    result: dict[str, datetime] = {}
    for row in rows:
        parsed = _parse_db_datetime(row["fired_at"])
        if parsed is not None:
            result[str(row["pattern_id"])] = parsed
    return result


def _load_recent_dismissals(conn: sqlite3.Connection) -> dict[str, datetime]:
    rows = conn.execute(
        """
        SELECT pattern_id, MAX(acted_at) AS acted_at
          FROM intervention_log
         WHERE user_action = 'dismissed'
           AND acted_at IS NOT NULL
           AND surface <> 'cli'
         GROUP BY pattern_id
        """
    ).fetchall()
    result: dict[str, datetime] = {}
    for row in rows:
        parsed = _parse_db_datetime(row["acted_at"])
        if parsed is not None:
            result[str(row["pattern_id"])] = parsed
    return result


def _load_muted_patterns(conn: sqlite3.Connection) -> frozenset[str]:
    rows = conn.execute("SELECT pattern_id FROM intervention_mutes").fetchall()
    return frozenset(str(row["pattern_id"]) for row in rows)


def _load_goals(conn: sqlite3.Connection) -> tuple[Goal, ...]:
    rows = conn.execute(
        """
        SELECT id, name, metric, target_cents, target_pct, starting_cents, starting_pct,
               direction, deadline
          FROM goals
         WHERE is_active = 1
         ORDER BY created_at, id
        """
    ).fetchall()
    return tuple(
        Goal(
            id=str(row["id"]),
            name=str(row["name"]),
            metric=str(row["metric"]),
            target_cents=None if row["target_cents"] is None else int(row["target_cents"]),
            target_pct=None if row["target_pct"] is None else float(row["target_pct"]),
            starting_cents=None if row["starting_cents"] is None else int(row["starting_cents"]),
            starting_pct=None if row["starting_pct"] is None else float(row["starting_pct"]),
            direction=str(row["direction"]),
            deadline=None if row["deadline"] is None else str(row["deadline"]),
        )
        for row in rows
    )


def _load_strategy_prefs(conn: sqlite3.Connection) -> StrategyPrefs:
    row = conn.execute(
        """
        SELECT strategy, rationale, source
          FROM user_strategy_preferences
         WHERE domain = 'debt'
        """
    ).fetchone()
    if row is None:
        return StrategyPrefs()
    return StrategyPrefs(
        debt_strategy=str(row["strategy"]),
        debt_rationale=None if row["rationale"] is None else str(row["rationale"]),
        debt_source=None if row["source"] is None else str(row["source"]),
    )


def build_context(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    rules_path: Path | None = None,
    data_dir: Path | None = None,
) -> InterventionContext:
    resolved_now = (now or datetime.now()).replace(microsecond=0)
    as_of = resolved_now.date()
    db_path = _connected_main_db_path(conn)
    resolved_data_dir = data_dir or (db_path.parent if db_path is not None else None)
    trailing_3mo_avg_expense_cents = trailing_avg_expenses_cents(conn, 3, as_of=as_of)
    trailing_6mo_avg_expense_cents = trailing_avg_expenses_cents(conn, 6, as_of=as_of)
    return InterventionContext(
        now=resolved_now,
        data_dir=resolved_data_dir,
        rules_path=rules_path,
        goals=_load_goals(conn),
        fallback_goal=build_fallback_goal(trailing_3mo_avg_expense_cents),
        strategy_prefs=_load_strategy_prefs(conn),
        trailing_3mo_avg_expense_cents=trailing_3mo_avg_expense_cents,
        trailing_6mo_avg_expense_cents=trailing_6mo_avg_expense_cents,
        recent_fires=_load_recent_fires(conn),
        recent_dismissals=_load_recent_dismissals(conn),
        muted_patterns=_load_muted_patterns(conn),
    )
