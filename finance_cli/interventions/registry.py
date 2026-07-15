from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum, StrEnum
import sqlite3
from typing import Any, Callable

from .context import InterventionContext


class Move(StrEnum):
    DIAGNOSE = "diagnose"
    PRESCRIBE = "prescribe"
    WARN = "warn"
    COMPARE = "compare"
    PATTERN_CATCH = "pattern_catch"
    COACH = "coach"


class Priority(IntEnum):
    HIGH = 0
    MEDIUM = 1
    LOW = 2


class CFPDomain(StrEnum):
    PROFESSIONAL_CONDUCT = "professional_conduct"
    GENERAL_PRINCIPLES = "general_principles"
    RISK_INSURANCE = "risk_insurance"
    INVESTMENT = "investment"
    TAX = "tax"
    RETIREMENT = "retirement"
    ESTATE = "estate"
    PSYCHOLOGY = "psychology"


class CFPProcessStep(StrEnum):
    UNDERSTAND = "understand"
    IDENTIFY = "identify"
    ANALYZE = "analyze"
    DEVELOP = "develop"
    PRESENT = "present"
    IMPLEMENT = "implement"
    MONITOR = "monitor"


@dataclass(frozen=True)
class InterventionAction:
    label: str
    tool: str
    params: dict[str, Any]
    build_stub: bool


@dataclass(frozen=True)
class Intervention:
    pattern_id: str
    move: Move
    tiers: tuple[int, ...]
    priority: Priority
    headline: str
    detail_bullets: tuple[str, ...]
    tier4_ladder: str | None
    tier4_is_fallback: bool
    action: InterventionAction | None
    dollar_impact_cents: int
    goal_link: str | None
    log_id: int | None
    fired_at: datetime
    last_fired_at: datetime | None
    goal_ladder_delta_cents: int | None = None


EvaluatePattern = Callable[[sqlite3.Connection, InterventionContext], Intervention | None]
ContextCheck = Callable[[InterventionContext], bool]


@dataclass(frozen=True)
class RegisteredPattern:
    id: str
    move: Move
    tiers: tuple[int, ...]
    priority: Priority
    cooldown: timedelta
    tool: str | None
    evaluate: EvaluatePattern
    context_check: ContextCheck | None = None
    strategy_check: ContextCheck | None = None
    cfp_domains: tuple[CFPDomain, ...] = ()
    cfp_steps: tuple[CFPProcessStep, ...] = ()


DEFAULT_COOLDOWNS: dict[Move, timedelta] = {
    Move.WARN: timedelta(days=1),
    Move.DIAGNOSE: timedelta(days=14),
    Move.COMPARE: timedelta(days=14),
    Move.PATTERN_CATCH: timedelta(days=14),
    Move.PRESCRIBE: timedelta(days=14),
    Move.COACH: timedelta(days=30),
}

CFP_TAXONOMY_REVIEWED_AT = "2026-04-20"

PATTERN_REGISTRY: dict[str, RegisteredPattern] = {}


def register_pattern(
    *,
    id: str,
    move: Move,
    tiers: tuple[int, ...],
    priority: Priority = Priority.MEDIUM,
    cooldown: timedelta | None = None,
    tool: str | None = None,
    context_check: ContextCheck | None = None,
    strategy_check: ContextCheck | None = None,
    cfp_domains: tuple[CFPDomain, ...] = (),
    cfp_steps: tuple[CFPProcessStep, ...] = (),
) -> Callable[[EvaluatePattern], EvaluatePattern]:
    def decorator(func: EvaluatePattern) -> EvaluatePattern:
        if id in PATTERN_REGISTRY:
            raise ValueError(f"Pattern already registered: {id}")

        normalized_cfp_domains = tuple(cfp_domains)
        normalized_cfp_steps = tuple(cfp_steps)
        for domain in normalized_cfp_domains:
            if not isinstance(domain, CFPDomain):
                raise TypeError(f"{id}: cfp_domains entry is not a CFPDomain: {domain!r}")
        for step in normalized_cfp_steps:
            if not isinstance(step, CFPProcessStep):
                raise TypeError(f"{id}: cfp_steps entry is not a CFPProcessStep: {step!r}")
        if not normalized_cfp_steps:
            raise ValueError(f"{id}: cfp_steps required")

        PATTERN_REGISTRY[id] = RegisteredPattern(
            id=id,
            move=move,
            tiers=tuple(int(tier) for tier in tiers),
            priority=priority,
            cooldown=cooldown or DEFAULT_COOLDOWNS[move],
            tool=tool,
            evaluate=func,
            context_check=context_check,
            strategy_check=strategy_check,
            cfp_domains=normalized_cfp_domains,
            cfp_steps=normalized_cfp_steps,
        )
        return func

    return decorator
