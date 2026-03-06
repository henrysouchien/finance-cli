"""User rules loader and match helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .config import DEFAULT_DATA_DIR, ensure_data_dir

_EXTRACTOR_BACKENDS: tuple[str, ...] = ("ai", "azure", "bsc")

_rules_cache: tuple[int, Path, UserRules] | None = None


def invalidate_rules_cache() -> None:
    """Clear the mtime cache so load_rules() re-reads from disk."""
    global _rules_cache
    _rules_cache = None


CANONICAL_CATEGORIES: frozenset[str] = frozenset(
    {
        # Parent categories (level 0)
        "Food & Drink",
        "Travel & Vacation",
        "Housing",
        "Financial",
        "Lifestyle",
        "Professional",
        "Health",
        "Other",
        "Income",
        # Leaf categories (level 1)
        "Coffee",
        "Dining",
        "Groceries",
        "Transportation",
        "Travel",
        "Shopping",
        "Entertainment",
        "Software & Subscriptions",
        "Health & Wellness",
        "Professional Fees",
        "Advertising",
        "Contract Labor",
        "Personal Expense",
        "Bank Charges & Fees",
        "Payments & Transfers",
        "Utilities",
        "Rent",
        "Office Expense",
        "Supplies",
        "Insurance",
        "Home Improvement",
        "Donations",
        "Taxes",
        "Depreciation",
        "Taxes & Licenses",
        "Income: Salary",
        "Income: Business",
        "Income: Other",
        "Cost of Goods Sold",
        "Childcare",
    }
)


_CATEGORY_HIERARCHY: dict[str, list[str]] = {
    "Food & Drink": ["Coffee", "Dining", "Groceries"],
    "Travel & Vacation": ["Travel", "Transportation"],
    "Housing": ["Rent", "Utilities", "Home Improvement", "Insurance", "Office Expense", "Supplies"],
    "Financial": ["Bank Charges & Fees", "Payments & Transfers", "Taxes", "Depreciation", "Taxes & Licenses"],
    "Lifestyle": ["Shopping", "Entertainment", "Donations", "Childcare"],
    "Professional": ["Professional Fees", "Software & Subscriptions", "Advertising", "Contract Labor"],
    "Health": ["Health & Wellness"],
    "Other": ["Personal Expense"],
    "Income": ["Income: Salary", "Income: Business", "Income: Other", "Cost of Goods Sold"],
}


@dataclass(frozen=True)
class KeywordRule:
    keywords: list[str]
    category: str
    use_type: str | None
    priority: int
    rule_index: int


@dataclass(frozen=True)
class KeywordMatch:
    category: str
    use_type: str | None
    matched_keyword: str
    rule_index: int


@dataclass(frozen=True)
class SplitRule:
    match_category: str | None
    match_keywords: list[str]
    business_pct: float
    business_category: str
    personal_category: str
    note: str | None
    rule_index: int


@dataclass(frozen=True)
class CategoryOverride:
    categories: list[str]
    force_use_type: str
    note: str | None
    rule_index: int


@dataclass(frozen=True)
class UserRules:
    keyword_rules: list[KeywordRule]
    split_rules: list[SplitRule]
    category_overrides: list[CategoryOverride]
    category_aliases: dict[str, str | None]
    income_sources: dict[str, dict[str, Any]]
    ai_categorizer: dict[str, Any]
    revenue_streams: list[dict[str, Any]] = field(default_factory=list)
    payment_keywords: list[str] = field(default_factory=list)
    payment_exclusions: list[str] = field(default_factory=list)
    ai_parser: dict[str, Any] = field(default_factory=dict)
    extractors: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


def _empty_rules() -> UserRules:
    return UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={},
        ai_categorizer={},
        revenue_streams=[],
        payment_keywords=[],
        payment_exclusions=[],
        ai_parser={},
        extractors={},
        raw={},
    )


def _workspace_rules_path() -> Path:
    return ensure_data_dir() / "rules.yaml"


def _package_default_rules_path() -> Path:
    return DEFAULT_DATA_DIR / "rules.yaml"


def resolve_rules_path(path: Path | None = None) -> Path:
    if path is not None:
        return path.expanduser().resolve()

    workspace_path = _workspace_rules_path()
    if workspace_path.exists():
        return workspace_path

    package_path = _package_default_rules_path()
    # Only fall back to the packaged default when it is also the workspace path
    # (the default DB location). When FINANCE_CLI_DB points elsewhere, prefer a
    # sibling rules.yaml in that workspace instead of mutating packaged defaults.
    if workspace_path == package_path and package_path.exists():
        return package_path

    return workspace_path


def load_rules(path: Path | None = None) -> UserRules:
    """Load rules from YAML config and validate basic schema."""
    global _rules_cache

    rules_path = resolve_rules_path(path)
    try:
        stat_result = rules_path.stat()
        mtime_ns = stat_result.st_mtime_ns
        if _rules_cache is not None:
            cache_mtime_ns, cache_path, cache_rules = _rules_cache
            if cache_path == rules_path and cache_mtime_ns == mtime_ns:
                return cache_rules

        payload = yaml.safe_load(rules_path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        _rules_cache = None
        return _empty_rules()

    if not isinstance(payload, dict):
        raise ValueError("rules.yaml root must be a mapping")

    keyword_rules_raw = payload.get("keyword_rules") or []
    if not isinstance(keyword_rules_raw, list):
        raise ValueError("keyword_rules must be a list")

    keyword_rules: list[KeywordRule] = []
    for index, item in enumerate(keyword_rules_raw):
        if not isinstance(item, dict):
            raise ValueError(f"keyword_rules[{index}] must be a mapping")
        keywords_raw = item.get("keywords") or []
        if not isinstance(keywords_raw, list) or not keywords_raw:
            raise ValueError(f"keyword_rules[{index}].keywords must be a non-empty list")

        keywords = [str(value).strip() for value in keywords_raw if str(value).strip()]
        if not keywords:
            raise ValueError(f"keyword_rules[{index}].keywords must include at least one non-empty keyword")

        category = str(item.get("category") or "").strip()
        if not category:
            raise ValueError(f"keyword_rules[{index}].category is required")

        use_type = item.get("use_type")
        if use_type is not None:
            use_type = str(use_type).strip()
            if use_type not in {"Business", "Personal"}:
                raise ValueError(f"keyword_rules[{index}].use_type must be Business or Personal")

        priority = int(item.get("priority") or 0)
        keyword_rules.append(
            KeywordRule(
                keywords=keywords,
                category=category,
                use_type=use_type,
                priority=priority,
                rule_index=index,
            )
        )

    aliases_raw = payload.get("category_aliases") or {}
    if not isinstance(aliases_raw, dict):
        raise ValueError("category_aliases must be a mapping")
    category_aliases: dict[str, str | None] = {}
    for alias, target in aliases_raw.items():
        key = str(alias).strip()
        if not key:
            continue
        category_aliases[key] = str(target).strip() if target is not None else None

    split_rules_raw = payload.get("split_rules") or []
    if not isinstance(split_rules_raw, list):
        raise ValueError("split_rules must be a list")

    split_rules: list[SplitRule] = []
    for index, item in enumerate(split_rules_raw):
        if not isinstance(item, dict):
            raise ValueError(f"split_rules[{index}] must be a mapping")

        match_raw = item.get("match") or {}
        if not isinstance(match_raw, dict):
            raise ValueError(f"split_rules[{index}].match must be a mapping")

        match_category = match_raw.get("category")
        match_category_value = str(match_category).strip() if match_category is not None else None

        keywords_raw = match_raw.get("keywords") or []
        if not isinstance(keywords_raw, list):
            raise ValueError(f"split_rules[{index}].match.keywords must be a list")
        match_keywords = [str(value).strip() for value in keywords_raw if str(value).strip()]

        if not match_category_value and not match_keywords:
            raise ValueError(f"split_rules[{index}] must match by category or keywords")

        business_pct = float(item.get("business_pct") or 0)
        if business_pct <= 0 or business_pct >= 100:
            raise ValueError(f"split_rules[{index}].business_pct must be > 0 and < 100")

        business_category = str(item.get("business_category") or "").strip()
        personal_category = str(item.get("personal_category") or "").strip()
        if not business_category or not personal_category:
            raise ValueError(f"split_rules[{index}] business_category and personal_category are required")

        note = item.get("note")
        split_rules.append(
            SplitRule(
                match_category=match_category_value,
                match_keywords=match_keywords,
                business_pct=business_pct,
                business_category=business_category,
                personal_category=personal_category,
                note=str(note).strip() if note is not None and str(note).strip() else None,
                rule_index=index,
            )
        )

    overrides_raw = payload.get("category_overrides") or []
    if not isinstance(overrides_raw, list):
        raise ValueError("category_overrides must be a list")

    category_overrides: list[CategoryOverride] = []
    for index, item in enumerate(overrides_raw):
        if not isinstance(item, dict):
            raise ValueError(f"category_overrides[{index}] must be a mapping")

        categories_raw = item.get("categories") or []
        if not isinstance(categories_raw, list) or not categories_raw:
            raise ValueError(f"category_overrides[{index}].categories must be a non-empty list")

        categories = [str(value).strip() for value in categories_raw if str(value).strip()]
        if not categories:
            raise ValueError(f"category_overrides[{index}].categories must include non-empty names")

        force_use_type = str(item.get("force_use_type") or "").strip()
        if force_use_type not in {"Business", "Personal"}:
            raise ValueError(f"category_overrides[{index}].force_use_type must be Business or Personal")

        note = item.get("note")
        category_overrides.append(
            CategoryOverride(
                categories=categories,
                force_use_type=force_use_type,
                note=str(note).strip() if note is not None and str(note).strip() else None,
                rule_index=index,
            )
        )

    income_sources_raw = payload.get("income_sources") or {}
    if not isinstance(income_sources_raw, dict):
        raise ValueError("income_sources must be a mapping")

    income_sources: dict[str, dict[str, Any]] = {}
    for name, config in income_sources_raw.items():
        key = str(name).strip()
        if not key:
            continue
        if not isinstance(config, dict):
            raise ValueError(f"income_sources.{key} must be a mapping")
        income_sources[key] = config

    ai_categorizer_raw = payload.get("ai_categorizer") or {}
    if not isinstance(ai_categorizer_raw, dict):
        raise ValueError("ai_categorizer must be a mapping")
    revenue_streams_raw = payload.get("revenue_streams") or []
    if not isinstance(revenue_streams_raw, list):
        raise ValueError("revenue_streams must be a list")
    revenue_streams = [item for item in revenue_streams_raw if isinstance(item, dict)]
    payment_keywords_raw = payload.get("payment_keywords") or []
    if not isinstance(payment_keywords_raw, list):
        raise ValueError("payment_keywords must be a list")
    payment_keywords = [str(value).strip() for value in payment_keywords_raw if str(value).strip()]
    payment_exclusions_raw = payload.get("payment_exclusions") or []
    if not isinstance(payment_exclusions_raw, list):
        raise ValueError("payment_exclusions must be a list")
    payment_exclusions = [str(value).strip() for value in payment_exclusions_raw if str(value).strip()]
    ai_parser_raw = payload.get("ai_parser") or {}
    if not isinstance(ai_parser_raw, dict):
        raise ValueError("ai_parser must be a mapping")
    extractors_raw = payload.get("extractors") or {}
    if not isinstance(extractors_raw, dict):
        raise ValueError("extractors must be a mapping")

    default_backend = extractors_raw.get("default_backend")
    if default_backend is not None:
        backend = str(default_backend).strip().lower()
        if backend not in _EXTRACTOR_BACKENDS:
            supported = ", ".join(_EXTRACTOR_BACKENDS)
            raise ValueError(f"extractors.default_backend must be one of: {supported}")

    for backend_key in ("azure", "bsc"):
        backend_cfg = extractors_raw.get(backend_key)
        if backend_cfg is not None and not isinstance(backend_cfg, dict):
            raise ValueError(f"extractors.{backend_key} must be a mapping")

    rules = UserRules(
        keyword_rules=keyword_rules,
        split_rules=split_rules,
        category_overrides=category_overrides,
        category_aliases=category_aliases,
        income_sources=income_sources,
        ai_categorizer=ai_categorizer_raw,
        revenue_streams=revenue_streams,
        payment_keywords=payment_keywords,
        payment_exclusions=payment_exclusions,
        ai_parser=ai_parser_raw,
        extractors=extractors_raw,
        raw=payload,
    )
    _rules_cache = (mtime_ns, rules_path, rules)
    return rules


def match_keyword_rule(description: str, rules: UserRules) -> KeywordMatch | None:
    """Match keyword rules with precedence: longest keyword, then priority, then file order."""
    haystack = description.lower()

    best_score: tuple[int, int, int] | None = None
    best_rule: KeywordRule | None = None
    best_keyword: str | None = None
    for rule in rules.keyword_rules:
        for keyword in rule.keywords:
            probe = keyword.lower()
            if probe and probe in haystack:
                score = (len(keyword), rule.priority, -rule.rule_index)
                if best_score is None or score > best_score:
                    best_score = score
                    best_rule = rule
                    best_keyword = keyword

    if best_rule is None or best_keyword is None:
        return None

    return KeywordMatch(
        category=best_rule.category,
        use_type=best_rule.use_type,
        matched_keyword=best_keyword,
        rule_index=best_rule.rule_index,
    )


def match_payment_keyword(description: str, rules: UserRules) -> bool:
    """Return True when description matches configured payment keywords."""
    haystack = description.lower()
    return any(keyword.lower() in haystack for keyword in rules.payment_keywords)


def match_payment_exclusion(description: str, rules: UserRules) -> bool:
    """Return True when description matches a payment exclusion pattern."""
    haystack = description.lower()
    return any(keyword.lower() in haystack for keyword in rules.payment_exclusions)


def get_split_rule(description: str, category: str, rules: UserRules) -> SplitRule | None:
    """Return split rule matched by category or keyword."""
    haystack = description.lower()
    category_lc = category.lower().strip()

    for rule in rules.split_rules:
        if rule.match_category and rule.match_category.lower() == category_lc:
            return rule
        if any(keyword.lower() in haystack for keyword in rule.match_keywords):
            return rule
    return None


def get_category_override(category: str, category_source: str, rules: UserRules) -> str | None:
    """Return forced use_type for category, unless source is user/keyword_rule."""
    if category_source.lower() in {"user", "keyword_rule"}:
        return None

    category_lc = category.lower().strip()
    for rule in rules.category_overrides:
        if any(cat.lower().strip() == category_lc for cat in rule.categories):
            return rule.force_use_type
    return None


def resolve_category_alias(category: str, rules: UserRules) -> str | None:
    """Map aliased category names to canonical names."""
    probe = category.strip()
    if not probe:
        return category

    probe_lc = probe.lower()
    for alias, canonical in rules.category_aliases.items():
        if alias.lower() == probe_lc:
            return canonical
    return category
