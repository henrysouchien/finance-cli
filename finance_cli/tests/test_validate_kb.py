from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "validate_kb.py"
SPEC = importlib.util.spec_from_file_location("validate_kb", SCRIPT_PATH)
validate_kb = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = validate_kb
SPEC.loader.exec_module(validate_kb)


def _result(path: str, topic_id: str, related_topics: list[str]) -> validate_kb.Result:
    result = validate_kb.Result(Path(path))
    result.topic_id = topic_id
    result.related_topics = related_topics
    return result


def test_related_topic_graph_accepts_reciprocal_edges() -> None:
    first = _result("first.md", "cfp.general_principles.first", ["cfp.tax.second"])
    second = _result("second.md", "cfp.tax.second", ["cfp.general_principles.first"])

    validate_kb.add_related_topic_issues([first, second])

    assert first.issues == []
    assert second.issues == []


def test_related_topic_graph_flags_missing_unknown_and_self_edges() -> None:
    first = _result(
        "first.md",
        "cfp.general_principles.first",
        [
            "cfp.tax.second",
            "cfp.investment.missing",
            "cfp.general_principles.first",
        ],
    )
    second = _result("second.md", "cfp.tax.second", [])

    validate_kb.add_related_topic_issues([first, second])

    messages = [message for _location, message in first.issues]
    assert messages == [
        "missing reciprocal related_topics edge in second.md",
        "unknown related topic_id: cfp.investment.missing",
        "must not reference this topic's own topic_id",
    ]


def test_related_topic_frontmatter_entries_must_be_unique_nonempty_strings() -> None:
    result = validate_kb.Result(Path("topic.md"))

    validate_kb.validate_optional(
        {
            "related_topics": [
                "cfp.general_principles.first",
                "cfp.general_principles.first",
                "",
                123,
            ]
        },
        result,
        known_legal=set(),
        namespace="cfp",
    )

    assert result.related_topics == ["cfp.general_principles.first"]
    assert result.issues == [
        ("related_topics[1]", "duplicate related topic_id: cfp.general_principles.first"),
        ("related_topics[2]", "must be a non-empty string"),
        ("related_topics[3]", "must be a non-empty string"),
    ]


def test_referral_related_topics_are_type_checked_but_not_graph_edges() -> None:
    result = validate_kb.Result(Path("referral.md"))

    validate_kb.validate_optional(
        {"related_topics": ["cfp.general_principles.first"]},
        result,
        known_legal=set(),
        namespace="referrals",
    )

    assert result.issues == []
    assert result.related_topics == []
