from __future__ import annotations

import finance_cli.interventions  # noqa: F401
import pytest

from finance_cli.interventions.registry import (
    CFPDomain,
    CFPProcessStep,
    Move,
    PATTERN_REGISTRY,
    register_pattern,
)


TEST_PATTERN_PREFIX = "TEST-CFP-"


@pytest.fixture(autouse=True)
def cleanup_test_patterns() -> None:
    yield
    for pattern_id in list(PATTERN_REGISTRY):
        if pattern_id.startswith(TEST_PATTERN_PREFIX):
            PATTERN_REGISTRY.pop(pattern_id, None)


def _register_test_pattern(
    *,
    pattern_id: str,
    cfp_domains: tuple[CFPDomain, ...] | tuple[object, ...] = (),
    cfp_steps: tuple[CFPProcessStep, ...] | tuple[object, ...] = (),
) -> None:
    @register_pattern(
        id=pattern_id,
        move=Move.WARN,
        tiers=(1,),
        cfp_domains=cfp_domains,
        cfp_steps=cfp_steps,
    )
    def _evaluate_pattern(conn, ctx):
        return None


def test_cfp_domain_enum_values() -> None:
    assert {domain.name: domain.value for domain in CFPDomain} == {
        "PROFESSIONAL_CONDUCT": "professional_conduct",
        "GENERAL_PRINCIPLES": "general_principles",
        "RISK_INSURANCE": "risk_insurance",
        "INVESTMENT": "investment",
        "TAX": "tax",
        "RETIREMENT": "retirement",
        "ESTATE": "estate",
        "PSYCHOLOGY": "psychology",
    }


def test_cfp_process_step_enum_values() -> None:
    assert {step.name: step.value for step in CFPProcessStep} == {
        "UNDERSTAND": "understand",
        "IDENTIFY": "identify",
        "ANALYZE": "analyze",
        "DEVELOP": "develop",
        "PRESENT": "present",
        "IMPLEMENT": "implement",
        "MONITOR": "monitor",
    }


def test_register_pattern_accepts_valid_tags() -> None:
    pattern_id = f"{TEST_PATTERN_PREFIX}VALID"

    _register_test_pattern(
        pattern_id=pattern_id,
        cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
        cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
    )

    registered = PATTERN_REGISTRY[pattern_id]
    assert registered.cfp_domains == (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY)
    assert registered.cfp_steps == (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP)


def test_register_pattern_rejects_wrong_type_domain() -> None:
    with pytest.raises(TypeError, match="cfp_domains entry is not a CFPDomain"):
        _register_test_pattern(
            pattern_id=f"{TEST_PATTERN_PREFIX}BAD-DOMAIN",
            cfp_domains=("general_principles",),
            cfp_steps=(CFPProcessStep.MONITOR,),
        )


def test_register_pattern_rejects_wrong_type_step() -> None:
    with pytest.raises(TypeError, match="cfp_steps entry is not a CFPProcessStep"):
        _register_test_pattern(
            pattern_id=f"{TEST_PATTERN_PREFIX}BAD-STEP",
            cfp_domains=(CFPDomain.TAX,),
            cfp_steps=("monitor",),
        )


def test_register_pattern_rejects_empty_steps_by_default() -> None:
    with pytest.raises(ValueError, match="cfp_steps required"):
        _register_test_pattern(
            pattern_id=f"{TEST_PATTERN_PREFIX}EMPTY-DEFAULT",
            cfp_domains=(CFPDomain.TAX,),
        )


def test_register_pattern_allows_empty_domains_with_steps() -> None:
    pattern_id = f"{TEST_PATTERN_PREFIX}EMPTY-DOMAINS"

    _register_test_pattern(
        pattern_id=pattern_id,
        cfp_steps=(CFPProcessStep.MONITOR,),
    )

    registered = PATTERN_REGISTRY[pattern_id]
    assert registered.cfp_domains == ()
    assert registered.cfp_steps == (CFPProcessStep.MONITOR,)


def test_registered_pattern_cfp_fields_match_expected() -> None:
    expected = {
        "B-1": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
        ),
        "B-2": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.MONITOR),
        ),
        "B-3": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
        ),
        "B-4": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "B-5": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (
                CFPProcessStep.ANALYZE,
                CFPProcessStep.DEVELOP,
                CFPProcessStep.IMPLEMENT,
                CFPProcessStep.MONITOR,
            ),
        ),
        "B-6": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
        ),
        "B-7": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.MONITOR),
        ),
        "K-1": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "K-2": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "K-3": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "K-4": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT, CFPProcessStep.MONITOR),
        ),
        "K-5": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "D-1": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
        ),
        "D-2": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
        ),
        "D-3": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
        ),
        "D-4": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
        ),
        "D-5": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "D-6": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "D-7": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT, CFPProcessStep.MONITOR),
        ),
        "W-1": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "W-2": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT, CFPDomain.RETIREMENT),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "W-3": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT, CFPDomain.RETIREMENT),
            (
                CFPProcessStep.ANALYZE,
                CFPProcessStep.DEVELOP,
                CFPProcessStep.PRESENT,
                CFPProcessStep.IMPLEMENT,
            ),
        ),
        "W-4": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT),
            (
                CFPProcessStep.ANALYZE,
                CFPProcessStep.DEVELOP,
                CFPProcessStep.IMPLEMENT,
                CFPProcessStep.MONITOR,
            ),
        ),
        "C-1": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
        ),
        "C-2": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "C-3": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "C-4": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
        ),
        "C-5": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
        ),
        "C-6": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "T-1": (
            (CFPDomain.TAX,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "T-2": (
            (CFPDomain.TAX,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
        ),
        "T-3": (
            (CFPDomain.TAX,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "T-4": (
            (CFPDomain.TAX,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "T-5": (
            (CFPDomain.TAX,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
        ),
        "T-6": (
            (CFPDomain.TAX, CFPDomain.RETIREMENT),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "T-7": (
            (CFPDomain.TAX, CFPDomain.PSYCHOLOGY),
            (CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT, CFPProcessStep.MONITOR),
        ),
        "I-1": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
        ),
        "I-2": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
        ),
        "I-3": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
        ),
        "I-4": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.TAX),
            (CFPProcessStep.IDENTIFY, CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT),
        ),
        "I-5": (
            (CFPDomain.GENERAL_PRINCIPLES,),
            (CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
        ),
        "S-1": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.RISK_INSURANCE),
            (CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT),
        ),
        "S-2": (
            (CFPDomain.GENERAL_PRINCIPLES, CFPDomain.RISK_INSURANCE),
            (CFPProcessStep.IDENTIFY, CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
        ),
    }
    for pattern_id, (expected_domains, expected_steps) in expected.items():
        registered = PATTERN_REGISTRY[pattern_id]
        assert registered.cfp_domains == expected_domains, pattern_id
        assert registered.cfp_steps == expected_steps, pattern_id
