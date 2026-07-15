from __future__ import annotations

import pytest

from finance_cli.ai_egress import (
    AIEgressBlockedError,
    assert_raw_financial_ai_allowed,
    normalize_ai_egress_mode,
)


def test_normalize_ai_egress_mode_defaults_unknown_values_to_full() -> None:
    assert normalize_ai_egress_mode(None) == "full"
    assert normalize_ai_egress_mode("") == "full"
    assert normalize_ai_egress_mode("FULL") == "full"
    assert normalize_ai_egress_mode("redacted") == "redacted"
    assert normalize_ai_egress_mode("off") == "off"
    assert normalize_ai_egress_mode("private") == "full"


def test_assert_raw_financial_ai_allowed_blocks_non_full_modes() -> None:
    assert_raw_financial_ai_allowed("full", surface="AI categorization")

    with pytest.raises(AIEgressBlockedError, match="redacted"):
        assert_raw_financial_ai_allowed("redacted", surface="AI categorization")

    with pytest.raises(AIEgressBlockedError, match="disabled"):
        assert_raw_financial_ai_allowed("off", surface="AI categorization")
