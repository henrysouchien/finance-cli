from __future__ import annotations

import json
from pathlib import Path

import pytest

from finance_cli.redaction import redact_text


def _load_corpus() -> list[dict[str, str]]:
    path = Path(__file__).parent / "fixtures" / "redaction_corpus.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", _load_corpus(), ids=lambda case: case["id"])
def test_redaction_corpus(case: dict[str, str]) -> None:
    assert redact_text(case["input"]) == case["expected"]
