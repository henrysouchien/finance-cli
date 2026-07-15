"""Shared finance sandbox preamble helpers."""

from __future__ import annotations

from agent_gateway.code_execution._helpers import _default_code_execute_preamble


ADVISORY_SYSPATH_PROLOGUE = (
    "import sys\n"
    "if '/app' in sys.path:\n"
    "    sys.path.remove('/app')\n"
    "sys.path.insert(0, '/app')\n"
)


def build_finance_preamble(task_id: str) -> str:
    """Return the shared finance sandbox preamble for code execution tasks."""
    base = _default_code_execute_preamble(task_id).rstrip()
    suffix = (
        ADVISORY_SYSPATH_PROLOGUE
        + "try:\n"
        "    import numpy_financial as npf\n"
        "except ImportError:\n"
        "    pass\n\n"
        "from finance_client import FinanceClient\n"
        "_finance = FinanceClient()"
    )
    return f"{base}\n\n{suffix}\n"
