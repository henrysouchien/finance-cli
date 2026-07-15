from __future__ import annotations

import subprocess
import sys

from finance_cli.gateway.code_exec_preamble import ADVISORY_SYSPATH_PROLOGUE, build_finance_preamble


def test_code_execution_preamble_prepends_app() -> None:
    preamble = build_finance_preamble("task-123")

    assert ADVISORY_SYSPATH_PROLOGUE in preamble

    script = (
        "import sys\n"
        "sys.path.insert(0, '/workspace')\n"
        "sys.path.append('/app')\n"
        f"{ADVISORY_SYSPATH_PROLOGUE}"
        "print(sys.path[0])\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "/app"
