from __future__ import annotations

import json

from .conftest import ADVISORY_SYSPATH_PROLOGUE_LITERAL


def test_advisory_sandbox_import(run_advisory_container) -> None:
    script = (
        "from decimal import Decimal\n"
        "import json\n"
        f"{ADVISORY_SYSPATH_PROLOGUE_LITERAL}"
        "import finance_cli.advisory as adv\n"
        "assert adv.__file__.startswith('/app/finance_cli/advisory/'), adv.__file__\n"
        "future_value_cents = adv.future_value(100_000_00, Decimal('0.08'), 10)\n"
        "assert future_value_cents == 215_892_50, future_value_cents\n"
        "print(json.dumps({'module': adv.__file__, 'future_value_cents': future_value_cents}))\n"
    )

    result = run_advisory_container(script)

    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout.strip())
    assert payload["module"].startswith("/app/finance_cli/advisory/")
    assert payload["future_value_cents"] == 21_589_250
