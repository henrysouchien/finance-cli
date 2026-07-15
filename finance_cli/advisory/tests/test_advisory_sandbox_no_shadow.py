from __future__ import annotations

from pathlib import Path

from .conftest import ADVISORY_SYSPATH_PROLOGUE_LITERAL


def test_advisory_sandbox_no_shadow(tmp_path: Path, run_advisory_container) -> None:
    shadow_pkg = tmp_path / "finance_cli"
    shadow_pkg.mkdir()
    (shadow_pkg / "__init__.py").write_text('raise RuntimeError("shadow attempt")\n', encoding="utf-8")

    script = (
        f"{ADVISORY_SYSPATH_PROLOGUE_LITERAL}"
        "import finance_cli.advisory as adv\n"
        "assert adv.__file__.startswith('/app/finance_cli/advisory/'), adv.__file__\n"
        "print(adv.__file__)\n"
    )

    result = run_advisory_container(script, ["-v", f"{tmp_path}:/workspace"])

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip().startswith("/app/finance_cli/advisory/")
