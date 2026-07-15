from __future__ import annotations

import json
import subprocess
from textwrap import dedent

from .conftest import REPO_ROOT


def test_advisory_sandbox_contents(docker_cli: str, advisory_sandbox_image: str, run_advisory_container) -> None:
    script = dedent(
        """
        import json
        import os

        files = []
        for root, _, names in os.walk("/app/finance_cli"):
            for name in names:
                files.append(os.path.join(root, name))
        print(json.dumps(sorted(files)))
        """
    )

    result = run_advisory_container(script)
    assert result.returncode == 0, result.stderr or result.stdout
    files = json.loads(result.stdout.strip())

    expected = {"/app/finance_cli/__init__.py"}
    advisory_root = REPO_ROOT / "finance_cli" / "advisory"
    expected.update(
        "/app/finance_cli/advisory/" + path.relative_to(advisory_root).as_posix()
        for path in advisory_root.rglob("*")
        if path.is_file() and "/tests/" not in str(path)
    )

    assert set(files) == expected

    inspect_result = subprocess.run(
        [docker_cli, "image", "inspect", advisory_sandbox_image, "--format", "{{.Size}}"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert inspect_result.returncode == 0, inspect_result.stderr or inspect_result.stdout
    assert int(inspect_result.stdout.strip()) < 500 * 1024 * 1024
