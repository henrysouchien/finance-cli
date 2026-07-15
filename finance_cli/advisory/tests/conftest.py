from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Callable

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE_PATH = REPO_ROOT / "finance_cli" / "gateway" / "execution" / "Dockerfile.code-exec"
ADVISORY_SANDBOX_IMAGE = "finance-cli-code-exec:test"
ADVISORY_SYSPATH_PROLOGUE_LITERAL = (
    "import sys\n"
    "if '/app' in sys.path:\n"
    "    sys.path.remove('/app')\n"
    "sys.path.insert(0, '/app')\n"
)


@pytest.fixture(scope="session")
def docker_cli() -> str:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("docker not on PATH")
    return docker


@pytest.fixture(scope="session")
def advisory_sandbox_image(docker_cli: str) -> str:
    result = subprocess.run(
        [
            docker_cli,
            "build",
            "-t",
            ADVISORY_SANDBOX_IMAGE,
            "-f",
            str(DOCKERFILE_PATH),
            ".",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    return ADVISORY_SANDBOX_IMAGE


@pytest.fixture()
def run_advisory_container(
    docker_cli: str,
    advisory_sandbox_image: str,
) -> Callable[[str, list[str] | None], subprocess.CompletedProcess[str]]:
    def _run(script: str, extra_args: list[str] | None = None) -> subprocess.CompletedProcess[str]:
        cmd = [docker_cli, "run", "--rm"]
        if extra_args:
            cmd.extend(extra_args)
        cmd.extend([advisory_sandbox_image, "python", "-c", script])
        return subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    return _run
