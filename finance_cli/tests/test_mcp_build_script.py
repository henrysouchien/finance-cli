from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

from scripts.build_mcp_binary import (
    ANTI_BLOAT_MODES,
    DATA_DIRS,
    DEFAULT_BINARY_NAME,
    ENTRYPOINT,
    NOFOLLOW_IMPORTS,
    nuitka_command,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_nuitka_command_targets_local_mcp_entrypoint(tmp_path: Path) -> None:
    command = nuitka_command(output_dir=tmp_path, binary_name="cashnerd-test")

    assert command[:4] == [sys.executable, "-m", "nuitka", "--standalone"]
    assert "--onefile" in command
    assert f"--output-dir={tmp_path}" in command
    assert "--output-filename=cashnerd-test" in command
    assert "--assume-yes-for-downloads" in command
    assert command[-1] == str(ENTRYPOINT)


def test_nuitka_command_includes_distribution_boundaries(tmp_path: Path) -> None:
    command = nuitka_command(output_dir=tmp_path, binary_name=DEFAULT_BINARY_NAME)

    for module_name in NOFOLLOW_IMPORTS:
        assert f"--nofollow-import-to={module_name}" in command

    for package_name, mode in ANTI_BLOAT_MODES:
        assert f"--noinclude-{package_name}-mode={mode}" in command

    for source, target in DATA_DIRS:
        assert f"--include-data-dir={source}={target}" in command


def test_build_script_print_command_smoke(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/build_mcp_binary.py",
            "--print-command",
            "--output-dir",
            str(tmp_path),
            "--name",
            "cashnerd-test",
            "--extra-arg=--show-progress",
        ],
        capture_output=True,
        cwd=_PROJECT_ROOT,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    printed = shlex.split(result.stdout.strip())
    assert printed == nuitka_command(
        output_dir=tmp_path.resolve(),
        binary_name="cashnerd-test",
        extra_args=("--show-progress",),
    )
