from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

from agent_gateway.code_execution import CodeExecutionConfig

from finance_cli.gateway import secure_code_execution as secure_code_execution_module
from finance_cli.gateway.secure_code_execution import (
    SecureDockerBackend,
    build_secure_code_execution,
    build_secure_docker_run_command,
)


def test_secure_docker_run_command_enforces_sandbox_flags(tmp_path: Path) -> None:
    command = build_secure_docker_run_command(
        image="finance-cli-code-exec:test",
        work_dir_path=tmp_path,
        script_name="_code_execute.py",
        env_args=["-e", "PYTHONPATH=/app"],
    )

    assert command[:3] == ["docker", "run", "-d"]
    assert _option_value(command, "--network") == "none"
    assert _option_value(command, "--cap-drop") == "ALL"
    assert _option_value(command, "--security-opt") == "no-new-privileges"
    assert "--read-only" in command
    assert _option_value(command, "--tmpfs") == "/tmp:rw,noexec,nosuid,size=64m"
    assert _option_value(command, "--memory") == "512m"
    assert _option_value(command, "--cpus") == "1.0"
    assert _option_value(command, "--pids-limit") == "128"
    assert _option_value(command, "-v") == f"{tmp_path}:/workspace:rw"
    assert _option_value(command, "-w") == "/workspace"
    assert "--privileged" not in command
    assert "seccomp=unconfined" not in command
    assert command[-4:] == [
        "finance-cli-code-exec:test",
        "python3",
        "-u",
        "/workspace/_code_execute.py",
    ]


def test_build_secure_code_execution_uses_secure_docker_backend(tmp_path: Path) -> None:
    session = SimpleNamespace(code_execution_work_dir=None, background_tasks={})
    bundle = build_secure_code_execution(
        session,
        CodeExecutionConfig(
            docker_image="finance-cli-code-exec:test",
            register_subprocess=False,
            work_dir_root=str(tmp_path),
            work_dir_prefix="cashnerd-code-",
        ),
    )

    assert bundle.approval_qualifier("code_execute", {"host": "docker"}) == "docker"
    assert bundle.needs_approval("code_execute", {"code": "print(1)"}, "docker") is False
    assert bundle.needs_approval("code_execute", {"code": "print(1)"}, "") is True
    assert bundle.needs_approval("goal_list", {}, "") is False
    assert [tool["name"] for tool in bundle.tool_definitions] == [
        "code_execute",
        "code_execute_status",
    ]
    work_dir = bundle.ensure_work_dir()
    assert Path(work_dir).parent == tmp_path
    assert Path(work_dir).name.startswith("cashnerd-code-")
    assert session.code_execution_work_dir == work_dir
    assert bundle.ensure_work_dir() == work_dir


def test_code_execution_work_dir_is_singleton_under_concurrent_access(
    tmp_path: Path,
    monkeypatch,
) -> None:
    session = SimpleNamespace(code_execution_work_dir=None, background_tasks={})
    bundle = build_secure_code_execution(
        session,
        CodeExecutionConfig(
            docker_image="finance-cli-code-exec:test",
            register_subprocess=False,
            work_dir_root=str(tmp_path),
            work_dir_prefix="cashnerd-code-",
        ),
    )
    original_mkdtemp = secure_code_execution_module.tempfile.mkdtemp
    created: list[str] = []

    def slow_mkdtemp(*, prefix: str | None = None, dir: str | None = None) -> str:
        time.sleep(0.02)
        path = original_mkdtemp(prefix=prefix, dir=dir)
        created.append(path)
        return path

    monkeypatch.setattr(secure_code_execution_module.tempfile, "mkdtemp", slow_mkdtemp)
    with ThreadPoolExecutor(max_workers=16) as pool:
        work_dirs = list(pool.map(lambda _: bundle.ensure_work_dir(), range(16)))

    assert len(set(work_dirs)) == 1
    assert created == [work_dirs[0]]
    assert session.code_execution_work_dir == work_dirs[0]


def test_secure_backend_keeps_docker_sandbox_classification() -> None:
    backend = SecureDockerBackend(image="finance-cli-code-exec:test")

    assert backend.name == "docker"
    assert backend.sandboxed is True


def _option_value(command: list[str], option: str) -> str:
    index = command.index(option)
    return command[index + 1]
