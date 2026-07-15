"""Hardened code-execution integration for the finance gateway."""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from agent_gateway.code_execution import (
    BackgroundTask,
    CodeExecutionBundle,
    CodeExecutionConfig,
    DockerBackend,
    ExecutionBackend,
    ExecutionHandle,
    OnOutputChunk,
    OutputRingBuffer,
    SubprocessBackend,
    make_code_execute_status_tool_def,
    make_code_execute_tool_def,
    strip_code_execute_base64_hook,
)
from agent_gateway.code_execution._backends._docker import (
    _STREAM_READER_LIMIT,
    _read_stream_to_file,
    _snapshot_image_mtimes,
    _task_stderr_path,
    _task_stdout_path,
    _write_code_execute_script,
)
from agent_gateway.code_execution._helpers import _prepare_code_execute_env, code_execute


def build_secure_docker_run_command(
    *,
    image: str,
    work_dir_path: Path,
    script_name: str,
    env_args: list[str],
) -> list[str]:
    """Build the hardened `docker run` command used for code execution."""
    command = [
        "docker",
        "run",
        "-d",
        "--network",
        "none",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,size=64m",
        "--memory",
        "512m",
        "--cpus",
        "1.0",
        "--pids-limit",
        "128",
        "-v",
        f"{work_dir_path}:/workspace:rw",
        "-w",
        "/workspace",
        "-e",
        "PYTHONUNBUFFERED=1",
        "-e",
        "MPLCONFIGDIR=/workspace/.code_execute_cache/mplconfig",
        "-e",
        "XDG_CACHE_HOME=/workspace/.code_execute_cache/xdg",
    ]
    command.extend(env_args)
    command.extend(
        [
            image,
            "python3",
            "-u",
            f"/workspace/{script_name}",
        ]
    )
    return command


class SecureDockerBackend(DockerBackend):
    """Docker backend with explicit sandbox hardening flags.

    Docker applies its default seccomp profile unless the daemon or caller
    disables it. This backend adds runtime controls that are not defaults:
    no network, dropped capabilities, no-new-privileges, read-only rootfs,
    tmpfs-backed `/tmp`, and resource limits.
    """

    async def start(
        self,
        code: str,
        work_dir: str,
        *,
        task_id: str = "",
        timeout_ms: int = 30_000,
        env: Optional[Dict[str, str]] = None,
        on_output: Optional[OnOutputChunk] = None,
    ) -> ExecutionHandle:
        work_dir_path = Path(work_dir)
        work_dir_path.mkdir(parents=True, exist_ok=True)
        cache_root = work_dir_path / ".code_execute_cache"
        (cache_root / "mplconfig").mkdir(parents=True, exist_ok=True)
        (cache_root / "xdg").mkdir(parents=True, exist_ok=True)
        handle_id = uuid.uuid4().hex
        script_path = _write_code_execute_script(
            work_dir_path,
            code,
            self._config,
            task_id=task_id,
        )
        stdout_file = (
            _task_stdout_path(work_dir_path, task_id)
            if task_id
            else work_dir_path / f"_code_execute_{handle_id}_stdout.log"
        )
        stderr_file = (
            _task_stderr_path(work_dir_path, task_id)
            if task_id
            else work_dir_path / f"_code_execute_{handle_id}_stderr.log"
        )
        before_mtimes = None if task_id else _snapshot_image_mtimes(work_dir_path)
        command = build_secure_docker_run_command(
            image=self._image,
            work_dir_path=work_dir_path,
            script_name=script_path.name,
            env_args=self._env_args(env),
        )
        run_proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_STREAM_READER_LIMIT,
        )
        stdout_bytes, stderr_bytes = await run_proc.communicate()
        if run_proc.returncode != 0:
            stderr = stderr_bytes.decode("utf-8", errors="replace").strip() or "docker run failed"
            raise RuntimeError(stderr)
        container_id = stdout_bytes.decode("utf-8", errors="replace").strip()
        if not container_id:
            raise RuntimeError("docker run did not return a container id")

        data: Dict[str, Any] = {
            "task_id": task_id,
            "container_id": container_id,
            "script_path": script_path,
            "stdout_file": stdout_file,
            "stderr_file": stderr_file,
            "started_at": time.time(),
            "started_ns": time.time_ns(),
            "before_mtimes": before_mtimes,
            "completed": False,
            "timed_out": False,
            "return_code": None,
        }
        handle = ExecutionHandle(
            backend_name=self.name,
            handle_id=handle_id,
            work_dir=str(work_dir_path),
            _backend_data=data,
        )

        async def _monitor() -> None:
            logs_proc = await asyncio.create_subprocess_exec(
                "docker",
                "logs",
                "-f",
                container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_STREAM_READER_LIMIT,
            )
            data["logs_process"] = logs_proc
            stdout_task = asyncio.create_task(
                _read_stream_to_file(logs_proc.stdout, stdout_file, "stdout", on_output)
            )
            stderr_task = asyncio.create_task(
                _read_stream_to_file(logs_proc.stderr, stderr_file, "stderr", on_output)
            )
            wait_proc = await asyncio.create_subprocess_exec(
                "docker",
                "wait",
                container_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_STREAM_READER_LIMIT,
            )
            data["wait_process"] = wait_proc
            try:
                try:
                    wait_stdout, _wait_stderr = await asyncio.wait_for(
                        wait_proc.communicate(),
                        timeout=timeout_ms / 1000,
                    )
                    wait_text = wait_stdout.decode("utf-8", errors="replace").strip()
                    if wait_text:
                        try:
                            data["return_code"] = int(wait_text.splitlines()[-1])
                        except ValueError:
                            data["return_code"] = None
                except asyncio.TimeoutError:
                    data["timed_out"] = True
                    data["return_code"] = 124
                    await self.cancel(handle)
            finally:
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
                await asyncio.gather(logs_proc.wait(), return_exceptions=True)
                data["completed"] = True

        data["reader_task"] = asyncio.create_task(_monitor())
        return handle


def build_secure_code_execution(
    session: Any,
    config: CodeExecutionConfig | None = None,
) -> CodeExecutionBundle:
    """Create code-execution tools using the secure Docker backend."""
    cfg = config or CodeExecutionConfig()

    backends: Dict[str, ExecutionBackend] = {}
    if cfg.register_subprocess:
        backends["subprocess"] = SubprocessBackend(config=cfg)
    if cfg.register_docker:
        backends["docker"] = SecureDockerBackend(image=cfg.docker_image or None, config=cfg)

    def _get_backend(name: str | None = None) -> ExecutionBackend:
        if name:
            backend = backends.get(name)
            if backend is not None:
                return backend
            raise ValueError(f"Unknown backend: '{name}'. Available: {sorted(backends)}")
        for preferred in ("docker", "subprocess"):
            backend = backends.get(preferred)
            if backend is not None and backend.available():
                return backend
        raise RuntimeError("No execution backend available")

    def _get_registered_backend_names() -> list[str]:
        return list(backends.keys())

    work_dir_lock = threading.Lock()

    def _ensure_code_execution_work_dir() -> str:
        with work_dir_lock:
            if not session.code_execution_work_dir:
                session.code_execution_work_dir = tempfile.mkdtemp(
                    prefix=cfg.work_dir_prefix,
                    dir=cfg.work_dir_root,
                )
            return session.code_execution_work_dir

    def _handle_has_exited(handle: Any) -> bool:
        backend_data = getattr(handle, "_backend_data", None)
        if not isinstance(backend_data, dict):
            return False
        process = backend_data.get("process")
        return process is not None and getattr(process, "returncode", None) is not None

    async def _handle_code_execute(tool_input: Dict[str, Any], **kwargs: Any):
        host = str(tool_input.get("host") or "auto")
        valid_hosts = {"auto"} | set(_get_registered_backend_names())
        if host not in valid_hosts:
            return None, {"code": "invalid_input", "message": f"Unknown host: '{host}'"}

        tool_ctx = kwargs.get("tool_ctx")
        resolved_host = (
            getattr(tool_ctx, "resolved_qualifier", "") if tool_ctx is not None else ""
        )
        if not resolved_host:
            return None, {"code": "internal_error", "message": "Backend resolution failed"}

        backend = _get_backend(resolved_host)
        if not backend.available():
            return None, {
                "code": "backend_unavailable",
                "message": f"Backend '{resolved_host}' unavailable",
            }

        work_dir = _ensure_code_execution_work_dir()
        background = bool(tool_input.get("background", False))
        if background:
            code = str(tool_input.get("code") or "")
            if not code.strip():
                return None, {"code": "invalid_input", "message": "code is required"}
            timeout_ms_raw = tool_input.get("timeout_ms", cfg.default_timeout_ms)
            try:
                timeout_ms = int(timeout_ms_raw)
            except (TypeError, ValueError):
                return None, {
                    "code": "invalid_input",
                    "message": "timeout_ms must be an integer",
                }
            timeout_ms = max(1000, min(timeout_ms, cfg.max_timeout_ms))
            env = _prepare_code_execute_env(cfg)
            task_id = f"ce_{os.urandom(4).hex()}"
            stdout_buf = OutputRingBuffer()
            stderr_buf = OutputRingBuffer()

            def _on_bg_output(stream_name: str, text: str) -> None:
                if stream_name == "stderr":
                    stderr_buf.append(stream_name, text)
                    return
                stdout_buf.append(stream_name, text)

            handle = await backend.start(
                code,
                work_dir,
                task_id=task_id,
                timeout_ms=timeout_ms,
                env=env,
                on_output=_on_bg_output,
            )
            session.background_tasks[task_id] = BackgroundTask(
                task_id=task_id,
                handle=handle,
                backend=backend,
                stdout_buf=stdout_buf,
                stderr_buf=stderr_buf,
                started_at=time.time(),
            )
            return {
                "status": "running",
                "task_id": task_id,
                "message": "Use code_execute_status(task_id=...) to check progress.",
            }, None

        chunk_seq = [0]

        def _on_chunk(stream_name: str, text: str) -> None:
            if tool_ctx is None:
                return
            chunk_seq[0] += 1
            tool_ctx.emit(
                {
                    "type": "tool_output_chunk",
                    "tool_call_id": tool_ctx.tool_call_id,
                    "tool_name": "code_execute",
                    "stream": stream_name,
                    "text": text,
                    "seq": chunk_seq[0],
                }
            )

        return await code_execute(
            tool_input,
            session_work_dir=work_dir,
            on_output=_on_chunk,
            backend=backend,
            config=cfg,
        )

    async def _handle_code_execute_status(tool_input: Dict[str, Any], **_: Any):
        task_id = str(tool_input.get("task_id") or "").strip()
        if not task_id:
            return None, {"code": "invalid_input", "message": "task_id is required"}
        task = session.background_tasks.get(task_id)
        if task is None:
            return None, {"code": "not_found", "message": f"Unknown task_id: {task_id}"}

        backend = task.backend
        if task._in_progress:
            return {
                "status": "running",
                "task_id": task_id,
                "message": "Lifecycle op in progress",
            }, None

        poll_result = await backend.poll(task.handle)
        cancel = bool(tool_input.get("cancel", False))
        if cancel:
            if poll_result.get("status") == "completed" or _handle_has_exited(task.handle):
                result = await task.safe_collect(backend)
                if task._terminated:
                    session.background_tasks.pop(task_id, None)
                return result, None
            await task.safe_cancel(backend)
            if task._terminated:
                session.background_tasks.pop(task_id, None)
            return {"status": "cancelled", "task_id": task_id}, None

        if poll_result.get("status") == "completed":
            result = await task.safe_collect(backend)
            if task._terminated:
                session.background_tasks.pop(task_id, None)
            return result, None

        return {
            "status": "running",
            "task_id": task_id,
            "stdout_tail": task.stdout_buf.tail(20),
            "stderr_tail": task.stderr_buf.tail(5),
        }, None

    def _approval_qualifier(tool_name: str, tool_input: Dict[str, Any]) -> str:
        if tool_name != "code_execute":
            return ""
        host = str(tool_input.get("host") or "auto")
        try:
            return _get_backend(host if host != "auto" else None).name
        except (RuntimeError, ValueError):
            return ""

    def _needs_approval(
        tool_name: str,
        tool_input: Dict[str, Any] | None = None,
        qualifier: str = "",
    ) -> bool:
        _ = tool_input
        if tool_name != "code_execute":
            return False
        if qualifier:
            try:
                return not _get_backend(qualifier).sandboxed
            except (RuntimeError, ValueError):
                return True
        return True

    available_hosts = ("auto",) + tuple(_get_registered_backend_names())
    bundle = CodeExecutionBundle(
        handlers={
            "code_execute": _handle_code_execute,
            "code_execute_status": _handle_code_execute_status,
        },
        tool_definitions=[
            make_code_execute_tool_def(cfg, available_hosts),
            make_code_execute_status_tool_def(),
        ],
        approval_qualifier=_approval_qualifier,
        needs_approval=_needs_approval,
        sanitize_hook=strip_code_execute_base64_hook,
    )
    bundle.ensure_work_dir = _ensure_code_execution_work_dir  # type: ignore[attr-defined]
    return bundle
