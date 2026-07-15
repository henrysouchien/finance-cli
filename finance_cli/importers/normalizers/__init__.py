"""CSV normalizer discovery, loading, and user-module subprocess proxies."""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from ... import normalizer_sidecars
from ...institution_names import is_known, user_registry_path
from ...user_context import get_user_context
from ..normalizer_sandbox import ModuleMetadata, validate_normalizer_source

logger = logging.getLogger(__name__)

BUILT_IN_TIER = "built_in"
USER_TIER = "user"
HARNESS_TIMEOUT_SECONDS = 30
_USER_HOME_ENV = "FINANCE_CLI_HOME"
_USER_DIR_ENV = "FINANCE_CLI_NORMALIZER_DIR"
_DOCKER_IMAGE = os.environ.get(
    "CODE_EXECUTE_DOCKER_IMAGE", "finance-cli-code-exec:latest"
)
_DOCKER_SANDBOX_ENABLED = os.environ.get(
    "NORMALIZER_DOCKER_SANDBOX", "0"
).strip().lower() not in {"0", "false", "no", ""}
_docker_ok: bool | None = None


@dataclass(frozen=True)
class RegistryEntry:
    primary_key: str
    aliases: tuple[str, ...]
    source_name: str
    tier: str
    file_path: Path
    detect_fn: Callable[[list[str]], bool]
    normalize_fn: Callable[[Path], Any]

    @property
    def keys(self) -> tuple[str, ...]:
        return (self.primary_key, *self.aliases)


def normalize_registry_key(value: str) -> str:
    return re.sub(r"[\s-]+", "_", str(value or "").strip().lower())


def resolve_user_normalizers_dir(user_dir: Path | None = None) -> Path:
    if user_dir is not None:
        return Path(user_dir).expanduser().resolve()
    explicit = os.getenv(_USER_DIR_ENV)
    if explicit:
        return Path(explicit).expanduser().resolve()
    user_context = get_user_context()
    if (
        user_context is not None
        and not user_context.local_mode
        and user_context.expected_user_id is not None
    ):
        return Path(user_context.db_path).expanduser().resolve().parent / "normalizers"
    home_override = os.getenv(_USER_HOME_ENV)
    if home_override:
        base = Path(home_override).expanduser().resolve()
    else:
        base = (Path.home() / ".finance_cli").expanduser().resolve()
    return base / "normalizers"


def staging_normalizers_dir(user_dir: Path | None = None) -> Path:
    return resolve_user_normalizers_dir(user_dir) / ".staging"


def load_user_module_metadata(module_path: Path) -> ModuleMetadata:
    return validate_normalizer_source(
        read_normalizer_text(module_path), filename=str(module_path)
    )


def _remote_normalizer_target() -> tuple[str, str] | None:
    if os.getenv(_USER_DIR_ENV):
        return None
    return normalizer_sidecars.remote_sidecar_target()


def remote_normalizers_enabled() -> bool:
    return _remote_normalizer_target() is not None


def normalizer_storage_relative_path(path: Path) -> str:
    base = resolve_user_normalizers_dir()
    relative = Path(path).expanduser().resolve().relative_to(base)
    return str(PurePosixPath("normalizers", *relative.parts))


def _write_cache_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            if path.read_text(encoding="utf-8") == text:
                return
        except OSError:
            pass
    path.write_text(text, encoding="utf-8")


def normalizer_file_exists(path: Path) -> bool:
    remote_target = _remote_normalizer_target()
    if remote_target is None:
        return Path(path).exists()
    return normalizer_sidecars.exists(
        normalizer_storage_relative_path(path),
        target_info=remote_target,
    )


def read_normalizer_text(path: Path) -> str:
    path = Path(path)
    remote_target = _remote_normalizer_target()
    if remote_target is None:
        return path.read_text(encoding="utf-8")
    content = normalizer_sidecars.read_text(
        normalizer_storage_relative_path(path),
        target_info=remote_target,
    )
    assert content is not None
    _write_cache_text(path, content)
    return content


def write_normalizer_text(path: Path, text: str) -> None:
    path = Path(path)
    remote_target = _remote_normalizer_target()
    if remote_target is None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return
    normalizer_sidecars.write_text(
        normalizer_storage_relative_path(path),
        text,
        target_info=remote_target,
    )
    _write_cache_text(path, text)


def delete_normalizer_file(path: Path) -> None:
    path = Path(path)
    remote_target = _remote_normalizer_target()
    if remote_target is None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return
    normalizer_sidecars.delete_file(
        normalizer_storage_relative_path(path),
        target_info=remote_target,
    )


def replace_normalizer_file(source_path: Path, target_path: Path) -> None:
    source_path = Path(source_path)
    target_path = Path(target_path)
    remote_target = _remote_normalizer_target()
    if remote_target is None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.replace(target_path)
        return
    content = read_normalizer_text(source_path)
    normalizer_sidecars.write_text(
        normalizer_storage_relative_path(target_path),
        content,
        target_info=remote_target,
    )
    try:
        _write_cache_text(target_path, content)
    except OSError as exc:
        logger.warning(
            "Failed to refresh normalizer cache path=%s error=%s", target_path, exc
        )
    try:
        normalizer_sidecars.delete_file(
            normalizer_storage_relative_path(source_path),
            target_info=remote_target,
        )
    except Exception as exc:
        logger.warning(
            "Failed to delete staged remote normalizer path=%s error=%s",
            source_path,
            exc,
        )


def normalizer_file_content_hash(path: Path) -> str:
    return hashlib.sha256(read_normalizer_text(path).encode("utf-8")).hexdigest()


def _remote_active_normalizer_paths(remote_target: tuple[str, str]) -> list[str]:
    paths = normalizer_sidecars.list_paths("normalizers", target_info=remote_target)
    active_paths: list[str] = []
    for raw_path in paths:
        path = PurePosixPath(raw_path)
        if len(path.parts) != 2:
            continue
        if path.parts[0] != "normalizers":
            continue
        name = path.parts[1]
        if name.startswith(".") or not name.endswith(".py"):
            continue
        active_paths.append(str(path))
    return sorted(active_paths)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _minimal_subprocess_env() -> dict[str, str]:
    env: dict[str, str] = {}
    current_path = os.environ.get("PATH")
    if current_path:
        env["PATH"] = current_path
    env["PYTHONPATH"] = str(_project_root())
    if os.name == "nt":
        system_root = os.environ.get("SYSTEMROOT")
        if system_root:
            env["SYSTEMROOT"] = system_root
    return env


def _docker_available() -> bool:
    global _docker_ok
    if _docker_ok is not None:
        return _docker_ok
    if not _DOCKER_SANDBOX_ENABLED:
        _docker_ok = False
        return False
    if not shutil.which("docker"):
        _docker_ok = False
        return False
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", _DOCKER_IMAGE],
            capture_output=True,
            timeout=5,
            check=False,
        )
        _docker_ok = result.returncode == 0
    except Exception:
        _docker_ok = False
    return _docker_ok


def _harness_source_paths() -> tuple[Path, Path]:
    """Return paths to normalizer_harness.py and normalizer_sandbox.py."""
    importers_dir = Path(__file__).resolve().parent.parent
    return (
        importers_dir / "normalizer_harness.py",
        importers_dir / "normalizer_sandbox.py",
    )


def _run_harness_docker(*args: str, stdin_payload: str | None = None) -> dict[str, Any]:
    """Run the normalizer harness inside a Docker container with minimal mounts."""
    harness_path, sandbox_path = _harness_source_paths()

    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        "--memory",
        "256m",
        "--cpus",
        "0.5",
        "--pids-limit",
        "64",
        "-v",
        f"{harness_path}:/workspace/normalizer_harness.py:ro",
        "-v",
        f"{sandbox_path}:/workspace/normalizer_sandbox.py:ro",
        "-w",
        "/workspace",
    ]

    remapped_args: list[str] = []
    seen_names: set[str] = set()
    for arg in args:
        if arg.startswith("-"):
            remapped_args.append(arg)
            continue
        path = Path(arg)
        if path.is_file():
            name = path.name
            if name in seen_names:
                stem, suffix = path.stem, path.suffix
                counter = 2
                while name in seen_names:
                    name = f"{stem}_{counter}{suffix}"
                    counter += 1
            seen_names.add(name)
            container_path = f"/workspace/input/{name}"
            cmd.extend(["-v", f"{path.resolve()}:{container_path}:ro"])
            remapped_args.append(container_path)
        else:
            remapped_args.append(arg)

    if stdin_payload is not None:
        cmd.append("-i")

    cmd.extend(
        [
            _DOCKER_IMAGE,
            "python3",
            "/workspace/normalizer_harness.py",
        ]
    )
    cmd.extend(remapped_args)

    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            input=stdin_payload,
            text=True,
            timeout=HARNESS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(
            f"normalizer subprocess timed out after {HARNESS_TIMEOUT_SECONDS} seconds"
        ) from exc

    if completed.returncode != 0:
        stderr = (
            completed.stderr.strip() or completed.stdout.strip() or "subprocess failed"
        )
        raise ValueError(stderr)

    try:
        return json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError("normalizer subprocess returned invalid JSON") from exc


def _run_harness(*args: str, stdin_payload: str | None = None) -> dict[str, Any]:
    if _docker_available():
        return _run_harness_docker(*args, stdin_payload=stdin_payload)

    cmd = [sys.executable, "-m", "finance_cli.importers.normalizer_harness", *args]
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            env=_minimal_subprocess_env(),
            input=stdin_payload,
            text=True,
            timeout=HARNESS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(
            f"normalizer subprocess timed out after {HARNESS_TIMEOUT_SECONDS} seconds"
        ) from exc

    if completed.returncode != 0:
        stderr = (
            completed.stderr.strip() or completed.stdout.strip() or "subprocess failed"
        )
        raise ValueError(stderr)

    try:
        return json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError("normalizer subprocess returned invalid JSON") from exc


def run_user_normalizer_detect(module_path: Path, lines: list[str]) -> bool:
    payload = _run_harness(
        "--detect", str(module_path), stdin_payload=json.dumps(lines)
    )
    return bool(payload.get("match"))


def run_user_normalizer_normalize_with_validation(
    module_path: Path,
    file_path: Path,
    *,
    expected_source_name: str | None = None,
):
    payload = _run_harness("--normalize", str(module_path), str(file_path))
    from ..csv_normalizers import (
        normalize_result_from_payload,
        validate_normalize_result,
    )

    result = normalize_result_from_payload(payload)
    validation = validate_normalize_result(
        result, expected_source_name=expected_source_name
    )
    return result, validation


def run_user_normalizer_normalize(
    module_path: Path,
    file_path: Path,
    *,
    expected_source_name: str | None = None,
):
    result, validation = run_user_normalizer_normalize_with_validation(
        module_path,
        file_path,
        expected_source_name=expected_source_name,
    )
    if not validation["valid"]:
        issues = "; ".join(validation["issues"][:5])
        raise ValueError(f"invalid normalizer output: {issues}")
    return result


class NormalizerLoader:
    def __init__(self, *, user_dir: Path | None = None) -> None:
        self.user_dir = resolve_user_normalizers_dir(user_dir)
        self.built_in_dir = Path(__file__).resolve().parent
        self._snapshot: dict[str, str] = {}
        self._entries_in_load_order: list[RegistryEntry] = []
        self._registry_by_key: dict[str, RegistryEntry] = {}

    def refresh(self) -> None:
        snapshot = self._snapshot_files()
        if snapshot != self._snapshot:
            self._reload(snapshot)

    def get_entry(self, key: str) -> RegistryEntry | None:
        self.refresh()
        return self._registry_by_key.get(normalize_registry_key(key))

    def list_entries(self) -> list[RegistryEntry]:
        self.refresh()
        return list(self._entries_in_load_order)

    def supported_keys(self) -> list[str]:
        self.refresh()
        return sorted(self._registry_by_key.keys())

    def detection_entries(self) -> list[RegistryEntry]:
        self.refresh()
        return list(self._entries_in_load_order)

    def _snapshot_files(self) -> dict[str, str]:
        snapshot: dict[str, str] = {}
        for path in self._built_in_module_files():
            try:
                snapshot[str(path)] = f"local:{path.stat().st_mtime_ns}"
            except FileNotFoundError:
                continue
        remote_target = _remote_normalizer_target()
        if remote_target is not None:
            self._sync_remote_user_modules(remote_target)
            registry_content = normalizer_sidecars.read_text(
                "institution_names.json",
                target_info=remote_target,
                missing_ok=True,
            )
            if registry_content is not None:
                snapshot["remote:institution_names.json"] = hashlib.sha256(
                    registry_content.encode("utf-8")
                ).hexdigest()
            for path in self._remote_user_module_files(remote_target):
                try:
                    content = path.read_text(encoding="utf-8")
                except FileNotFoundError:
                    continue
                snapshot[f"remote:{normalizer_storage_relative_path(path)}"] = (
                    hashlib.sha256(content.encode("utf-8")).hexdigest()
                )
            return snapshot

        for path in [*self._user_module_files(), user_registry_path()]:
            try:
                snapshot[str(path)] = f"local:{path.stat().st_mtime_ns}"
            except FileNotFoundError:
                continue
        return snapshot

    def _built_in_module_files(self) -> list[Path]:
        return sorted(
            path
            for path in self.built_in_dir.glob("*.py")
            if path.name != "__init__.py" and not path.name.startswith("_")
        )

    def _user_module_files(self) -> list[Path]:
        remote_target = _remote_normalizer_target()
        if remote_target is not None:
            return self._remote_user_module_files(remote_target)
        if not self.user_dir.exists():
            return []
        return sorted(path for path in self.user_dir.glob("*.py") if path.is_file())

    def _sync_remote_user_modules(self, remote_target: tuple[str, str]) -> None:
        for relative_path in _remote_active_normalizer_paths(remote_target):
            content = normalizer_sidecars.read_text(
                relative_path, target_info=remote_target
            )
            assert content is not None
            _write_cache_text(self.user_dir / Path(relative_path).name, content)

    def _remote_user_module_files(self, remote_target: tuple[str, str]) -> list[Path]:
        self._sync_remote_user_modules(remote_target)
        paths: list[Path] = []
        for relative_path in _remote_active_normalizer_paths(remote_target):
            paths.append(self.user_dir / Path(relative_path).name)
        return sorted(paths)

    def _reload(self, snapshot: dict[str, str]) -> None:
        entries: list[RegistryEntry] = []
        registry: dict[str, RegistryEntry] = {}

        for path in self._built_in_module_files():
            try:
                entry = self._load_built_in_entry(path)
            except Exception as exc:
                logger.warning(
                    "Failed to load built-in normalizer path=%s error=%s", path, exc
                )
                continue
            self._register_entry(entry, entries, registry)

        for path in self._user_module_files():
            try:
                entry = self._load_user_entry(path)
            except Exception as exc:
                logger.warning(
                    "Failed to load user normalizer path=%s error=%s", path, exc
                )
                continue
            self._register_entry(entry, entries, registry)

        self._entries_in_load_order = entries
        self._registry_by_key = registry
        self._snapshot = snapshot

    def _register_entry(
        self,
        entry: RegistryEntry,
        entries: list[RegistryEntry],
        registry: dict[str, RegistryEntry],
    ) -> None:
        conflicts = [key for key in entry.keys if key in registry]
        if conflicts:
            logger.warning(
                "Skipping normalizer primary_key=%s path=%s due to duplicate keys=%s",
                entry.primary_key,
                entry.file_path,
                ",".join(conflicts),
            )
            return
        for key in entry.keys:
            registry[key] = entry
        entries.append(entry)

    def _load_built_in_entry(self, path: Path) -> RegistryEntry:
        module_name = f"{__name__}.{path.stem}"
        if module_name in sys.modules:
            module = importlib.reload(sys.modules[module_name])
        else:
            module = importlib.import_module(module_name)
        return self._entry_from_module(
            primary_key=getattr(module, "PRIMARY_KEY", ""),
            aliases=getattr(module, "ALIASES", []),
            source_name=getattr(module, "SOURCE_NAME", ""),
            detect_fn=getattr(module, "detect", None),
            normalize_fn=getattr(module, "normalize", None),
            tier=BUILT_IN_TIER,
            file_path=path,
        )

    def _load_user_entry(self, path: Path) -> RegistryEntry:
        metadata = load_user_module_metadata(path)
        return self._entry_from_module(
            primary_key=metadata.primary_key,
            aliases=metadata.aliases,
            source_name=metadata.source_name,
            detect_fn=lambda lines, module_path=path: run_user_normalizer_detect(
                module_path, lines
            ),
            normalize_fn=lambda file_path, module_path=path, source_name=metadata.source_name: (
                run_user_normalizer_normalize(
                    module_path,
                    file_path,
                    expected_source_name=source_name,
                )
            ),
            tier=USER_TIER,
            file_path=path,
        )

    def _entry_from_module(
        self,
        *,
        primary_key: str,
        aliases: list[str] | tuple[str, ...],
        source_name: str,
        detect_fn: Any,
        normalize_fn: Any,
        tier: str,
        file_path: Path,
    ) -> RegistryEntry:
        normalized_primary_key = normalize_registry_key(primary_key)
        normalized_source_name = str(source_name or "").strip()
        if not normalized_primary_key:
            raise ValueError("PRIMARY_KEY must be a non-empty string")
        if not normalized_source_name:
            raise ValueError("SOURCE_NAME must be a non-empty string")
        if not is_known(normalized_source_name):
            raise ValueError(
                f"SOURCE_NAME '{normalized_source_name}' is not registered"
            )
        if not callable(detect_fn):
            raise ValueError("detect must be callable")
        if not callable(normalize_fn):
            raise ValueError("normalize must be callable")

        normalized_aliases: list[str] = []
        for alias in aliases:
            key = normalize_registry_key(alias)
            if not key or key == normalized_primary_key or key in normalized_aliases:
                continue
            normalized_aliases.append(key)

        return RegistryEntry(
            primary_key=normalized_primary_key,
            aliases=tuple(normalized_aliases),
            source_name=normalized_source_name,
            tier=tier,
            file_path=file_path,
            detect_fn=detect_fn,
            normalize_fn=normalize_fn,
        )


_LOADER_CACHE: dict[Path, NormalizerLoader] = {}
_LOADER_CACHE_LOCK = threading.RLock()


def get_normalizer_loader(*, user_dir: Path | None = None) -> NormalizerLoader:
    if user_dir is not None:
        return NormalizerLoader(user_dir=user_dir)

    resolved = resolve_user_normalizers_dir()
    with _LOADER_CACHE_LOCK:
        loader = _LOADER_CACHE.get(resolved)
        if loader is None:
            loader = NormalizerLoader(user_dir=resolved)
            _LOADER_CACHE[resolved] = loader
        return loader


def reset_normalizer_loader_cache() -> None:
    with _LOADER_CACHE_LOCK:
        _LOADER_CACHE.clear()


__all__ = [
    "BUILT_IN_TIER",
    "HARNESS_TIMEOUT_SECONDS",
    "NormalizerLoader",
    "RegistryEntry",
    "USER_TIER",
    "delete_normalizer_file",
    "get_normalizer_loader",
    "load_user_module_metadata",
    "normalizer_file_content_hash",
    "normalizer_file_exists",
    "normalizer_storage_relative_path",
    "normalize_registry_key",
    "read_normalizer_text",
    "remote_normalizers_enabled",
    "replace_normalizer_file",
    "reset_normalizer_loader_cache",
    "resolve_user_normalizers_dir",
    "run_user_normalizer_detect",
    "run_user_normalizer_normalize",
    "run_user_normalizer_normalize_with_validation",
    "staging_normalizers_dir",
    "write_normalizer_text",
]
