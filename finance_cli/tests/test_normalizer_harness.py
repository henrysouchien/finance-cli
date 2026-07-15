from __future__ import annotations

import importlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from textwrap import indent

import pytest

import finance_cli.importers.normalizers as user_normalizers


_DEFAULT_NORMALIZE_BODY = """
reader = csv.DictReader(io.StringIO("".join(lines)))
rows = []
for row in reader:
    rows.append(
        {
            "Date": row["Date"],
            "Description": row["Description"],
            "Amount": row["Amount"],
            "Account Type": "checking",
            "Source": "Wrong Source",
        }
    )
return NormalizeResult(
    rows=rows,
    source_name="Wrong Source",
    warnings=["demo warning"],
    raw_row_count=len(rows),
    skipped_row_count=0,
)
""".strip()


def _module_source(
    *,
    primary_key: str = "demo_bank",
    aliases: tuple[str, ...] = ("demo",),
    source_name: str = "Demo Bank",
    imports: str = "import csv\nimport io",
    detect_body: str = 'return any("Demo Header" in line for line in lines)',
    normalize_body: str = _DEFAULT_NORMALIZE_BODY,
) -> str:
    parts = [
        f"PRIMARY_KEY = {primary_key!r}",
        f"ALIASES = {[alias for alias in aliases]!r}",
        f"SOURCE_NAME = {source_name!r}",
    ]
    if imports:
        parts.append(imports)
    parts.append(f"def detect(lines):\n{indent(detect_body, '    ')}")
    parts.append(f"def normalize(lines, file_name):\n{indent(normalize_body, '    ')}")
    return "\n\n".join(parts) + "\n"


def _write_user_module(path: Path, *, source: str | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source or _module_source(), encoding="utf-8")
    return path


def _write_csv(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "Date,Description,Amount\n2026-03-01,Coffee,-4.50\n",
        encoding="utf-8-sig",
    )
    return path


def _write_text(path: Path, content: str = "# stub\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_harness_bundle(base_dir: Path) -> tuple[Path, Path]:
    return (
        _write_text(base_dir / "normalizer_harness.py"),
        _write_text(base_dir / "normalizer_sandbox.py"),
    )


def _docker_volume_specs(cmd: list[str]) -> list[str]:
    return [cmd[index + 1] for index, part in enumerate(cmd[:-1]) if part == "-v"]


def _reload_user_normalizers():
    module = importlib.reload(user_normalizers)
    module._docker_ok = None
    return module


def _load_module_from_path(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        sys.modules.pop(module_name, None)


@pytest.fixture(autouse=True)
def _reset_normalizer_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NORMALIZER_DOCKER_SANDBOX", raising=False)
    monkeypatch.delenv("CODE_EXECUTE_DOCKER_IMAGE", raising=False)
    _reload_user_normalizers()


def _run_harness(*args: str, stdin_payload: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "finance_cli.importers.normalizer_harness", *args],
        capture_output=True,
        input=stdin_payload,
        text=True,
        check=False,
    )


def test_harness_detect_mode_returns_match_flag(tmp_path: Path) -> None:
    module_path = _write_user_module(tmp_path / "demo_bank.py")

    result = _run_harness("--detect", str(module_path), stdin_payload=json.dumps(["Demo Header\n"]))

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"match": True}


def test_harness_detect_mode_returns_false_for_non_matching_lines(tmp_path: Path) -> None:
    module_path = _write_user_module(tmp_path / "demo_bank.py")

    result = _run_harness("--detect", str(module_path), stdin_payload=json.dumps(["Other Header\n"]))

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"match": False}


def test_harness_normalize_mode_pins_source_name_and_row_source(tmp_path: Path) -> None:
    module_path = _write_user_module(tmp_path / "demo_bank.py")
    csv_path = _write_csv(tmp_path / "demo.csv")

    result = _run_harness("--normalize", str(module_path), str(csv_path))

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["source_name"] == "Demo Bank"
    assert payload["rows"][0]["Source"] == "Demo Bank"
    assert payload["rows"][0]["Description"] == "Coffee"
    assert payload["warnings"] == ["demo warning"]


def test_run_user_normalizer_detect_times_out(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = _write_user_module(
        tmp_path / "loop.py",
        source=_module_source(
            imports="",
            detect_body="while True:\n    pass",
            normalize_body=(
                'return NormalizeResult(rows=[], source_name=SOURCE_NAME, warnings=[], raw_row_count=0, '
                "skipped_row_count=0)"
            ),
        ),
    )
    monkeypatch.setattr(user_normalizers, "HARNESS_TIMEOUT_SECONDS", 1)

    with pytest.raises(TimeoutError, match=r"timed out after 1 seconds"):
        user_normalizers.run_user_normalizer_detect(module_path, ["Demo Header\n"])


def test_run_user_normalizer_detect_reports_syntax_errors(tmp_path: Path) -> None:
    module_path = _write_user_module(
        tmp_path / "broken.py",
        source="def detect(lines)\n    return True\n",
    )

    with pytest.raises(ValueError, match=r"expected ':'"):
        user_normalizers.run_user_normalizer_detect(module_path, ["Demo Header\n"])


def test_run_user_normalizer_normalize_reports_runtime_errors(tmp_path: Path) -> None:
    module_path = _write_user_module(
        tmp_path / "boom.py",
        source=_module_source(
            source_name="Bank of America",
            imports="",
            normalize_body='raise RuntimeError("boom")',
        ),
    )
    csv_path = _write_csv(tmp_path / "demo.csv")

    with pytest.raises(ValueError, match="boom"):
        user_normalizers.run_user_normalizer_normalize_with_validation(
            module_path,
            csv_path,
            expected_source_name="Bank of America",
        )


def test_run_user_normalizer_normalize_rejects_dataclasses_module_escape(tmp_path: Path) -> None:
    module_path = _write_user_module(
        tmp_path / "dataclasses_probe.py",
        source=_module_source(
            source_name="Bank of America",
            imports="",
            normalize_body="""
dataclasses.sys.modules["os"].environ
return NormalizeResult(
    rows=[
        {
            "Date": "2026-03-01",
            "Description": "Coffee",
            "Amount": "-4.50",
            "Account Type": "checking",
            "Source": SOURCE_NAME,
        }
    ],
    source_name=SOURCE_NAME,
    warnings=[],
    raw_row_count=1,
    skipped_row_count=0,
)
""".strip(),
        ),
    )
    csv_path = _write_csv(tmp_path / "demo.csv")

    with pytest.raises(ValueError, match="name 'dataclasses' is not defined"):
        user_normalizers.run_user_normalizer_normalize_with_validation(
            module_path,
            csv_path,
            expected_source_name="Bank of America",
        )


def test_run_user_normalizer_normalize_rejects_io_fileio(tmp_path: Path) -> None:
    module_path = _write_user_module(
        tmp_path / "fileio_probe.py",
        source=_module_source(
            source_name="Bank of America",
            imports="import io",
            normalize_body="""
io.FileIO("/etc/hosts", "r").read()
return NormalizeResult(rows=[], source_name=SOURCE_NAME, warnings=[], raw_row_count=0, skipped_row_count=0)
""".strip(),
        ),
    )
    csv_path = _write_csv(tmp_path / "demo.csv")

    with pytest.raises(ValueError, match="FileIO"):
        user_normalizers.run_user_normalizer_normalize_with_validation(
            module_path,
            csv_path,
            expected_source_name="Bank of America",
        )


@pytest.mark.parametrize("builtin_name", ["license", "credits", "copyright"])
def test_run_user_normalizer_normalize_rejects_site_printer_builtins(
    tmp_path: Path,
    builtin_name: str,
) -> None:
    module_path = _write_user_module(
        tmp_path / f"{builtin_name}_probe.py",
        source=_module_source(
            source_name="Bank of America",
            imports="",
            normalize_body=f"""
str({builtin_name})
return NormalizeResult(rows=[], source_name=SOURCE_NAME, warnings=[], raw_row_count=0, skipped_row_count=0)
""".strip(),
        ),
    )
    csv_path = _write_csv(tmp_path / "demo.csv")

    with pytest.raises(ValueError, match=rf"name '{builtin_name}' is not defined"):
        user_normalizers.run_user_normalizer_normalize_with_validation(
            module_path,
            csv_path,
            expected_source_name="Bank of America",
        )


def test_docker_available_returns_false_without_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_normalizers, "_DOCKER_SANDBOX_ENABLED", True)
    monkeypatch.setattr(user_normalizers.shutil, "which", lambda _name: None)

    assert user_normalizers._docker_available() is False


def test_docker_available_returns_false_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NORMALIZER_DOCKER_SANDBOX", "0")
    module = _reload_user_normalizers()
    monkeypatch.setattr(module.shutil, "which", lambda _name: pytest.fail("docker should not be probed"))

    assert module._DOCKER_SANDBOX_ENABLED is False
    assert module._docker_available() is False


def test_docker_available_caches_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_normalizers, "_DOCKER_SANDBOX_ENABLED", True)
    monkeypatch.setattr(user_normalizers.shutil, "which", lambda _name: "/usr/bin/docker")
    calls = {"count": 0}

    def fake_run(cmd, **kwargs):
        calls["count"] += 1
        assert cmd == ["docker", "image", "inspect", user_normalizers._DOCKER_IMAGE]
        assert kwargs["capture_output"] is True
        assert kwargs["timeout"] == 5
        assert kwargs["check"] is False
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(user_normalizers.subprocess, "run", fake_run)

    assert user_normalizers._docker_available() is True
    assert user_normalizers._docker_available() is True
    assert calls["count"] == 1


def test_run_harness_docker_mounts_files_extension_agnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_harness, trusted_sandbox = _write_harness_bundle(tmp_path / "trusted")
    module_path = _write_user_module(tmp_path / "input" / "demo_bank.py")
    csv_path = _write_csv(tmp_path / "input" / "demo.csv")
    upper_csv_path = _write_csv(tmp_path / "input" / "statement.CSV")
    extensionless_path = _write_text(tmp_path / "input" / "ledger")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        user_normalizers,
        "_harness_source_paths",
        lambda: (trusted_harness, trusted_sandbox),
    )

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(cmd, 0, "{}", "")

    monkeypatch.setattr(user_normalizers.subprocess, "run", fake_run)

    assert (
        user_normalizers._run_harness_docker(
            "--normalize",
            str(module_path),
            str(csv_path),
            str(upper_csv_path),
            str(extensionless_path),
        )
        == {}
    )

    volumes = _docker_volume_specs(captured["cmd"])
    assert f"{module_path.resolve()}:/workspace/input/demo_bank.py:ro" in volumes
    assert f"{csv_path.resolve()}:/workspace/input/demo.csv:ro" in volumes
    assert f"{upper_csv_path.resolve()}:/workspace/input/statement.CSV:ro" in volumes
    assert f"{extensionless_path.resolve()}:/workspace/input/ledger:ro" in volumes


def test_run_harness_docker_preserves_original_basename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_harness, trusted_sandbox = _write_harness_bundle(tmp_path / "trusted")
    module_path = _write_user_module(tmp_path / "input" / "demo_bank.py")
    csv_path = _write_csv(tmp_path / "input" / "schwab_checking_2026.csv")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        user_normalizers,
        "_harness_source_paths",
        lambda: (trusted_harness, trusted_sandbox),
    )
    monkeypatch.setattr(
        user_normalizers.subprocess,
        "run",
        lambda cmd, **kwargs: captured.update({"cmd": cmd, "kwargs": kwargs})
        or subprocess.CompletedProcess(cmd, 0, "{}", ""),
    )

    user_normalizers._run_harness_docker("--normalize", str(module_path), str(csv_path))

    assert "/workspace/input/schwab_checking_2026.csv" in captured["cmd"]


def test_run_harness_docker_deduplicates_same_basename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_harness, trusted_sandbox = _write_harness_bundle(tmp_path / "trusted")
    first = _write_text(tmp_path / "one" / "statement.csv")
    second = _write_text(tmp_path / "two" / "statement.csv")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        user_normalizers,
        "_harness_source_paths",
        lambda: (trusted_harness, trusted_sandbox),
    )
    monkeypatch.setattr(
        user_normalizers.subprocess,
        "run",
        lambda cmd, **kwargs: captured.update({"cmd": cmd, "kwargs": kwargs})
        or subprocess.CompletedProcess(cmd, 0, "{}", ""),
    )

    user_normalizers._run_harness_docker(str(first), str(second))

    volumes = _docker_volume_specs(captured["cmd"])
    assert f"{first.resolve()}:/workspace/input/statement.csv:ro" in volumes
    assert f"{second.resolve()}:/workspace/input/statement_2.csv:ro" in volumes
    assert "/workspace/input/statement.csv" in captured["cmd"]
    assert "/workspace/input/statement_2.csv" in captured["cmd"]


def test_run_harness_docker_user_files_cannot_shadow_harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_harness, trusted_sandbox = _write_harness_bundle(tmp_path / "trusted")
    user_module = _write_user_module(tmp_path / "input" / "normalizer_harness.py")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        user_normalizers,
        "_harness_source_paths",
        lambda: (trusted_harness, trusted_sandbox),
    )
    monkeypatch.setattr(
        user_normalizers.subprocess,
        "run",
        lambda cmd, **kwargs: captured.update({"cmd": cmd, "kwargs": kwargs})
        or subprocess.CompletedProcess(cmd, 0, "{\"match\":true}", ""),
    )

    assert user_normalizers._run_harness_docker("--detect", str(user_module), stdin_payload="[]") == {"match": True}

    volumes = _docker_volume_specs(captured["cmd"])
    assert f"{trusted_harness}:/workspace/normalizer_harness.py:ro" in volumes
    assert f"{user_module.resolve()}:/workspace/input/normalizer_harness.py:ro" in volumes


def test_run_harness_docker_mounts_harness_and_sandbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_harness, trusted_sandbox = _write_harness_bundle(tmp_path / "trusted")
    module_path = _write_user_module(tmp_path / "input" / "demo_bank.py")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        user_normalizers,
        "_harness_source_paths",
        lambda: (trusted_harness, trusted_sandbox),
    )
    monkeypatch.setattr(
        user_normalizers.subprocess,
        "run",
        lambda cmd, **kwargs: captured.update({"cmd": cmd, "kwargs": kwargs})
        or subprocess.CompletedProcess(cmd, 0, "{\"match\":true}", ""),
    )

    user_normalizers._run_harness_docker("--detect", str(module_path), stdin_payload="[]")

    volumes = _docker_volume_specs(captured["cmd"])
    assert f"{trusted_harness}:/workspace/normalizer_harness.py:ro" in volumes
    assert f"{trusted_sandbox}:/workspace/normalizer_sandbox.py:ro" in volumes


def test_run_harness_docker_detect_mode_passes_stdin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_harness, trusted_sandbox = _write_harness_bundle(tmp_path / "trusted")
    module_path = _write_user_module(tmp_path / "input" / "demo_bank.py")
    captured: dict[str, object] = {}
    stdin_payload = json.dumps(["Demo Header\n"])

    monkeypatch.setattr(
        user_normalizers,
        "_harness_source_paths",
        lambda: (trusted_harness, trusted_sandbox),
    )

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(cmd, 0, "{\"match\":true}", "")

    monkeypatch.setattr(user_normalizers.subprocess, "run", fake_run)

    assert user_normalizers._run_harness_docker("--detect", str(module_path), stdin_payload=stdin_payload) == {
        "match": True
    }

    image_index = captured["cmd"].index(user_normalizers._DOCKER_IMAGE)
    assert captured["cmd"][image_index - 1] == "-i"
    assert captured["kwargs"]["input"] == stdin_payload


def test_run_harness_falls_back_to_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_path = _write_user_module(tmp_path / "demo_bank.py")
    captured: dict[str, object] = {}

    monkeypatch.setattr(user_normalizers, "_docker_available", lambda: False)

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(cmd, 0, "{\"match\":true}", "")

    monkeypatch.setattr(user_normalizers.subprocess, "run", fake_run)

    assert user_normalizers._run_harness("--detect", str(module_path), stdin_payload="[]") == {"match": True}
    assert captured["cmd"] == [
        sys.executable,
        "-m",
        "finance_cli.importers.normalizer_harness",
        "--detect",
        str(module_path),
    ]
    assert captured["kwargs"]["env"] == user_normalizers._minimal_subprocess_env()
    assert captured["kwargs"]["input"] == "[]"


def test_run_harness_docker_preserves_error_messages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_harness, trusted_sandbox = _write_harness_bundle(tmp_path / "trusted")
    module_path = _write_user_module(tmp_path / "input" / "demo_bank.py")

    monkeypatch.setattr(
        user_normalizers,
        "_harness_source_paths",
        lambda: (trusted_harness, trusted_sandbox),
    )

    def raise_timeout(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=user_normalizers.HARNESS_TIMEOUT_SECONDS)

    monkeypatch.setattr(user_normalizers.subprocess, "run", raise_timeout)
    with pytest.raises(
        TimeoutError,
        match=rf"^normalizer subprocess timed out after {user_normalizers.HARNESS_TIMEOUT_SECONDS} seconds$",
    ):
        user_normalizers._run_harness_docker("--detect", str(module_path), stdin_payload="[]")

    monkeypatch.setattr(
        user_normalizers.subprocess,
        "run",
        lambda cmd, **kwargs: subprocess.CompletedProcess(cmd, 1, "", ""),
    )
    with pytest.raises(ValueError, match=r"^subprocess failed$"):
        user_normalizers._run_harness_docker("--detect", str(module_path), stdin_payload="[]")

    monkeypatch.setattr(
        user_normalizers.subprocess,
        "run",
        lambda cmd, **kwargs: subprocess.CompletedProcess(cmd, 0, "not-json", ""),
    )
    with pytest.raises(ValueError, match=r"^normalizer subprocess returned invalid JSON$"):
        user_normalizers._run_harness_docker("--detect", str(module_path), stdin_payload="[]")


def test_normalizer_harness_standalone_import(monkeypatch: pytest.MonkeyPatch) -> None:
    importers_dir = Path(user_normalizers.__file__).resolve().parent.parent
    harness_path = importers_dir / "normalizer_harness.py"
    monkeypatch.syspath_prepend(str(importers_dir))
    sys.modules.pop("normalizer_sandbox", None)

    try:
        module = _load_module_from_path("standalone_normalizer_harness", harness_path)
    finally:
        sys.modules.pop("normalizer_sandbox", None)

    assert callable(module.main)
    assert module.ALLOWED_IMPORTS == {"csv", "decimal", "io", "re"}
