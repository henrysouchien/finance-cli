from __future__ import annotations

import importlib
from pathlib import PurePosixPath
from pathlib import Path
from textwrap import dedent

import pytest

from finance_cli import storage_files
from finance_cli.db import initialize_database
from finance_cli.importers import normalize_csv
from finance_cli.importers.normalizers import reset_normalizer_loader_cache
from finance_cli.storage_client import errors as storage_errors
from finance_cli.storage_lease import LeaseScope, RemoteLease
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@pytest.fixture()
def mcp_module(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.setenv("FINANCE_CLI_HOME", str(tmp_path / ".finance_cli"))
    monkeypatch.delenv("FINANCE_CLI_NORMALIZER_DIR", raising=False)
    monkeypatch.delenv("FINANCE_CLI_INSTITUTION_NAMES_PATH", raising=False)
    initialize_database(db_path)
    reset_normalizer_loader_cache()
    import finance_cli.mcp_server as mcp_server

    module = importlib.reload(mcp_server)
    reset_normalizer_loader_cache()
    yield module
    reset_normalizer_loader_cache()


def _write_csv(path: Path) -> Path:
    path.write_text(
        "Demo Header\nDate,Description,Amount\n2026-03-01,Coffee,-4.50\n",
        encoding="utf-8-sig",
    )
    return path


def _normalizer_source(
    amount: str,
    *,
    primary_key: str = "demo_bank",
    aliases: tuple[str, ...] = ("demo",),
    source_name: str = "Demo Bank",
) -> str:
    return (
        dedent(
            f"""
            PRIMARY_KEY = {primary_key!r}
            ALIASES = {list(aliases)!r}
            SOURCE_NAME = {source_name!r}

            import csv
            import io


            def detect(lines):
                return any("Demo Header" in line for line in lines)


            def normalize(lines, file_name):
                reader = csv.DictReader(io.StringIO("".join(lines[1:])))
                rows = []
                for row in reader:
                    rows.append(
                        {{
                            "Date": row["Date"],
                            "Description": row["Description"],
                            "Amount": "{amount}",
                            "Account Type": "checking",
                            "Source": "Wrong Source",
                        }}
                    )
                return NormalizeResult(
                    rows=rows,
                    source_name="Wrong Source",
                    warnings=[],
                    raw_row_count=len(rows),
                    skipped_row_count=0,
                )
            """
        ).strip()
        + "\n"
    )


def _register_demo_bank(mcp_module) -> dict:
    return mcp_module.normalizer_register_institution(
        canonical_name="Demo Bank", aliases=["demo"]
    )


def _assert_tool_error(response: dict, error_class: str, message: str) -> None:
    assert response["status"] == "error"
    assert response["error_class"] == error_class
    assert message in response["message"]
    assert response["error"] == response["message"]
    assert response["names_correction"]["tool"]
    assert response["suggested_tool_calls"]


def test_mcp_normalizer_stage_test_activate_update_workflow(
    mcp_module, tmp_path: Path
) -> None:
    csv_path = _write_csv(tmp_path / "demo.csv")

    response = mcp_module.statement_normalizer_stage(
        key="demo_bank", source=_normalizer_source("-1.00")
    )
    _assert_tool_error(response, "ValueError", "normalizer_register_institution")

    register_result = _register_demo_bank(mcp_module)
    assert register_result["summary"]["changed"] is True

    sample = mcp_module.statement_normalizer_sample_csv(file=str(csv_path), lines=2)
    assert sample["data"]["line_count"] == 2
    assert "Demo Header" in sample["data"]["text"]

    stage_result = mcp_module.statement_normalizer_stage(
        key="demo_bank", source=_normalizer_source("-1.00")
    )
    assert stage_result["summary"]["staged"] is True
    assert stage_result["data"]["primary_key"] == "demo_bank"

    listed_before = mcp_module.statement_normalizer_list()
    assert all(
        item["primary_key"] != "demo_bank"
        for item in listed_before["data"]["normalizers"]
    )

    test_result = mcp_module.statement_normalizer_test(
        file=str(csv_path), institution="demo_bank"
    )
    assert test_result["data"]["tier"] == "staged_user"
    assert test_result["data"]["validation"]["valid"] is True
    assert test_result["data"]["sample_rows"][0]["Source"] == "Demo Bank"

    validate_result = mcp_module.normalizer_validate(
        file=str(csv_path), institution="demo_bank"
    )
    assert validate_result["data"]["validation"]["valid"] is True

    activate_result = mcp_module.statement_normalizer_activate(key="demo_bank")
    assert activate_result["summary"]["activated"] is True

    detect_result = mcp_module.normalizer_detect(file=str(csv_path))
    assert detect_result["data"]["institution"] == "demo_bank"
    assert detect_result["data"]["tier"] == "user"

    listed_after = mcp_module.statement_normalizer_list()
    assert any(
        item["primary_key"] == "demo_bank" and item["tier"] == "user"
        for item in listed_after["data"]["normalizers"]
    )
    assert normalize_csv(csv_path, "demo_bank").rows[0]["Amount"] == "-1.00"

    update_result = mcp_module.normalizer_update(
        key="demo_bank", source=_normalizer_source("-2.00")
    )
    assert update_result["summary"]["updated"] is True

    response = mcp_module.statement_normalizer_activate(key="demo_bank")
    _assert_tool_error(response, "ValueError", "must pass statement_normalizer_test")

    mcp_module.statement_normalizer_test(file=str(csv_path), institution="demo_bank")
    mcp_module.statement_normalizer_activate(key="demo_bank")
    assert normalize_csv(csv_path, "demo_bank").rows[0]["Amount"] == "-2.00"


def test_mcp_normalizer_paths_are_scoped_to_request_user(
    mcp_module, tmp_path: Path
) -> None:
    alice_root = tmp_path / "users" / "alice"
    bob_root = tmp_path / "users" / "bob"
    uploads_dir = alice_root / "uploads"
    uploads_dir.mkdir(parents=True)
    csv_path = _write_csv(uploads_dir / "demo.csv")

    alice_token = set_user_context(
        UserContext.from_paths(
            db_path=alice_root / "finance.db",
            expected_user_id="alice",
            uploads_dir=uploads_dir,
        )
    )
    try:
        _register_demo_bank(mcp_module)
        stage_result = mcp_module.statement_normalizer_stage(
            key="demo_bank",
            source=_normalizer_source("-1.00"),
        )
        staged_path = Path(stage_result["data"]["staged_path"])
        assert staged_path.parent == (alice_root / "normalizers" / ".staging").resolve()

        mcp_module.statement_normalizer_test(
            file=str(csv_path), institution="demo_bank"
        )
        activate_result = mcp_module.statement_normalizer_activate(key="demo_bank")
        active_path = Path(activate_result["data"]["active_path"])
        assert active_path.parent == (alice_root / "normalizers").resolve()
    finally:
        reset_user_context(alice_token)

    bob_token = set_user_context(
        UserContext.from_paths(db_path=bob_root / "finance.db", expected_user_id="bob")
    )
    try:
        listed = mcp_module.statement_normalizer_list()
        assert all(
            item["primary_key"] != "demo_bank" for item in listed["data"]["normalizers"]
        )
    finally:
        reset_user_context(bob_token)


def test_mcp_normalizer_remote_lease_persists_sidecars_to_storage(
    mcp_module,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    remote_files: dict[str, bytes] = {}
    writes: list[str] = []
    deletes: list[str] = []

    def fake_list_files(
        target: str,
        *,
        user_id: str,
        product: str,
        prefix: str = "",
        **_kwargs,
    ) -> list[str]:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        if not prefix:
            return sorted(remote_files)
        prefix_path = PurePosixPath(prefix)
        result: list[str] = []
        for raw_path in remote_files:
            path = PurePosixPath(raw_path)
            if path.parts[: len(prefix_path.parts)] != prefix_path.parts:
                continue
            result.append(raw_path)
        return sorted(result)

    def fake_read_file(
        target: str,
        *,
        user_id: str,
        product: str,
        relative_path: str,
        **_kwargs,
    ) -> bytes:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        try:
            return remote_files[relative_path]
        except KeyError as exc:
            raise storage_errors.StorageClientError(relative_path) from exc

    def fake_write_file(
        target: str,
        *,
        user_id: str,
        product: str,
        relative_path: str,
        content: bytes,
        **_kwargs,
    ) -> None:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        remote_files[relative_path] = content
        writes.append(relative_path)

    def fake_delete_file(
        target: str,
        *,
        user_id: str,
        product: str,
        relative_path: str,
        **_kwargs,
    ) -> None:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        remote_files.pop(relative_path, None)
        deletes.append(relative_path)

    monkeypatch.setattr(storage_files, "list_files", fake_list_files)
    monkeypatch.setattr(storage_files, "read_file", fake_read_file)
    monkeypatch.setattr(storage_files, "write_file", fake_write_file)
    monkeypatch.setattr(storage_files, "delete_file", fake_delete_file)

    alice_root = tmp_path / "users" / "alice"
    uploads_dir = alice_root / "uploads"
    uploads_dir.mkdir(parents=True)
    csv_path = _write_csv(uploads_dir / "demo.csv")

    token = set_user_context(
        UserContext.from_paths(
            db_path=alice_root / "finance.db",
            expected_user_id="alice",
            uploads_dir=uploads_dir,
            storage_mode="remote",
        )
    )
    try:
        with LeaseScope(
            user_id="alice",
            lease=RemoteLease("lease-remote"),
            session_manager=object(),
            owns_lease=False,
        ):
            mcp_module.normalizer_register_institution(
                canonical_name="Demo Bank", aliases=["demo"]
            )
            stage = mcp_module.statement_normalizer_stage(
                key="demo_bank", source=_normalizer_source("-1.00")
            )
            assert stage["summary"]["staged"] is True
            assert "normalizers/.staging/demo_bank.py" in remote_files
            assert not (alice_root / "institution_names.json").exists()

            test = mcp_module.statement_normalizer_test(
                file=str(csv_path), institution="demo_bank"
            )
            assert test["summary"]["valid"] is True
            assert "normalizers/.staging/.test_passes.json" in remote_files

            activate = mcp_module.statement_normalizer_activate(key="demo_bank")
            assert activate["summary"]["activated"] is True
            assert "normalizers/demo_bank.py" in remote_files
            assert "normalizers/.staging/demo_bank.py" not in remote_files
            assert "normalizers/demo_bank.py" in writes
            assert "normalizers/.staging/demo_bank.py" in deletes

            listed = mcp_module.statement_normalizer_list()
            assert any(
                item["primary_key"] == "demo_bank"
                for item in listed["data"]["normalizers"]
            )
            assert normalize_csv(csv_path, "demo_bank").rows[0]["Amount"] == "-1.00"
    finally:
        reset_user_context(token)


def test_mcp_remote_activate_succeeds_when_staged_cleanup_fails_after_publish(
    mcp_module,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    remote_files: dict[str, bytes] = {}
    fail_state_cleanup = {"enabled": False}

    def fake_list_files(
        target: str,
        *,
        user_id: str,
        product: str,
        prefix: str = "",
        **_kwargs,
    ) -> list[str]:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        prefix_path = PurePosixPath(prefix) if prefix else None
        result: list[str] = []
        for raw_path in remote_files:
            if prefix_path is None:
                result.append(raw_path)
                continue
            path = PurePosixPath(raw_path)
            if path.parts[: len(prefix_path.parts)] == prefix_path.parts:
                result.append(raw_path)
        return sorted(result)

    def fake_read_file(
        target: str,
        *,
        user_id: str,
        product: str,
        relative_path: str,
        **_kwargs,
    ) -> bytes:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        try:
            return remote_files[relative_path]
        except KeyError as exc:
            raise storage_errors.StorageClientError(relative_path) from exc

    def fake_write_file(
        target: str,
        *,
        user_id: str,
        product: str,
        relative_path: str,
        content: bytes,
        **_kwargs,
    ) -> None:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        if (
            fail_state_cleanup["enabled"]
            and relative_path == "normalizers/.staging/.test_passes.json"
        ):
            raise storage_errors.StorageClientError("test_state_cleanup_failed")
        remote_files[relative_path] = content

    def fake_delete_file(
        target: str,
        *,
        user_id: str,
        product: str,
        relative_path: str,
        **_kwargs,
    ) -> None:
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        if relative_path == "normalizers/.staging/demo_bank.py":
            raise storage_errors.StorageClientError("delete_failed")
        remote_files.pop(relative_path, None)

    monkeypatch.setattr(storage_files, "list_files", fake_list_files)
    monkeypatch.setattr(storage_files, "read_file", fake_read_file)
    monkeypatch.setattr(storage_files, "write_file", fake_write_file)
    monkeypatch.setattr(storage_files, "delete_file", fake_delete_file)

    alice_root = tmp_path / "users" / "alice"
    uploads_dir = alice_root / "uploads"
    uploads_dir.mkdir(parents=True)
    csv_path = _write_csv(uploads_dir / "demo.csv")
    token = set_user_context(
        UserContext.from_paths(
            db_path=alice_root / "finance.db",
            expected_user_id="alice",
            uploads_dir=uploads_dir,
            storage_mode="remote",
        )
    )
    try:
        with LeaseScope(
            user_id="alice",
            lease=RemoteLease("lease-remote"),
            session_manager=object(),
            owns_lease=False,
        ):
            mcp_module.normalizer_register_institution(
                canonical_name="Demo Bank", aliases=["demo"]
            )
            mcp_module.statement_normalizer_stage(
                key="demo_bank", source=_normalizer_source("-1.00")
            )
            mcp_module.statement_normalizer_test(
                file=str(csv_path), institution="demo_bank"
            )
            fail_state_cleanup["enabled"] = True

            activate = mcp_module.statement_normalizer_activate(key="demo_bank")

            assert activate["summary"]["activated"] is True
            assert "normalizers/demo_bank.py" in remote_files
            assert "normalizers/.staging/demo_bank.py" in remote_files
            assert normalize_csv(csv_path, "demo_bank").rows[0]["Amount"] == "-1.00"
    finally:
        reset_user_context(token)


def test_mcp_normalizer_stage_rejects_built_in_key_conflict(mcp_module) -> None:
    response = mcp_module.statement_normalizer_stage(
        key="amex",
        source=_normalizer_source(
            "-1.00",
            primary_key="amex",
            aliases=("american_express",),
            source_name="American Express",
        ),
    )
    _assert_tool_error(response, "ValueError", "built-in and cannot be staged")


def test_mcp_normalizer_activate_requires_prior_test(mcp_module) -> None:
    _register_demo_bank(mcp_module)
    mcp_module.statement_normalizer_stage(
        key="demo_bank", source=_normalizer_source("-1.00")
    )

    response = mcp_module.statement_normalizer_activate(key="demo_bank")
    _assert_tool_error(response, "ValueError", "must pass statement_normalizer_test")


def test_mcp_normalizer_activate_rejects_stale_content_hash(
    mcp_module, tmp_path: Path
) -> None:
    csv_path = _write_csv(tmp_path / "demo.csv")
    _register_demo_bank(mcp_module)
    stage_result = mcp_module.statement_normalizer_stage(
        key="demo_bank", source=_normalizer_source("-1.00")
    )

    mcp_module.statement_normalizer_test(file=str(csv_path), institution="demo_bank")
    Path(stage_result["data"]["staged_path"]).write_text(
        _normalizer_source("-2.00"), encoding="utf-8"
    )

    response = mcp_module.statement_normalizer_activate(key="demo_bank")
    _assert_tool_error(response, "ValueError", "must pass statement_normalizer_test")


def test_mcp_normalizer_register_institution_rejects_builtin_override(
    mcp_module,
) -> None:
    response = mcp_module.normalizer_register_institution(canonical_name="Chase")
    _assert_tool_error(
        response, "ValueError", "already exists in the built-in registry"
    )


def test_mcp_normalizer_update_rejects_missing_normalizer(mcp_module) -> None:
    response = mcp_module.normalizer_update(
        key="demo_bank", source=_normalizer_source("-1.00")
    )
    _assert_tool_error(response, "ValueError", "does not exist")


def test_mcp_normalizer_sample_csv_rejects_missing_file(
    mcp_module, tmp_path: Path
) -> None:
    response = mcp_module.statement_normalizer_sample_csv(
        file=str(tmp_path / "missing.csv")
    )
    _assert_tool_error(response, "FileNotFoundError", "missing.csv")
