from __future__ import annotations

import time
from pathlib import Path
from textwrap import dedent

import pytest

from finance_cli import storage_files
from finance_cli import normalizer_sidecars
from finance_cli.importers import (
    detect_csv_institution,
    normalize_csv,
    supported_institutions,
)
from finance_cli.importers.normalizers import (
    BUILT_IN_TIER,
    USER_TIER,
    get_normalizer_loader,
    reset_normalizer_loader_cache,
    resolve_user_normalizers_dir,
)
from finance_cli.institution_names import register_user_institution
from finance_cli.storage_client import errors as storage_errors
from finance_cli.storage_lease import LeaseScope, RemoteLease
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@pytest.fixture()
def isolated_normalizer_home(tmp_path: Path, monkeypatch):
    home = tmp_path / ".finance_cli"
    monkeypatch.setenv("FINANCE_CLI_HOME", str(home))
    monkeypatch.delenv("FINANCE_CLI_NORMALIZER_DIR", raising=False)
    monkeypatch.delenv("FINANCE_CLI_INSTITUTION_NAMES_PATH", raising=False)
    reset_normalizer_loader_cache()
    yield home
    reset_normalizer_loader_cache()


def _write_user_normalizer(
    path: Path,
    *,
    primary_key: str,
    aliases: list[str],
    source_name: str,
    detect_token: str,
    amount: str,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        dedent(
            f"""
            PRIMARY_KEY = "{primary_key}"
            ALIASES = {aliases!r}
            SOURCE_NAME = "{source_name}"

            import csv
            import io


            def detect(lines):
                return any("{detect_token}" in line for line in lines)


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
                            "Source": SOURCE_NAME,
                        }}
                    )
                return NormalizeResult(
                    rows=rows,
                    source_name=SOURCE_NAME,
                    warnings=[],
                    raw_row_count=len(rows),
                    skipped_row_count=0,
                )
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _write_csv(path: Path, header_token: str) -> Path:
    path.write_text(
        f"{header_token}\nDate,Description,Amount\n2026-03-01,Coffee,-4.50\n",
        encoding="utf-8-sig",
    )
    return path


def test_loader_discovers_built_in_normalizers(isolated_normalizer_home: Path) -> None:
    loader = get_normalizer_loader()

    assert loader.get_entry("apple_card") is not None
    assert loader.get_entry("apple_card").tier == BUILT_IN_TIER
    assert supported_institutions() == [
        "american_express",
        "amex",
        "apple",
        "apple_card",
        "barclays",
        "bofa_checking",
        "chase_credit",
    ]


def test_user_context_scopes_normalizer_dir_per_remote_user(
    isolated_normalizer_home: Path,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "users" / "alice" / "finance.db"
    token = set_user_context(
        UserContext.from_paths(db_path=db_path, expected_user_id="alice")
    )
    try:
        assert (
            resolve_user_normalizers_dir() == db_path.parent.resolve() / "normalizers"
        )
    finally:
        reset_user_context(token)


def test_explicit_normalizer_dir_overrides_user_context(
    isolated_normalizer_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    explicit_dir = tmp_path / "explicit-normalizers"
    monkeypatch.setenv("FINANCE_CLI_NORMALIZER_DIR", str(explicit_dir))
    db_path = tmp_path / "users" / "alice" / "finance.db"
    token = set_user_context(
        UserContext.from_paths(db_path=db_path, expected_user_id="alice")
    )
    try:
        assert resolve_user_normalizers_dir() == explicit_dir.resolve()
    finally:
        reset_user_context(token)


def test_loader_cache_switches_between_request_scoped_users(
    isolated_normalizer_home: Path,
    tmp_path: Path,
) -> None:
    alice_db = tmp_path / "users" / "alice" / "finance.db"
    bob_db = tmp_path / "users" / "bob" / "finance.db"

    token = set_user_context(
        UserContext.from_paths(db_path=alice_db, expected_user_id="alice")
    )
    try:
        register_user_institution("Demo Bank", ["demo"])
        _write_user_normalizer(
            resolve_user_normalizers_dir() / "demo_bank.py",
            primary_key="demo_bank",
            aliases=["demo"],
            source_name="Demo Bank",
            detect_token="Demo Header",
            amount="-1.00",
        )
        alice_loader = get_normalizer_loader()
        assert alice_loader.get_entry("demo_bank") is not None
    finally:
        reset_user_context(token)

    token = set_user_context(
        UserContext.from_paths(db_path=bob_db, expected_user_id="bob")
    )
    try:
        assert resolve_user_normalizers_dir() == bob_db.parent.resolve() / "normalizers"
        bob_loader = get_normalizer_loader()
        assert bob_loader.get_entry("demo_bank") is None
    finally:
        reset_user_context(token)

    token = set_user_context(
        UserContext.from_paths(db_path=alice_db, expected_user_id="alice")
    )
    try:
        assert get_normalizer_loader() is alice_loader
    finally:
        reset_user_context(token)

    token = set_user_context(
        UserContext.from_paths(db_path=bob_db, expected_user_id="bob")
    )
    try:
        assert get_normalizer_loader() is bob_loader
    finally:
        reset_user_context(token)


def test_loader_reloads_when_request_scoped_registry_appears(
    isolated_normalizer_home: Path,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "users" / "alice" / "finance.db"
    token = set_user_context(
        UserContext.from_paths(db_path=db_path, expected_user_id="alice")
    )
    try:
        _write_user_normalizer(
            resolve_user_normalizers_dir() / "demo_bank.py",
            primary_key="demo_bank",
            aliases=["demo"],
            source_name="Demo Bank",
            detect_token="Demo Header",
            amount="-1.00",
        )
        loader = get_normalizer_loader()
        assert loader.get_entry("demo_bank") is None

        register_user_institution("Demo Bank", ["demo"])

        assert get_normalizer_loader() is loader
        assert loader.get_entry("demo_bank") is not None
    finally:
        reset_user_context(token)


def test_loader_reads_remote_normalizers_as_authoritative_cache(
    isolated_normalizer_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    remote_files: dict[str, bytes] = {
        "institution_names.json": b'{"canonical_names":{"demo bank":"Demo Bank","demo":"Demo Bank"}}\n',
    }
    remote_module_path = "normalizers/demo_bank.py"

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
        if prefix == "normalizers":
            return sorted(
                path for path in remote_files if path.startswith("normalizers/")
            )
        return []

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

    monkeypatch.setattr(storage_files, "list_files", fake_list_files)
    monkeypatch.setattr(storage_files, "read_file", fake_read_file)

    alice_db = tmp_path / "users" / "alice" / "finance.db"
    token = set_user_context(
        UserContext.from_paths(db_path=alice_db, expected_user_id="alice")
    )
    try:
        local_dir = resolve_user_normalizers_dir()
        _write_user_normalizer(
            local_dir / "local_bank.py",
            primary_key="local_bank",
            aliases=[],
            source_name="Demo Bank",
            detect_token="Local Header",
            amount="-9.00",
        )
        remote_files[remote_module_path] = (
            _write_user_normalizer(
                tmp_path / "remote_source.py",
                primary_key="demo_bank",
                aliases=["demo"],
                source_name="Demo Bank",
                detect_token="Remote Header",
                amount="-1.00",
            )
            .read_text(encoding="utf-8")
            .encode("utf-8")
        )

        with LeaseScope(
            user_id="alice",
            lease=RemoteLease("lease-remote"),
            session_manager=object(),
            owns_lease=False,
        ):
            loader = get_normalizer_loader()
            assert loader.get_entry("demo_bank") is not None
            assert loader.get_entry("local_bank") is None

            csv_path = _write_csv(tmp_path / "remote.csv", "Remote Header")
            assert detect_csv_institution(csv_path) == "demo_bank"
            assert normalize_csv(csv_path, "demo_bank").rows[0]["Amount"] == "-1.00"
            assert (local_dir / "demo_bank.py").exists()
    finally:
        reset_user_context(token)


def test_loader_remote_mode_without_lease_does_not_use_stale_local_cache(
    isolated_normalizer_home: Path,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "users" / "alice" / "finance.db"
    token = set_user_context(
        UserContext.from_paths(
            db_path=db_path,
            expected_user_id="alice",
            storage_mode="remote",
        )
    )
    try:
        local_dir = resolve_user_normalizers_dir()
        (db_path.parent / "institution_names.json").parent.mkdir(
            parents=True, exist_ok=True
        )
        (db_path.parent / "institution_names.json").write_text(
            '{"canonical_names":{"stale bank":"Stale Bank"}}\n',
            encoding="utf-8",
        )
        _write_user_normalizer(
            local_dir / "stale_bank.py",
            primary_key="stale_bank",
            aliases=[],
            source_name="Stale Bank",
            detect_token="Stale Header",
            amount="-9.00",
        )

        with pytest.raises(normalizer_sidecars.StorageSidecarUnavailable):
            get_normalizer_loader().list_entries()
    finally:
        reset_user_context(token)


def test_loader_discovers_user_generated_normalizers_and_skips_duplicates(
    isolated_normalizer_home: Path,
    tmp_path: Path,
) -> None:
    register_user_institution("Demo Bank", ["demo"])
    user_dir = resolve_user_normalizers_dir()
    _write_user_normalizer(
        user_dir / "demo_bank.py",
        primary_key="demo_bank",
        aliases=["demo"],
        source_name="Demo Bank",
        detect_token="Demo Header",
        amount="-1.00",
    )
    _write_user_normalizer(
        user_dir / "secondary_demo.py",
        primary_key="secondary_demo",
        aliases=["demo_bank"],
        source_name="Demo Bank",
        detect_token="Other Header",
        amount="-9.00",
    )
    _write_user_normalizer(
        user_dir / "apple_card.py",
        primary_key="apple_card",
        aliases=[],
        source_name="Demo Bank",
        detect_token="Conflicting Header",
        amount="-9.00",
    )

    csv_path = _write_csv(tmp_path / "demo.csv", "Demo Header")
    loader = get_normalizer_loader()

    assert loader.get_entry("demo_bank") is not None
    assert loader.get_entry("demo_bank").tier == USER_TIER
    assert loader.get_entry("secondary_demo") is None
    assert loader.get_entry("apple_card").tier == BUILT_IN_TIER
    assert detect_csv_institution(csv_path) == "demo_bank"
    assert normalize_csv(csv_path, "demo_bank").rows[0]["Amount"] == "-1.00"


def test_loader_isolates_bad_user_modules(isolated_normalizer_home: Path) -> None:
    register_user_institution("Good Bank", ["good"])
    user_dir = resolve_user_normalizers_dir()
    _write_user_normalizer(
        user_dir / "good_bank.py",
        primary_key="good_bank",
        aliases=["good"],
        source_name="Good Bank",
        detect_token="Good Header",
        amount="-1.00",
    )
    (user_dir / "broken_bank.py").write_text(
        "def this is not valid python\n", encoding="utf-8"
    )

    loader = get_normalizer_loader()

    assert loader.get_entry("good_bank") is not None
    assert loader.get_entry("broken_bank") is None


def test_loader_hot_reloads_user_module_on_mtime_change(
    isolated_normalizer_home: Path,
    tmp_path: Path,
) -> None:
    register_user_institution("Reload Bank", ["reload"])
    user_dir = resolve_user_normalizers_dir()
    module_path = _write_user_normalizer(
        user_dir / "reload_bank.py",
        primary_key="reload_bank",
        aliases=["reload"],
        source_name="Reload Bank",
        detect_token="Reload Header",
        amount="-1.00",
    )
    csv_path = _write_csv(tmp_path / "reload.csv", "Reload Header")

    first = normalize_csv(csv_path, "reload_bank")
    assert first.rows[0]["Amount"] == "-1.00"

    time.sleep(0.02)
    _write_user_normalizer(
        module_path,
        primary_key="reload_bank",
        aliases=["reload"],
        source_name="Reload Bank",
        detect_token="Reload Header",
        amount="-2.00",
    )

    second = normalize_csv(csv_path, "reload_bank")
    assert second.rows[0]["Amount"] == "-2.00"
