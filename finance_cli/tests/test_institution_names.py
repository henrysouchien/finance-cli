from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from finance_cli import storage_files
import finance_cli.institution_names as institution_names_module
from finance_cli import normalizer_sidecars
from finance_cli.storage_client import errors as storage_errors
from finance_cli.storage_lease import LeaseScope, RemoteLease
from finance_cli.institution_names import (
    CANONICAL_NAMES,
    canonicalize,
    is_known,
    normalize_key,
    register_user_institution,
    similar_names,
    user_registry_path,
)
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


def test_canonicalize_known_variants() -> None:
    assert canonicalize("BofA Checking") == "Bank of America"
    assert canonicalize("Barclays - Cards") == "Barclays"
    assert canonicalize("Goldman Sachs Bank USA") == "Apple Card"
    assert canonicalize("Apple Card (Goldman Sachs Bank USA)") == "Apple Card"
    assert canonicalize("Amex") == "American Express"
    assert canonicalize("Bloomingdale's") == "Bloomingdale's"
    assert canonicalize("Merrill") == "Merrill"


def test_normalize_key_strips_punctuation_and_whitespace() -> None:
    assert normalize_key("  Barclays - Cards  ") == "barclays cards"
    assert normalize_key("Bloomingdale's") == "bloomingdale s"


def test_unknown_canonicalize_passthrough() -> None:
    assert canonicalize("Unknown Credit Union") == "Unknown Credit Union"


def test_is_known_true_and_false() -> None:
    assert is_known("BofA Checking") is True
    assert is_known("Apple Card (Goldman Sachs Bank USA)") is True
    assert is_known("Unknown Credit Union") is False


def test_similar_names_heuristics() -> None:
    assert similar_names("Goldman Sachs", "Goldman Sachs Bank USA") is True
    assert similar_names("First National Credit Union", "National Credit") is True
    assert similar_names("Merrill", "Bank of America") is False


def test_canonical_names_keys_are_prenormalized() -> None:
    bad_keys = []
    for key in CANONICAL_NAMES:
        expected = normalize_key(key)
        if key != expected:
            bad_keys.append((key, expected))

    assert not bad_keys, f"Keys not pre-normalized: {bad_keys}"


def test_user_registry_adds_new_canonical_names(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_HOME", str(tmp_path / ".finance_cli"))
    result = register_user_institution("Wells Fargo", ["wf", "wells"])

    assert result["changed"] is True
    assert canonicalize("wf") == "Wells Fargo"
    assert canonicalize("Wells Fargo") == "Wells Fargo"
    assert is_known("wells") is True


def test_user_registry_scopes_to_request_user(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("FINANCE_CLI_INSTITUTION_NAMES_PATH", raising=False)
    alice_db = tmp_path / "users" / "alice" / "finance.db"
    bob_db = tmp_path / "users" / "bob" / "finance.db"

    token = set_user_context(
        UserContext.from_paths(db_path=alice_db, expected_user_id="alice")
    )
    try:
        result = register_user_institution("Acme Bank", ["acme"])
        assert (
            Path(result["path"]) == alice_db.parent.resolve() / "institution_names.json"
        )
        assert is_known("acme") is True
    finally:
        reset_user_context(token)

    token = set_user_context(
        UserContext.from_paths(db_path=bob_db, expected_user_id="bob")
    )
    try:
        assert (
            user_registry_path() == bob_db.parent.resolve() / "institution_names.json"
        )
        assert is_known("acme") is False
    finally:
        reset_user_context(token)


def test_user_registry_remote_lease_reads_and_writes_storage_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FINANCE_CLI_INSTITUTION_NAMES_PATH", raising=False)
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    institution_names_module._USER_REGISTRY_CACHES.clear()
    remote_files: dict[str, bytes] = {}
    calls: list[tuple[str, str, str]] = []

    def fake_read_file(
        target: str,
        *,
        user_id: str,
        product: str,
        relative_path: str,
        **_kwargs,
    ) -> bytes:
        calls.append(("read", user_id, relative_path))
        assert target == "storage.example:50051"
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
        calls.append(("write", user_id, relative_path))
        assert target == "storage.example:50051"
        assert user_id == "alice"
        assert product == "finance_cli"
        remote_files[relative_path] = content

    monkeypatch.setattr(storage_files, "read_file", fake_read_file)
    monkeypatch.setattr(storage_files, "write_file", fake_write_file)

    db_path = tmp_path / "users" / "alice" / "finance.db"
    token = set_user_context(
        UserContext.from_paths(db_path=db_path, expected_user_id="alice")
    )
    try:
        with LeaseScope(
            user_id="alice",
            lease=RemoteLease("lease-1"),
            session_manager=object(),
            owns_lease=False,
        ):
            result = register_user_institution("Remote Bank", ["rb"])
            assert canonicalize("rb") == "Remote Bank"
    finally:
        reset_user_context(token)
        institution_names_module._USER_REGISTRY_CACHES.clear()

    assert result["path"] == str(db_path.parent.resolve() / "institution_names.json")
    assert not (db_path.parent / "institution_names.json").exists()
    assert "institution_names.json" in remote_files
    assert ("write", "alice", "institution_names.json") in calls


def test_user_registry_remote_mode_without_lease_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FINANCE_CLI_INSTITUTION_NAMES_PATH", raising=False)
    db_path = tmp_path / "users" / "alice" / "finance.db"
    registry = db_path.parent / "institution_names.json"
    registry.parent.mkdir(parents=True)
    registry.write_text(
        json.dumps({"canonical_names": {"stale": "Stale Local Bank"}}) + "\n",
        encoding="utf-8",
    )
    token = set_user_context(
        UserContext.from_paths(
            db_path=db_path,
            expected_user_id="alice",
            storage_mode="remote",
        )
    )
    try:
        with pytest.raises(normalizer_sidecars.StorageSidecarUnavailable):
            canonicalize("stale")
    finally:
        reset_user_context(token)


def test_user_registry_remote_lease_with_disabled_storage_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FINANCE_CLI_INSTITUTION_NAMES_PATH", raising=False)
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.delenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", raising=False)
    db_path = tmp_path / "users" / "alice" / "finance.db"
    token = set_user_context(
        UserContext.from_paths(
            db_path=db_path,
            expected_user_id="alice",
            storage_mode="remote",
        )
    )
    try:
        with LeaseScope(
            user_id="alice",
            lease=RemoteLease("lease-1"),
            session_manager=object(),
            owns_lease=False,
        ):
            with pytest.raises(normalizer_sidecars.StorageSidecarUnavailable):
                register_user_institution("Remote Bank", ["rb"])
    finally:
        reset_user_context(token)


def test_explicit_user_registry_path_overrides_request_user(
    tmp_path: Path, monkeypatch
) -> None:
    explicit_path = tmp_path / "shared" / "institution_names.json"
    monkeypatch.setenv("FINANCE_CLI_INSTITUTION_NAMES_PATH", str(explicit_path))
    token = set_user_context(
        UserContext.from_paths(
            db_path=tmp_path / "users" / "alice" / "finance.db",
            expected_user_id="alice",
        )
    )
    try:
        assert user_registry_path() == explicit_path.resolve()
    finally:
        reset_user_context(token)


def test_user_registry_rejects_builtin_overrides(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_HOME", str(tmp_path / ".finance_cli"))

    with pytest.raises(ValueError, match="built-in registry"):
        register_user_institution("New Chase", ["chase"])


def test_user_registry_mtime_reload(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_HOME", str(tmp_path / ".finance_cli"))
    path = user_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"canonical_names": {"wf": "Wells Fargo"}}, indent=2) + "\n",
        encoding="utf-8",
    )

    assert canonicalize("wf") == "Wells Fargo"

    time.sleep(0.02)
    path.write_text(
        json.dumps({"canonical_names": {"cu": "Credit Union X"}}, indent=2) + "\n",
        encoding="utf-8",
    )

    assert canonicalize("cu") == "Credit Union X"
    assert is_known("wf") is False
