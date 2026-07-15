from __future__ import annotations

from datetime import datetime, timezone

from finance_cli.scripts import migrate_db_dek_to_vault as script


def _has_db_dek(*_args, **_kwargs) -> bool:
    return True


def test_cleanup_sm_user_blocks_when_durability_gate_fails(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    (data_root / "1").mkdir(parents=True)
    delete_calls: list[str] = []

    monkeypatch.setattr(script.crypto_envelope, "has_db_dek", _has_db_dek)
    monkeypatch.setattr(script, "_legacy_sm_db_key_state", lambda _user_id: ("active", "legacy active"))
    monkeypatch.setattr(
        script,
        "_cleanup_gate_allows_sm_delete",
        lambda **_kwargs: (False, "durability gate did not pass"),
    )
    monkeypatch.setattr(script.db_keys, "delete_user_db_key", delete_calls.append)

    status, detail = script.cleanup_sm_user(user_id="1", data_root=data_root, dry_run=False)

    assert status == "blocked"
    assert "legacy active" in detail
    assert "durability gate did not pass" in detail
    assert delete_calls == []


def test_cleanup_sm_user_dry_run_reports_active_secret_and_gate(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    (data_root / "1").mkdir(parents=True)
    delete_calls: list[str] = []

    monkeypatch.setattr(script.crypto_envelope, "has_db_dek", _has_db_dek)
    monkeypatch.setattr(script, "_legacy_sm_db_key_state", lambda _user_id: ("active", "legacy active"))
    monkeypatch.setattr(
        script,
        "_cleanup_gate_allows_sm_delete",
        lambda **_kwargs: (True, "durability gate passed"),
    )
    monkeypatch.setattr(script.db_keys, "delete_user_db_key", delete_calls.append)

    status, detail = script.cleanup_sm_user(user_id="1", data_root=data_root, dry_run=True)

    assert status == "would-cleanup"
    assert "legacy active" in detail
    assert "durability gate passed" in detail
    assert "would schedule SM db-key" in detail
    assert delete_calls == []


def test_cleanup_sm_user_skips_pending_or_missing_secret_without_gate(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    (data_root / "1").mkdir(parents=True)
    gate_calls: list[str] = []

    monkeypatch.setattr(script.crypto_envelope, "has_db_dek", _has_db_dek)
    monkeypatch.setattr(
        script,
        "_cleanup_gate_allows_sm_delete",
        lambda **_kwargs: gate_calls.append("called") or (True, "durability gate passed"),
    )

    monkeypatch.setattr(
        script,
        "_legacy_sm_db_key_state",
        lambda _user_id: ("pending-delete", "legacy pending"),
    )
    status, detail = script.cleanup_sm_user(user_id="1", data_root=data_root, dry_run=False)
    assert status == "skip"
    assert detail == "legacy pending"

    monkeypatch.setattr(script, "_legacy_sm_db_key_state", lambda _user_id: ("missing", "legacy missing"))
    status, detail = script.cleanup_sm_user(user_id="1", data_root=data_root, dry_run=False)
    assert status == "skip"
    assert detail == "legacy missing"
    assert gate_calls == []


def test_cleanup_sm_user_deletes_only_after_gate_passes(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    (data_root / "1").mkdir(parents=True)
    delete_calls: list[str] = []

    monkeypatch.setattr(script.crypto_envelope, "has_db_dek", _has_db_dek)
    monkeypatch.setattr(script, "_legacy_sm_db_key_state", lambda _user_id: ("active", "legacy active"))
    monkeypatch.setattr(
        script,
        "_cleanup_gate_allows_sm_delete",
        lambda **_kwargs: (True, "durability gate passed"),
    )
    monkeypatch.setattr(script.db_keys, "delete_user_db_key", delete_calls.append)

    status, detail = script.cleanup_sm_user(user_id="1", data_root=data_root, dry_run=False)

    assert status == "cleaned-up"
    assert detail == "SM db-key scheduled for soft-delete (7d recovery window)"
    assert delete_calls == ["1"]


def test_legacy_sm_db_key_state_reports_missing_active_and_pending(monkeypatch) -> None:
    monkeypatch.setattr(script.db_keys, "user_db_key_secret_ref", lambda _user_id: "finance-cli/users/1/db-key")

    monkeypatch.setattr(script.secrets_backend, "describe_secret", lambda *_args, **_kwargs: None)
    state, detail = script._legacy_sm_db_key_state("1")
    assert state == "missing"
    assert "already missing" in detail

    monkeypatch.setattr(script.secrets_backend, "describe_secret", lambda *_args, **_kwargs: {"DeletedDate": None})
    state, detail = script._legacy_sm_db_key_state("1")
    assert state == "active"
    assert "is active" in detail

    deleted_at = datetime(2026, 5, 8, 19, 12, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(
        script.secrets_backend,
        "describe_secret",
        lambda *_args, **_kwargs: {"DeletedDate": deleted_at},
    )
    state, detail = script._legacy_sm_db_key_state("1")
    assert state == "pending-delete"
    assert "2026-05-08T19:12:15+00:00" in detail
