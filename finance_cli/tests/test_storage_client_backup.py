from __future__ import annotations

from typing import Any

from finance_cli.storage_client import backup as backup_client


class _AuthProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, list[str]]] = []

    def get_token(self, product: str, user_id: str, scopes: list[str]) -> str:
        self.calls.append((product, user_id, scopes))
        return "signed-token"


class _ChannelPool:
    def __init__(self) -> None:
        self.targets: list[str] = []

    def get(self, target: str) -> str:
        self.targets.append(target)
        return "fake-channel"


def test_restore_user_backup_streams_chunks_with_admin_token(
    monkeypatch, tmp_path
) -> None:
    bundle_path = tmp_path / "restore.bundle"
    bundle_path.write_bytes(b"abcde")
    auth_provider = _AuthProvider()
    channel_pool = _ChannelPool()
    captured: dict[str, Any] = {}

    class Stub:
        def __init__(self, channel: str) -> None:
            captured["channel"] = channel

        def RestoreUserBackup(self, chunks, *, metadata):
            materialized = list(chunks)
            captured["chunks"] = materialized
            captured["metadata"] = metadata
            return type("Response", (), {"restored": True})()

    monkeypatch.setattr(backup_client.pb2_grpc, "SqliteProxyStub", Stub)

    result = backup_client.restore_user_backup(
        "storage.example:50051",
        user_id="42",
        bundle_path=bundle_path,
        product="finance_cli",
        auth_provider=auth_provider,
        channel_pool=channel_pool,
        chunk_size=2,
    )

    assert result.restored is True
    assert channel_pool.targets == ["storage.example:50051"]
    assert captured["channel"] == "fake-channel"
    assert captured["metadata"] == (("authorization", "Bearer signed-token"),)
    assert auth_provider.calls == [("finance_cli", "42", ["admin"])]
    assert [chunk.content for chunk in captured["chunks"]] == [b"ab", b"cd", b"e"]
    assert {chunk.product for chunk in captured["chunks"]} == {"finance_cli"}
    assert {chunk.user_id for chunk in captured["chunks"]} == {"42"}
