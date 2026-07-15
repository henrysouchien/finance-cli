"""Client helpers for storage-server backup restore RPCs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import grpc

from . import auth, channel, errors
from ._generated import storage_server_pb2 as pb2
from ._generated import storage_server_pb2_grpc as pb2_grpc


@dataclass(frozen=True)
class RemoteRestoreResult:
    restored: bool


def restore_user_backup(
    target: str,
    *,
    user_id: str,
    bundle_path: Path,
    product: str = "finance_cli",
    auth_provider=None,
    channel_pool=None,
    chunk_size: int = 1024 * 1024,
) -> RemoteRestoreResult:
    """Stream a backup bundle to the storage server's authoritative restore RPC."""

    resolved_path = bundle_path.expanduser().resolve()
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    provider = auth_provider or auth.get_default_provider()
    stub = pb2_grpc.SqliteProxyStub((channel_pool or channel._default_pool).get(target))
    metadata = (
        (
            "authorization",
            f"Bearer {provider.get_token(product, str(user_id), ['admin'])}",
        ),
    )

    def chunks():
        with resolved_path.open("rb") as handle:
            while True:
                content = handle.read(chunk_size)
                if not content:
                    break
                yield pb2.RestoreUserBackupChunk(
                    product=product,
                    user_id=str(user_id),
                    content=content,
                )

    try:
        response = stub.RestoreUserBackup(chunks(), metadata=metadata)
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="RestoreUserBackup") from exc
    return RemoteRestoreResult(restored=bool(response.restored))
