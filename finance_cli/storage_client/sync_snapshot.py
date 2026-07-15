"""Client helper for storage-server sync snapshot export."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import grpc

from . import auth, channel, errors
from ._generated import storage_server_pb2 as pb2
from ._generated import storage_server_pb2_grpc as pb2_grpc


@dataclass(frozen=True)
class RemoteSyncSnapshot:
    path: Path
    snapshot_op_id: int
    schema_version: int
    snapshot_id: str


def export_sync_snapshot(
    target: str,
    *,
    user_id: str,
    product: str = "finance_cli",
    auth_provider=None,
    channel_pool=None,
) -> RemoteSyncSnapshot:
    """Stream a sanitized sync snapshot to a temporary local tar.gz file."""

    fd, temp_name = tempfile.mkstemp(prefix=f"sync-remote-{user_id}-", suffix=".tar.gz")
    os.close(fd)
    path = Path(temp_name)
    provider = auth_provider or auth.get_default_provider()
    stub = pb2_grpc.SqliteProxyStub((channel_pool or channel._default_pool).get(target))
    request = pb2.ExportSyncSnapshotRequest(product=product, user_id=str(user_id))
    metadata = (("authorization", f"Bearer {provider.get_token(product, str(user_id), ['admin'])}"),)

    snapshot_op_id = 0
    schema_version = 0
    snapshot_id = ""
    saw_chunk = False
    try:
        with path.open("wb") as handle:
            for chunk in stub.ExportSyncSnapshot(request, metadata=metadata):
                saw_chunk = True
                if chunk.snapshot_op_id:
                    snapshot_op_id = int(chunk.snapshot_op_id)
                if chunk.schema_version:
                    schema_version = int(chunk.schema_version)
                if chunk.snapshot_id:
                    snapshot_id = str(chunk.snapshot_id)
                handle.write(bytes(chunk.content))
        if not saw_chunk:
            raise errors.StorageClientError("empty_sync_snapshot")
        return RemoteSyncSnapshot(
            path=path,
            snapshot_op_id=snapshot_op_id,
            schema_version=schema_version,
            snapshot_id=snapshot_id,
        )
    except grpc.RpcError as exc:
        path.unlink(missing_ok=True)
        raise errors.from_grpc_error(exc, rpc="ExportSyncSnapshot") from exc
    except Exception:
        path.unlink(missing_ok=True)
        raise
