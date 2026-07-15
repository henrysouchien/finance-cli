"""gRPC file operations for the Phase 4 storage server client."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Iterator

import grpc

from .storage_client import auth, channel, errors
from .storage_client._generated import storage_server_pb2 as pb2
from .storage_client._generated import storage_server_pb2_grpc as pb2_grpc
from .storage_lease import enforce_active_lease_if_required

DEFAULT_CHUNK_SIZE = 1024 * 1024
_USER_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_ALLOWED_FILE_NAMES = frozenset(
    {
        "rules.yaml",
        "agent_memory.md",
        "skill_state.json",
        "telegram_token.json",
        "db-dek.enc",
        "institution_names.json",
    }
)
_ALLOWED_SUBDIRS = frozenset({"uploads", "sessions", "mcp_cache", "backups", "normalizers"})
_NORMALIZER_TEST_STATE_FILE = ".test_passes.json"


def read_file(
    target: str,
    *,
    user_id: str,
    product: str,
    relative_path: str,
    auth_provider=None,
    channel_pool=None,
) -> bytes:
    _validate_identity(user_id=user_id, product=product)
    enforce_active_lease_if_required(user_id=user_id, resource="storage_files.read_file")
    path = _validate_relative_file(relative_path)
    stub = _stub(target, channel_pool=channel_pool)
    request = pb2.FileReadRequest(product=product, user_id=user_id, path=path)
    try:
        response = stub.ReadFile(request, metadata=_metadata(auth_provider, product, user_id))
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="ReadFile") from exc
    return bytes(response.content)


def write_file(
    target: str,
    *,
    user_id: str,
    product: str,
    relative_path: str,
    content: bytes,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    auth_provider=None,
    channel_pool=None,
) -> None:
    _validate_identity(user_id=user_id, product=product)
    enforce_active_lease_if_required(user_id=user_id, resource="storage_files.write_file")
    path = _validate_relative_file(relative_path)
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    payload = bytes(content)
    stub = _stub(target, channel_pool=channel_pool)
    try:
        stub.WriteFile(
            _write_chunks(
                product=product,
                user_id=user_id,
                path=path,
                content=payload,
                chunk_size=int(chunk_size),
            ),
            metadata=_metadata(auth_provider, product, user_id),
        )
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="WriteFile") from exc


def list_files(
    target: str,
    *,
    user_id: str,
    product: str,
    prefix: str = "",
    auth_provider=None,
    channel_pool=None,
) -> list[str]:
    _validate_identity(user_id=user_id, product=product)
    enforce_active_lease_if_required(user_id=user_id, resource="storage_files.list_files")
    path = _validate_list_prefix(prefix)
    stub = _stub(target, channel_pool=channel_pool)
    request = pb2.FileListRequest(product=product, user_id=user_id, path=path)
    try:
        response = stub.ListFiles(request, metadata=_metadata(auth_provider, product, user_id))
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="ListFiles") from exc
    return [str(item.path) for item in response.files]


def delete_file(
    target: str,
    *,
    user_id: str,
    product: str,
    relative_path: str,
    auth_provider=None,
    channel_pool=None,
) -> None:
    _validate_identity(user_id=user_id, product=product)
    enforce_active_lease_if_required(user_id=user_id, resource="storage_files.delete_file")
    path = _validate_relative_file(relative_path)
    stub = _stub(target, channel_pool=channel_pool)
    request = pb2.FileDeleteRequest(product=product, user_id=user_id, path=path)
    try:
        stub.DeleteFile(request, metadata=_metadata(auth_provider, product, user_id))
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="DeleteFile") from exc


def _write_chunks(
    *,
    product: str,
    user_id: str,
    path: str,
    content: bytes,
    chunk_size: int,
) -> Iterator[pb2.FileWriteChunk]:
    first_content = content[:chunk_size]
    yield pb2.FileWriteChunk(
        product=product,
        user_id=user_id,
        path=path,
        content=first_content,
    )
    offset = len(first_content)
    while offset < len(content):
        next_offset = offset + chunk_size
        yield pb2.FileWriteChunk(content=content[offset:next_offset])
        offset = next_offset


def _metadata(auth_provider, product: str, user_id: str) -> tuple[tuple[str, str], ...]:
    provider = auth_provider or auth.get_default_provider()
    token = provider.get_token(product, user_id, [])
    return (("authorization", f"Bearer {token}"),)


def _stub(target: str, *, channel_pool=None) -> pb2_grpc.SqliteProxyStub:
    pool = channel_pool or channel._default_pool
    return pb2_grpc.SqliteProxyStub(pool.get(target))


def _validate_identity(*, user_id: str, product: str) -> None:
    if not isinstance(product, str) or not product.strip() or product != product.strip():
        raise errors.PathInvalidError("product_invalid")
    if Path(product).name != product or "\\" in product or product in {".", ".."}:
        raise errors.PathInvalidError("product_invalid")
    if not isinstance(user_id, str) or not _USER_ID_RE.fullmatch(user_id):
        raise errors.PathInvalidError("user_id_invalid")


def _validate_relative_file(relative_path: str) -> str:
    path = _validate_relative_parts(relative_path)
    parts = path.parts
    if len(parts) == 1 and parts[0] in _ALLOWED_FILE_NAMES:
        return str(path)
    if len(parts) > 1 and _is_allowed_subdir_file(parts):
        return str(path)
    raise errors.PathInvalidError("relative_path_not_allowed")


def _validate_list_prefix(prefix: str) -> str:
    if prefix == "":
        return ""
    path = _validate_relative_parts(prefix)
    parts = path.parts
    if len(parts) == 1 and (parts[0] in _ALLOWED_FILE_NAMES or parts[0] in _ALLOWED_SUBDIRS):
        return str(path)
    if len(parts) > 1 and _is_allowed_subdir_prefix(parts):
        return str(path)
    raise errors.PathInvalidError("relative_path_not_allowed")


def _validate_relative_parts(relative_path: str) -> Path:
    if not isinstance(relative_path, str) or not relative_path:
        raise errors.PathInvalidError("relative_path_invalid")
    path = Path(relative_path)
    if path.is_absolute():
        raise errors.PathInvalidError("relative_path_invalid")
    parts = path.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise errors.PathInvalidError("relative_path_invalid")
    return path


def _is_allowed_subdir_file(parts: tuple[str, ...]) -> bool:
    if not parts:
        return False
    if parts[0] == "normalizers":
        return _is_allowed_normalizer_file(parts)
    return parts[0] in _ALLOWED_SUBDIRS


def _is_allowed_subdir_prefix(parts: tuple[str, ...]) -> bool:
    if not parts:
        return False
    if parts[0] == "normalizers":
        return _is_allowed_normalizer_prefix(parts)
    return parts[0] in _ALLOWED_SUBDIRS


def _is_allowed_normalizer_file(parts: tuple[str, ...]) -> bool:
    if len(parts) == 2:
        return parts[1].endswith(".py") and not parts[1].startswith(".")
    if len(parts) == 3 and parts[1] == ".staging":
        return (parts[2].endswith(".py") and not parts[2].startswith(".")) or parts[2] == _NORMALIZER_TEST_STATE_FILE
    return False


def _is_allowed_normalizer_prefix(parts: tuple[str, ...]) -> bool:
    if len(parts) == 1:
        return True
    if len(parts) == 2 and parts[1] == ".staging":
        return True
    return _is_allowed_normalizer_file(parts)
