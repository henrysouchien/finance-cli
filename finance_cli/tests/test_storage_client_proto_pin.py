from __future__ import annotations

import hashlib
from pathlib import Path


def test_storage_client_proto_pin_matches_source_proto() -> None:
    root = Path(__file__).resolve().parents[2]
    proto_path = root / "services/storage_server/proto/storage_server.proto"
    pin_path = root / "finance_cli/storage_client/_generated/proto.sha256"

    actual = hashlib.sha256(proto_path.read_bytes()).hexdigest()
    recorded = pin_path.read_text().strip()

    assert recorded == actual, (
        "storage_server.proto SHA mismatch; run "
        "scripts/regenerate_storage_client_proto.sh"
    )
