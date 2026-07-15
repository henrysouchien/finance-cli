from __future__ import annotations

from finance_cli import sync_protocol
from finance_cli.response_sanitizer import (
    SECRET_KEYS,
    _is_path_field,
    _scrub_server_paths,
    sanitize_envelope,
)


def test_sanitize_envelope_recursively_strips_secret_keys() -> None:
    payload = {
        "data": {
            "plaid": {
                "items": [
                    {
                        "plaid_item_id": "item-1",
                        "access_token_ref": "secret-ref",
                        "next_cursor": "next-secret",
                    }
                ]
            },
            "stripe": {"api_key_ref": "stripe-secret"},
        },
        "summary": {"warnings": ["Sync cursor was /var/app/current.log"]},
    }

    sanitized = sanitize_envelope(payload)

    assert sanitized["data"]["plaid"]["items"] == [{"plaid_item_id": "item-1"}]
    assert sanitized["data"]["stripe"] == {}
    assert sanitized["summary"]["warnings"] == ["Sync cursor was current.log"]


def test_sanitize_envelope_preserves_link_token_and_hosted_link_url() -> None:
    sanitized = sanitize_envelope({"data": {"link_token": "X", "hosted_link_url": "Y"}})

    assert sanitized["data"] == {"link_token": "X", "hosted_link_url": "Y"}


def test_sanitize_envelope_scrubs_paths_in_path_fields_and_messages() -> None:
    payload = {
        "data": {
            "backup_path": "/data/finance/users/abc/backup.tar.gz",
            "files": ["/data/finance/users/abc/file.csv", "relative/file.csv"],
        },
        "summary": {"message": "Saved /var/www/finance_web/app.py"},
    }

    sanitized = sanitize_envelope(payload)

    assert sanitized["data"]["backup_path"] == "backup.tar.gz"
    assert sanitized["data"]["files"] == ["file.csv", "relative/file.csv"]
    assert sanitized["summary"]["message"] == "Saved app.py"


def test_secret_keys_cover_sync_protocol_secret_columns_and_cursor_fields() -> None:
    required = set().union(*sync_protocol.SECRET_COLUMNS.values()) | {"sync_cursor", "next_cursor"}

    assert set(SECRET_KEYS) >= required


def test_scrub_server_paths_does_not_match_api_routes() -> None:
    assert _scrub_server_paths("GET /api/v1/sessions") == "GET /api/v1/sessions"


def test_scrub_server_paths_scrubs_full_local_server_path_token() -> None:
    text = (
        "Failed decrypting DB for user '2': "
        "finance_cli/finance-web/data/users/2/finance.db"
    )

    assert _scrub_server_paths(text) == "Failed decrypting DB for user '2': finance.db"


def test_is_path_field_recognizes_file_and_path() -> None:
    assert _is_path_field("file") is True
    assert _is_path_field("path") is True
