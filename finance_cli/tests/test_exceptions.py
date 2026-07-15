from __future__ import annotations

import pytest

from finance_cli.exceptions import (
    ConflictError,
    ConfigurationError,
    EngagementRequiredError,
    FinanceCLIError,
    IntegrationError,
    NotFoundError,
    TenantMismatchError,
    ValidationError,
)
from finance_cli.plaid_client import PlaidUnavailableError


@pytest.mark.parametrize(
    ("exc", "expected_str", "expected_message", "expected_status"),
    [
        (
            FinanceCLIError("Something went wrong"),
            "[internal_error] Something went wrong",
            "Something went wrong",
            500,
        ),
        (
            ValidationError("Budget amount must be positive"),
            "[validation_error] Budget amount must be positive",
            "Budget amount must be positive",
            422,
        ),
        (
            ConfigurationError(
                "FINANCE_CLI_REQUIRE_DB_ENCRYPTION='on' is not a recognized encryption mode"
            ),
            "[configuration_error] FINANCE_CLI_REQUIRE_DB_ENCRYPTION='on' is not a recognized encryption mode",
            "FINANCE_CLI_REQUIRE_DB_ENCRYPTION='on' is not a recognized encryption mode",
            500,
        ),
        (
            NotFoundError("Transaction abc not found"),
            "[not_found] Transaction abc not found",
            "Transaction abc not found",
            404,
        ),
        (
            ConflictError("Duplicate subscription"),
            "[conflict] Duplicate subscription",
            "Duplicate subscription",
            409,
        ),
        (
            EngagementRequiredError("Membership required"),
            "[engagement_required] Membership required",
            "Membership required",
            403,
        ),
        (
            IntegrationError("Plaid API unavailable"),
            "[integration_error] Plaid API unavailable",
            "Plaid API unavailable",
            502,
        ),
    ],
)
def test_finance_cli_error_exposes_user_message_and_status(
    exc: FinanceCLIError,
    expected_str: str,
    expected_message: str,
    expected_status: int,
) -> None:
    assert str(exc) == expected_str
    assert exc.user_message == expected_message
    assert exc.http_status == expected_status


def test_plaid_unavailable_error_uses_service_unavailable_status() -> None:
    exc = PlaidUnavailableError("Plaid is not configured")

    assert str(exc) == "[integration_error] Plaid is not configured"
    assert exc.user_message == "Plaid is not configured"
    assert exc.http_status == 503


def test_tenant_mismatch_public_message_is_generic() -> None:
    exc = TenantMismatchError(
        "DB tenant marker 'bob' does not match expected user 'alice': /Users/foo/data/alice.db",
        expected_user_id="alice",
        actual_user_id="bob",
        db_path="/Users/foo/data/alice.db",
        reason="mismatch",
    )

    assert exc.user_message == "Unable to complete request."
    assert "alice" not in exc.user_message
    assert "bob" not in exc.user_message
    assert "/Users/" not in exc.user_message
    assert "tenant_marker" not in exc.user_message


def test_tenant_mismatch_exception_preserves_log_context() -> None:
    db_path = "/Users/foo/data/alice.db"
    exc = TenantMismatchError(
        f"DB tenant marker 'bob' does not match expected user 'alice': {db_path}",
        expected_user_id="alice",
        actual_user_id="bob",
        db_path=db_path,
        reason="mismatch",
    )

    assert "expected user 'alice'" in str(exc)
    assert db_path in str(exc)
    assert exc.expected_user_id == "alice"
    assert exc.actual_user_id == "bob"
    assert exc.db_path == db_path
    assert exc.reason == "mismatch"
