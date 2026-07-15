"""Shared exception hierarchy for finance_cli."""

from __future__ import annotations


class FinanceCLIError(Exception):
    """Base exception for all finance-cli errors."""

    error_code: str = "internal_error"
    http_status: int = 500

    @property
    def user_message(self) -> str:
        return super().__str__()

    def __str__(self) -> str:
        return f"[{self.error_code}] {super().__str__()}"


class ValidationError(FinanceCLIError, ValueError):
    """Bad input, invalid range, constraint violation."""

    error_code = "validation_error"
    http_status = 422


class ConfigurationError(FinanceCLIError):
    """Misconfigured startup env var, settings file, or required runtime parameter."""

    error_code = "configuration_error"
    http_status = 500


class NotFoundError(FinanceCLIError, ValueError):
    """Entity not found (transaction, account, loan, etc.)."""

    error_code = "not_found"
    http_status = 404


class ConflictError(FinanceCLIError, ValueError):
    """Duplicate or conflicting state."""

    error_code = "conflict"
    http_status = 409


class EngagementRequiredError(FinanceCLIError, PermissionError):
    """The requested server-side coaching operation requires an active engagement."""

    error_code = "engagement_required"
    http_status = 403


class IntegrationError(FinanceCLIError, RuntimeError):
    """External service failure (Plaid, Stripe, etc.)."""

    error_code = "integration_error"
    http_status = 502


class TenantMismatchError(FinanceCLIError):
    """Tenant marker does not match expected user."""

    error_code = "tenant_mismatch"
    http_status = 500

    def __init__(
        self,
        message: str,
        *,
        expected_user_id: str | None = None,
        actual_user_id: str | None = None,
        db_path: str | None = None,
        reason: str = "mismatch",
    ) -> None:
        super().__init__(message)
        self.expected_user_id = expected_user_id
        self.actual_user_id = actual_user_id
        self.db_path = db_path
        self.reason = reason

    @property
    def user_message(self) -> str:
        return "Unable to complete request."


class KMSUnavailableError(FinanceCLIError, RuntimeError):
    """KMS is unavailable, throttling, or unreachable."""

    error_code = "kms_unavailable"
    http_status = 503


class KMSAccessDeniedError(FinanceCLIError, PermissionError):
    """KMS rejected the caller because access is denied."""

    error_code = "kms_access_denied"
    http_status = 403


class InvalidCiphertextError(FinanceCLIError, ValueError):
    """KMS or local AEAD rejected ciphertext."""

    error_code = "invalid_ciphertext"
    http_status = 500


class CrossUserBundleError(FinanceCLIError, PermissionError):
    """A bundle header identifies a different user than the caller."""

    error_code = "cross_user_bundle"
    http_status = 403


class DBDEKNotFoundError(FinanceCLIError, FileNotFoundError):
    """The DB DEK artifact is not provisioned."""

    error_code = "db_dek_not_found"
    http_status = 500


class BackendMismatchError(FinanceCLIError, RuntimeError):
    """Recorded storage backend differs from the live backend."""

    error_code = "backend_mismatch"
    http_status = 500


class EnvelopeVersionError(FinanceCLIError, ValueError):
    """Unsupported or malformed envelope version."""

    error_code = "envelope_version"
    http_status = 500


class ProviderSecretNotFoundError(FinanceCLIError, KeyError):
    """A provider secret ref is unknown."""

    error_code = "provider_secret_not_found"
    http_status = 404
