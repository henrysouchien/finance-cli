from __future__ import annotations

import time

import boto3
import jwt
import pytest
from botocore.stub import Stubber
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from finance_cli.storage_client import auth


def _keypair() -> tuple[str, str]:
    private_key = ed25519.Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    return private_pem, public_pem


def _secrets_client():
    return boto3.client(
        "secretsmanager",
        region_name="us-east-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        aws_session_token="test",
    )


def _stub_secret(
    stubber: Stubber,
    *,
    private_pem: str,
    kid: str,
    secret_id: str = auth.DEFAULT_SECRET_ID,
) -> None:
    stubber.add_response(
        "get_secret_value",
        {
            "ARN": "arn:aws:secretsmanager:us-east-2:123456789012:secret:test",
            "Name": secret_id,
            "SecretString": private_pem,
        },
        {"SecretId": secret_id},
    )
    stubber.add_response(
        "describe_secret",
        {
            "ARN": "arn:aws:secretsmanager:us-east-2:123456789012:secret:test",
            "Name": secret_id,
            "Tags": [{"Key": "kid", "Value": kid}],
        },
        {"SecretId": secret_id},
    )


def _provider(private_pem: str, kid: str) -> tuple[auth.JWTAuthProvider, Stubber]:
    client = _secrets_client()
    stubber = Stubber(client)
    _stub_secret(stubber, private_pem=private_pem, kid=kid)
    stubber.activate()
    provider = auth.JWTAuthProvider(secrets_client=client)
    return provider, stubber


def _decode(token: str, public_pem: str) -> dict:
    return jwt.decode(token, public_pem, algorithms=["EdDSA"])


def test_jwt_auth_provider_init_fetches_pem_and_kid() -> None:
    private_pem, _public_pem = _keypair()
    provider, stubber = _provider(private_pem, "kid-init")
    try:
        assert provider.kid == "kid-init"
        stubber.assert_no_pending_responses()
    finally:
        provider.close()
        stubber.deactivate()


def test_get_token_signs_valid_jwt_with_expected_claims() -> None:
    private_pem, public_pem = _keypair()
    provider, stubber = _provider(private_pem, "kid-sign")
    try:
        token = provider.get_token("finance-cli", "synthetic-user-1", ["admin"])
        header = jwt.get_unverified_header(token)
        claims = _decode(token, public_pem)

        assert header["kid"] == "kid-sign"
        assert header["alg"] == "EdDSA"
        assert claims["kid"] == "kid-sign"
        assert claims["product"] == "finance-cli"
        assert claims["user_id"] == "synthetic-user-1"
        assert claims["scopes"] == ["admin"]
        assert 0 < claims["exp"] - int(time.time()) <= auth.TOKEN_LIFETIME_SECONDS
        stubber.assert_no_pending_responses()
    finally:
        provider.close()
        stubber.deactivate()


def test_cache_hit_returns_same_token_within_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    private_pem, _public_pem = _keypair()
    provider, stubber = _provider(private_pem, "kid-cache")
    now = float(int(time.time()))
    monkeypatch.setattr(auth.time, "time", lambda: now)
    try:
        first = provider.get_token("finance-cli", "synthetic-user-1")
        now += 19
        second = provider.get_token("finance-cli", "synthetic-user-1")

        assert second == first
        stubber.assert_no_pending_responses()
    finally:
        provider.close()
        stubber.deactivate()


def test_cache_miss_after_20_seconds_mints_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    private_pem, public_pem = _keypair()
    provider, stubber = _provider(private_pem, "kid-stale")
    now = float(int(time.time()))
    monkeypatch.setattr(auth.time, "time", lambda: now)
    try:
        first = provider.get_token("finance-cli", "synthetic-user-1")
        first_claims = _decode(first, public_pem)
        now += 20
        second = provider.get_token("finance-cli", "synthetic-user-1")
        second_claims = _decode(second, public_pem)

        assert second != first
        assert second_claims["exp"] == first_claims["exp"] + 20
        stubber.assert_no_pending_responses()
    finally:
        provider.close()
        stubber.deactivate()


def test_different_cache_keys_are_independent(monkeypatch: pytest.MonkeyPatch) -> None:
    private_pem, public_pem = _keypair()
    provider, stubber = _provider(private_pem, "kid-scope")
    now = float(int(time.time()))
    monkeypatch.setattr(auth.time, "time", lambda: now)
    try:
        base = provider.get_token("finance-cli", "synthetic-user-1")
        scoped = provider.get_token("finance-cli", "synthetic-user-1", ["admin"])
        other_user = provider.get_token("finance-cli", "synthetic-user-2")

        assert len({base, scoped, other_user}) == 3
        assert _decode(base, public_pem)["scopes"] == []
        assert _decode(scoped, public_pem)["scopes"] == ["admin"]
        assert _decode(other_user, public_pem)["user_id"] == "synthetic-user-2"
        stubber.assert_no_pending_responses()
    finally:
        provider.close()
        stubber.deactivate()


def test_close_stops_refresh_thread() -> None:
    private_pem, _public_pem = _keypair()
    provider, stubber = _provider(private_pem, "kid-close")
    try:
        assert provider._refresh_thread.is_alive()
        provider.close()
        assert not provider._refresh_thread.is_alive()
        stubber.assert_no_pending_responses()
    finally:
        provider.close()
        stubber.deactivate()
