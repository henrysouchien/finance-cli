from __future__ import annotations

import base64
import hashlib
import json
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from joserfc import jwk, jwt
from plaid.model.jwk_public_key import JWKPublicKey

import finance_cli.plaid_client as plaid_client


@pytest.fixture(autouse=True)
def clear_plaid_webhook_jwk_cache() -> None:
    with plaid_client._PLAID_WEBHOOK_JWK_CACHE_LOCK:
        plaid_client._PLAID_WEBHOOK_JWK_CACHE.clear()


@pytest.fixture
def fixed_now(monkeypatch) -> int:
    now = 1_700_000_000
    monkeypatch.setattr(plaid_client.time, "time", lambda: float(now))
    return now


@pytest.fixture
def request_body() -> bytes:
    return b'{"webhook_type":"ITEM","webhook_code":"DEFAULT_UPDATE"}'


@pytest.fixture
def es256_key_material(fixed_now: int) -> dict[str, object]:
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    private_jwk = jwk.import_key(private_pem, "EC")
    public_jwk = jwk.import_key(public_pem, "EC").as_dict()
    public_jwk["kid"] = "plaid-test-kid"
    return {
        "kid": "plaid-test-kid",
        "private_key": private_jwk,
        "public_key": JWKPublicKey(
            alg="ES256",
            crv=str(public_jwk["crv"]),
            kid=str(public_jwk["kid"]),
            kty=str(public_jwk["kty"]),
            use="sig",
            x=str(public_jwk["x"]),
            y=str(public_jwk["y"]),
            created_at=fixed_now,
            expired_at=None,
        ),
    }


@pytest.fixture
def mock_webhook_verification_key(monkeypatch, es256_key_material):
    requested_kids: list[str] = []

    class _Client:
        def webhook_verification_key_get(self, request):
            requested_kids.append(str(request.key_id))
            return SimpleNamespace(key=es256_key_material["public_key"])

    monkeypatch.setattr(plaid_client, "_create_plaid_api_client", lambda: _Client())
    return requested_kids


def _body_digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _encode_segment(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _make_token(
    private_key,
    *,
    kid: str | None,
    claims: dict[str, object],
    alg: str = "ES256",
) -> str:
    header: dict[str, object] = {"alg": alg}
    if kid is not None:
        header["kid"] = kid
    return jwt.encode(header, claims, private_key, algorithms=[alg])


def _make_none_token(*, kid: str | None, claims: dict[str, object]) -> str:
    header: dict[str, object] = {"alg": "none"}
    if kid is not None:
        header["kid"] = kid
    return f"{_encode_segment(header)}.{_encode_segment(claims)}."


def test_verify_plaid_webhook_rejects_none_algorithm(
    request_body,
    fixed_now,
    es256_key_material,
    mock_webhook_verification_key,
) -> None:
    token = _make_none_token(
        kid=str(es256_key_material["kid"]),
        claims={
            "iat": fixed_now,
            "request_body_sha256": _body_digest(request_body),
        },
    )

    with pytest.raises(ValueError, match="verification failed"):
        plaid_client.verify_plaid_webhook(request_body, token)


def test_verify_plaid_webhook_rejects_hs256(
    request_body,
    fixed_now,
    es256_key_material,
    mock_webhook_verification_key,
) -> None:
    token = jwt.encode(
        {"alg": "HS256", "kid": str(es256_key_material["kid"])},
        {
            "iat": fixed_now,
            "request_body_sha256": _body_digest(request_body),
        },
        jwk.import_key("shared-secret-for-hs256-tests", "oct"),
        algorithms=["HS256"],
    )

    with pytest.raises(ValueError, match="verification failed"):
        plaid_client.verify_plaid_webhook(request_body, token)


def test_verify_plaid_webhook_rejects_expired_iat(
    request_body,
    fixed_now,
    es256_key_material,
    mock_webhook_verification_key,
) -> None:
    token = _make_token(
        es256_key_material["private_key"],
        kid=str(es256_key_material["kid"]),
        claims={
            "iat": fixed_now - 301,
            "request_body_sha256": _body_digest(request_body),
        },
    )

    with pytest.raises(ValueError, match="Invalid Plaid webhook iat"):
        plaid_client.verify_plaid_webhook(request_body, token)


def test_verify_plaid_webhook_rejects_future_iat(
    request_body,
    fixed_now,
    es256_key_material,
    mock_webhook_verification_key,
) -> None:
    token = _make_token(
        es256_key_material["private_key"],
        kid=str(es256_key_material["kid"]),
        claims={
            "iat": fixed_now + 60,
            "request_body_sha256": _body_digest(request_body),
        },
    )

    with pytest.raises(ValueError, match="Invalid Plaid webhook iat"):
        plaid_client.verify_plaid_webhook(request_body, token)


def test_verify_plaid_webhook_rejects_wrong_request_body_hash(
    request_body,
    fixed_now,
    es256_key_material,
    mock_webhook_verification_key,
) -> None:
    token = _make_token(
        es256_key_material["private_key"],
        kid=str(es256_key_material["kid"]),
        claims={
            "iat": fixed_now,
            "request_body_sha256": "0" * 64,
        },
    )

    with pytest.raises(ValueError, match="request body hash mismatch"):
        plaid_client.verify_plaid_webhook(request_body, token)


def test_verify_plaid_webhook_accepts_valid_es256_and_caches_jwk(
    request_body,
    fixed_now,
    es256_key_material,
    mock_webhook_verification_key,
) -> None:
    token = _make_token(
        es256_key_material["private_key"],
        kid=str(es256_key_material["kid"]),
        claims={
            "iat": fixed_now,
            "request_body_sha256": _body_digest(request_body),
        },
    )

    plaid_client.verify_plaid_webhook(request_body, token)
    plaid_client.verify_plaid_webhook(request_body, token)

    assert mock_webhook_verification_key == [str(es256_key_material["kid"])]


def test_verify_plaid_webhook_rejects_missing_kid(request_body, fixed_now, es256_key_material) -> None:
    token = _make_token(
        es256_key_material["private_key"],
        kid=None,
        claims={
            "iat": fixed_now,
            "request_body_sha256": _body_digest(request_body),
        },
    )

    with pytest.raises(ValueError, match="Missing Plaid webhook kid"):
        plaid_client.verify_plaid_webhook(request_body, token)
