"""Encryption helpers for per-user API keys."""

from __future__ import annotations

import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

_API_KEY_SALT = b"cashnerd-api-key-v1"


def _build_fernet(secret: str) -> Fernet:
    key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=_API_KEY_SALT,
        info=None,
    ).derive(secret.encode("utf-8"))
    return Fernet(base64.urlsafe_b64encode(key))


def encrypt_api_key(plaintext: str, secret: str) -> str:
    return _build_fernet(secret).encrypt(plaintext.encode("utf-8")).decode("utf-8")


def decrypt_api_key(ciphertext: str, secret: str) -> str | None:
    try:
        plaintext = _build_fernet(secret).decrypt(ciphertext.encode("utf-8"))
    except Exception:
        return None
    try:
        return plaintext.decode("utf-8")
    except UnicodeDecodeError:
        return None
