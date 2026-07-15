from __future__ import annotations

from botocore.exceptions import ClientError


def _client_error(code: str, message: str) -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": code,
                "Message": message,
            }
        },
        "SecretsManager",
    )


class FakeSecretsBackend:
    def __init__(self) -> None:
        self._secrets: dict[str, str] = {}

    def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
        if SecretId not in self._secrets:
            raise _client_error(
                "ResourceNotFoundException",
                f"Secret {SecretId} was not found.",
            )
        return {"SecretString": self._secrets[SecretId]}

    def create_secret(
        self,
        *,
        Name: str,
        SecretString: str,
        Description: str | None = None,
    ) -> dict[str, str]:
        del Description
        if Name in self._secrets:
            raise _client_error(
                "ResourceExistsException",
                f"Secret {Name} already exists.",
            )
        self._secrets[Name] = SecretString
        return {"ARN": Name, "Name": Name}

    def put_secret_value(self, *, SecretId: str, SecretString: str) -> dict[str, str]:
        if SecretId not in self._secrets:
            raise _client_error(
                "ResourceNotFoundException",
                f"Secret {SecretId} was not found.",
            )
        self._secrets[SecretId] = SecretString
        return {"ARN": SecretId, "Name": SecretId}

    def describe_secret(self, *, SecretId: str) -> dict[str, object]:
        if SecretId not in self._secrets:
            raise _client_error(
                "ResourceNotFoundException",
                f"Secret {SecretId} was not found.",
            )
        return {"DeletedDate": None}
