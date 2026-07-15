import pytest

from finance_cli import db
from finance_cli.exceptions import ConfigurationError


@pytest.fixture(autouse=True)
def _reset_encryption_mode_override():
    previous = db.set_db_encryption_mode_override(None)
    try:
        yield
    finally:
        db.set_db_encryption_mode_override(previous)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("off", "off"),
        ("OFF", "off"),
        ("  require  ", "require"),
        ("Provision", "provision"),
    ],
)
def test_canonical_modes_pass(monkeypatch, value, expected):
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", value)
    assert db.db_encryption_mode() == expected


def test_default_when_unset(monkeypatch):
    monkeypatch.delenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", raising=False)
    assert db.db_encryption_mode() == "off"


@pytest.mark.parametrize(
    "bogus",
    [
        "on",
        "ON",
        " On ",
        "true",
        "1",
        "yes",
        "enabled",
        "garbage",
        "",
        "   ",
    ],
)
def test_unknown_values_raise(monkeypatch, bogus):
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", bogus)

    with pytest.raises(ConfigurationError) as exc:
        db.db_encryption_mode()

    assert not isinstance(exc.value, ValueError)
    msg = str(exc.value)
    assert repr(bogus) in msg, f"raw value {bogus!r} missing from error: {msg}"
    normalized = bogus.strip().lower()
    assert repr(normalized) in msg, f"normalized {normalized!r} missing from error: {msg}"


def test_process_override_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "require")

    db.set_db_encryption_mode_override("off")

    assert db.db_encryption_mode() == "off"


def test_process_override_can_be_cleared(monkeypatch):
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "require")
    db.set_db_encryption_mode_override("off")

    db.set_db_encryption_mode_override(None)

    assert db.db_encryption_mode() == "require"


def test_invalid_process_override_raises_configuration_error():
    with pytest.raises(ConfigurationError, match="DB_ENCRYPTION_MODE_OVERRIDE='on'"):
        db.set_db_encryption_mode_override("on")
