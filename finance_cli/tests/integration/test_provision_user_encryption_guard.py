import pytest

from finance_cli.exceptions import ConfigurationError
from finance_cli.user_provisioning import provision_user


def test_provision_user_rejects_bogus_encryption_mode(monkeypatch, tmp_path):
    """Reproduces the 2026-05-03 staging rehearsal failure mode.

    With FINANCE_CLI_REQUIRE_DB_ENCRYPTION=on, provision_user must raise
    ConfigurationError before any per-user disk side-effect. Pins the
    no-half-state contract: no user directory, no finance.db, and no
    db-dek.enc may exist after the failed call.
    """
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "on")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))
    rules = tmp_path / "rules-template.yaml"
    rules.write_text("# empty\n", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="not a recognized encryption mode"):
        provision_user(data_root=tmp_path, user_id="1", template_rules_path=rules)

    user_dir = tmp_path / "1"
    assert not user_dir.exists(), f"user dir {user_dir} created despite bogus env"
    assert not (user_dir / "finance.db").exists()
    assert not (user_dir / "db-dek.enc").exists()
