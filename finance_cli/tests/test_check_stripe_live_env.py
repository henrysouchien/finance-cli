from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_stripe_live_env.py"
SPEC = importlib.util.spec_from_file_location("check_stripe_live_env", SCRIPT_PATH)
assert SPEC is not None
check_stripe_live_env = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(check_stripe_live_env)


def _valid_values(secret_key: str = "sk_live_" + "x" * 24) -> dict[str, str]:
    return {
        "STRIPE_SECRET_KEY": secret_key,
        "STRIPE_WEBHOOK_SECRET": "whsec_" + "x" * 24,
        "STRIPE_PRICE_LITE": "price_live_lite",
        "STRIPE_PRICE_MONTHLY": "price_live_monthly",
        "STRIPE_PRICE_ANNUAL": "price_live_annual",
        "STRIPE_PRICE_LIFETIME": "price_live_lifetime",
        "STRIPE_PRICE_CREDIT_PACK_SMALL": "price_live_small",
        "STRIPE_PRICE_CREDIT_PACK_MEDIUM": "price_live_medium",
        "STRIPE_PRICE_CREDIT_PACK_LARGE": "price_live_large",
        "STRIPE_PRICE_DEBT_PAYOFF_PILOT": "price_live_debt",
    }


def test_check_values_accepts_live_secret_or_restricted_key() -> None:
    rows, ok = check_stripe_live_env.check_values(_valid_values())
    restricted_rows, restricted_ok = check_stripe_live_env.check_values(
        _valid_values(secret_key="rk_live_" + "x" * 24)
    )

    assert ok is True
    assert restricted_ok is True
    assert rows[0]["status"] == "live"
    assert restricted_rows[0]["status"] == "live"


def test_check_values_rejects_missing_and_test_mode_without_leaking_secret() -> None:
    secret = "sk_test_should_not_be_printed"
    rows, ok = check_stripe_live_env.check_values(
        _valid_values(secret_key=secret) | {"STRIPE_PRICE_LITE": ""}
    )

    assert ok is False
    assert rows[0] == {
        "key": "STRIPE_SECRET_KEY",
        "status": "test",
        "prefix": "sk_test_",
        "length": len(secret),
    }
    assert {
        "key": "STRIPE_PRICE_LITE",
        "status": "missing",
        "prefix": "",
        "length": 0,
    } in rows
    assert secret not in str(rows)


def test_main_reads_env_file_and_returns_failure_for_test_key(
    tmp_path: Path,
    capsys,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in _valid_values("sk_test_123").items())
    )

    exit_code = check_stripe_live_env.main(["--env-file", str(env_file)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "STRIPE_SECRET_KEY: status=test prefix=sk_test_" in captured.out
    assert "sk_test_123" not in captured.out


def test_env_file_loader_accepts_export_lines(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"export {key}={value}" for key, value in _valid_values().items())
    )

    rows, ok = check_stripe_live_env.check_values(
        check_stripe_live_env._load_env_file(env_file)
    )

    assert ok is True
    assert rows[0]["status"] == "live"
