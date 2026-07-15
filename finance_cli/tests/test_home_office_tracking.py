from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import home_office_tracking
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def test_setup_home_office_tracking_writes_existing_tax_config(db_path: Path) -> None:
    with connect(db_path) as conn:
        preview = home_office_tracking.setup_home_office_tracking(
            conn,
            year="2026",
            sqft=180,
            total_sqft=900,
            dry_run=True,
        )
        preview_rows = conn.execute("SELECT COUNT(*) AS n FROM tax_config").fetchone()["n"]

        result = home_office_tracking.setup_home_office_tracking(
            conn,
            year="2026",
            sqft=180,
            total_sqft=900,
        )
        repeat = home_office_tracking.setup_home_office_tracking(
            conn,
            year="2026",
            sqft=180,
            total_sqft=900,
        )
        rows = conn.execute(
            """
            SELECT config_key, config_value
              FROM tax_config
             WHERE tax_year = 2026
             ORDER BY config_key
            """
        ).fetchall()

    assert preview["summary"]["would_update_count"] == 3
    assert preview["summary"]["updated_count"] == 0
    assert preview_rows == 0
    assert result["summary"]["updated_count"] == 3
    assert result["summary"]["tentative_deduction_cents"] == 90_000
    assert repeat["summary"]["updated_count"] == 0
    assert {row["config_key"]: row["config_value"] for row in rows} == {
        "home_office_method": "simplified",
        "home_office_sqft": "180",
        "home_total_sqft": "900",
    }


def test_setup_home_office_tracking_caps_simplified_square_feet(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = home_office_tracking.setup_home_office_tracking(
            conn,
            year="2026",
            sqft=450,
        )

    assert result["data"]["office_sqft"] == 450
    assert result["data"]["eligible_sqft"] == 300
    assert result["summary"]["tentative_deduction_cents"] == 150_000


def test_setup_home_office_tracking_validation(db_path: Path, tmp_path: Path) -> None:
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        """
        split_rules:
          - match:
              category: Rent
            business_pct: 25
            business_category: Rent
            personal_category: Rent
        """,
        encoding="utf-8",
    )
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="YYYY"):
            home_office_tracking.setup_home_office_tracking(conn, year="26", sqft=120)
        with pytest.raises(ValidationError, match="greater than 0"):
            home_office_tracking.setup_home_office_tracking(conn, year="2026", sqft=0)
        with pytest.raises(ValidationError, match="actual-method"):
            home_office_tracking.setup_home_office_tracking(
                conn,
                year="2026",
                sqft=120,
                method="actual",
            )
        with pytest.raises(ValidationError, match="less than or equal"):
            home_office_tracking.setup_home_office_tracking(
                conn,
                year="2026",
                sqft=1200,
                total_sqft=900,
            )
        with pytest.raises(ValidationError, match="split rules"):
            home_office_tracking.setup_home_office_tracking(
                conn,
                year="2026",
                sqft=120,
                rules_path=rules_path,
            )


def test_home_office_tracking_tool_is_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    assert "setup_home_office_tracking" in gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert "setup_home_office_tracking" in DB_WRITE_TOOLS
