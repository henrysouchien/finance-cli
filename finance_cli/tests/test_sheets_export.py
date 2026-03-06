from __future__ import annotations

import argparse
import importlib
import json
import os
import stat
import sys
import types
import uuid
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database


class WorksheetNotFound(Exception):
    pass


class FakeResponse:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.headers = headers or {}


class FakeAPIError(Exception):
    def __init__(self, status_code: int, message: str = "api error", headers: dict[str, str] | None = None):
        super().__init__(message)
        self.response = FakeResponse(status_code=status_code, headers=headers)


class FakeWorksheet:
    def __init__(self, spreadsheet: "FakeSpreadsheet", title: str):
        self.spreadsheet = spreadsheet
        self.title = title
        self.clear_calls = 0
        self.resize_calls: list[tuple[int, int]] = []
        self.update_calls: list[dict[str, object]] = []

    def clear(self):
        self.clear_calls += 1

    def resize(self, rows: int, cols: int):
        self.resize_calls.append((rows, cols))

    def update(self, range_name: str, values: list[list[object]], value_input_option: str | None = None):
        exc = self.spreadsheet.fail_update_by_title.get(self.title)
        if exc is not None:
            raise exc
        self.update_calls.append(
            {
                "range": range_name,
                "values": values,
                "value_input_option": value_input_option,
            }
        )


class FakeSpreadsheet:
    def __init__(self, spreadsheet_id: str):
        self.id = spreadsheet_id
        self.url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        self._worksheets: dict[str, FakeWorksheet] = {}
        self.fail_update_by_title: dict[str, Exception] = {}
        self.worksheet_calls: list[str] = []
        self.add_calls: list[tuple[str, int, int]] = []

    def worksheet(self, title: str) -> FakeWorksheet:
        self.worksheet_calls.append(title)
        if title not in self._worksheets:
            raise WorksheetNotFound(title)
        return self._worksheets[title]

    def add_worksheet(self, title: str, rows: int, cols: int) -> FakeWorksheet:
        ws = FakeWorksheet(self, title)
        self._worksheets[title] = ws
        self.add_calls.append((title, rows, cols))
        return ws

    def ensure_worksheet(self, title: str) -> FakeWorksheet:
        if title not in self._worksheets:
            self._worksheets[title] = FakeWorksheet(self, title)
        return self._worksheets[title]


class FakeClient:
    def __init__(self):
        self.spreadsheets: dict[str, FakeSpreadsheet] = {}
        self.open_errors: dict[str, Exception] = {}
        self.open_calls: list[str] = []
        self.create_calls: list[str] = []

    def create(self, title: str) -> FakeSpreadsheet:
        self.create_calls.append(title)
        sid = f"sheet_{len(self.create_calls)}"
        ss = FakeSpreadsheet(sid)
        self.spreadsheets[sid] = ss
        return ss

    def open_by_key(self, key: str) -> FakeSpreadsheet:
        self.open_calls.append(key)
        if key in self.open_errors:
            raise self.open_errors[key]
        if key not in self.spreadsheets:
            raise FakeAPIError(404, "not found")
        return self.spreadsheets[key]


class FakeCredentials:
    def __init__(
        self,
        *,
        scopes: list[str] | None = None,
        valid: bool = True,
        expired: bool = False,
        refresh_token: str | None = "refresh",
        refresh_error: Exception | None = None,
    ):
        self.scopes = list(scopes or [])
        self.valid = bool(valid)
        self.expired = bool(expired)
        self.refresh_token = refresh_token
        self.refresh_error = refresh_error
        self.refresh_calls = 0

    def refresh(self, _request) -> None:
        self.refresh_calls += 1
        if self.refresh_error is not None:
            raise self.refresh_error
        self.expired = False
        self.valid = True

    def to_json(self) -> str:
        return json.dumps(
            {
                "token": "token",
                "refresh_token": self.refresh_token,
                "scopes": self.scopes,
            }
        )


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


@pytest.fixture()
def conn(db_path: Path):
    c = connect(db_path)
    yield c
    c.close()


@pytest.fixture()
def fake_home(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def _reload_sheets_export():
    import finance_cli.sheets_export as sheets_export

    return importlib.reload(sheets_export)


def _write_credentials_file(home_dir: Path, monkeypatch, *, env_path: str | None = None) -> Path:
    config_dir = home_dir / ".config" / "finance_cli"
    config_dir.mkdir(parents=True, exist_ok=True)
    creds_path = config_dir / "google_credentials.json"
    creds_path.write_text("{}", encoding="utf-8")
    if env_path is None:
        monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS", raising=False)
    else:
        monkeypatch.setenv("GOOGLE_SHEETS_CREDENTIALS", env_path)
    return creds_path


def _token_path(home_dir: Path) -> Path:
    return home_dir / ".config" / "finance_cli" / "google_token.json"


def _install_google_stubs(monkeypatch, *, client: FakeClient, state: dict[str, object]) -> None:
    gspread_mod = types.ModuleType("gspread")

    def _authorize(_creds):
        return client

    gspread_mod.authorize = _authorize

    google_mod = types.ModuleType("google")
    google_auth_mod = types.ModuleType("google.auth")
    google_auth_transport_mod = types.ModuleType("google.auth.transport")
    google_auth_requests_mod = types.ModuleType("google.auth.transport.requests")

    class Request:  # noqa: D401
        """Dummy request type for credential refresh."""

    google_auth_requests_mod.Request = Request

    google_oauth2_mod = types.ModuleType("google.oauth2")
    google_oauth2_credentials_mod = types.ModuleType("google.oauth2.credentials")

    class Credentials:
        @classmethod
        def from_authorized_user_file(cls, path: str, scopes: list[str]):
            state.setdefault("from_file_calls", []).append((path, tuple(scopes)))
            exc = state.get("from_file_error")
            if exc is not None:
                raise exc
            creds = state.get("file_creds")
            if creds is None:
                raise ValueError("missing token")
            return creds

    google_oauth2_credentials_mod.Credentials = Credentials

    google_auth_oauthlib_mod = types.ModuleType("google_auth_oauthlib")
    google_auth_oauthlib_flow_mod = types.ModuleType("google_auth_oauthlib.flow")

    class _FakeFlow:
        def run_local_server(self, open_browser: bool = False, port: int = 0):
            state.setdefault("run_local_server_calls", []).append((open_browser, port))
            runner = state.get("run_local_server")
            if callable(runner):
                return runner(open_browser=open_browser, port=port)
            creds = state.get("flow_creds")
            if creds is None:
                raise RuntimeError("flow creds not configured")
            return creds

    class InstalledAppFlow:
        @classmethod
        def from_client_secrets_file(cls, path: str, scopes: list[str]):
            state.setdefault("flow_calls", []).append((path, tuple(scopes)))
            return _FakeFlow()

    google_auth_oauthlib_flow_mod.InstalledAppFlow = InstalledAppFlow

    monkeypatch.setitem(sys.modules, "gspread", gspread_mod)
    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.auth", google_auth_mod)
    monkeypatch.setitem(sys.modules, "google.auth.transport", google_auth_transport_mod)
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests", google_auth_requests_mod)
    monkeypatch.setitem(sys.modules, "google.oauth2", google_oauth2_mod)
    monkeypatch.setitem(sys.modules, "google.oauth2.credentials", google_oauth2_credentials_mod)
    monkeypatch.setitem(sys.modules, "google_auth_oauthlib", google_auth_oauthlib_mod)
    monkeypatch.setitem(sys.modules, "google_auth_oauthlib.flow", google_auth_oauthlib_flow_mod)


def _seed_category(conn, name: str, *, is_income: int = 0, level: int = 1) -> str:
    cid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
        VALUES (?, ?, NULL, ?, ?, 0, 0)
        """,
        (cid, name, level, is_income),
    )
    return cid


def _seed_account(
    conn,
    *,
    institution: str = "Bank",
    name: str = "Checking",
    account_type: str = "checking",
    balance_cents: int | None = 100_000,
    available_cents: int | None = None,
    limit_cents: int | None = None,
) -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            source, is_active, balance_current_cents, balance_available_cents, balance_limit_cents
        ) VALUES (?, ?, ?, ?, 'manual', 1, ?, ?, ?)
        """,
        (aid, institution, name, account_type, balance_cents, available_cents, limit_cents),
    )
    return aid


def _seed_txn(
    conn,
    *,
    amount_cents: int,
    txn_date: str,
    category_id: str | None = None,
    account_id: str | None = None,
    description: str = "txn",
    use_type: str | None = None,
    is_payment: int = 0,
    is_reviewed: int = 0,
) -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents,
            category_id, source, use_type, is_payment, is_reviewed, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, 'manual', ?, ?, ?, 1)
        """,
        (tid, account_id, txn_date, description, amount_cents, category_id, use_type, is_payment, is_reviewed),
    )
    return tid


def _seed_pl_map(conn, category_id: str, section: str, display_order: int = 10) -> None:
    conn.execute(
        "INSERT INTO pl_section_map (id, category_id, pl_section, display_order) VALUES (?, ?, ?, ?)",
        (uuid.uuid4().hex, category_id, section, display_order),
    )


def _seed_schedule_map(
    conn,
    category_id: str,
    *,
    year: int,
    line_number: str,
    line_label: str,
    deduction_pct: float = 1.0,
) -> None:
    conn.execute(
        """
        INSERT INTO schedule_c_map (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (uuid.uuid4().hex, category_id, line_label, line_number, deduction_pct, year),
    )


def _seed_business_data(conn, *, tax_year: int = 2025) -> None:
    income_cat = _seed_category(conn, "Income: Business", is_income=1)
    expense_cat = _seed_category(conn, "Advertising", is_income=0)
    _seed_pl_map(conn, income_cat, "revenue", 10)
    _seed_pl_map(conn, expense_cat, "opex_marketing", 20)
    _seed_schedule_map(conn, expense_cat, year=tax_year, line_number="8", line_label="Advertising", deduction_pct=1.0)
    account_id = _seed_account(conn, institution="Biz Bank", name="Biz Checking", account_type="checking")
    _seed_txn(
        conn,
        amount_cents=150_000,
        txn_date=f"{tax_year}-01-15",
        category_id=income_cat,
        account_id=account_id,
        description="Consulting income",
        use_type="Business",
    )
    _seed_txn(
        conn,
        amount_cents=-25_000,
        txn_date=f"{tax_year}-01-20",
        category_id=expense_cat,
        account_id=account_id,
        description="Ad spend",
        use_type="Business",
    )


# ---------------------------------------------------------------------------
# Auth + dependency tests
# ---------------------------------------------------------------------------


def test_import_error_when_optional_dependencies_missing(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)

    monkeypatch.setitem(sys.modules, "gspread", None)
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests", None)
    monkeypatch.setitem(sys.modules, "google.oauth2.credentials", None)
    monkeypatch.setitem(sys.modules, "google_auth_oauthlib.flow", None)

    sheets_export = _reload_sheets_export()

    with pytest.raises(ImportError, match="pip install gspread google-auth-oauthlib"):
        sheets_export.export_to_sheets(conn, auth_only=True)


def test_missing_credentials_file_errors(fake_home, monkeypatch, conn):
    monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS", raising=False)

    state: dict[str, object] = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        ),
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="Google credentials file not found"):
        sheets_export.export_to_sheets(conn, auth_only=True)


def test_relative_credentials_path_rejected(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch, env_path="./creds.json")

    state: dict[str, object] = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        ),
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="absolute"):
        sheets_export.export_to_sheets(conn, auth_only=True)


def test_tilde_credentials_path_accepted(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch, env_path="~/.config/finance_cli/google_credentials.json")

    state: dict[str, object] = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(conn, auth_only=True)

    assert isinstance(result["warnings"], list)
    assert len(state.get("flow_calls", [])) == 1


def test_auth_flow_writes_no_stdout(fake_home, monkeypatch, capsys, conn):
    _write_credentials_file(fake_home, monkeypatch)

    def _runner(**_kwargs):
        print("AUTH URL from flow")
        return FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )

    state: dict[str, object] = {"run_local_server": _runner}
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(conn, auth_only=True)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "AUTH URL from flow" in captured.err


def test_auth_mode_skips_date_validation(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state: dict[str, object] = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(
        conn,
        auth_only=True,
        date_from="bad-date",
        date_to="still-bad",
        year="not-year",
    )
    assert result["tabs"] == []


def test_mcp_fail_fast_when_no_token(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)

    state: dict[str, object] = {}
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="Run `export sheets --auth` first"):
        sheets_export.export_to_sheets(conn, interactive=False)


def test_mcp_fail_fast_invalid_grant(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    token_path = _token_path(fake_home)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text("{}", encoding="utf-8")

    file_creds = FakeCredentials(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ],
        valid=False,
        expired=True,
        refresh_token="refresh",
        refresh_error=RuntimeError("invalid_grant"),
    )
    state: dict[str, object] = {"file_creds": file_creds}
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="revoked"):
        sheets_export.export_to_sheets(conn, interactive=False)


def test_mcp_fail_fast_under_scoped_token(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    token_path = _token_path(fake_home)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text("{}", encoding="utf-8")

    file_creds = FakeCredentials(
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
        valid=True,
        expired=False,
        refresh_token="refresh",
    )
    state: dict[str, object] = {"file_creds": file_creds}
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="required scopes"):
        sheets_export.export_to_sheets(conn, interactive=False)


def test_token_permissions_tightened_when_owned(fake_home, monkeypatch, capsys, conn):
    _write_credentials_file(fake_home, monkeypatch)
    token_path = _token_path(fake_home)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text("{}", encoding="utf-8")
    os.chmod(token_path, 0o644)

    file_creds = FakeCredentials(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ],
        valid=True,
    )
    state: dict[str, object] = {"file_creds": file_creds}
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(conn, year="2025")

    mode = stat.S_IMODE(token_path.stat().st_mode)
    assert mode == 0o600
    assert "tightened to 0600" in capsys.readouterr().err


def test_token_permissions_not_modified_when_not_owned(fake_home, monkeypatch, capsys, conn):
    _write_credentials_file(fake_home, monkeypatch)
    token_path = _token_path(fake_home)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text("{}", encoding="utf-8")
    os.chmod(token_path, 0o644)

    file_creds = FakeCredentials(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ],
        valid=True,
    )
    state: dict[str, object] = {"file_creds": file_creds}
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    monkeypatch.setattr(os, "getuid", lambda: 999999)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(conn, year="2025")

    mode = stat.S_IMODE(token_path.stat().st_mode)
    assert mode == 0o644
    assert "not owned by current user" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Date resolution + validation tests
# ---------------------------------------------------------------------------


def test_date_validation_from_after_to(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)
    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="cannot be after"):
        sheets_export.export_to_sheets(conn, date_from="2025-03-02", date_to="2025-03-01")


def test_date_validation_partial_range_rejected(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)
    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="must be provided together"):
        sheets_export.export_to_sheets(conn, date_from="2025-03-01")


def test_same_year_from_to_infers_business_year(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()
    resolved = sheets_export._resolve_windows(conn, date_from="2025-01-01", date_to="2025-05-01", year=None)

    assert resolved.business_year == "2025"
    assert resolved.txn_from == "2025-01-01"
    assert resolved.txn_to == "2025-05-01"


def test_cross_year_from_to_requires_year(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()

    with pytest.raises(ValueError, match="Cross-year"):
        sheets_export.export_to_sheets(conn, date_from="2025-12-31", date_to="2026-01-01")


def test_no_flags_defaults_to_all_time_and_latest_tax_year(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)
    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    resolved = sheets_export._resolve_windows(conn, date_from=None, date_to=None, year=None)

    assert resolved.txn_from is None
    assert resolved.txn_to is None
    assert resolved.business_year == "2025"


def test_year_only_applies_full_year_windows(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()
    resolved = sheets_export._resolve_windows(conn, date_from=None, date_to=None, year="2024")

    assert resolved.txn_from == "2024-01-01"
    assert resolved.txn_to == "2024-12-31"
    assert resolved.business_year == "2024"


def test_all_three_flags_use_date_window_and_explicit_year(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()
    resolved = sheets_export._resolve_windows(
        conn,
        date_from="2025-02-01",
        date_to="2025-02-28",
        year="2024",
    )

    assert resolved.txn_from == "2025-02-01"
    assert resolved.txn_to == "2025-02-28"
    assert resolved.business_year == "2024"


# ---------------------------------------------------------------------------
# Export behavior tests
# ---------------------------------------------------------------------------


def test_export_creates_spreadsheet_and_persists_settings(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    first = sheets_export.export_to_sheets(conn, year="2025")
    second = sheets_export.export_to_sheets(conn, year="2025")

    assert first["spreadsheet_id"] == "sheet_1"
    assert second["spreadsheet_id"] == "sheet_1"
    assert client.create_calls == ["Finance CLI Export"]
    assert client.open_calls == ["sheet_1"]

    row = conn.execute("SELECT value FROM settings WHERE key = 'google_sheets_spreadsheet_id'").fetchone()
    assert row is not None
    assert row["value"] == "sheet_1"


def test_settings_upsert_refreshes_updated_at(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.execute(
        """
        INSERT INTO settings (key, value, updated_at)
        VALUES ('google_sheets_spreadsheet_id', 'sheet_1', '2000-01-01 00:00:00')
        """
    )
    client.spreadsheets["sheet_1"] = FakeSpreadsheet("sheet_1")
    conn.commit()

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(conn, year="2025")

    row = conn.execute(
        "SELECT value, updated_at FROM settings WHERE key = 'google_sheets_spreadsheet_id'"
    ).fetchone()
    assert row is not None
    assert row["value"] == "sheet_1"
    assert row["updated_at"] != "2000-01-01 00:00:00"


def test_tab_creation_and_clear_logic(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    spreadsheet = FakeSpreadsheet("existing")
    for tab_name in ("Transactions", "Business Financials", "Monthly Spending", "Net Worth"):
        spreadsheet.ensure_worksheet(tab_name)
    client.spreadsheets["existing"] = spreadsheet
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES ('google_sheets_spreadsheet_id', 'existing', datetime('now'))"
    )
    conn.commit()

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(conn, year="2025")

    for tab_name in ("Transactions", "Business Financials", "Monthly Spending", "Net Worth"):
        ws = spreadsheet._worksheets[tab_name]
        assert ws.clear_calls == 1


def test_chunked_writes_for_large_transactions(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    cat_id = _seed_category(conn, "Dining")
    account_id = _seed_account(conn)
    _seed_business_data(conn, tax_year=2025)
    for index in range(1_105):
        _seed_txn(
            conn,
            amount_cents=-100,
            txn_date="2025-02-01",
            category_id=cat_id,
            account_id=account_id,
            description=f"Txn {index}",
            use_type="Personal",
        )
    conn.commit()

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(conn, year="2025")

    ss = client.spreadsheets[result["spreadsheet_id"]]
    txn_ws = ss._worksheets["Transactions"]
    assert len(txn_ws.update_calls) >= 2


def test_monthly_spending_pivot_excludes_payments_and_positive_amounts(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    cat_id = _seed_category(conn, "Travel")
    account_id = _seed_account(conn)
    _seed_business_data(conn, tax_year=2025)

    _seed_txn(
        conn,
        amount_cents=-2000,
        txn_date="2025-01-10",
        category_id=cat_id,
        account_id=account_id,
        description="Valid expense",
        use_type="Business",
        is_payment=0,
    )
    _seed_txn(
        conn,
        amount_cents=-3000,
        txn_date="2025-01-11",
        category_id=cat_id,
        account_id=account_id,
        description="Payment should be excluded",
        use_type="Business",
        is_payment=1,
    )
    _seed_txn(
        conn,
        amount_cents=4000,
        txn_date="2025-01-12",
        category_id=cat_id,
        account_id=account_id,
        description="Positive should be excluded",
        use_type="Business",
        is_payment=0,
    )
    conn.commit()

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(conn, year="2025")

    ss = client.spreadsheets[result["spreadsheet_id"]]
    ws = ss._worksheets["Monthly Spending"]
    values = [
        row
        for call in ws.update_calls
        for row in call["values"]
    ]

    matching = [row for row in values if row and row[0] == "Travel"]
    assert len(matching) == 1
    assert matching[0][2] == 20.0


def test_empty_data_tabs_still_written(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(conn)

    assert "Business Financials" in result["skipped_tabs"]
    assert set(result["tabs"]) == {"Transactions", "Business Financials", "Monthly Spending", "Net Worth"}


def test_stale_spreadsheet_id_error_messages(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()

    client.open_errors["bad404"] = FakeAPIError(404, "missing")
    with pytest.raises(ValueError, match="--new"):
        sheets_export.export_to_sheets(conn, year="2025", spreadsheet_id="bad404")

    client.open_errors["bad400"] = FakeAPIError(400, "bad request")
    with pytest.raises(ValueError, match="--new"):
        sheets_export.export_to_sheets(conn, year="2025", spreadsheet_id="bad400")

    client.open_errors["bad403"] = FakeAPIError(403, "forbidden")
    with pytest.raises(ValueError, match="Permission denied"):
        sheets_export.export_to_sheets(conn, year="2025", spreadsheet_id="bad403")


def test_403_error_does_not_suggest_new(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    client.open_errors["forbidden"] = FakeAPIError(403, "forbidden")
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    with pytest.raises(ValueError) as exc_info:
        sheets_export.export_to_sheets(conn, year="2025", spreadsheet_id="forbidden")

    assert "--new" not in str(exc_info.value)


def test_no_schedule_c_rows_skips_business_tab_with_marker(fake_home, monkeypatch, capsys, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    cat_id = _seed_category(conn, "Groceries")
    acc_id = _seed_account(conn)
    _seed_txn(
        conn,
        amount_cents=-5000,
        txn_date="2025-02-01",
        category_id=cat_id,
        account_id=acc_id,
        description="Groceries",
        use_type="Personal",
    )
    conn.commit()

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(conn, year="2025")

    assert "Business Financials" in result["skipped_tabs"]
    ss = client.spreadsheets[result["spreadsheet_id"]]
    ws = ss._worksheets["Business Financials"]
    flat_values = [
        str(item)
        for call in ws.update_calls
        for row in call["values"]
        for item in row
    ]
    assert any("skipped" in item.lower() for item in flat_values)

    captured = capsys.readouterr()
    assert "No schedule_c_map entries" in captured.err


def test_no_flags_emits_note_level_mixed_timeframe(fake_home, monkeypatch, capsys, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(conn)

    err = capsys.readouterr().err
    assert "Note:" in err


def test_explicit_mismatch_emits_warning_level(fake_home, monkeypatch, capsys, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(
        conn,
        year="2025",
        date_from="2025-01-01",
        date_to="2025-03-01",
    )

    err = capsys.readouterr().err
    assert "Warning:" in err
    assert "Transactions/Monthly Spending cover" in err


def test_skipped_business_tab_suppresses_mixed_timeframe_notice(fake_home, monkeypatch, capsys, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    sheets_export = _reload_sheets_export()
    sheets_export.export_to_sheets(conn)

    err = capsys.readouterr().err
    assert "Transactions/Monthly Spending cover" not in err


def test_formula_injection_sanitized_all_tabs_and_raw_mode(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    income_cat = _seed_category(conn, "=Income", is_income=1)
    expense_cat = _seed_category(conn, "+Ads", is_income=0)
    _seed_pl_map(conn, income_cat, "revenue", 10)
    _seed_pl_map(conn, expense_cat, "opex_marketing", 20)
    _seed_schedule_map(conn, expense_cat, year=2025, line_number="8", line_label="@Advertising")

    account_id = _seed_account(conn, institution="@Bank", name="-Card", account_type="credit_card", balance_cents=-5000)
    _seed_txn(
        conn,
        amount_cents=100_000,
        txn_date="2025-01-01",
        category_id=income_cat,
        account_id=account_id,
        description="=Income txn",
        use_type="Business",
    )
    _seed_txn(
        conn,
        amount_cents=-10_000,
        txn_date="2025-01-02",
        category_id=expense_cat,
        account_id=account_id,
        description="+Expense txn",
        use_type="Business",
    )
    conn.commit()

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(conn, year="2025")

    ss = client.spreadsheets[result["spreadsheet_id"]]

    for ws in ss._worksheets.values():
        for call in ws.update_calls:
            assert call["value_input_option"] == "RAW"
            for row in call["values"]:
                for item in row:
                    if isinstance(item, str) and item:
                        if item[0] in ("=", "+", "-", "@"):
                            pytest.fail(f"Unsafe cell value found: {item}")


def test_partial_failure_reports_successful_and_failed_tabs(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()

    # First call creates spreadsheet so we can attach tab-level failure behavior.
    first = sheets_export.export_to_sheets(conn, year="2025")
    ss = client.spreadsheets[first["spreadsheet_id"]]
    ss.fail_update_by_title["Business Financials"] = RuntimeError("boom")

    second = sheets_export.export_to_sheets(conn, year="2025")

    assert second["tabs"] == ["Transactions"]
    assert second["failed_tabs"][0]["tab"] == "Business Financials"


def test_mutual_exclusive_new_and_spreadsheet_id_in_handler(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    _install_google_stubs(monkeypatch, client=FakeClient(), state=state)

    sheets_export = _reload_sheets_export()
    with pytest.raises(ValueError, match="cannot be used together"):
        sheets_export.export_to_sheets(conn, year="2025", force_new=True, spreadsheet_id="abc")


def test_namespace_types_for_business_handlers(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    _seed_business_data(conn, tax_year=2025)
    conn.commit()

    sheets_export = _reload_sheets_export()
    captured_args: dict[str, argparse.Namespace] = {}

    original_pl = sheets_export.biz_cmd.handle_pl
    original_tax = sheets_export.biz_cmd.handle_tax

    def fake_pl(ns, c):
        captured_args["pl"] = ns
        return original_pl(ns, c)

    def fake_tax(ns, c):
        captured_args["tax"] = ns
        return original_tax(ns, c)

    monkeypatch.setattr(sheets_export.biz_cmd, "handle_pl", fake_pl)
    monkeypatch.setattr(sheets_export.biz_cmd, "handle_tax", fake_tax)

    sheets_export.export_to_sheets(conn, year="2025")

    pl_ns = captured_args["pl"]
    tax_ns = captured_args["tax"]

    assert isinstance(pl_ns.year, str)
    assert pl_ns.year == "2025"
    assert pl_ns.compare is False
    assert pl_ns.format == "json"
    assert tax_ns.detail is None
    assert tax_ns.salary is None


def test_transactions_amounts_are_converted_to_dollars(fake_home, monkeypatch, conn):
    _write_credentials_file(fake_home, monkeypatch)
    state = {
        "flow_creds": FakeCredentials(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
    }
    client = FakeClient()
    _install_google_stubs(monkeypatch, client=client, state=state)

    cat_id = _seed_category(conn, "Food")
    acc_id = _seed_account(conn)
    _seed_business_data(conn, tax_year=2025)
    _seed_txn(
        conn,
        amount_cents=-12345,
        txn_date="2025-02-02",
        category_id=cat_id,
        account_id=acc_id,
        description="Lunch",
        use_type="Personal",
    )
    conn.commit()

    sheets_export = _reload_sheets_export()
    result = sheets_export.export_to_sheets(conn, year="2025")

    ss = client.spreadsheets[result["spreadsheet_id"]]
    ws = ss._worksheets["Transactions"]
    rows = [row for call in ws.update_calls for row in call["values"]]
    amount_cells = [row[2] for row in rows if row and row[0] == "2025-02-02"]
    assert amount_cells
    assert amount_cells[0] == -123.45


def test_net_worth_tab_excludes_hash_alias_accounts(conn):
    sheets_export = _reload_sheets_export()
    canonical_id = _seed_account(
        conn,
        institution="Test Bank",
        name="Canonical Checking",
        account_type="checking",
        balance_cents=100_000,
    )
    hash_id = _seed_account(
        conn,
        institution="Test Bank",
        name="Hash Checking",
        account_type="checking",
        balance_cents=25_000,
    )
    conn.commit()

    no_alias = sheets_export._build_net_worth_tab(conn)
    no_alias_account_rows = [row for row in no_alias.rows if len(row) >= 3 and row[2] == "checking"]
    assert len(no_alias_account_rows) == 2
    assert no_alias.rows[-3][0] == "Total Assets"
    assert no_alias.rows[-3][3] == 1250.0

    conn.execute(
        "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
        (hash_id, canonical_id),
    )
    conn.commit()

    with_alias = sheets_export._build_net_worth_tab(conn)
    with_alias_account_rows = [row for row in with_alias.rows if len(row) >= 3 and row[2] == "checking"]
    assert len(with_alias_account_rows) == 1
    assert with_alias_account_rows[0][1] == "Canonical Checking"
    assert with_alias.rows[-3][0] == "Total Assets"
    assert with_alias.rows[-3][3] == 1000.0


# ---------------------------------------------------------------------------
# CLI handler, MCP wrapper, migration
# ---------------------------------------------------------------------------


def test_cli_handler_returns_envelope_structure(monkeypatch):
    from finance_cli.commands import export as export_cmd

    fake_payload = {
        "spreadsheet_id": "sheet_1",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/sheet_1",
        "tabs": ["Transactions", "Business Financials", "Monthly Spending", "Net Worth"],
        "row_counts": {},
        "skipped_tabs": [],
        "truncated_tabs": {},
        "warnings": [],
    }

    def fake_export(conn, **kwargs):  # noqa: ARG001
        assert kwargs["interactive"] is True
        return fake_payload

    monkeypatch.setattr("finance_cli.sheets_export.export_to_sheets", fake_export)

    args = argparse.Namespace(
        date_from="2025-01-01",
        date_to="2025-01-31",
        year="2025",
        spreadsheet_id=None,
        new=False,
        auth=False,
    )

    result = export_cmd.handle_sheets(args, conn=None)

    assert "data" in result
    assert "summary" in result
    assert "cli_report" in result
    assert result["summary"]["total_tabs"] == 4


def test_mcp_tool_wrapper_sets_interactive_false(monkeypatch):
    import finance_cli.mcp_server as mcp_server

    captured: dict[str, object] = {}

    def fake_call(handler, ns_kwargs):
        captured["handler"] = handler
        captured["ns_kwargs"] = ns_kwargs
        return {"data": {"ok": True}, "summary": {}}

    monkeypatch.setattr(mcp_server, "_call", fake_call)

    result = mcp_server.export_sheets(year="2025", new=True)

    assert result["data"]["ok"] is True
    ns_kwargs = captured["ns_kwargs"]
    assert ns_kwargs["interactive"] is False
    assert ns_kwargs["auth"] is False
    assert ns_kwargs["new"] is True


def test_migration_024_creates_settings_table(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)

    with connect(db_path) as verify_conn:
        row = verify_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='settings'"
        ).fetchone()
        assert row is not None
