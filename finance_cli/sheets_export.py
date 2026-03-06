"""Google Sheets export helpers for finance_cli."""

from __future__ import annotations

import argparse
import contextlib
import math
import os
import random
import re
import stat
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable

from .commands import biz_cmd
from .models import cents_to_dollars

_REQUIRED_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
)

_SETTINGS_SPREADSHEET_KEY = "google_sheets_spreadsheet_id"
_DEFAULT_SPREADSHEET_TITLE = "Finance CLI Export"
_YEAR_RE = re.compile(r"^\d{4}$")
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503}
_MAX_RETRIES = 3
_RETRY_BASE_SECONDS = 1.0
_CELL_BUDGET = 5_000_000


@dataclass
class _ResolvedWindows:
    txn_from: str | None
    txn_to: str | None
    business_year: str | None
    explicit_flags_used: bool
    is_default_case: bool


@dataclass
class _TabPayload:
    title: str
    header: list[str] | None
    rows: list[list[Any]]
    skipped: bool = False
    truncated_rows: int = 0

    def col_count(self) -> int:
        if self.header:
            return len(self.header)
        if self.rows:
            return max(len(row) for row in self.rows)
        return 1

    def values_for_sheet(self) -> list[list[Any]]:
        values: list[list[Any]] = []
        if self.header:
            values.append(list(self.header))
        values.extend(list(row) for row in self.rows)
        if self.truncated_rows > 0:
            tail = [f"... {self.truncated_rows} rows truncated"]
            tail.extend([""] * (self.col_count() - 1))
            values.append(tail)
        return values

    def projected_cells(self) -> int:
        values = self.values_for_sheet()
        return len(values) * self.col_count()


def _emit_warning(warnings: list[str], message: str) -> None:
    warnings.append(message)
    print(f"Warning: {message}", file=sys.stderr)


def _emit_note(message: str) -> None:
    print(f"Note: {message}", file=sys.stderr)


def _sanitize_cell(value: Any) -> Any:
    """Prefix dangerous strings so Sheets never treats them as formulas."""
    if value is None:
        return ""
    if isinstance(value, str):
        if value.startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
            return "'" + value
        return value
    return value


def _pad_and_sanitize_rows(values: list[list[Any]], col_count: int) -> list[list[Any]]:
    out: list[list[Any]] = []
    for row in values:
        current = list(row)
        if len(current) < col_count:
            current.extend([""] * (col_count - len(current)))
        elif len(current) > col_count:
            current = current[:col_count]
        out.append([_sanitize_cell(value) for value in current])
    return out


def _column_name(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be >= 1")
    out = ""
    probe = index
    while probe:
        probe, rem = divmod(probe - 1, 26)
        out = chr(65 + rem) + out
    return out


def _api_status_code(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    for attr in ("status_code", "status"):
        value = getattr(response, attr, None)
        if value is None:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return None


def _retry_after_seconds(exc: Exception) -> float | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    headers = getattr(response, "headers", None)
    if not headers:
        return None

    value = None
    if isinstance(headers, dict):
        for key, item in headers.items():
            if str(key).lower() == "retry-after":
                value = item
                break
    else:
        value = getattr(headers, "get", lambda _k, _d=None: None)("Retry-After", None)

    if value is None:
        return None
    try:
        parsed = float(str(value).strip())
    except Exception:
        return None
    return max(0.0, parsed)


def _is_timeout_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    return "timeout" in str(exc).lower()


def _call_with_retry(fn: Callable[[], Any], *, idempotent: bool) -> Any:
    if not idempotent:
        return fn()

    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            status_code = _api_status_code(exc)
            retryable = status_code in _RETRYABLE_STATUS_CODES or _is_timeout_error(exc)
            if not retryable or attempt >= _MAX_RETRIES:
                raise

            delay = _retry_after_seconds(exc)
            if delay is None:
                backoff = _RETRY_BASE_SECONDS * (2 ** attempt)
                jitter = random.uniform(0.0, 0.25)
                delay = backoff + jitter

            time.sleep(delay)
            attempt += 1


def _import_google_modules():
    try:
        import gspread
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except Exception as exc:
        raise ImportError(
            "Google Sheets export requires optional dependencies. Install with: "
            "pip install gspread google-auth-oauthlib"
        ) from exc
    return gspread, Request, Credentials, InstalledAppFlow


def _config_dir() -> Path:
    return Path("~/.config/finance_cli").expanduser()


def _token_path() -> Path:
    return (_config_dir() / "google_token.json").expanduser().resolve()


def _credentials_path() -> Path:
    raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or str(_config_dir() / "google_credentials.json")
    expanded = Path(raw).expanduser()
    if not expanded.is_absolute():
        raise ValueError(
            "Google credentials path must be absolute. "
            "Use ~/... or set GOOGLE_SHEETS_CREDENTIALS to an absolute path."
        )
    return expanded.resolve()


def _ensure_config_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except OSError:
        # Best effort only; failure here should not block auth.
        pass


def _permission_bits_too_broad(path: Path) -> bool:
    mode = stat.S_IMODE(path.stat().st_mode)
    return bool(mode & (stat.S_IRWXG | stat.S_IRWXO))


def _warn_if_credentials_permissions_wide(path: Path, warnings: list[str]) -> None:
    try:
        if _permission_bits_too_broad(path):
            _emit_warning(
                warnings,
                f"Credentials file permissions are broad for {path}; consider restricting access.",
            )
    except OSError:
        return


def _tighten_token_permissions_if_needed(path: Path, warnings: list[str]) -> None:
    try:
        file_stat = path.stat()
    except OSError:
        return

    mode = stat.S_IMODE(file_stat.st_mode)
    if not (mode & (stat.S_IRWXG | stat.S_IRWXO)):
        return

    owner_uid = getattr(file_stat, "st_uid", None)
    getuid = getattr(os, "getuid", None)
    current_uid = getuid() if callable(getuid) else None

    if owner_uid is not None and current_uid is not None and owner_uid == current_uid:
        try:
            os.chmod(path, 0o600)
            _emit_warning(warnings, f"Token file permissions tightened to 0600: {path}")
        except OSError:
            _emit_warning(
                warnings,
                f"Token file permissions are broad for {path} and could not be tightened automatically.",
            )
        return

    _emit_warning(
        warnings,
        f"Token file permissions are broad for {path}; file is not owned by current user so permissions were not modified.",
    )


def _validate_credentials_file(path: Path, warnings: list[str]) -> None:
    if not path.exists():
        raise ValueError(
            "Google credentials file not found. "
            f"Expected {path}. Place OAuth client JSON there or set GOOGLE_SHEETS_CREDENTIALS."
        )
    if not path.is_file():
        raise ValueError(f"Google credentials path is not a file: {path}")
    if not os.access(path, os.R_OK):
        raise ValueError(f"Google credentials file is not readable: {path}")
    _warn_if_credentials_permissions_wide(path, warnings)


def _has_required_scopes(creds: Any) -> bool:
    scopes = set(str(scope) for scope in (getattr(creds, "scopes", None) or []))
    return set(_REQUIRED_SCOPES).issubset(scopes)


def _is_invalid_grant_error(exc: Exception) -> bool:
    return "invalid_grant" in str(exc).lower()


def _save_token_json(path: Path, creds: Any, warnings: list[str]) -> None:
    _ensure_config_dir(path.parent)
    payload = getattr(creds, "to_json", None)
    if callable(payload):
        raw_json = creds.to_json()
    else:
        raw_json = "{}"
    path.write_text(raw_json, encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        _emit_warning(warnings, f"Could not enforce 0600 permissions on token file: {path}")


def _run_oauth_flow(installed_app_flow: Any, credentials_path: Path) -> Any:
    flow = installed_app_flow.from_client_secrets_file(str(credentials_path), scopes=list(_REQUIRED_SCOPES))
    print("Starting Google OAuth flow (local callback on an ephemeral localhost port).", file=sys.stderr)
    with contextlib.redirect_stdout(sys.stderr):
        return flow.run_local_server(open_browser=False, port=0)


def _get_gspread_client(*, interactive: bool, warnings: list[str]) -> Any:
    gspread, Request, Credentials, InstalledAppFlow = _import_google_modules()

    credentials_path = _credentials_path()
    _validate_credentials_file(credentials_path, warnings)

    token_path = _token_path()
    _ensure_config_dir(token_path.parent)

    creds = None
    token_changed = False

    if token_path.exists():
        _tighten_token_permissions_if_needed(token_path, warnings)
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), scopes=list(_REQUIRED_SCOPES))
        except Exception:
            if not interactive:
                raise ValueError("Google token is invalid. Run `export sheets --auth` first.")
            _emit_warning(warnings, "Cached Google token is invalid; running OAuth consent again.")
            creds = None

    if creds is not None and creds.expired and getattr(creds, "refresh_token", None):
        try:
            creds.refresh(Request())
            token_changed = True
        except Exception as exc:
            if _is_invalid_grant_error(exc):
                with contextlib.suppress(OSError):
                    token_path.unlink(missing_ok=True)
                if not interactive:
                    raise ValueError("Google token has been revoked. Run `export sheets --auth` first.")
                _emit_warning(warnings, "Cached Google token was revoked; running OAuth consent again.")
                creds = None
            else:
                raise

    if creds is not None and not _has_required_scopes(creds):
        if not interactive:
            raise ValueError("Google token is missing required scopes. Run `export sheets --auth` first.")
        _emit_warning(warnings, "Cached token scopes are outdated; running OAuth consent again.")
        creds = None

    if creds is None or not getattr(creds, "valid", False):
        if not interactive:
            raise ValueError("Google auth is not initialized. Run `export sheets --auth` first.")
        creds = _run_oauth_flow(InstalledAppFlow, credentials_path)
        token_changed = True

    if token_changed:
        _save_token_json(token_path, creds, warnings)

    return gspread.authorize(creds)


def _ensure_settings_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )


def _setting_get(conn, key: str) -> str | None:
    _ensure_settings_table(conn)
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if row and row["value"] is not None:
        return str(row["value"])
    return None


def _setting_upsert(conn, key: str, value: str) -> None:
    _ensure_settings_table(conn)
    conn.execute(
        """
        INSERT INTO settings (key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE
            SET value = excluded.value,
                updated_at = datetime('now')
        """,
        (key, value),
    )


def _spreadsheet_id_from_object(spreadsheet: Any) -> str:
    for attr in ("id", "spreadsheet_id"):
        probe = getattr(spreadsheet, attr, None)
        if probe:
            return str(probe)
    metadata = getattr(spreadsheet, "_properties", None)
    if isinstance(metadata, dict) and metadata.get("id"):
        return str(metadata["id"])
    raise ValueError("Could not determine spreadsheet id from API response")


def _get_or_create_spreadsheet(
    client: Any,
    conn,
    *,
    spreadsheet_id: str | None,
    force_new: bool,
) -> Any:
    if force_new and spreadsheet_id:
        raise ValueError("--new and --spreadsheet-id cannot be used together")

    target_id = (spreadsheet_id or "").strip() or None
    if target_id is None and not force_new:
        target_id = _setting_get(conn, _SETTINGS_SPREADSHEET_KEY)

    if force_new or not target_id:
        spreadsheet = client.create(_DEFAULT_SPREADSHEET_TITLE)
        _setting_upsert(conn, _SETTINGS_SPREADSHEET_KEY, _spreadsheet_id_from_object(spreadsheet))
        return spreadsheet

    try:
        spreadsheet = _call_with_retry(lambda: client.open_by_key(target_id), idempotent=True)
    except Exception as exc:
        code = _api_status_code(exc)
        if code == 404:
            raise ValueError(f"Spreadsheet {target_id} was not found. Use --new to create a replacement.")
        if code == 400:
            raise ValueError(f"Spreadsheet id {target_id} is invalid. Use --new to create a replacement.")
        if code == 403:
            raise ValueError(
                f"Permission denied for spreadsheet {target_id}. "
                "Check sharing/ownership or run `export sheets --auth` again."
            )
        raise

    _setting_upsert(conn, _SETTINGS_SPREADSHEET_KEY, target_id)
    return spreadsheet


def _parse_iso_date(value: str, flag_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{flag_name} must be in YYYY-MM-DD format") from exc


def _resolve_windows(conn, *, date_from: str | None, date_to: str | None, year: str | None) -> _ResolvedWindows:
    has_from = bool(date_from)
    has_to = bool(date_to)
    has_year = bool(year)

    if has_year and not _YEAR_RE.match(str(year)):
        raise ValueError("--year must be in YYYY format")

    if has_from != has_to:
        raise ValueError("--from and --to must be provided together")

    if has_from and has_to:
        parsed_from = _parse_iso_date(str(date_from), "--from")
        parsed_to = _parse_iso_date(str(date_to), "--to")
        if parsed_from > parsed_to:
            raise ValueError("--from cannot be after --to")

        if has_year:
            business_year = str(year)
        else:
            if parsed_from.year != parsed_to.year:
                raise ValueError("Cross-year --from/--to requires --year")
            business_year = str(parsed_from.year)

        return _ResolvedWindows(
            txn_from=parsed_from.isoformat(),
            txn_to=parsed_to.isoformat(),
            business_year=business_year,
            explicit_flags_used=True,
            is_default_case=False,
        )

    if has_year:
        year_int = int(str(year))
        return _ResolvedWindows(
            txn_from=f"{year_int:04d}-01-01",
            txn_to=f"{year_int:04d}-12-31",
            business_year=f"{year_int:04d}",
            explicit_flags_used=True,
            is_default_case=False,
        )

    latest_year = biz_cmd._latest_tax_year(conn)
    return _ResolvedWindows(
        txn_from=None,
        txn_to=None,
        business_year=(str(latest_year) if latest_year is not None else None),
        explicit_flags_used=False,
        is_default_case=True,
    )


def _build_transactions_tab(conn, *, date_from: str | None, date_to: str | None) -> _TabPayload:
    where = ["t.is_active = 1"]
    params: list[str] = []

    if date_from:
        where.append("t.date >= ?")
        params.append(date_from)
    if date_to:
        where.append("t.date <= ?")
        params.append(date_to)

    rows = conn.execute(
        f"""
        SELECT t.date,
               t.description,
               t.amount_cents,
               COALESCE(c.name, 'Uncategorized') AS category_name,
               a.institution_name,
               a.account_name,
               a.card_ending,
               t.use_type,
               t.source,
               t.is_reviewed
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN accounts a ON a.id = t.account_id
         WHERE {' AND '.join(where)}
         ORDER BY t.date ASC, t.created_at ASC
        """,
        tuple(params),
    ).fetchall()

    data_rows: list[list[Any]] = []
    for row in rows:
        institution = str(row["institution_name"] or "")
        account_name = str(row["account_name"] or "")
        card_ending = str(row["card_ending"] or "").strip()

        account_label = account_name
        if institution and account_name:
            account_label = f"{institution} - {account_name}"
        elif institution:
            account_label = institution
        if card_ending:
            account_label = f"{account_label} ****{card_ending}".strip()

        data_rows.append(
            [
                str(row["date"] or ""),
                str(row["description"] or ""),
                cents_to_dollars(int(row["amount_cents"] or 0)),
                str(row["category_name"] or "Uncategorized"),
                account_label,
                str(row["use_type"] or ""),
                str(row["source"] or ""),
                "Yes" if int(row["is_reviewed"] or 0) else "No",
            ]
        )

    return _TabPayload(
        title="Transactions",
        header=["Date", "Description", "Amount", "Category", "Account", "Use Type", "Source", "Reviewed"],
        rows=data_rows,
    )


def _business_namespace(year: str) -> argparse.Namespace:
    return argparse.Namespace(
        month=None,
        quarter=None,
        year=str(year),
        compare=False,
        format="json",
        detail=None,
        salary=None,
    )


def _build_business_financials_tab(conn, *, business_year: str | None, warnings: list[str]) -> _TabPayload:
    total_row = conn.execute("SELECT COUNT(*) AS cnt FROM schedule_c_map").fetchone()
    total_mappings = int(total_row["cnt"] or 0)

    if business_year is None:
        marker = "Business Financials skipped - no schedule_c_map data for this year"
        if total_mappings == 0:
            _emit_warning(warnings, "No schedule_c_map entries found - run `biz tax-setup` to configure")
        else:
            _emit_warning(warnings, "No schedule_c_map entries found for the resolved year")
        return _TabPayload(title="Business Financials", header=None, rows=[[marker]], skipped=True)

    year_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM schedule_c_map WHERE tax_year = ?",
        (int(business_year),),
    ).fetchone()
    year_mappings = int(year_row["cnt"] or 0)

    if total_mappings == 0:
        _emit_warning(warnings, "No schedule_c_map entries found - run `biz tax-setup` to configure")
        marker = "Business Financials skipped - no schedule_c_map data for this year"
        return _TabPayload(title="Business Financials", header=None, rows=[[marker]], skipped=True)

    if year_mappings == 0:
        _emit_warning(warnings, f"No schedule_c_map entries for year {business_year}")
        marker = "Business Financials skipped - no schedule_c_map data for this year"
        return _TabPayload(title="Business Financials", header=None, rows=[[marker]], skipped=True)

    pl_result = biz_cmd.handle_pl(_business_namespace(business_year), conn)
    tax_result = biz_cmd.handle_tax(_business_namespace(business_year), conn)

    pl_data = pl_result.get("data", {})
    tax_data = tax_result.get("data", {})

    rows: list[list[Any]] = []

    rows.append(["P&L", "", "", ""])
    rows.append(["Section", "Category", "Amount", "Txn Count"])
    sections = pl_data.get("sections", {})
    for section_name, section_rows in sections.items():
        for entry in section_rows:
            rows.append(
                [
                    str(section_name),
                    str(entry.get("category_name") or ""),
                    cents_to_dollars(int(entry.get("total_cents") or 0)),
                    int(entry.get("txn_count") or 0),
                ]
            )
    rows.append(["Net Income", "", cents_to_dollars(int(pl_data.get("net_income_cents") or 0)), ""])

    rows.append(["", "", "", ""])
    rows.append(["Schedule C", "", "", ""])
    rows.append(["Line #", "Description", "Actual", "Deductible"])
    for item in tax_data.get("line_items", []):
        rows.append(
            [
                str(item.get("line_number") or ""),
                str(item.get("line_label") or ""),
                cents_to_dollars(int(item.get("actual_cents") or 0)),
                cents_to_dollars(int(item.get("deductible_cents") or 0)),
            ]
        )

    tax_summary = tax_data.get("tax_summary", {})
    rows.append(["", "", "", ""])
    rows.append(["Tax Summary", "", "", ""])
    rows.append(["Key", "Value", "", ""])
    rows.extend(
        [
            ["Tax Year", str(tax_data.get("tax_year") or business_year), "", ""],
            ["Net Profit", cents_to_dollars(int(tax_data.get("line_31_net_profit_cents") or 0)), "", ""],
            [
                "Total Estimated Tax",
                cents_to_dollars(int(tax_summary.get("total_estimated_tax_cents") or 0)),
                "",
                "",
            ],
            [
                "Quarterly Payment",
                cents_to_dollars(int(tax_summary.get("quarterly_payment_cents") or 0)),
                "",
                "",
            ],
        ]
    )

    return _TabPayload(title="Business Financials", header=None, rows=rows)


def _build_monthly_spending_tab(conn, *, date_from: str | None, date_to: str | None) -> _TabPayload:
    where = [
        "t.is_active = 1",
        "t.is_payment = 0",
        "t.amount_cents < 0",
    ]
    params: list[str] = []

    if date_from:
        where.append("t.date >= ?")
        params.append(date_from)
    if date_to:
        where.append("t.date <= ?")
        params.append(date_to)

    rows = conn.execute(
        f"""
        SELECT COALESCE(c.name, 'Uncategorized') AS category_name,
               COALESCE(t.use_type, 'Unspecified') AS use_type,
               strftime('%Y-%m', t.date) AS month,
               ABS(SUM(t.amount_cents)) AS total_cents
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE {' AND '.join(where)}
         GROUP BY COALESCE(c.name, 'Uncategorized'), COALESCE(t.use_type, 'Unspecified'), strftime('%Y-%m', t.date)
         ORDER BY COALESCE(c.name, 'Uncategorized') ASC,
                  COALESCE(t.use_type, 'Unspecified') ASC,
                  strftime('%Y-%m', t.date) ASC
        """,
        tuple(params),
    ).fetchall()

    months = sorted({str(row["month"]) for row in rows if row["month"]})
    matrix: dict[tuple[str, str], dict[str, int]] = {}
    for row in rows:
        key = (str(row["category_name"]), str(row["use_type"]))
        bucket = matrix.setdefault(key, {})
        bucket[str(row["month"])] = int(row["total_cents"] or 0)

    data_rows: list[list[Any]] = []
    for (category_name, use_type), month_totals in sorted(matrix.items(), key=lambda item: (item[0][0], item[0][1])):
        row_values: list[Any] = [category_name, use_type]
        for month in months:
            row_values.append(cents_to_dollars(int(month_totals.get(month, 0))))
        data_rows.append(row_values)

    return _TabPayload(
        title="Monthly Spending",
        header=["Category", "Use Type", *months],
        rows=data_rows,
    )


def _is_liability_account_type(account_type: str | None) -> bool:
    return (account_type or "") in {"credit_card", "loan"}


def _build_net_worth_tab(conn) -> _TabPayload:
    rows = conn.execute(
        """
        SELECT a.id,
               a.institution_name,
               a.account_name,
               a.account_type,
               a.balance_current_cents,
               a.balance_available_cents,
               a.balance_limit_cents
         FROM accounts a
         WHERE a.is_active = 1
           AND a.balance_current_cents IS NOT NULL
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
         ORDER BY a.institution_name ASC, a.account_name ASC
        """
    ).fetchall()

    assets_cents = 0
    liabilities_cents = 0
    data_rows: list[list[Any]] = []

    for row in rows:
        current_cents = int(row["balance_current_cents"] or 0)
        available_cents = row["balance_available_cents"]
        limit_cents = row["balance_limit_cents"]

        if _is_liability_account_type(str(row["account_type"] or "")):
            liabilities_cents += abs(current_cents)
        else:
            assets_cents += current_cents

        data_rows.append(
            [
                str(row["institution_name"] or ""),
                str(row["account_name"] or ""),
                str(row["account_type"] or ""),
                cents_to_dollars(current_cents),
                cents_to_dollars(int(available_cents)) if available_cents is not None else "",
                cents_to_dollars(int(limit_cents)) if limit_cents is not None else "",
            ]
        )

    net_worth_cents = assets_cents - liabilities_cents
    data_rows.append(["", "", "", "", "", ""])
    data_rows.append(["Total Assets", "", "", cents_to_dollars(assets_cents), "", ""])
    data_rows.append(["Total Liabilities", "", "", cents_to_dollars(liabilities_cents), "", ""])
    data_rows.append(["Net Worth", "", "", cents_to_dollars(net_worth_cents), "", ""])

    return _TabPayload(
        title="Net Worth",
        header=["Institution", "Account", "Type", "Balance", "Available", "Limit"],
        rows=data_rows,
    )


def _apply_cell_budget_guard(tabs: list[_TabPayload], warnings: list[str]) -> dict[str, int]:
    total_cells = sum(tab.projected_cells() for tab in tabs)
    if total_cells <= _CELL_BUDGET:
        return {}

    truncated: dict[str, int] = {}
    by_name = {tab.title: tab for tab in tabs}
    for tab_name in ("Transactions", "Monthly Spending"):
        if total_cells <= _CELL_BUDGET:
            break
        tab = by_name.get(tab_name)
        if tab is None or not tab.rows:
            continue

        overflow = total_cells - _CELL_BUDGET
        rows_to_remove = int(math.ceil(overflow / max(1, tab.col_count())))
        rows_to_remove = min(rows_to_remove, len(tab.rows))
        if rows_to_remove <= 0:
            continue

        tab.rows = tab.rows[:-rows_to_remove]
        tab.truncated_rows += rows_to_remove
        truncated[tab.title] = truncated.get(tab.title, 0) + rows_to_remove
        total_cells -= rows_to_remove * max(1, tab.col_count())

        _emit_warning(
            warnings,
            f"{tab.title} truncated by {rows_to_remove} rows to fit workbook cell budget.",
        )

    if total_cells > _CELL_BUDGET:
        raise ValueError("Projected export exceeds workbook cell budget even after truncation")

    return truncated


def _emit_mixed_timeframe_notice(resolved: _ResolvedWindows, *, business_skipped: bool, warnings: list[str]) -> None:
    if business_skipped or not resolved.business_year:
        return

    business_start = f"{resolved.business_year}-01-01"
    business_end = f"{resolved.business_year}-12-31"

    if resolved.txn_from is None or resolved.txn_to is None:
        if resolved.is_default_case:
            _emit_note(
                "Transactions and Monthly Spending cover all time while "
                f"Business Financials covers {business_start} to {business_end}."
            )
        return

    if resolved.txn_from == business_start and resolved.txn_to == business_end:
        return

    _emit_warning(
        warnings,
        "Transactions/Monthly Spending cover "
        f"{resolved.txn_from} to {resolved.txn_to}, "
        f"while Business Financials covers {business_start} to {business_end}.",
    )


def _worksheet_not_found(exc: Exception) -> bool:
    return exc.__class__.__name__ == "WorksheetNotFound"


def _ensure_worksheet(spreadsheet: Any, title: str, *, rows: int, cols: int) -> Any:
    try:
        ws = _call_with_retry(lambda: spreadsheet.worksheet(title), idempotent=True)
    except Exception as exc:
        if not _worksheet_not_found(exc):
            raise
        ws = spreadsheet.add_worksheet(title=title, rows=max(rows, 1), cols=max(cols, 1))

    _call_with_retry(lambda: ws.resize(rows=max(rows, 1), cols=max(cols, 1)), idempotent=True)
    return ws


def _write_tab(spreadsheet: Any, tab: _TabPayload) -> int:
    values = tab.values_for_sheet()
    col_count = tab.col_count()

    worksheet = _ensure_worksheet(
        spreadsheet,
        tab.title,
        rows=max(len(values), 1),
        cols=max(col_count, 1),
    )

    _call_with_retry(lambda: worksheet.clear(), idempotent=True)
    if not values:
        return 0

    values = _pad_and_sanitize_rows(values, col_count)
    chunk_size = 1000
    end_col = _column_name(col_count)

    for idx in range(0, len(values), chunk_size):
        chunk = values[idx: idx + chunk_size]
        start_row = idx + 1
        end_row = idx + len(chunk)
        a1_range = f"A{start_row}:{end_col}{end_row}"
        _call_with_retry(
            lambda c=chunk, rng=a1_range: worksheet.update(rng, c, value_input_option="RAW"),
            idempotent=True,
        )

    return len(values)


def _spreadsheet_url(spreadsheet: Any) -> str:
    url = getattr(spreadsheet, "url", None)
    if url:
        return str(url)
    sid = _spreadsheet_id_from_object(spreadsheet)
    return f"https://docs.google.com/spreadsheets/d/{sid}"


def export_to_sheets(
    conn,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    year: str | None = None,
    spreadsheet_id: str | None = None,
    force_new: bool = False,
    auth_only: bool = False,
    interactive: bool = True,
) -> dict[str, Any]:
    """Export finance data to Google Sheets.

    Returns a structured report with spreadsheet metadata and per-tab stats.
    """
    warnings: list[str] = []

    if force_new and spreadsheet_id:
        raise ValueError("--new and --spreadsheet-id cannot be used together")

    client = _get_gspread_client(interactive=interactive, warnings=warnings)

    if auth_only:
        print("Google Sheets OAuth setup completed.", file=sys.stderr)
        return {
            "spreadsheet_id": None,
            "spreadsheet_url": None,
            "tabs": [],
            "row_counts": {},
            "skipped_tabs": [],
            "truncated_tabs": {},
            "warnings": warnings,
        }

    resolved = _resolve_windows(conn, date_from=date_from, date_to=date_to, year=year)

    spreadsheet = _get_or_create_spreadsheet(
        client,
        conn,
        spreadsheet_id=spreadsheet_id,
        force_new=force_new,
    )

    tabs: list[_TabPayload] = [
        _build_transactions_tab(conn, date_from=resolved.txn_from, date_to=resolved.txn_to),
        _build_business_financials_tab(conn, business_year=resolved.business_year, warnings=warnings),
        _build_monthly_spending_tab(conn, date_from=resolved.txn_from, date_to=resolved.txn_to),
        _build_net_worth_tab(conn),
    ]

    skipped_tabs = [tab.title for tab in tabs if tab.skipped]
    truncated_tabs = _apply_cell_budget_guard(tabs, warnings)

    _emit_mixed_timeframe_notice(
        resolved,
        business_skipped=("Business Financials" in skipped_tabs),
        warnings=warnings,
    )

    successful_tabs: list[str] = []
    failed_tabs: list[dict[str, str]] = []
    row_counts: dict[str, int] = {}

    for tab in tabs:
        try:
            written_rows = _write_tab(spreadsheet, tab)
        except Exception as exc:
            failed_tabs.append({"tab": tab.title, "error": str(exc)})
            _emit_warning(warnings, f"Failed writing tab {tab.title}: {exc}")
            break
        successful_tabs.append(tab.title)
        row_counts[tab.title] = written_rows

    result: dict[str, Any] = {
        "spreadsheet_id": _spreadsheet_id_from_object(spreadsheet),
        "spreadsheet_url": _spreadsheet_url(spreadsheet),
        "tabs": successful_tabs,
        "row_counts": row_counts,
        "skipped_tabs": skipped_tabs,
        "truncated_tabs": truncated_tabs,
        "warnings": warnings,
    }
    if failed_tabs:
        result["failed_tabs"] = failed_tabs
    return result
