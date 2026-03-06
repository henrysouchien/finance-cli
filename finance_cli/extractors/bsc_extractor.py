"""BankStatementConverter extractor adapter.

NOT CURRENTLY IN USE. Kept as a reference implementation for wiring in a new
deterministic / template-based extraction service. The extractor protocol
(StatementExtractor) and factory (get_extractor) are ready — just register a
new backend name.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
import uuid as uuid_lib
from pathlib import Path
from typing import Any

from ..importers.pdf import ExtractResult, extract_pdf_text
from ..institution_names import CANONICAL_NAMES
from ..institution_names import canonicalize as canonicalize_institution_name
from . import ExtractOptions, ExtractorMeta, ExtractorOutput, normalize_date, parse_amount_to_cents

_FILTER_TERMS = (
    "PREVIOUS BALANCE",
    "NEW BALANCE",
    "TOTAL",
    "SUBTOTAL",
)


class BSCExtractor:
    name = "bsc"

    def __init__(self, config: dict[str, Any]) -> None:
        cfg = config if isinstance(config, dict) else {}
        key_env = str(cfg.get("api_key_env") or "BSC_API_KEY")
        api_key = os.getenv(key_env)
        if not api_key:
            raise ValueError(f"Missing BSC API key env var: {key_env}")

        self.api_key = api_key
        self.base_url = str(cfg.get("base_url") or "https://api2.bankstatementconverter.com/api/v1").rstrip("/")
        self.poll_interval_seconds = max(float(cfg.get("poll_interval_seconds") or 2), 0.1)
        self.poll_max_seconds = max(float(cfg.get("poll_max_seconds") or 120), 1.0)

    def extract(self, pdf_path: Path, options: ExtractOptions) -> ExtractorOutput:
        started = time.perf_counter()
        upload_id = self._upload(pdf_path)
        self._poll_until_ready(upload_id)
        raw_json = self._convert(upload_id)

        extracted = self._normalize(raw_json, pdf_path, options)
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        meta = ExtractorMeta(
            backend="bsc",
            bank_parser_label="bsc:api",
            provider="bsc",
            model_version="api-v1",
            reconcile_status="no_totals",
            raw_api_response=json.dumps(raw_json, ensure_ascii=True),
            elapsed_ms=elapsed_ms,
        )
        return ExtractorOutput(result=extracted, meta=meta)

    def _upload(self, pdf_path: Path) -> str:
        file_name = Path(pdf_path).name
        boundary = f"----finance-cli-{uuid_lib.uuid4().hex}"
        payload = [
            f"--{boundary}\r\n".encode("utf-8"),
            f'Content-Disposition: form-data; name="file"; filename="{file_name}"\r\n'.encode("utf-8"),
            b"Content-Type: application/pdf\r\n\r\n",
            Path(pdf_path).read_bytes(),
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
        body = b"".join(payload)

        request = urllib.request.Request(
            f"{self.base_url}/BankStatement",
            data=body,
            method="POST",
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "X-API-Key": self.api_key,
                "Authorization": self.api_key,
            },
        )

        data = self._request_json(request)
        upload_id = self._extract_uuid(data)
        if not upload_id:
            raise ValueError(f"BSC upload response missing UUID: {data!r}")
        return upload_id

    def _poll_until_ready(self, upload_id: str) -> None:
        deadline = time.monotonic() + self.poll_max_seconds
        while True:
            request = urllib.request.Request(
                f"{self.base_url}/BankStatement/status",
                data=json.dumps([upload_id]).encode("utf-8"),
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "X-API-Key": self.api_key,
                    "Authorization": self.api_key,
                },
            )
            payload = self._request_json(request)
            status = self._extract_status(payload)

            if status in {"READY", "DONE", "COMPLETED", "SUCCESS", "SUCCEEDED"}:
                return
            if status in {"FAILED", "ERROR"}:
                raise ValueError(f"BSC conversion failed for {upload_id}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for BSC conversion for {upload_id}")

            time.sleep(self.poll_interval_seconds)

    def _convert(self, upload_id: str) -> Any:
        request = urllib.request.Request(
            f"{self.base_url}/BankStatement/convert?format=JSON",
            data=json.dumps([upload_id]).encode("utf-8"),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-API-Key": self.api_key,
                "Authorization": self.api_key,
            },
        )
        return self._request_json(request)

    def _request_json(self, request: urllib.request.Request) -> Any:
        try:
            with urllib.request.urlopen(request, timeout=60) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise ValueError(f"BSC API error {exc.code}: {detail or exc.reason}") from exc

        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"BSC API returned invalid JSON: {body[:200]!r}") from exc

    def _extract_uuid(self, payload: Any) -> str | None:
        if isinstance(payload, str):
            return payload.strip() or None
        if isinstance(payload, dict):
            for key in ("uuid", "id", "result", "data"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, str):
                return first.strip() or None
            if isinstance(first, dict):
                for key in ("uuid", "id"):
                    value = first.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
        return None

    def _extract_status(self, payload: Any) -> str:
        if isinstance(payload, dict):
            for key in ("status", "state"):
                value = payload.get(key)
                if isinstance(value, str):
                    return value.strip().upper()
            for value in payload.values():
                if isinstance(value, dict):
                    nested = self._extract_status(value)
                    if nested:
                        return nested
                if isinstance(value, list):
                    nested = self._extract_status(value)
                    if nested:
                        return nested
            return ""

        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, str):
                return first.strip().upper()
            if isinstance(first, dict):
                return self._extract_status(first)

        return ""

    def _normalize(self, raw_json: Any, pdf_path: Path, options: ExtractOptions) -> ExtractResult:
        warnings: list[str] = []

        records = raw_json if isinstance(raw_json, list) else [raw_json]
        if not records or not isinstance(records[0], dict):
            raise ValueError("Unexpected BSC convert response shape")

        root = records[0]
        if "normalised" in root:
            rows = root.get("normalised")
        elif "transactions" in root:
            rows = root.get("transactions")
        elif "grids" in root:
            rows = _flatten_grids(root["grids"])
        else:
            keys = ", ".join(sorted(str(key) for key in root.keys()))
            raise ValueError(f"BSC response missing transactions payload. Keys: {keys}")

        if not isinstance(rows, list):
            raise ValueError("BSC transaction payload is not a list")

        institution = _resolve_bsc_institution(pdf_path, options)
        card_ending = str(options.card_ending_hint or "").strip() or None
        if not card_ending:
            warnings.append(
                "No card ending available. Account identity may merge with other accounts for this institution. "
                "Use --card-ending to specify."
            )

        transactions: list[dict[str, object]] = []
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue

            description = str(
                row.get("description")
                or row.get("desc")
                or row.get("memo")
                or row.get("narrative")
                or ""
            ).strip()
            if _is_summary_row(description):
                continue

            date_raw = str(row.get("date") or row.get("transaction_date") or "").strip()
            if not date_raw:
                warnings.append(f"row {index}: missing date; skipped")
                continue

            try:
                date_iso = normalize_date(date_raw)
            except ValueError:
                warnings.append(f"row {index}: invalid date '{date_raw}'; skipped")
                continue

            amount_raw = row.get("amount")
            try:
                amount_cents = parse_amount_to_cents(amount_raw)
            except ValueError:
                warnings.append(f"row {index}: invalid amount '{amount_raw}'; skipped")
                continue

            transactions.append(
                {
                    "date": date_iso,
                    "description": description,
                    "amount_cents": amount_cents,
                    "card_ending": card_ending,
                    "source": institution,
                }
            )

        extracted_total_cents = sum(int(txn["amount_cents"]) for txn in transactions)
        return ExtractResult(
            transactions=transactions,
            statement_total_cents=None,
            extracted_total_cents=extracted_total_cents,
            reconciled=False,
            warnings=warnings,
            statement_card_ending=card_ending,
        )


def _resolve_bsc_institution(pdf_path: Path, options: ExtractOptions) -> str:
    hint = str(options.institution_hint or "").strip()
    if hint:
        return canonicalize_institution_name(hint)

    text = ""
    try:
        text = extract_pdf_text(pdf_path).lower()
    except Exception:
        text = ""

    for alias, canonical in CANONICAL_NAMES.items():
        if alias and alias in text:
            return canonical

    raise ValueError("Could not determine institution. Use --institution flag.")


def _flatten_grids(grids: Any) -> list[dict[str, str]]:
    """Convert BSC raw 'grids' format into normalised-style row dicts.

    Grids is typically a list of tables. Each table is either:
      - A list of lists (first row = headers, rest = data rows)
      - A dict with 'headers' and 'rows' keys
    We map columns to the normalised keys (date, description, amount).
    """
    if not isinstance(grids, list):
        return []

    _DATE_NAMES = {"date", "trans date", "transaction date", "posting date", "post date"}
    _DESC_NAMES = {"description", "desc", "memo", "narrative", "details", "transaction description"}
    _AMT_NAMES = {"amount", "amt", "debit", "credit", "charges", "payments"}

    rows: list[dict[str, str]] = []
    for table in grids:
        headers: list[str] = []
        data_rows: list[list[Any]] = []

        if isinstance(table, dict):
            headers = [str(h).strip().lower() for h in (table.get("headers") or [])]
            data_rows = table.get("rows") or table.get("data") or []
        elif isinstance(table, list) and table:
            if isinstance(table[0], list):
                headers = [str(h).strip().lower() for h in table[0]]
                data_rows = table[1:]
            elif isinstance(table[0], dict):
                # Already normalised dicts inside a grid
                rows.extend(table)
                continue

        if not headers or not data_rows:
            continue

        # Map header indices to normalised field names
        date_idx = next((i for i, h in enumerate(headers) if h in _DATE_NAMES), None)
        desc_idx = next((i for i, h in enumerate(headers) if h in _DESC_NAMES), None)
        amt_idx = next((i for i, h in enumerate(headers) if h in _AMT_NAMES), None)

        if date_idx is None or amt_idx is None:
            continue

        for data_row in data_rows:
            if not isinstance(data_row, list) or len(data_row) <= max(date_idx, amt_idx):
                continue
            row_dict: dict[str, str] = {
                "date": str(data_row[date_idx] or "").strip(),
                "amount": str(data_row[amt_idx] or "").strip(),
            }
            if desc_idx is not None and desc_idx < len(data_row):
                row_dict["description"] = str(data_row[desc_idx] or "").strip()
            else:
                row_dict["description"] = ""
            rows.append(row_dict)

    return rows


def _is_summary_row(description: str) -> bool:
    upper = description.upper()
    return any(token in upper for token in _FILTER_TERMS)


__all__ = ["BSCExtractor"]
