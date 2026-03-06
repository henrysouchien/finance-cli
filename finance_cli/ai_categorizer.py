"""LLM-backed transaction categorization with Claude/OpenAI backends."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any

from .categorizer import normalize_description
from .user_rules import CANONICAL_CATEGORIES, load_rules, resolve_category_alias

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AICategorization:
    transaction_id: str
    category_name: str | None
    use_type: str | None
    confidence: float | None
    reasoning: str | None
    error: str | None = None


@dataclass(frozen=True)
class BatchResult:
    results: list[AICategorization]
    provider: str
    model: str
    prompt_hash: str
    input_tokens: int = 0
    output_tokens: int = 0


def _chunked(items: list[sqlite3.Row], size: int) -> list[list[sqlite3.Row]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _default_model(provider: str) -> str:
    if provider == "openai":
        return "gpt-4o-mini"
    if provider == "claude":
        return "claude-sonnet-4-5-20250929"
    raise ValueError(f"Unsupported AI provider '{provider}'")


def _normalize_use_type(value: Any) -> str | None:
    normalized = str(value or "").strip()
    if normalized in {"Business", "Personal"}:
        return normalized
    return None


def _build_prompt(transactions: list[dict[str, str]], categories: list[str]) -> tuple[str, str, str]:
    categories_block = "\n".join(f"- {name}" for name in categories)
    tx_payload = [
        {
            "id": txn["id"],
            "description": txn.get("description", ""),
        }
        for txn in transactions
    ]

    system_prompt = "You are a financial transaction categorizer. Return strict JSON only."
    user_prompt = (
        "Given bank transaction descriptions, classify each into EXACTLY ONE of these categories:\n\n"
        f"{categories_block}\n\n"
        "For each transaction determine:\n"
        '1. category: must be one of the categories above\n'
        '2. use_type: "Business" or "Personal"\n'
        "3. reasoning: 10 words max\n\n"
        "IMPORTANT: Use the transaction id field exactly in your response.\n"
        "Respond as a JSON array of objects with keys id, category, use_type, reasoning.\n\n"
        "Transactions:\n"
        f"{json.dumps(tx_payload, ensure_ascii=True)}"
    )

    prompt_hash = hashlib.sha256(f"{system_prompt}\n{user_prompt}".encode("utf-8")).hexdigest()
    return system_prompt, user_prompt, prompt_hash


def _extract_json_array(raw_text: str) -> list[dict[str, Any]]:
    payload = raw_text.strip()
    if not payload:
        raise ValueError("empty LLM response")

    def _coerce(decoded: Any) -> list[dict[str, Any]]:
        if isinstance(decoded, list):
            return [item for item in decoded if isinstance(item, dict)]
        if isinstance(decoded, dict):
            # Some models may wrap the list under common keys.
            for key in ("results", "items", "transactions", "data"):
                candidate = decoded.get(key)
                if isinstance(candidate, list):
                    return [item for item in candidate if isinstance(item, dict)]
        raise ValueError("response is not a JSON array")

    try:
        return _coerce(json.loads(payload))
    except (ValueError, json.JSONDecodeError):
        pass

    start = payload.find("[")
    end = payload.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("could not locate JSON array in response")

    try:
        return _coerce(json.loads(payload[start : end + 1]))
    except json.JSONDecodeError as exc:
        raise ValueError("malformed JSON response") from exc


def _to_usage_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _usage_from_openai(body: dict[str, Any]) -> dict[str, int]:
    usage = body.get("usage")
    usage_map = usage if isinstance(usage, dict) else {}
    return {
        "input_tokens": _to_usage_int(usage_map.get("prompt_tokens")),
        "output_tokens": _to_usage_int(usage_map.get("completion_tokens")),
    }


def _usage_from_claude(body: dict[str, Any]) -> dict[str, int]:
    usage = body.get("usage")
    usage_map = usage if isinstance(usage, dict) else {}
    return {
        "input_tokens": _to_usage_int(usage_map.get("input_tokens")),
        "output_tokens": _to_usage_int(usage_map.get("output_tokens")),
    }


def _send_openai_request(system_prompt: str, user_prompt: str, model: str) -> tuple[str, dict[str, int]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")

    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"OpenAI API error: {detail or exc.reason}") from exc

    choices = body.get("choices") or []
    if not choices:
        raise RuntimeError("OpenAI API returned no choices")

    usage = _usage_from_openai(body)
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content, usage
    if isinstance(content, list):
        parts = [str(item.get("text", "")) for item in content if isinstance(item, dict)]
        return "".join(parts), usage
    raise RuntimeError("OpenAI API returned unexpected content format")


def _send_claude_request(system_prompt: str, user_prompt: str, model: str) -> tuple[str, dict[str, int]]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY is not set")

    payload = {
        "model": model,
        "max_tokens": 2048,
        "temperature": 0,
        "system": system_prompt,
        "messages": [
            {
                "role": "user",
                "content": user_prompt,
            }
        ],
    }

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Anthropic API error: {detail or exc.reason}") from exc

    content = body.get("content") or []
    usage = _usage_from_claude(body)
    if isinstance(content, list):
        text_parts = [str(item.get("text", "")) for item in content if isinstance(item, dict)]
        if text_parts:
            return "".join(text_parts), usage
    raise RuntimeError("Anthropic API returned unexpected content format")


def _request_provider(provider: str, system_prompt: str, user_prompt: str, model: str) -> tuple[str, dict[str, int]]:
    if provider == "openai":
        return _send_openai_request(system_prompt, user_prompt, model)
    if provider == "claude":
        return _send_claude_request(system_prompt, user_prompt, model)
    raise ValueError(f"Unsupported AI provider '{provider}'")


def categorize_batch(
    transactions: list[dict[str, str]],
    categories: list[str],
    provider: str | None = None,
    model: str | None = None,
) -> BatchResult:
    """Categorize a transaction batch with one retry on malformed JSON."""
    provider_name = str(provider or "").strip().lower()
    if not provider_name:
        raise ValueError("AI provider is required; set --provider or ai_categorizer.provider in rules.yaml")
    model_name = model or _default_model(provider_name)
    system_prompt, user_prompt, prompt_hash = _build_prompt(transactions, categories)
    logger.info(
        "AI categorize batch starting batch_size=%s provider=%s model=%s",
        len(transactions),
        provider_name,
        model_name,
    )

    id_order = [str(txn.get("id") or "").strip() for txn in transactions]
    id_set = {txn_id for txn_id in id_order if txn_id}

    parsed_items: list[dict[str, Any]] | None = None
    parse_error: str | None = None
    input_tokens = 0
    output_tokens = 0

    for attempt in range(2):
        response = _request_provider(provider_name, system_prompt, user_prompt, model_name)
        if isinstance(response, tuple) and len(response) == 2:
            raw, usage = response
            input_tokens += _to_usage_int(usage.get("input_tokens"))
            output_tokens += _to_usage_int(usage.get("output_tokens"))
        else:
            raw = str(response)
        try:
            parsed_items = _extract_json_array(raw)
            parse_error = None
            break
        except ValueError as exc:
            parse_error = str(exc)
            logger.warning(
                "AI categorize batch JSON parse failed attempt=%s error=%s",
                attempt + 1,
                parse_error,
            )

    if parsed_items is None:
        return BatchResult(
            results=[
                AICategorization(
                    transaction_id=txn_id,
                    category_name=None,
                    use_type=None,
                    confidence=None,
                    reasoning=None,
                    error=f"parse_failed: {parse_error}",
                )
                for txn_id in id_order
            ],
            provider=provider_name,
            model=model_name,
            prompt_hash=prompt_hash,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

    by_id: dict[str, AICategorization] = {}
    for item in parsed_items:
        txn_id = str(item.get("id") or "").strip()
        if not txn_id or txn_id not in id_set or txn_id in by_id:
            continue

        category = str(item.get("category") or "").strip() or None
        use_type = _normalize_use_type(item.get("use_type"))
        reasoning = str(item.get("reasoning") or "").strip() or None

        confidence = None
        raw_conf = item.get("confidence")
        if raw_conf is not None:
            try:
                confidence = float(raw_conf)
            except (TypeError, ValueError):
                confidence = None

        error = None
        if not category:
            error = "missing_category"

        by_id[txn_id] = AICategorization(
            transaction_id=txn_id,
            category_name=category,
            use_type=use_type,
            confidence=confidence,
            reasoning=reasoning,
            error=error,
        )

    results: list[AICategorization] = []
    for txn_id in id_order:
        if txn_id in by_id:
            results.append(by_id[txn_id])
            continue
        results.append(
            AICategorization(
                transaction_id=txn_id,
                category_name=None,
                use_type=None,
                confidence=None,
                reasoning=None,
                error="missing_result_for_id",
            )
        )

    categorized = sum(1 for item in results if not item.error)
    failed = len(results) - categorized
    logger.info(
        "AI categorize batch complete batch_size=%s categorized=%s failed=%s input_tokens=%s output_tokens=%s",
        len(transactions),
        categorized,
        failed,
        input_tokens,
        output_tokens,
    )
    return BatchResult(
        results=results,
        provider=provider_name,
        model=model_name,
        prompt_hash=prompt_hash,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


def _available_categories(conn: sqlite3.Connection, configured: object) -> list[str]:
    rows = conn.execute("SELECT name FROM categories ORDER BY sort_order ASC, name ASC").fetchall()
    return [str(row["name"]) for row in rows if str(row["name"]) in CANONICAL_CATEGORIES]


def _category_id_from_name(conn: sqlite3.Connection, category_name: str) -> str | None:
    row = conn.execute(
        "SELECT id FROM categories WHERE lower(name) = lower(?)",
        (category_name,),
    ).fetchone()
    if row:
        return str(row["id"])
    return None


def _get_or_create_category_id(conn: sqlite3.Connection, category_name: str) -> str:
    rules = load_rules()
    resolved = resolve_category_alias(category_name, rules)
    if resolved is None:
        raise ValueError(f"Category '{category_name}' resolves to null and cannot be created")

    existing = _category_id_from_name(conn, resolved)
    if existing:
        return existing

    if resolved not in CANONICAL_CATEGORIES:
        raise ValueError(f"Non-canonical category '{resolved}' (from '{category_name}')")

    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 1)",
        (category_id, resolved),
    )
    return category_id


def _upsert_memory(
    conn: sqlite3.Connection,
    description: str,
    category_id: str,
    use_type: str | None,
    confidence: float,
    is_confirmed: bool,
) -> None:
    pattern = normalize_description(description)
    if not pattern:
        return

    memory_use_type = use_type if use_type in {"Business", "Personal"} else "Any"
    existing = conn.execute(
        "SELECT id FROM vendor_memory WHERE description_pattern = ? AND use_type = ?",
        (pattern, memory_use_type),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE vendor_memory
               SET category_id = ?,
                   confidence = ?,
                   is_enabled = 1,
                   is_confirmed = ?
             WHERE id = ?
            """,
            (category_id, confidence, 1 if is_confirmed else 0, existing["id"]),
        )
        return

    conn.execute(
        """
        INSERT INTO vendor_memory (
            id,
            description_pattern,
            category_id,
            use_type,
            confidence,
            priority,
            is_enabled,
            is_confirmed,
            match_count
        ) VALUES (?, ?, ?, ?, ?, 0, 1, ?, 0)
        """,
        (
            uuid.uuid4().hex,
            pattern,
            category_id,
            memory_use_type,
            confidence,
            1 if is_confirmed else 0,
        ),
    )


def _log_result(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    transaction_id: str,
    provider: str,
    model: str,
    prompt_hash: str,
    category_name: str | None,
    use_type: str | None,
    confidence: float | None,
    reasoning: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO ai_categorization_log (
            id,
            batch_id,
            transaction_id,
            provider,
            model,
            category_name,
            use_type,
            confidence,
            reasoning,
            prompt_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            batch_id,
            transaction_id,
            provider,
            model,
            category_name,
            use_type,
            confidence,
            reasoning,
            prompt_hash,
        ),
    )


def categorize_uncategorized(
    conn: sqlite3.Connection,
    limit: int = 100,
    dry_run: bool = False,
    provider: str | None = None,
    batch_size: int | None = None,
) -> dict[str, Any]:
    """Categorize uncategorized transactions using configured AI provider."""
    started_at = time.perf_counter()
    rules = load_rules()
    ai_cfg = dict(rules.ai_categorizer or {})

    raw_provider = provider if str(provider or "").strip() else ai_cfg.get("provider")
    provider_name = str(raw_provider or "").strip().lower()
    if not provider_name:
        raise ValueError("AI provider is required; set --provider or ai_categorizer.provider in rules.yaml")
    model_name = str(ai_cfg.get("model") or _default_model(provider_name)).strip()
    configured_batch_size = ai_cfg.get("batch_size", 50)
    effective_batch_size = int(batch_size or configured_batch_size or 50)
    if effective_batch_size <= 0:
        effective_batch_size = 50

    ai_confidence = float(ai_cfg.get("confidence", 0.7))
    auto_remember = bool(ai_cfg.get("auto_remember", True))
    auto_remember_confirmed = bool(ai_cfg.get("auto_remember_confirmed", False))

    rows = conn.execute(
        """
        SELECT id, description, use_type
          FROM transactions
         WHERE is_active = 1
           AND category_id IS NULL
         ORDER BY date ASC, created_at ASC
         LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    logger.info(
        "AI categorize starting total_uncategorized=%s batch_size=%s provider=%s",
        len(rows),
        effective_batch_size,
        provider_name,
    )

    if not rows:
        return {
            "categorized": 0,
            "failed": 0,
            "batches": 0,
            "cost_estimate": "n/a",
            "provider": provider_name,
            "model": model_name,
            "input_tokens": 0,
            "output_tokens": 0,
            "elapsed_ms": 0,
        }

    categories = _available_categories(conn, ai_cfg.get("available_categories"))
    if not categories:
        raise ValueError("No categories available for AI categorization")

    rows_by_id = {str(row["id"]): row for row in rows}

    categorized = 0
    failed = 0
    batch_count = 0
    total_input_tokens = 0
    total_output_tokens = 0
    batches = _chunked(list(rows), effective_batch_size)
    total_batches = len(batches)

    for batch_index, batch_rows in enumerate(batches, start=1):
        tx_payload = [
            {
                "id": str(row["id"]),
                "description": str(row["description"] or ""),
            }
            for row in batch_rows
        ]
        batch_result = categorize_batch(
            tx_payload,
            categories,
            provider=provider_name,
            model=model_name,
        )

        batch_id = uuid.uuid4().hex
        batch_count += 1
        total_input_tokens += int(batch_result.input_tokens)
        total_output_tokens += int(batch_result.output_tokens)
        batch_categorized = 0
        batch_failed = 0

        for item in batch_result.results:
            txn_id = item.transaction_id
            row = rows_by_id.get(txn_id)
            if row is None:
                continue

            category_name: str | None = None
            resolved_use_type = _normalize_use_type(item.use_type) or _normalize_use_type(row["use_type"])
            confidence = item.confidence if item.confidence is not None else ai_confidence
            reasoning = item.reasoning

            if item.error:
                failed += 1
                batch_failed += 1
                reasoning = (reasoning or "") + (f" [error={item.error}]" if reasoning else f"error={item.error}")
            else:
                resolved_name = resolve_category_alias(str(item.category_name or ""), rules)
                if resolved_name is None:
                    failed += 1
                    batch_failed += 1
                    reasoning = (reasoning or "") + (" [error=resolved_null_category]" if reasoning else "error=resolved_null_category")
                else:
                    category_name = resolved_name.strip()
                if resolved_name is not None and not category_name:
                    failed += 1
                    batch_failed += 1
                    reasoning = (reasoning or "") + (" [error=resolved_empty_category]" if reasoning else "error=resolved_empty_category")
                elif category_name:
                    category_id = _category_id_from_name(conn, category_name)
                    if category_id is None and not dry_run:
                        category_id = _get_or_create_category_id(conn, category_name)
                    if category_id is not None or dry_run:
                        categorized += 1
                        batch_categorized += 1
                        if not dry_run and category_id is not None:
                            if resolved_use_type:
                                conn.execute(
                                    """
                                    UPDATE transactions
                                       SET category_id = ?,
                                           category_source = 'ai',
                                           category_confidence = ?,
                                           use_type = ?,
                                           updated_at = datetime('now')
                                     WHERE id = ?
                                    """,
                                    (category_id, confidence, resolved_use_type, txn_id),
                                )
                            else:
                                conn.execute(
                                    """
                                    UPDATE transactions
                                       SET category_id = ?,
                                           category_source = 'ai',
                                           category_confidence = ?,
                                           updated_at = datetime('now')
                                     WHERE id = ?
                                    """,
                                    (category_id, confidence, txn_id),
                                )

                            if auto_remember:
                                _upsert_memory(
                                    conn,
                                    str(row["description"] or ""),
                                    category_id,
                                    resolved_use_type,
                                    ai_confidence,
                                    auto_remember_confirmed,
                                )
                    else:
                        failed += 1
                        batch_failed += 1
                        reasoning = (reasoning or "") + (" [error=category_lookup_failed]" if reasoning else "error=category_lookup_failed")

            if not dry_run:
                _log_result(
                    conn,
                    batch_id=batch_id,
                    transaction_id=txn_id,
                    provider=batch_result.provider,
                    model=batch_result.model,
                    prompt_hash=batch_result.prompt_hash,
                    category_name=category_name,
                    use_type=resolved_use_type,
                    confidence=confidence,
                    reasoning=reasoning,
                )
        logger.info(
            "AI categorize batch %s/%s complete categorized=%s failed=%s",
            batch_index,
            total_batches,
            batch_categorized,
            batch_failed,
        )

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "AI categorize complete total_categorized=%s total_failed=%s batches=%s input_tokens=%s output_tokens=%s elapsed_ms=%s",
        categorized,
        failed,
        batch_count,
        total_input_tokens,
        total_output_tokens,
        elapsed_ms,
    )
    return {
        "categorized": categorized,
        "failed": failed,
        "batches": batch_count,
        "cost_estimate": "n/a",
        "provider": provider_name,
        "model": model_name,
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "elapsed_ms": elapsed_ms,
    }


__all__ = [
    "AICategorization",
    "BatchResult",
    "categorize_batch",
    "categorize_uncategorized",
]
