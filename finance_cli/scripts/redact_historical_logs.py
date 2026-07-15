"""One-shot redaction audit for historical frontend log rows."""

from __future__ import annotations

import argparse
import importlib.util
import json
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
import os
import sqlite3
from typing import Any

from finance_cli.db import connect
from finance_cli.redaction import redact_text
from finance_cli.user_provisioning import user_db_path

MARKERS = (
    "[REDACTED]",
    "[CARD]",
    "[SSN]",
    "[ACCT]",
    "[EMAIL]",
    "[JWT]",
    "[KEY]",
    "[IP]",
    "[USER_PATH]",
    "[SERVER_PATH]",
)


@dataclass
class AuditSummary:
    scanned: int = 0
    changed: int = 0
    updated: int = 0
    stale_skipped: int = 0
    malformed_skipped: int = 0
    marker_counts: Counter[str] = field(default_factory=Counter)
    samples: list[dict[str, Any]] = field(default_factory=list)

    def add(self, other: "AuditSummary") -> None:
        self.scanned += other.scanned
        self.changed += other.changed
        self.updated += other.updated
        self.stale_skipped += other.stale_skipped
        self.malformed_skipped += other.malformed_skipped
        self.marker_counts.update(other.marker_counts)
        self.samples.extend(other.samples)


BeforeUpdateHook = Callable[[sqlite3.Connection, sqlite3.Row], None]


def default_data_root() -> Path:
    env_root = os.getenv("FINANCE_WEB_DATA_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / "finance-web" / "server" / "config.py"
    spec = importlib.util.spec_from_file_location("finance_web_server_config", config_path)
    if spec is not None and spec.loader is not None:
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            default_factory = getattr(module, "_default_data_root", None)
            if callable(default_factory):
                return Path(default_factory()).expanduser().resolve()
        except Exception:
            pass
    return (repo_root / "finance-web" / "data" / "users").resolve()


def iter_user_ids(data_root: Path, only_user_id: str | None = None) -> Iterable[str]:
    if only_user_id is not None:
        yield str(only_user_id)
        return
    if not data_root.exists():
        return
    for child in sorted(data_root.iterdir()):
        if child.name.startswith(".") or not child.is_dir():
            continue
        yield child.name


def redact_metadata(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, list):
        return [redact_metadata(item) for item in value]
    if isinstance(value, dict):
        return {key: redact_metadata(item) for key, item in value.items()}
    return value


def _redact_optional(value: Any) -> str | None:
    if value is None:
        return None
    return redact_text(str(value))


def _marker_delta(before: str | None, after: str | None) -> Counter[str]:
    counts: Counter[str] = Counter()
    before_text = before or ""
    after_text = after or ""
    for marker in MARKERS:
        delta = after_text.count(marker) - before_text.count(marker)
        if delta > 0:
            counts[marker] += delta
    return counts


def _metadata_marker_delta(before: str | None, after: str | None) -> Counter[str]:
    return _marker_delta(before, after)


def _sample_row(
    row_id: int,
    before: tuple[str | None, str | None, str | None, str | None],
    after: tuple[str | None, str | None, str | None, str | None],
) -> dict[str, Any]:
    return {
        "id": row_id,
        "before": {
            "message": before[0],
            "namespace": before[1],
            "page": before[2],
            "metadata": before[3],
        },
        "after": {
            "message": after[0],
            "namespace": after[1],
            "page": after[2],
            "metadata": after[3],
        },
    }


def redact_database(
    *,
    data_root: Path,
    user_id: str,
    apply: bool = False,
    sample: int = 0,
    before_update: BeforeUpdateHook | None = None,
) -> AuditSummary:
    db_path = user_db_path(data_root, user_id)
    summary = AuditSummary()
    if not db_path.exists():
        return summary

    with connect(db_path=db_path, expected_user_id=user_id, busy_timeout=5000) as conn:
        rows = conn.execute(
            "SELECT id, message, namespace, page, metadata FROM frontend_logs ORDER BY id"
        ).fetchall()
        for row in rows:
            summary.scanned += 1
            metadata_text = row["metadata"]
            redacted_metadata_text: str | None = None
            if metadata_text is not None:
                try:
                    metadata_value = json.loads(metadata_text)
                except json.JSONDecodeError:
                    summary.malformed_skipped += 1
                    continue
                redacted_metadata = redact_metadata(metadata_value)
                redacted_metadata_shape = redact_text(metadata_text)
                if redacted_metadata_shape != metadata_text:
                    try:
                        json.loads(redacted_metadata_shape)
                    except json.JSONDecodeError:
                        redacted_metadata_text = json.dumps(
                            redacted_metadata,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        )
                    else:
                        redacted_metadata_text = redacted_metadata_shape
                elif redacted_metadata == metadata_value:
                    redacted_metadata_text = metadata_text
                else:
                    redacted_metadata_text = json.dumps(
                        redacted_metadata,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )

            before = (
                row["message"],
                row["namespace"],
                row["page"],
                metadata_text,
            )
            after = (
                redact_text(str(row["message"] or "")),
                _redact_optional(row["namespace"]),
                _redact_optional(row["page"]),
                redacted_metadata_text,
            )
            if before == after:
                continue

            summary.changed += 1
            for original, redacted in zip(before[:3], after[:3], strict=True):
                summary.marker_counts.update(_marker_delta(original, redacted))
            summary.marker_counts.update(_metadata_marker_delta(before[3], after[3]))
            if sample and len(summary.samples) < sample:
                summary.samples.append(_sample_row(int(row["id"]), before, after))
            if not apply:
                continue

            if before_update is not None:
                before_update(conn, row)
            cursor = conn.execute(
                """
                UPDATE frontend_logs
                SET message = ?,
                    namespace = ?,
                    page = ?,
                    metadata = ?
                WHERE id = ?
                  AND message IS ?
                  AND namespace IS ?
                  AND page IS ?
                  AND metadata IS ?
                """,
                (
                    after[0],
                    after[1],
                    after[2],
                    after[3],
                    row["id"],
                    before[0],
                    before[1],
                    before[2],
                    before[3],
                ),
            )
            if cursor.rowcount == 0:
                summary.stale_skipped += 1
                print(f"user={user_id} row={row['id']} stale, skipped")
            else:
                summary.updated += 1
        if apply:
            conn.commit()
    return summary


def run_audit(
    *,
    data_root: Path,
    apply: bool = False,
    user_id: str | None = None,
    sample: int = 0,
) -> AuditSummary:
    total = AuditSummary()
    for current_user_id in iter_user_ids(data_root, user_id):
        summary = redact_database(
            data_root=data_root,
            user_id=current_user_id,
            apply=apply,
            sample=sample,
        )
        total.add(summary)
        print(
            "user={user_id} scanned={scanned} changed={changed} updated={updated} "
            "stale_skipped={stale} malformed_skipped={malformed} markers={markers}".format(
                user_id=current_user_id,
                scanned=summary.scanned,
                changed=summary.changed,
                updated=summary.updated,
                stale=summary.stale_skipped,
                malformed=summary.malformed_skipped,
                markers=dict(sorted(summary.marker_counts.items())),
            )
        )
        for preview in summary.samples:
            print(json.dumps(preview, sort_keys=True, separators=(",", ":")))
    print(
        "total scanned={scanned} changed={changed} updated={updated} "
        "stale_skipped={stale} malformed_skipped={malformed} markers={markers}".format(
            scanned=total.scanned,
            changed=total.changed,
            updated=total.updated,
            stale=total.stale_skipped,
            malformed=total.malformed_skipped,
            markers=dict(sorted(total.marker_counts.items())),
        )
    )
    return total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write redacted rows")
    parser.add_argument("--user-id", help="audit a single user id")
    parser.add_argument("--sample", type=int, default=0, help="print up to N changed-row previews")
    parser.add_argument("--data-root", type=Path, default=default_data_root())
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_audit(
        data_root=Path(args.data_root).expanduser().resolve(),
        apply=bool(args.apply),
        user_id=args.user_id,
        sample=max(int(args.sample), 0),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
