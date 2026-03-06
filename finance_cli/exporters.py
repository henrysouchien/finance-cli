"""CSV exporters for transactions and monthly summaries."""

from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path


def export_transactions_csv(
    conn: sqlite3.Connection,
    output_path: str | Path,
    date_from: str | None = None,
    date_to: str | None = None,
    category_name: str | None = None,
) -> int:
    where = ["t.is_active = 1"]
    params: list[str] = []

    if date_from:
        where.append("t.date >= ?")
        params.append(date_from)
    if date_to:
        where.append("t.date <= ?")
        params.append(date_to)
    if category_name:
        where.append("c.name = ?")
        params.append(category_name)

    rows = conn.execute(
        f"""
        SELECT t.id, t.date, t.description, t.amount_cents, t.source,
               t.use_type, t.is_payment, t.is_reviewed,
               c.name AS category_name, p.name AS project_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN projects p ON p.id = t.project_id
         WHERE {' AND '.join(where)}
         ORDER BY t.date ASC, t.created_at ASC
        """,
        tuple(params),
    ).fetchall()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "id",
                "date",
                "description",
                "amount_cents",
                "source",
                "use_type",
                "is_payment",
                "is_reviewed",
                "category_name",
                "project_name",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["id"],
                    row["date"],
                    row["description"],
                    row["amount_cents"],
                    row["source"],
                    row["use_type"],
                    row["is_payment"],
                    row["is_reviewed"],
                    row["category_name"],
                    row["project_name"],
                ]
            )

    return len(rows)


def export_monthly_summary_csv(conn: sqlite3.Connection, month: str, output_path: str | Path) -> int:
    start = f"{month}-01"
    end = conn.execute(
        "SELECT date(?, '+1 month', '-1 day') AS month_end",
        (start,),
    ).fetchone()["month_end"]

    rows = conn.execute(
        """
        SELECT COALESCE(p.name, c.name, 'Uncategorized') AS group_name,
               COALESCE(c.name, 'Uncategorized') AS category_name,
               COUNT(*) AS transaction_count,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN -t.amount_cents ELSE 0 END), 0) AS expense_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents > 0 THEN t.amount_cents ELSE 0 END), 0) AS income_cents
         FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN categories p ON p.id = c.parent_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY group_name, category_name
         ORDER BY expense_cents DESC, category_name ASC
        """,
        (start, end),
    ).fetchall()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["month", "group_name", "category_name", "transaction_count", "total_cents", "expense_cents", "income_cents"])
        for row in rows:
            writer.writerow(
                [
                    month,
                    row["group_name"],
                    row["category_name"],
                    row["transaction_count"],
                    row["total_cents"],
                    row["expense_cents"],
                    row["income_cents"],
                ]
            )

    return len(rows)


def _slugify_category(value: str) -> str:
    slug = value.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = re.sub(r"_+", "_", slug)
    return slug.strip("_") or "uncategorized"


def export_wave(conn: sqlite3.Connection, month: str, output_dir: str | Path) -> dict[str, object]:
    start = f"{month}-01"
    end = conn.execute(
        "SELECT date(?, '+1 month', '-1 day') AS month_end",
        (start,),
    ).fetchone()["month_end"]

    rows = conn.execute(
        """
        SELECT t.date, t.description, t.amount_cents, COALESCE(c.name, 'Uncategorized') AS category_name
         FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
           AND t.amount_cents < 0
         ORDER BY category_name ASC, t.date ASC, t.created_at ASC
        """,
        (start, end),
    ).fetchall()

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        category = str(row["category_name"])
        grouped.setdefault(category, []).append(row)

    files: list[str] = []
    total_rows = 0
    for category, category_rows in grouped.items():
        slug = _slugify_category(category)
        filename = f"wave_{month}_{slug}_expense.csv"
        path = output / filename

        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["Date", "Amount", "Description", "Account", "Category"])
            for row in category_rows:
                writer.writerow(
                    [
                        row["date"],
                        f"{(-int(row['amount_cents'])) / 100:.2f}",
                        row["description"],
                        "Expenses",
                        category,
                    ]
                )
                total_rows += 1

        files.append(str(path))

    return {
        "month": month,
        "output_dir": str(output),
        "files": files,
        "rows": total_rows,
    }
