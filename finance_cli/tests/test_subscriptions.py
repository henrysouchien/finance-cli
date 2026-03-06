from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.subscriptions import (
    _build_cluster_group,
    _can_merge_groups_relaxed,
    _first_token_merge_match,
    _high_jaccard_merge_match,
    _normalize_for_clustering,
    _prefix_merge_match,
    detect_recurring_patterns,
    detect_subscriptions,
)


def _setup_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _add_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)", (category_id, name))
    return category_id


def _add_account(conn, account_id: str, account_name: str) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, institution_name, account_name, account_type, is_active
        ) VALUES (?, ?, 'Test Bank', ?, 'credit_card', 1)
        """,
        (account_id, f"ext_{account_id}", account_name),
    )


def _add_transaction(
    conn,
    *,
    txn_date: str,
    description: str,
    amount_cents: int,
    category_id: str | None = None,
    account_id: str | None = None,
    use_type: str | None = "Personal",
    is_payment: int = 0,
    is_active: int = 1,
    is_recurring: int = 0,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id,
            account_id,
            date,
            description,
            amount_cents,
            category_id,
            use_type,
            is_payment,
            is_active,
            is_recurring,
            source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual')
        """,
        (
            txn_id,
            account_id,
            txn_date,
            description,
            amount_cents,
            category_id,
            use_type,
            is_payment,
            is_active,
            is_recurring,
        ),
    )
    return txn_id


def _build_group(conn, *, description: str, category_id: str, txns: list[tuple[str, int]]):
    txn_ids = [
        _add_transaction(conn, txn_date=txn_date, description=description, amount_cents=amount, category_id=category_id)
        for txn_date, amount in txns
    ]
    placeholders = ", ".join("?" for _ in txn_ids)
    rows = conn.execute(
        f"SELECT * FROM transactions WHERE id IN ({placeholders})",
        tuple(txn_ids),
    ).fetchall()
    by_id = {str(row["id"]): row for row in rows}
    ordered_rows = [by_id[txn_id] for txn_id in txn_ids]
    return _build_cluster_group(description, None, ordered_rows)


def _iso_days_ago(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def test_vendor_clustering_merges_prefix_token_first_token_and_respects_category_gate(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        telecom = _add_category(conn, "Telecom")
        transit = _add_category(conn, "Transit")
        shopping = _add_category(conn, "Shopping")
        entertainment = _add_category(conn, "Entertainment")

        for txn_date in ("2025-01-05", "2025-02-05", "2025-03-05"):
            _add_transaction(conn, txn_date=txn_date, description="Spotify", amount_cents=-1099, category_id=streaming)
        for txn_date in ("2025-04-05", "2025-05-05"):
            _add_transaction(conn, txn_date=txn_date, description="Spotify USA", amount_cents=-1099, category_id=streaming)

        for txn_date in ("2025-01-07", "2025-02-07", "2025-03-07"):
            _add_transaction(conn, txn_date=txn_date, description="Verizon Wireless", amount_cents=-7000, category_id=telecom)
        for txn_date in ("2025-04-07", "2025-05-07", "2025-06-07"):
            _add_transaction(conn, txn_date=txn_date, description="Wireless Verizon AutoPay", amount_cents=-7000, category_id=telecom)

        for txn_date in ("2025-01-09", "2025-02-09", "2025-03-09"):
            _add_transaction(conn, txn_date=txn_date, description="MTA", amount_cents=-500, category_id=transit)
        for txn_date in ("2025-04-09", "2025-05-09", "2025-06-09"):
            _add_transaction(conn, txn_date=txn_date, description="MTA NYCT PAYGO CP NEW YORK", amount_cents=-600, category_id=transit)

        for txn_date in ("2025-01-11", "2025-02-11", "2025-03-11"):
            _add_transaction(conn, txn_date=txn_date, description="Amazon", amount_cents=-4000, category_id=shopping)
        for txn_date in ("2025-01-12", "2025-02-12", "2025-03-12"):
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Amazon Prime Video",
                amount_cents=-1499,
                category_id=entertainment,
            )
        conn.commit()

        patterns = detect_recurring_patterns(conn)

    by_vendor = {pattern.vendor_name: pattern for pattern in patterns}
    assert "Spotify" in by_vendor
    assert by_vendor["Spotify"].occurrence_count == 5
    assert "Verizon Wireless" in by_vendor
    assert by_vendor["Verizon Wireless"].occurrence_count == 6
    assert "Mta" in by_vendor
    assert by_vendor["Mta"].occurrence_count == 6
    assert "Amazon" in by_vendor
    assert "Amazon Prime Video" in by_vendor
    assert len(patterns) == 5


def test_normalize_for_clustering_real_world_cases() -> None:
    assert _normalize_for_clustering("www.snaptrade.com fredericton, nb") == "snaptrade"
    assert _normalize_for_clustering("apple.com/bill one apple park") == "apple bill one apple park"
    assert _normalize_for_clustering("crunch gym new york ny") == "crunch gym"
    assert _normalize_for_clustering("apple.com/bill 866-712-7753 ca usa") == "apple bill"
    assert _normalize_for_clustering("ecm, a legalzoom co.glendale ca") == "ecm a legalzoom"
    assert _normalize_for_clustering("youtube tv") == "youtube tv"
    assert _normalize_for_clustering("leonardo ai") == "leonardo ai"
    assert _normalize_for_clustering("ca usa") == "ca usa"


def test_prefix_merge_match_uses_clustering_description(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Subscriptions")
        first_txn = _add_transaction(
            conn,
            txn_date="2025-01-05",
            description="www.snaptrade.com fredericton, nb",
            amount_cents=-1000,
            category_id=category_id,
        )
        second_txn = _add_transaction(
            conn,
            txn_date="2025-02-05",
            description="snaptrade",
            amount_cents=-1000,
            category_id=category_id,
        )
        conn.commit()
        rows = conn.execute(
            "SELECT * FROM transactions WHERE id IN (?, ?) ORDER BY date ASC",
            (first_txn, second_txn),
        ).fetchall()

    group_a = _build_cluster_group("www.snaptrade.com fredericton, nb", None, [rows[0]])
    group_b = _build_cluster_group("snaptrade", None, [rows[1]])
    assert _prefix_merge_match(group_a, group_b)
    assert _can_merge_groups_relaxed(group_a, group_b)


def test_high_jaccard_cross_category_boundaries_and_amount_gate(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        shopping = _add_category(conn, "Shopping")
        entertainment = _add_category(conn, "Entertainment")

        group_80_a = _build_group(
            conn,
            description="moonflash limited billing service",
            category_id=shopping,
            txns=[("2025-01-01", -1000)],
        )
        group_80_b = _build_group(
            conn,
            description="billing moonflash limited service online",
            category_id=entertainment,
            txns=[("2025-01-02", -1000)],
        )
        assert _high_jaccard_merge_match(group_80_a, group_80_b)
        assert _can_merge_groups_relaxed(group_80_a, group_80_b)

        group_79_a = _build_group(
            conn,
            description="alpha beta gamma delta epsilon zeta eta theta iota kappa lambda",
            category_id=shopping,
            txns=[("2025-01-03", -1000)],
        )
        group_79_b = _build_group(
            conn,
            description="alpha beta gamma delta epsilon zeta eta theta iota kappa lambda muon nova xray",
            category_id=entertainment,
            txns=[("2025-01-04", -1000)],
        )
        assert not _high_jaccard_merge_match(group_79_a, group_79_b)

        group_ratio_high = _build_group(
            conn,
            description="billing moonflash limited service online",
            category_id=entertainment,
            txns=[("2025-01-05", -1600)],
        )
        assert _high_jaccard_merge_match(group_80_a, group_ratio_high)
        assert not _can_merge_groups_relaxed(group_80_a, group_ratio_high)


def test_high_jaccard_cross_category_groups_merge_in_recurring_detection(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        shopping = _add_category(conn, "Shopping")
        entertainment = _add_category(conn, "Entertainment")

        moonflash_ids: list[str] = []
        billing_ids: list[str] = []
        for txn_date in ("2025-01-05", "2025-02-05", "2025-03-05"):
            moonflash_ids.append(
                _add_transaction(
                    conn,
                    txn_date=txn_date,
                    description="Moonflash Limited Billing Service",
                    amount_cents=-1274,
                    category_id=shopping,
                )
            )
        for txn_date in ("2025-04-05", "2025-05-05", "2025-06-05"):
            billing_ids.append(
                _add_transaction(
                    conn,
                    txn_date=txn_date,
                    description="Billing Moonflash Limited Service Online",
                    amount_cents=-1274,
                    category_id=entertainment,
                )
            )
        conn.commit()

        patterns = detect_recurring_patterns(conn)

    expected = set(moonflash_ids + billing_ids)
    assert expected in [set(pattern.transaction_ids) for pattern in patterns]


def test_high_jaccard_cross_category_rejects_amount_ratio_over_one_point_five(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        shopping = _add_category(conn, "Shopping")
        entertainment = _add_category(conn, "Entertainment")

        low_ids: list[str] = []
        high_ids: list[str] = []
        for txn_date in ("2025-01-07", "2025-02-07", "2025-03-07"):
            low_ids.append(
                _add_transaction(
                    conn,
                    txn_date=txn_date,
                    description="Moonflash Limited Billing Service",
                    amount_cents=-1000,
                    category_id=shopping,
                )
            )
        for txn_date in ("2025-04-07", "2025-05-07", "2025-06-07"):
            high_ids.append(
                _add_transaction(
                    conn,
                    txn_date=txn_date,
                    description="Billing Moonflash Limited Service Online",
                    amount_cents=-1600,
                    category_id=entertainment,
                )
            )
        conn.commit()

        patterns = detect_recurring_patterns(conn)

    pattern_sets = [set(pattern.transaction_ids) for pattern in patterns]
    assert set(low_ids) in pattern_sets
    assert set(high_ids) in pattern_sets


def test_first_token_merge_requires_frequency_signal_and_amount_similarity(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Subscriptions")
        a1 = _add_transaction(conn, txn_date="2025-01-01", description="Apple One", amount_cents=-1000, category_id=category_id)
        a2 = _add_transaction(conn, txn_date="2025-02-01", description="Apple One", amount_cents=-1000, category_id=category_id)
        b1 = _add_transaction(conn, txn_date="2025-01-02", description="Apple Store", amount_cents=-3500, category_id=category_id)
        b2 = _add_transaction(conn, txn_date="2025-02-02", description="Apple Store", amount_cents=-3500, category_id=category_id)
        c1 = _add_transaction(conn, txn_date="2025-03-02", description="Apple TV", amount_cents=-1200, category_id=category_id)
        d1 = _add_transaction(conn, txn_date="2025-03-04", description="Apple Arcade", amount_cents=-1300, category_id=category_id)
        conn.commit()
        rows = conn.execute(
            "SELECT * FROM transactions WHERE id IN (?, ?, ?, ?, ?, ?)",
            (a1, a2, b1, b2, c1, d1),
        ).fetchall()

    row_by_id = {str(row["id"]): row for row in rows}
    group_monthly_low = _build_cluster_group("apple one", None, [row_by_id[a1], row_by_id[a2]])
    group_monthly_high = _build_cluster_group("apple store", None, [row_by_id[b1], row_by_id[b2]])
    assert not _first_token_merge_match(group_monthly_low, group_monthly_high)

    group_unknown_a = _build_cluster_group("apple tv", None, [row_by_id[c1]])
    group_unknown_b = _build_cluster_group("apple arcade", None, [row_by_id[d1]])
    assert not _first_token_merge_match(group_unknown_a, group_unknown_b)


def test_first_token_merge_adjacent_frequency_ratio_boundaries(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Subscriptions")

        biweekly_group = _build_group(
            conn,
            description="crunch fitness",
            category_id=category_id,
            txns=[("2025-01-01", -1000), ("2025-01-15", -1000), ("2025-01-29", -1000)],
        )
        monthly_group_15 = _build_group(
            conn,
            description="crunch gym",
            category_id=category_id,
            txns=[("2025-02-01", -1500), ("2025-03-01", -1500), ("2025-04-01", -1500)],
        )
        monthly_group_16 = _build_group(
            conn,
            description="crunch club",
            category_id=category_id,
            txns=[("2025-02-02", -1600), ("2025-03-02", -1600), ("2025-04-02", -1600)],
        )

    assert _first_token_merge_match(biweekly_group, monthly_group_15)
    assert not _first_token_merge_match(biweekly_group, monthly_group_16)


def test_first_token_merge_rejects_non_adjacent_frequency_pairs(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Subscriptions")
        weekly_group = _build_group(
            conn,
            description="crunch fitness",
            category_id=category_id,
            txns=[("2025-01-01", -1000), ("2025-01-08", -1000), ("2025-01-15", -1000)],
        )
        monthly_group = _build_group(
            conn,
            description="crunch gym",
            category_id=category_id,
            txns=[("2025-02-01", -1200), ("2025-03-01", -1200), ("2025-04-01", -1200)],
        )

    assert not _first_token_merge_match(weekly_group, monthly_group)


def test_first_token_merge_same_frequency_ratio_boundaries(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Subscriptions")
        monthly_base = _build_group(
            conn,
            description="crunch fitness",
            category_id=category_id,
            txns=[("2025-01-01", -1000), ("2025-02-01", -1000), ("2025-03-01", -1000)],
        )
        monthly_20 = _build_group(
            conn,
            description="crunch gym",
            category_id=category_id,
            txns=[("2025-01-02", -2000), ("2025-02-02", -2000), ("2025-03-02", -2000)],
        )
        monthly_21 = _build_group(
            conn,
            description="crunch plus",
            category_id=category_id,
            txns=[("2025-01-03", -2100), ("2025-02-03", -2100), ("2025-03-03", -2100)],
        )

    assert _first_token_merge_match(monthly_base, monthly_20)
    assert not _first_token_merge_match(monthly_base, monthly_21)


def test_first_token_merge_one_known_one_unknown_allows_ratio_up_to_two(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Subscriptions")
        monthly_group = _build_group(
            conn,
            description="crunch fitness",
            category_id=category_id,
            txns=[("2025-01-01", -1000), ("2025-02-01", -1000)],
        )
        unknown_group = _build_group(
            conn,
            description="crunch pass",
            category_id=category_id,
            txns=[("2025-03-01", -2000)],
        )

    assert _first_token_merge_match(monthly_group, unknown_group)


def test_first_token_merge_both_unknown_frequencies_do_not_merge(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Subscriptions")
        unknown_group_a = _build_group(
            conn,
            description="crunch fitness",
            category_id=category_id,
            txns=[("2025-01-01", -1000)],
        )
        unknown_group_b = _build_group(
            conn,
            description="crunch gym",
            category_id=category_id,
            txns=[("2025-02-01", -1100)],
        )

    assert not _first_token_merge_match(unknown_group_a, unknown_group_b)


def test_known_same_account_pairs_cluster_into_single_patterns(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_account_id, institution_name, account_name, account_type, card_ending, is_active)
            VALUES ('acct_shared', 'acct_shared_external', 'Test Bank', 'Shared Card', 'credit_card', '0001', 1)
            """
        )
        pro_fees = _add_category(conn, "Professional Fees")
        shopping = _add_category(conn, "Shopping")
        entertainment = _add_category(conn, "Entertainment")

        pair_specs = [
            ("legalzoom", "ECM, A LEGALZOOM CO.GLENDALE CA", "ECM A LEGALZOOM CO.", -3899, pro_fees, shopping, 5),
            ("snaptrade", "WWW.SNAPTRADE.COM FREDERICTON, NB", "SNAPTRADE", -1799, shopping, entertainment, 6),
            (
                "apple",
                "APPLE.COM/BILL ONE APPLE PARK CUPERTINO CA USA",
                "APPLE.COM/BILL ONE APPLE PARK WAY 866-712-7753 CA USA",
                -999,
                entertainment,
                shopping,
                7,
            ),
            ("openai", "OPENAI *CHATGPT SUBSSAN FRANCISCO CA", "OPENAI", -2000, shopping, pro_fees, 8),
            ("crunch", "CRUNCH GYM NEW YORK NY", "CRUNCH FITNESS", -3499, pro_fees, shopping, 9),
        ]

        pair_txn_ids: dict[str, list[str]] = {}
        for key, description_a, description_b, amount, category_a, category_b, day in pair_specs:
            ids = [
                _add_transaction(
                    conn,
                    txn_date=f"2025-01-{day:02d}",
                    description=description_a,
                    amount_cents=amount,
                    category_id=category_a,
                    account_id="acct_shared",
                ),
                _add_transaction(
                    conn,
                    txn_date=f"2025-02-{day:02d}",
                    description=description_a,
                    amount_cents=amount,
                    category_id=category_a,
                    account_id="acct_shared",
                ),
                _add_transaction(
                    conn,
                    txn_date=f"2025-03-{day:02d}",
                    description=description_b,
                    amount_cents=amount,
                    category_id=category_b,
                    account_id="acct_shared",
                ),
            ]
            pair_txn_ids[key] = ids
        conn.commit()

        patterns = detect_recurring_patterns(conn)

    pattern_sets = [set(pattern.transaction_ids) for pattern in patterns]
    for pair_key, txn_ids in pair_txn_ids.items():
        assert set(txn_ids) in pattern_sets, f"{pair_key} did not merge into one recurring pattern"


def test_category_exclusion_filters_shopping_and_personal_expense(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        shopping = _add_category(conn, "Shopping")
        personal_expense = _add_category(conn, "Personal Expense")
        software = _add_category(conn, "Software & Subscriptions")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Snacks Station Mini Mart",
                amount_cents=-2600,
                category_id=shopping,
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Simplified Cleaner",
                amount_cents=-2800,
                category_id=personal_expense,
            )
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=software)
        conn.commit()

        report = detect_subscriptions(conn)
        active_auto = conn.execute(
            """
            SELECT vendor_name
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name
            """
        ).fetchall()

    vendor_names = [str(row["vendor_name"]) for row in active_auto]
    assert report["detected"] == 1
    assert vendor_names == ["Netflix"]


def test_category_exclusion_filters_payments_and_transfers(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        payments = _add_category(conn, "Payments & Transfers")
        streaming = _add_category(conn, "Streaming")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Card Statement Transfer",
                amount_cents=-32800,
                category_id=payments,
                is_payment=0,
            )
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=streaming)
        conn.commit()

        report = detect_subscriptions(conn)
        active_auto = conn.execute(
            """
            SELECT vendor_name
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name
            """
        ).fetchall()

    vendor_names = [str(row["vendor_name"]) for row in active_auto]
    assert report["detected"] == 1
    assert vendor_names == ["Netflix"]


def test_short_intervals_filtered_before_frequency_inference_and_weekly_preserved(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        category_id = _add_category(conn, "Utilities")

        for txn_date in ("2025-01-01", "2025-01-02", "2025-01-03", "2025-02-02", "2025-03-04"):
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Monthly Service",
                amount_cents=-1200,
                category_id=category_id,
            )
        for txn_date in ("2025-04-01", "2025-04-02", "2025-04-09", "2025-04-16"):
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Weekly Service",
                amount_cents=-500,
                category_id=category_id,
            )
        conn.commit()

        patterns = detect_recurring_patterns(conn)

    by_vendor = {pattern.vendor_name: pattern for pattern in patterns}
    assert by_vendor["Monthly Service"].frequency == "monthly"
    assert by_vendor["Weekly Service"].frequency == "weekly"


def test_keyword_exclusion_filters_fees_but_keeps_real_subscriptions(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        fees = _add_category(conn, "Fees")
        streaming = _add_category(conn, "Streaming")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(conn, txn_date=txn_date, description="Plan Fee - Amex Gold", amount_cents=-2500, category_id=fees)
            _add_transaction(conn, txn_date=txn_date, description="Interest Charge On Purchases", amount_cents=-1800, category_id=fees)
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=streaming)
        conn.commit()

        report = detect_subscriptions(conn)
        active_auto = conn.execute(
            """
            SELECT vendor_name
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
            """
        ).fetchall()

    vendor_names = [str(row["vendor_name"]) for row in active_auto]
    lowered = [name.lower() for name in vendor_names]
    assert report["detected"] == 1
    assert vendor_names == ["Netflix"]
    assert all("plan fee" not in name for name in lowered)
    assert all("interest charge" not in name for name in lowered)


def test_keyword_exclusion_blocks_credit_card_payment_descriptions(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        fees = _add_category(conn, "Fees")
        streaming = _add_category(conn, "Streaming")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Barclaycard US DES:CREDITCARD PAYMENT",
                amount_cents=-32800,
                category_id=fees,
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Credit Card Payment",
                amount_cents=-12000,
                category_id=fees,
            )
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=streaming)
        conn.commit()

        report = detect_subscriptions(conn)
        active_auto = conn.execute(
            """
            SELECT vendor_name
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name
            """
        ).fetchall()

    vendor_names = [str(row["vendor_name"]) for row in active_auto]
    assert report["detected"] == 1
    assert vendor_names == ["Netflix"]


def test_keyword_exclusion_blocks_new_false_positive_vendors(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        wellness = _add_category(conn, "Wellness")
        streaming = _add_category(conn, "Streaming")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Health & Harmony",
                amount_cents=-6500,
                category_id=wellness,
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Juice Generation",
                amount_cents=-1400,
                category_id=wellness,
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="SQ *The Players Theat",
                amount_cents=-2000,
                category_id=wellness,
            )
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=streaming)
        conn.commit()

        report = detect_subscriptions(conn)
        active_auto = conn.execute(
            """
            SELECT vendor_name
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name
            """
        ).fetchall()

    vendor_names = [str(row["vendor_name"]) for row in active_auto]
    assert report["detected"] == 1
    assert vendor_names == ["Netflix"]


def test_detect_subscriptions_is_idempotent(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        for days_ago in (90, 60, 30):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Netflix",
                amount_cents=-1599,
                category_id=streaming,
            )
        conn.commit()

        first_report = detect_subscriptions(conn)
        second_report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT vendor_name, frequency, amount_cents, is_active
              FROM subscriptions
             WHERE is_auto_detected = 1
            """
        ).fetchall()

    assert first_report["inserted"] == 1
    assert second_report["inserted"] == 0
    assert second_report["updated"] == 1
    assert len(rows) == 1
    assert str(rows[0]["vendor_name"]) == "Netflix"
    assert str(rows[0]["frequency"]) == "monthly"
    assert int(rows[0]["amount_cents"]) == 1599
    assert int(rows[0]["is_active"]) == 1


def test_variance_threshold_split_and_is_recurring_flags(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        cat_id = _add_category(conn, "Mixed")
        for txn_date, amount in (("2025-01-05", -1000), ("2025-02-05", -1100), ("2025-03-05", -900)):
            _add_transaction(conn, txn_date=txn_date, description="Stable Service", amount_cents=amount, category_id=cat_id)
        for txn_date, amount in (("2025-01-06", -1000), ("2025-02-06", -1300), ("2025-03-06", -700)):
            _add_transaction(conn, txn_date=txn_date, description="Variable Grocery Box", amount_cents=amount, category_id=cat_id)
        for txn_date, amount in (("2025-01-07", -1000), ("2025-02-07", -2200), ("2025-03-07", -200)):
            _add_transaction(conn, txn_date=txn_date, description="Wild Charge", amount_cents=amount, category_id=cat_id)
        conn.commit()

        report = detect_subscriptions(conn)

        stable_flags = conn.execute(
            "SELECT is_recurring FROM transactions WHERE description = 'Stable Service'"
        ).fetchall()
        medium_flags = conn.execute(
            "SELECT is_recurring FROM transactions WHERE description = 'Variable Grocery Box'"
        ).fetchall()
        high_flags = conn.execute(
            "SELECT is_recurring FROM transactions WHERE description = 'Wild Charge'"
        ).fetchall()

    assert report["detected"] == 2
    assert report["metered_detected"] == 1
    assert report["recurring_patterns"] == 2
    assert report["recurring_txns"] == 6
    assert all(int(row["is_recurring"]) == 1 for row in stable_flags)
    assert all(int(row["is_recurring"]) == 1 for row in medium_flags)
    assert all(int(row["is_recurring"]) == 0 for row in high_flags)


def test_is_recurring_reset_scoped_to_detection_universe(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        cat_id = _add_category(conn, "Utilities")
        recurring_ids = [
            _add_transaction(conn, txn_date="2025-01-10", description="Electric Co", amount_cents=-10000, category_id=cat_id),
            _add_transaction(conn, txn_date="2025-02-10", description="Electric Co", amount_cents=-10000, category_id=cat_id),
        ]
        income_id = _add_transaction(
            conn,
            txn_date="2025-02-15",
            description="Payroll",
            amount_cents=500000,
            category_id=None,
            is_recurring=1,
        )
        conn.commit()

        detect_recurring_patterns(conn)
        first_pass = conn.execute(
            "SELECT id, is_recurring FROM transactions WHERE id IN (?, ?, ?)",
            (recurring_ids[0], recurring_ids[1], income_id),
        ).fetchall()
        assert {int(row["is_recurring"]) for row in first_pass if row["id"] in recurring_ids} == {1}
        assert int(next(row["is_recurring"] for row in first_pass if row["id"] == income_id)) == 1

        conn.execute(
            "UPDATE transactions SET amount_cents = -90000 WHERE id = ?",
            (recurring_ids[1],),
        )
        conn.commit()

        detect_recurring_patterns(conn)
        second_pass = conn.execute(
            "SELECT id, is_recurring FROM transactions WHERE id IN (?, ?, ?)",
            (recurring_ids[0], recurring_ids[1], income_id),
        ).fetchall()

    assert {int(row["is_recurring"]) for row in second_pass if row["id"] in recurring_ids} == {0}
    assert int(next(row["is_recurring"] for row in second_pass if row["id"] == income_id)) == 1


def test_recurring_grouping_works_with_canonical_account_ids(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_account_id, institution_name, account_name, account_type, card_ending, is_active)
            VALUES ('plaid_chase_1234', 'plaid_ext_chase_1234', 'Chase', 'Chase 1234', 'credit_card', '1234', 1)
            """
        )
        cat_id = _add_category(conn, "Streaming")
        for txn_date in ("2025-01-01", "2025-02-01", "2025-03-01"):
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Netflix",
                amount_cents=-1599,
                category_id=cat_id,
                account_id="plaid_chase_1234",
            )
        conn.commit()

        patterns = detect_recurring_patterns(conn)

    netflix = [pattern for pattern in patterns if pattern.vendor_name == "Netflix"]
    assert len(netflix) == 1
    assert netflix[0].account_id == "plaid_chase_1234"
    assert netflix[0].occurrence_count == 3


def test_stale_subscription_cleanup_only_deactivates_auto_detected(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        manual_id = uuid.uuid4().hex
        manual_netflix_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO subscriptions (
                id, vendor_name, category_id, amount_cents, frequency, next_expected, is_active, use_type, is_auto_detected
            ) VALUES (?, 'Manual Vendor', ?, 1200, 'monthly', NULL, 1, 'Personal', 0)
            """,
            (manual_id, streaming),
        )
        conn.execute(
            """
            INSERT INTO subscriptions (
                id, vendor_name, category_id, amount_cents, frequency, next_expected, is_active, use_type, is_auto_detected
            ) VALUES (?, 'Netflix', ?, 9999, 'monthly', NULL, 1, 'Personal', 0)
            """,
            (manual_netflix_id, streaming),
        )

        hulu_ids: list[str] = []
        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=streaming)
            hulu_ids.append(_add_transaction(conn, txn_date=txn_date, description="Hulu", amount_cents=-899, category_id=streaming))
        conn.commit()

        first_report = detect_subscriptions(conn)
        assert first_report["detected"] == 2

        manual_netflix_amount = conn.execute(
            "SELECT amount_cents FROM subscriptions WHERE id = ?",
            (manual_netflix_id,),
        ).fetchone()
        assert int(manual_netflix_amount["amount_cents"]) == 9999

        conn.executemany("UPDATE transactions SET is_active = 0 WHERE id = ?", [(txn_id,) for txn_id in hulu_ids])
        conn.commit()

        second_report = detect_subscriptions(conn)
        assert second_report["deactivated"] >= 1

        auto_rows = conn.execute(
            """
            SELECT vendor_name, is_active
              FROM subscriptions
             WHERE is_auto_detected = 1
            """
        ).fetchall()
        manual_rows = conn.execute(
            """
            SELECT id, is_active
              FROM subscriptions
             WHERE is_auto_detected = 0
            """
        ).fetchall()

    auto_map = {str(row["vendor_name"]): int(row["is_active"]) for row in auto_rows}
    manual_map = {str(row["id"]): int(row["is_active"]) for row in manual_rows}
    assert auto_map["Netflix"] == 1
    assert auto_map["Hulu"] == 0
    assert manual_map[manual_id] == 1
    assert manual_map[manual_netflix_id] == 1


def test_end_to_end_mixed_data_detects_real_subscriptions_only(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        dining = _add_category(conn, "Dining")
        transit = _add_category(conn, "Transit")
        fees = _add_category(conn, "Fees")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=streaming)
            _add_transaction(conn, txn_date=txn_date, description="Zoom", amount_cents=-1499, category_id=streaming)
            _add_transaction(conn, txn_date=txn_date, description="Plan Fee - Installment", amount_cents=-2500, category_id=fees)

        for days_ago in (150, 120, 90):
            _add_transaction(conn, txn_date=_iso_days_ago(days_ago), description="Spotify", amount_cents=-1099, category_id=streaming)
        for days_ago in (60, 30):
            _add_transaction(conn, txn_date=_iso_days_ago(days_ago), description="Spotify USA", amount_cents=-1099, category_id=streaming)

        for days_ago, amount in ((120, -2200), (90, -3500), (60, -1800), (30, -3000)):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Chipotle",
                amount_cents=amount,
                category_id=dining,
            )
        for days_ago, amount in ((90, -500), (60, -900), (30, -300)):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="MTA",
                amount_cents=amount,
                category_id=transit,
            )
        for days_ago, amount in ((60, -1000), (30, -400)):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="MTA NYCT PAYGO",
                amount_cents=amount,
                category_id=transit,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        active_auto = conn.execute(
            """
            SELECT vendor_name
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name
            """
        ).fetchall()

    vendors = [str(row["vendor_name"]) for row in active_auto]
    assert vendors == ["Netflix", "Spotify", "Zoom"]
    assert report["detected"] == 3
    assert report["recurring_patterns"] > report["detected"]


def test_end_to_end_regression_netflix_spotify_zoom_still_detected(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        shopping = _add_category(conn, "Shopping")
        personal_expense = _add_category(conn, "Personal Expense")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(conn, txn_date=txn_date, description="Netflix", amount_cents=-1599, category_id=streaming)
            _add_transaction(conn, txn_date=txn_date, description="Zoom", amount_cents=-1499, category_id=streaming)
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Health & Harmony",
                amount_cents=-6500,
                category_id=streaming,
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Snacks Station Mini Mart",
                amount_cents=-2600,
                category_id=shopping,
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Simplified Cleaner",
                amount_cents=-2800,
                category_id=personal_expense,
            )

        for days_ago in (150, 120, 90):
            _add_transaction(conn, txn_date=_iso_days_ago(days_ago), description="Spotify", amount_cents=-1099, category_id=streaming)
        for days_ago in (60, 30):
            _add_transaction(conn, txn_date=_iso_days_ago(days_ago), description="Spotify USA", amount_cents=-1099, category_id=streaming)
        conn.commit()

        report = detect_subscriptions(conn)
        active_auto = conn.execute(
            """
            SELECT vendor_name
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name
            """
        ).fetchall()

    vendors = [str(row["vendor_name"]) for row in active_auto]
    assert vendors == ["Netflix", "Spotify", "Zoom"]
    assert report["detected"] == 3


def test_cross_account_same_vendor_same_amount_deduped(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        _add_account(conn, "acct_a", "Card A")
        _add_account(conn, "acct_b", "Card B")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Netflix",
                amount_cents=-1599,
                category_id=streaming,
                account_id="acct_a",
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Netflix",
                amount_cents=-1599,
                category_id=streaming,
                account_id="acct_b",
            )
        conn.commit()

        report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT vendor_name, account_id, amount_cents
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name, account_id
            """
        ).fetchall()

    assert report["detected"] == 1
    assert len(rows) == 1
    assert str(rows[0]["vendor_name"]) == "Netflix"
    assert int(rows[0]["amount_cents"]) == 1599
    assert str(rows[0]["account_id"]) in {"acct_a", "acct_b"}


def test_cross_account_same_vendor_different_amounts_preserved(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        _add_account(conn, "acct_a", "Card A")
        _add_account(conn, "acct_b", "Card B")

        for days_ago in (90, 60, 30):
            txn_date = _iso_days_ago(days_ago)
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Netflix",
                amount_cents=-1599,
                category_id=streaming,
                account_id="acct_a",
            )
            _add_transaction(
                conn,
                txn_date=txn_date,
                description="Netflix",
                amount_cents=-2599,
                category_id=streaming,
                account_id="acct_b",
            )
        conn.commit()

        report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT vendor_name, account_id, amount_cents
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY amount_cents
            """
        ).fetchall()

    assert report["detected"] == 2
    assert len(rows) == 2
    assert [int(row["amount_cents"]) for row in rows] == [1599, 2599]
    assert {str(row["account_id"]) for row in rows} == {"acct_a", "acct_b"}


def test_plan_fee_transactions_excluded_from_detection(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        fees = _add_category(conn, "Fees")
        software = _add_category(conn, "Software")
        for days_ago in (90, 60, 30):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Plan Fee - Amex Gold",
                amount_cents=-190,
                category_id=fees,
            )
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Netflix",
                amount_cents=-1599,
                category_id=software,
            )
        conn.commit()

        patterns = detect_recurring_patterns(conn)
        plan_fee_flags = conn.execute(
            "SELECT is_recurring FROM transactions WHERE description LIKE '%Plan Fee%'"
        ).fetchall()

    vendors = [pattern.vendor_name.lower() for pattern in patterns]
    assert "netflix" in vendors
    assert all("plan fee" not in vendor for vendor in vendors)
    assert all(int(row["is_recurring"]) == 0 for row in plan_fee_flags)


def test_metered_subscription_detected_variable_amounts(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        software = _add_category(conn, "Software")
        amounts = [544, 1044, 1544, 2044, 2544]
        for days_ago, amount in zip((150, 120, 90, 60, 30), amounts):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Anthropic API Usage",
                amount_cents=-amount,
                category_id=software,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        row = conn.execute(
            """
            SELECT vendor_name, frequency, amount_cents, sub_type, is_active
              FROM subscriptions
             WHERE vendor_name = 'Anthropic'
               AND is_auto_detected = 1
            """
        ).fetchone()

    assert report["metered_inserted"] == 1
    assert report["metered_updated"] == 0
    assert report["metered_detected"] == 1
    assert row is not None
    assert str(row["frequency"]) == "monthly"
    assert int(row["amount_cents"]) == 1544
    assert str(row["sub_type"]) == "metered"
    assert int(row["is_active"]) == 1


def test_multi_charge_vendor_aggregated_monthly(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        software = _add_category(conn, "Software")
        seats = [22000, 5000, 18000, 9000]
        usage = [3000, 20000, 7000, 16000]
        plan_fees = [190, 180, 170, 160]
        for days_ago, seat_amount, usage_amount, plan_fee_amount in zip((120, 90, 60, 30), seats, usage, plan_fees):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="OPENAI *SEATS",
                amount_cents=-seat_amount,
                category_id=software,
            )
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="OPENAI *USAGE",
                amount_cents=-usage_amount,
                category_id=software,
            )
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="OPENAI *PLAN FEE",
                amount_cents=-plan_fee_amount,
                category_id=software,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT vendor_name, amount_cents, sub_type
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
            """
        ).fetchall()

    assert report["metered_inserted"] == 1
    assert report["detected"] == 1
    assert len(rows) == 1
    assert str(rows[0]["vendor_name"]) == "Openai"
    assert int(rows[0]["amount_cents"]) == 25000
    assert str(rows[0]["sub_type"]) == "metered"


def test_metered_skips_already_fixed_vendors(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        for days_ago in (120, 90, 60, 30):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Netflix",
                amount_cents=-1599,
                category_id=streaming,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT vendor_name, sub_type
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
            """
        ).fetchall()

    assert report["detected"] == 1
    assert report["metered_detected"] == 0
    assert len(rows) == 1
    assert str(rows[0]["vendor_name"]) == "Netflix"
    assert str(rows[0]["sub_type"]) == "fixed"


def test_metered_excluded_categories_filtered(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        shopping = _add_category(conn, "Shopping")
        for days_ago, amount in zip((120, 90, 60, 30), (1500, 2400, 1100, 2800)):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Amazon Marketplace",
                amount_cents=-amount,
                category_id=shopping,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT id
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
            """
        ).fetchall()

    assert report["detected"] == 0
    assert report["metered_detected"] == 0
    assert rows == []


def test_staleness_deactivates_old_subscription(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        software = _add_category(conn, "Software")
        for days_ago in (180, 150, 120):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Cursor Pro",
                amount_cents=-2000,
                category_id=software,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        row = conn.execute(
            """
            SELECT is_active
              FROM subscriptions
             WHERE vendor_name = 'Cursor Pro'
               AND is_auto_detected = 1
            """
        ).fetchone()

    assert report["detected"] == 1
    assert report["deactivated"] >= 1
    assert row is not None
    assert int(row["is_active"]) == 0


def test_staleness_preserves_manual_subscriptions(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        manual_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO subscriptions (
                id, vendor_name, amount_cents, frequency, is_active, is_auto_detected
            ) VALUES (?, 'Manual Legacy Subscription', 2300, 'monthly', 1, 0)
            """,
            (manual_id,),
        )
        conn.commit()

        report = detect_subscriptions(conn)
        manual_row = conn.execute(
            "SELECT is_active FROM subscriptions WHERE id = ?",
            (manual_id,),
        ).fetchone()

    assert report["detected"] == 0
    assert manual_row is not None
    assert int(manual_row["is_active"]) == 1


def test_staleness_threshold_scales_by_frequency(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        software = _add_category(conn, "Software")

        for days_ago in (50, 40, 30):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Dogwalker Service",
                amount_cents=-2500,
                category_id=software,
            )
        for days_ago in (90, 60, 30):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Cloudstorage Plus",
                amount_cents=-999,
                category_id=software,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT frequency, is_active
              FROM subscriptions
             WHERE is_auto_detected = 1
            """
        ).fetchall()

    by_frequency = {str(row["frequency"]): int(row["is_active"]) for row in rows}
    assert report["detected"] == 2
    assert by_frequency["weekly"] == 0
    assert by_frequency["monthly"] == 1


def test_existing_fixed_detection_unchanged(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        streaming = _add_category(conn, "Streaming")
        for days_ago in (90, 60, 30):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Netflix",
                amount_cents=-1599,
                category_id=streaming,
            )
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Spotify",
                amount_cents=-1099,
                category_id=streaming,
            )
        conn.commit()

        report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT vendor_name, sub_type
              FROM subscriptions
             WHERE is_active = 1
               AND is_auto_detected = 1
             ORDER BY vendor_name
            """
        ).fetchall()

    assert report["detected"] == 2
    assert [str(row["vendor_name"]) for row in rows] == ["Netflix", "Spotify"]
    assert all(str(row["sub_type"]) == "fixed" for row in rows)


def test_detect_subscriptions_returns_metered_count(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        software = _add_category(conn, "Software")
        for days_ago, amount in zip((125, 95, 65, 35, 5), (500, 1500, 2500, 3500, 4500)):
            _add_transaction(
                conn,
                txn_date=_iso_days_ago(days_ago),
                description="Anthropic API Usage",
                amount_cents=-amount,
                category_id=software,
            )
        conn.commit()

        first_report = detect_subscriptions(conn)
        second_report = detect_subscriptions(conn)
        rows = conn.execute(
            """
            SELECT sub_type, is_active
              FROM subscriptions
             WHERE vendor_name = 'Anthropic'
               AND is_auto_detected = 1
            """
        ).fetchall()

    assert first_report["metered_inserted"] == 1
    assert first_report["metered_updated"] == 0
    assert first_report["metered_detected"] == 1
    assert second_report["metered_inserted"] == 0
    assert second_report["metered_updated"] == 1
    assert second_report["metered_detected"] == 1
    assert len(rows) == 1
    assert str(rows[0]["sub_type"]) == "metered"
    assert int(rows[0]["is_active"]) == 1
