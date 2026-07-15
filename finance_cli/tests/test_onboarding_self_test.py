from __future__ import annotations

import importlib
from datetime import date, timedelta
from pathlib import Path

from finance_cli.db import initialize_database
from finance_cli.importers.normalizers import reset_normalizer_loader_cache


def _load_isolated_mcp(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.setenv("FINANCE_CLI_HOME", str(tmp_path / ".finance_cli"))
    monkeypatch.delenv("FINANCE_CLI_DATA_DIR", raising=False)
    monkeypatch.delenv("FINANCE_CLI_NORMALIZER_DIR", raising=False)
    initialize_database(db_path)
    reset_normalizer_loader_cache()

    import finance_cli.mcp_server as mcp_server

    module = importlib.reload(mcp_server)
    reset_normalizer_loader_cache()
    return module


def _write_current_relative_csvs(tmp_path: Path) -> list[Path]:
    today = date.today()
    day_7 = (today - timedelta(days=7)).isoformat()
    day_10 = (today - timedelta(days=10)).isoformat()
    day_14 = (today - timedelta(days=14)).isoformat()
    day_21 = (today - timedelta(days=21)).isoformat()
    day_25 = (today - timedelta(days=25)).isoformat()
    filename_date = today.strftime("%Y%m%d")

    apple = tmp_path / "apple_card_current.csv"
    apple.write_text(
        "\n".join(
            [
                "Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD)",
                f"{day_7},{day_7},COFFEE SHOP,COFFEE SHOP,Dining,Purchase,12.50",
                (
                    f"{day_10},{day_10},APPLE.COM/BILL,APPLE.COM/BILL,"
                    "Software & Subscriptions,Purchase,9.99"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    chase = tmp_path / f"Chase9999_Activity{filename_date}_{filename_date}_{filename_date}.CSV"
    chase.write_text(
        "\n".join(
            [
                "Transaction Date,Post Date,Description,Category,Type,Amount,Memo",
                f"{day_7},{day_7},GROCERY STORE,Groceries,Sale,-82.10,",
                f"{day_14},{day_14},DELTA AIR LINES,Travel,Sale,-210.00,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    bofa = tmp_path / "stmt.csv"
    bofa.write_text(
        "\n".join(
            [
                "Summary Amt.",
                "Date,Description,Amount,Running Bal.",
                f"{day_21},ACME PAYROLL,3200.00,5200.00",
                f"{day_7},ACME PAYROLL,3200.00,6700.00",
                f"{day_10},RENT PAYMENT,-1500.00,3700.00",
                f"{day_25},UTILITY COMPANY,-120.00,3580.00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    return [apple, chase, bofa]


def _assert_checkpoint(mcp, expected: str | None) -> dict:
    detected = mcp.onboarding_detect()
    assert detected["data"]["resume_checkpoint"] == expected
    return detected


def _set_onboarding_state(mcp, **state):
    result = mcp.skill_state_set("onboarding", state)
    assert result["summary"]["updated"] is True
    return result


def _memory_with_self_test_section(
    existing: str,
    *,
    user_type: str,
    user_type_path: str,
    priority: str,
    priority_path: str,
    completed: bool,
) -> str:
    section_lines = [
        "## Onboarding Self-Test",
        f"- Type: {user_type} (path: {user_type_path})",
        f"- Priority: {priority} (path: {priority_path})",
        f"- Last run: {date.today().isoformat()}" + (" (completed)" if completed else ""),
    ]
    section = "\n".join(section_lines)
    marker = "## Onboarding Self-Test"
    if marker not in existing:
        return f"{existing.rstrip()}\n\n{section}\n".lstrip()
    before, _, rest = existing.partition(marker)
    after = rest.split("\n## ", 1)
    suffix = f"\n## {after[1]}" if len(after) == 2 else ""
    return f"{before.rstrip()}\n\n{section}\n{suffix}".lstrip()


def test_onboarding_self_test_mcp_happy_path_and_resume_branches(tmp_path: Path, monkeypatch) -> None:
    mcp = _load_isolated_mcp(tmp_path, monkeypatch)
    csv_paths = _write_current_relative_csvs(tmp_path)

    initial = _assert_checkpoint(mcp, None)
    assert initial["data"]["is_new_user"] is True
    assert initial["summary"]["next_step"] == "connect"

    setup = mcp.setup_init(dry_run=False)
    assert setup["summary"]["categories_created"] >= 30
    assert mcp.cat_list()["summary"]["total_categories"] >= 30

    inserted = 0
    for csv_path in csv_paths:
        detected = mcp.normalizer_detect(file=str(csv_path))
        assert detected["summary"]["detected"] is True
        import_result = mcp.ingest_csv(
            file=str(csv_path),
            institution=detected["data"]["institution"],
            commit=True,
        )
        assert import_result["data"]["errors"] == 0
        assert import_result["data"]["inserted"] >= 2
        inserted += import_result["data"]["inserted"]

    status = mcp.db_status()
    assert status["data"]["transaction_counts"]["active"] == inserted == 8
    assert status["data"]["active_account_count"] >= 3

    assert mcp.dedup_backfill_aliases(commit=True)["summary"]["total_scanned"] >= 3
    dedup = mcp.dedup_cross_format(dry_run=False)
    assert {"total_matches", "total_removed", "key_only_count"} <= set(dedup["summary"])

    assert mcp.cat_auto_categorize(dry_run=False)["data"]["updated"] >= 0
    assert mcp.cat_normalize(dry_run=False)["data"]["dry_run"] is False
    assert mcp.txn_list(uncategorized=True, limit=20)["data"]["transactions"]

    for query, category in [
        ("%COFFEE%", "Dining"),
        ("%GROCERY%", "Groceries"),
        ("%DELTA%", "Travel"),
        ("%RENT%", "Rent"),
        ("%UTILITY%", "Utilities"),
    ]:
        categorized = mcp.txn_bulk_categorize(query=query, category=category, remember=True)
        assert categorized["data"]["updated"] >= 1
        assert categorized["data"]["remembered_count"] >= 1

    status = mcp.db_status()
    assert status["data"]["uncategorized_count"] == 0
    assert mcp.cat_memory_list(limit=10)["summary"]["total_rules"] >= 5

    _set_onboarding_state(
        mcp,
        phase="connect",
        data_minimal_acknowledged=True,
    )
    assert mcp.agent_session_write(
        content="onboarding:data_connected onboarding:first_categorization"
    )["data"]["ok"] is True
    _assert_checkpoint(mcp, "profile")

    accounts = mcp.account_list()
    assert accounts["summary"]["active_accounts"] >= 3
    transactions = mcp.txn_list(limit=20)["data"]["transactions"]
    payroll = [row for row in transactions if row["amount"] > 0 and "PAYROLL" in row["description"]]
    assert len(payroll) == 2
    assert {row["amount"] for row in payroll} == {3200.0}
    assert mcp.spending_trends(months=3)["summary"]["total_categories"] >= 5
    assert mcp.subs_total()["summary"]["total_subscriptions"] >= 0
    assert mcp.debt_dashboard()["summary"]["total_cards"] == 0

    user_type = "salaried"
    user_type_path = "two same-amount payroll deposits"
    income_stability = "steady"
    priority = "spending_clarity"
    priority_path = "no debt dashboard liabilities"
    _set_onboarding_state(
        mcp,
        phase="profile",
        data_minimal_acknowledged=True,
        user_type=user_type,
        income_stability=income_stability,
    )
    existing_memory = mcp.agent_memory_read()["data"]["content"]
    memory = _memory_with_self_test_section(
        existing_memory,
        user_type=user_type,
        user_type_path=user_type_path,
        priority=priority,
        priority_path=priority_path,
        completed=False,
    )
    assert mcp.agent_memory_update(content=memory)["data"]["ok"] is True
    assert mcp.agent_session_write(
        content=(
            f"onboarding:profile user_type={user_type}, "
            f"income_stability={income_stability}. Self-test run."
        )
    )["data"]["ok"] is True
    assert "Onboarding Self-Test" in mcp.agent_memory_read()["data"]["content"]
    _assert_checkpoint(mcp, "focus")

    _set_onboarding_state(
        mcp,
        phase="focus",
        data_minimal_acknowledged=True,
        user_type=user_type,
        income_stability=income_stability,
        priority=priority,
    )
    assert mcp.agent_session_write(
        content=f"onboarding:focus priority={priority}. Self-test run."
    )["data"]["ok"] is True
    _assert_checkpoint(mcp, "setup")

    summary = mcp.financial_summary()
    for key in ["net_worth", "income_30d", "expense_30d", "savings_rate", "uncategorized"]:
        assert key in summary["data"]
    assert summary["data"]["income_30d"] > 0
    assert summary["data"]["expense_30d"] > 0
    assert summary["data"]["savings_rate"] is not None
    assert "accounts" in mcp.balance_show()["data"]
    assert "net_worth" in mcp.balance_net_worth()["data"]
    assert "projected_net" in mcp.liquidity()["data"]
    assert mcp.subs_detect()["data"]["detected"] >= 0
    assert "subscriptions" in mcp.subs_list(limit=20)["data"]

    assert mcp.agent_session_write(
        content="onboarding:setup_context financial summary shown. Self-test run."
    )["data"]["ok"] is True
    _assert_checkpoint(mcp, "setup")

    budget_suggestions = mcp.budget_suggest()
    assert isinstance(budget_suggestions["data"]["suggestions"], list)
    for category, amount in [("Dining", 400), ("Shopping", 150), ("Entertainment", 100)]:
        budget = mcp.budget_set(category=category, amount=amount, period="monthly")
        assert budget["data"]["action"] in {"created", "updated"}
        assert budget["data"]["amount"] == amount
    budgets = mcp.budget_list()["data"]["budgets"]
    budget_categories = {row["category_name"] for row in budgets}
    assert {"Dining", "Shopping", "Entertainment"} <= budget_categories

    goal_name = f"Self-test emergency fund {date.today().isoformat()}"
    goal = mcp.goal_set(
        name=goal_name,
        metric="liquid_cash",
        target=5000,
        direction="up",
        deadline="2026-12-31",
    )
    assert goal["data"]["goal"]["name"] == goal_name
    assert any(row["name"] == goal_name for row in mcp.goal_list()["data"]["goals"])

    assert mcp.agent_session_write(
        content=(
            "onboarding:budgets_set Dining $400, Shopping $150, Entertainment $100. "
            f"onboarding:goals_set {goal_name} $5k by 2026-12-31. Self-test run."
        )
    )["data"]["ok"] is True
    _assert_checkpoint(mcp, "setup")

    business_account = next(
        row for row in mcp.account_list()["data"]["accounts"] if row["institution"] == "Bank of America"
    )
    business_flag = mcp.account_set_business(
        id=business_account["id"],
        is_business=True,
        backfill=True,
    )
    assert business_flag["data"]["new_is_business"] == 1
    assert business_flag["data"]["backfill"]["transactions_updated"] >= 4

    split_rule = mcp.rules_add_split(
        match_category="Software & Subscriptions",
        business_pct=50,
        business_category="Software & Subscriptions",
        personal_category="Software & Subscriptions",
        note="Self-test: 50% business use",
    )
    assert split_rule["data"]["rule"]["business_pct"] == 50
    assert split_rule["data"]["split_rule_count"] >= 1

    keyword_rule = mcp.rules_add_keyword(
        keyword="COWORKING",
        category="Office Expense",
        use_type="Business",
        priority=0,
    )
    assert keyword_rule["data"]["action"] in {"added", "appended"}
    rules = mcp.rules_show()
    assert any(
        row["match"]["category"] == "Software & Subscriptions"
        for row in rules["data"]["split_rules"]
    )

    split_apply = mcp.cat_apply_splits(commit=True, backfill=True, summary_only=False)
    assert split_apply["data"]["candidate_transactions"] >= 1
    assert split_apply["data"]["created_children"] >= 2
    assert mcp.cat_classify_use_type(commit=True)["data"]["updated"] >= 0

    current_year = date.today().year
    tax_setup = mcp.biz_tax_setup(
        year=str(current_year),
        method="simplified",
        sqft=150,
        total_sqft=800,
        filing_status="single",
        state="NY",
    )
    assert tax_setup["data"]["tax_year"] == current_year
    assert tax_setup["data"]["config"]["home_office_method"] == "simplified"
    estimated_tax = mcp.biz_estimated_tax(year=current_year)
    assert estimated_tax["data"]["year"] == current_year
    assert {
        "federal_tax_cents",
        "se_tax_cents",
    } <= set(estimated_tax["data"]["components_cents"])
    profit_loss = mcp.biz_pl(year=str(current_year))
    assert profit_loss["data"]["period"]["label"] == str(current_year)
    assert "net_income_cents" in profit_loss["data"]
    forecast = mcp.biz_forecast(months=3, streams=True)
    assert forecast["data"]["months"] == 3
    assert mcp.agent_session_write(
        content=(
            f"onboarding:business_setup account {business_account['id']} flagged as business. "
            "Split rule: Software & Subscriptions 50% business. "
            "Keyword: COWORKING -> Office Expense (Business). "
            "Tax: simplified, NY, single. Self-test run."
        )
    )["data"]["ok"] is True

    _set_onboarding_state(
        mcp,
        phase="setup",
        data_minimal_acknowledged=True,
        user_type=user_type,
        income_stability=income_stability,
        priority=priority,
        setup_acknowledged=True,
    )
    assert mcp.agent_session_write(
        content="onboarding:setup_acknowledged starter setup reviewed. Self-test run."
    )["data"]["ok"] is True
    _assert_checkpoint(mcp, None)

    final_summary = mcp.financial_summary()
    assert "debt_to_income" in final_summary["data"]
    completed_memory = _memory_with_self_test_section(
        mcp.agent_memory_read()["data"]["content"],
        user_type=user_type,
        user_type_path=user_type_path,
        priority=priority,
        priority_path=priority_path,
        completed=True,
    )
    assert mcp.agent_memory_update(content=completed_memory)["data"]["ok"] is True
    assert mcp.agent_session_write(
        content=(
            "onboarding:complete Self-test completed all 4 phases. "
            "Budgets: Dining $400, Shopping $150, Entertainment $100. "
            f"Goal: {goal_name} $5k by 2026-12-31."
        )
    )["data"]["ok"] is True
    complete_state = {
        "phase": "complete",
        "complete": True,
        "data_minimal_acknowledged": True,
        "user_type": user_type,
        "income_stability": income_stability,
        "priority": priority,
        "setup_acknowledged": True,
    }
    _set_onboarding_state(mcp, **complete_state)
    completed = _assert_checkpoint(mcp, None)
    assert completed["data"]["is_onboarding_complete"] is True
    assert mcp.agent_session_search(query="onboarding:complete")["data"]["count"] >= 1
    assert "(completed)" in mcp.agent_memory_read()["data"]["content"]

    branch_states = [
        ({"data_minimal_acknowledged": True}, "profile"),
        ({"data_minimal_acknowledged": True, "user_type": user_type}, "profile"),
        (
            {
                "data_minimal_acknowledged": True,
                "user_type": user_type,
                "income_stability": income_stability,
            },
            "focus",
        ),
        (
            {
                "data_minimal_acknowledged": True,
                "user_type": user_type,
                "income_stability": income_stability,
                "priority": priority,
            },
            "setup",
        ),
        (
            {
                "data_minimal_acknowledged": True,
                "user_type": user_type,
                "income_stability": income_stability,
                "priority": priority,
                "setup_acknowledged": True,
            },
            None,
        ),
    ]
    for state, checkpoint in branch_states:
        _set_onboarding_state(mcp, **state)
        branch = _assert_checkpoint(mcp, checkpoint)
        assert branch["data"]["is_new_user"] is False

    _set_onboarding_state(mcp, **complete_state)
    assert _assert_checkpoint(mcp, None)["data"]["is_onboarding_complete"] is True
