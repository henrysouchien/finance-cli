from __future__ import annotations


def test_prompt_chip_select_returns_structured_card_payload() -> None:
    from finance_cli.mcp_server import prompt_chip_select

    result = prompt_chip_select(
        question="What do you do for work?",
        field="user_type",
        options=[
            {"label": "Salaried", "value": "salaried"},
            {"label": "Self-employed", "value": "self_employed"},
        ],
    )

    assert result["data"] == {
        "type": "chip_select",
        "question": "What do you do for work?",
        "field": "user_type",
        "options": [
            {"label": "Salaried", "value": "salaried"},
            {"label": "Self-employed", "value": "self_employed"},
        ],
        "allow_free_text": False,
    }
    assert result["summary"] == {"type": "chip_select", "field": "user_type", "options": 2}


def test_prompt_chip_select_normalizes_string_options() -> None:
    from finance_cli.mcp_server import prompt_chip_select

    result = prompt_chip_select(
        question="Pick a priority",
        field="priority",
        options=["save_more", "pay_down_debt"],
        allow_free_text=True,
    )

    assert result["data"]["options"] == [
        {"label": "Save More", "value": "save_more"},
        {"label": "Pay Down Debt", "value": "pay_down_debt"},
    ]
    assert result["data"]["allow_free_text"] is True
