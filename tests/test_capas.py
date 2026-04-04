from typing import Any

import pytest

from mcp_server.database.utils.capabilities import (
    EMPTY_RETURN,
    DatabaseCapabilities,
    Structured,
)

db = DatabaseCapabilities()

INPUTS_SEARCHPRODUCTS: list[dict[str, Any]] = [
    {
        "case": "search_product with search_text='Vogel'",
        "inputs": {"search_text": "Vogel"},
    },
    {
        "case": "search_product with search_text='Vogelfutter'",
        "inputs": {"search_text": "Vogelfutter"},
    },
    {
        "case": "search_product with search_text='Käfig'",
        "inputs": {"search_text": "Käfig"},
    },
    {
        "case": "search_product with search_text='Ohrentropfen Vögel'",
        "inputs": {"search_text": "Ohrentropfen Vögel"},
    },
    {
        "case": "search_product with product_id=1",
        "inputs": {"product_id": 1},
    },
    {
        "case": "search_product with category_id='1'",
        "inputs": {"category_id": "15"},
        "expect_error": True
    },
]


@pytest.mark.parametrize(
    ("case_description", "inputs", "expect_error"),
    [(case["case"], case["inputs"], case.get("expect_error", False)) for case in INPUTS_SEARCHPRODUCTS],
)
def test_search_product_returns_at_least_one_row(
    case_description: str,
    inputs: dict[str, Any],
    expect_error: bool,
) -> None:
    """Test that search_product returns a Structured result with at least one row."""
    result = db.search_product(**inputs)

    assert isinstance(result, Structured), f"{case_description}: result is not Structured"
    assert isinstance(result.data, dict), f"{case_description}: result.data is not a dict"
    assert len(result.data) >= 1, f"{case_description}: result.data is empty"
    if expect_error:
        assert result.data == {"1": EMPTY_RETURN}, f"{case_description}: expected an error but got real rows"
    else:
        assert result.data != {"1": EMPTY_RETURN}, f"{case_description}: query returned no real rows"


INPUTS_SEARCH_CUSTOMERS: list[dict[str, Any]] = [
    {
        "case": "search_customer with search_text='John'",
        "inputs": {"search_text": "John"},
    },
    {
        "case": "search_customer with search_text='Max Mustermann'",
        "inputs": {"search_text": "Max Mustermann"},
    },
    {
        "case": "search_customer with city='Berlin'",
        "inputs": {"city": "Berlin"},
    },
    {
        "case": "search_customer with search_text='Sophia' and city='Leipzig'",
        "inputs": {"search_text": "Sophia", "city": "Leipzig"},
    },
    {
        "case": "search_customer with customer_id=1",
        "inputs": {"customer_id": 1},
    },
    {
        "case": "search_customer with customer_id=100000",
        "inputs": {"customer_id": 100000},
        "expect_error": True
    },
]


@pytest.mark.parametrize(
    ("case_description", "inputs", "expect_error"),
    [(case["case"], case["inputs"], case.get("expect_error", False)) for case in INPUTS_SEARCH_CUSTOMERS],
)
def test_search_customer_returns_real_result_rows(
    case_description: str,
    inputs: dict[str, Any],
    expect_error: bool,
) -> None:
    """Test that search_customer returns at least one real result row."""
    result = db.search_customer(**inputs)

    assert isinstance(result, Structured), f"{case_description}: result is not Structured"
    assert isinstance(result.data, dict), f"{case_description}: result.data is not a dict"
    assert len(result.data) >= 1, f"{case_description}: result.data is empty"
    if expect_error:
        assert result.data == {"1": EMPTY_RETURN}, f"{case_description}: expected no real rows but got real rows"
    else:
        assert result.data != {"1": EMPTY_RETURN}, f"{case_description}: query returned no real rows"