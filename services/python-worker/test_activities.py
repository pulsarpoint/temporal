from __future__ import annotations
import pytest
import respx
import httpx

from contracts import FetchPageInput, FetchResult
from activities.fetch_companies_house_list import fetch_companies_house_list


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")


@respx.mock
@pytest.mark.asyncio
async def test_fetch_companies_house_list_returns_records():
    mock_response = {
        "total_results": 1,
        "items": [
            {
                "company_number": "12345678",
                "company_name": "ACME LIMITED",
                "company_status": "active",
                "company_type": "ltd",
            }
        ],
    }
    respx.get("https://api.company-information.service.gov.uk/advanced-search/companies").mock(
        return_value=httpx.Response(200, json=mock_response)
    )

    result = await fetch_companies_house_list(FetchPageInput(source="companies_house", country="GB", page=1))

    assert isinstance(result, FetchResult)
    assert len(result.records) == 1
    assert result.records[0].native_id == "12345678"
    assert result.records[0].name == "ACME LIMITED"
    assert result.records[0].status == "active"
    assert result.has_more is False


@respx.mock
@pytest.mark.asyncio
async def test_fetch_companies_house_list_has_more_when_more_results():
    items = [
        {"company_number": f"0000000{i}", "company_name": f"CO {i}", "company_status": "active", "company_type": "ltd"}
        for i in range(100)
    ]
    mock_response = {"total_results": 250, "items": items}
    respx.get("https://api.company-information.service.gov.uk/advanced-search/companies").mock(
        return_value=httpx.Response(200, json=mock_response)
    )

    result = await fetch_companies_house_list(FetchPageInput(source="companies_house", country="GB", page=1))
    assert result.has_more is True
    assert result.next_cursor == ",1"
