from __future__ import annotations
import pytest
import respx
import httpx

from contracts import FetchPageInput, FetchResult, FetchCompanyDetailInput, CompanyDetailResult
from activities.fetch_companies_house_list import fetch_companies_house_list
from activities.fetch_companies_house_detail import fetch_companies_house_detail


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


@respx.mock
@pytest.mark.asyncio
async def test_fetch_companies_house_detail_returns_profile():
    mock_response = {
        "company_number": "12345678",
        "company_name": "ACME LIMITED",
        "company_status": "active",
        "company_type": "ltd",
        "date_of_creation": "2010-01-15",
        "registered_office_address": {
            "address_line_1": "10 High Street",
            "locality": "London",
            "postal_code": "EC1A 1BB",
            "country": "England",
        },
        "sic_codes": ["62020"],
    }
    respx.get("https://api.company-information.service.gov.uk/company/12345678").mock(
        return_value=httpx.Response(200, json=mock_response)
    )

    result = await fetch_companies_house_detail(
        FetchCompanyDetailInput(source="companies_house", native_id="12345678")
    )

    assert isinstance(result, CompanyDetailResult)
    assert result.native_id == "12345678"
    assert result.name == "ACME LIMITED"
    assert result.status == "active"
    assert result.type == "ltd"
    assert result.date_of_creation == "2010-01-15"
    assert result.address_line_1 == "10 High Street"
    assert result.locality == "London"
    assert result.postal_code == "EC1A 1BB"
    assert result.country == "England"
    assert result.address_line_2 is None
    assert result.sic_codes == ["62020"]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_companies_house_detail_missing_address_fields():
    mock_response = {
        "company_number": "99999999",
        "company_name": "MINIMAL CO",
        "company_status": "active",
        "registered_office_address": {},
    }
    respx.get("https://api.company-information.service.gov.uk/company/99999999").mock(
        return_value=httpx.Response(200, json=mock_response)
    )

    result = await fetch_companies_house_detail(
        FetchCompanyDetailInput(source="companies_house", native_id="99999999")
    )

    assert result.native_id == "99999999"
    assert result.address_line_1 is None
    assert result.locality is None
    assert result.sic_codes == []
