from __future__ import annotations
import pytest
import respx
import httpx
from unittest.mock import patch, AsyncMock, MagicMock

from contracts import (
    FetchPageInput,
    FetchResult,
    CompanyLookup,
    DiscoverDomainsInput,
    DiscoverDomainsResult,
    DomainDiscovery,
)
from activities.fetch_companies_house_list import fetch_companies_house_list
from activities.discover_company_domains import discover_company_domains


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")


# ── fetch_companies_house_list ────────────────────────────────────────────────

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


# ── discover_company_domains ──────────────────────────────────────────────────

def _mock_signals(ddg=None, wikidata=None, certsh=None, heuristic=None):
    """Return a context-manager stack patching all four signal helpers."""
    import contextlib
    return contextlib.ExitStack(), {
        "activities.discover_company_domains._duckduckgo_signal": AsyncMock(return_value=ddg or []),
        "activities.discover_company_domains._wikidata_signal": AsyncMock(return_value=wikidata or []),
        "activities.discover_company_domains._certsh_signal": AsyncMock(return_value=certsh or []),
        "activities.discover_company_domains._heuristic_signal": AsyncMock(return_value=heuristic or []),
    }


@pytest.mark.asyncio
async def test_discover_company_domains_finds_via_wikidata():
    with patch("activities.discover_company_domains._duckduckgo_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._wikidata_signal", new=AsyncMock(return_value=[("acme.co.uk", "wikidata", 85)])), \
         patch("activities.discover_company_domains._certsh_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._heuristic_signal", new=AsyncMock(return_value=[])):
        result = await discover_company_domains(DiscoverDomainsInput(
            source="companies_house",
            country="GB",
            companies=[CompanyLookup(native_id="12345678", name="ACME LIMITED")],
        ))

    assert len(result.discoveries) == 1
    d = result.discoveries[0]
    assert d.domain == "acme.co.uk"
    assert d.signal == "wikidata"
    assert d.confidence == 85
    assert d.native_id == "12345678"


@pytest.mark.asyncio
async def test_discover_company_domains_finds_via_certsh():
    with patch("activities.discover_company_domains._duckduckgo_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._wikidata_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._certsh_signal", new=AsyncMock(return_value=[("acme.co.uk", "certsh", 60)])), \
         patch("activities.discover_company_domains._heuristic_signal", new=AsyncMock(return_value=[])):
        result = await discover_company_domains(DiscoverDomainsInput(
            source="companies_house",
            country="GB",
            companies=[CompanyLookup(native_id="12345678", name="ACME LIMITED")],
        ))

    assert len(result.discoveries) == 1
    d = result.discoveries[0]
    assert d.domain == "acme.co.uk"
    assert d.signal == "certsh"
    assert d.confidence == 60


@pytest.mark.asyncio
async def test_discover_company_domains_finds_via_duckduckgo():
    with patch("activities.discover_company_domains._duckduckgo_signal", new=AsyncMock(return_value=[("acme.co.uk", "duckduckgo", 70)])), \
         patch("activities.discover_company_domains._wikidata_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._certsh_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._heuristic_signal", new=AsyncMock(return_value=[])):
        result = await discover_company_domains(DiscoverDomainsInput(
            source="companies_house",
            country="GB",
            companies=[CompanyLookup(native_id="12345678", name="ACME LIMITED")],
        ))

    assert len(result.discoveries) == 1
    assert result.discoveries[0].signal == "duckduckgo"
    assert result.discoveries[0].confidence == 70


@pytest.mark.asyncio
async def test_discover_company_domains_deduplicates_across_signals():
    """Same domain from multiple signals keeps only the first occurrence."""
    with patch("activities.discover_company_domains._duckduckgo_signal", new=AsyncMock(return_value=[("acme.co.uk", "duckduckgo", 70)])), \
         patch("activities.discover_company_domains._wikidata_signal", new=AsyncMock(return_value=[("acme.co.uk", "wikidata", 85)])), \
         patch("activities.discover_company_domains._certsh_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._heuristic_signal", new=AsyncMock(return_value=[])):
        result = await discover_company_domains(DiscoverDomainsInput(
            source="companies_house",
            country="GB",
            companies=[CompanyLookup(native_id="12345678", name="ACME LIMITED")],
        ))

    assert len(result.discoveries) == 1
    assert result.discoveries[0].domain == "acme.co.uk"


@pytest.mark.asyncio
async def test_discover_company_domains_empty_when_no_signals_match():
    with patch("activities.discover_company_domains._duckduckgo_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._wikidata_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._certsh_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._heuristic_signal", new=AsyncMock(return_value=[])):
        result = await discover_company_domains(DiscoverDomainsInput(
            source="companies_house",
            country="GB",
            companies=[CompanyLookup(native_id="12345678", name="ACME LIMITED")],
        ))

    assert result.discoveries == []


@pytest.mark.asyncio
async def test_discover_company_domains_multiple_companies():
    async def mock_ddg(name, country):
        if name == "ACME LIMITED":
            return [("acme.co.uk", "duckduckgo", 70)]
        return []

    with patch("activities.discover_company_domains._duckduckgo_signal", new=AsyncMock(side_effect=mock_ddg)), \
         patch("activities.discover_company_domains._wikidata_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._certsh_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._heuristic_signal", new=AsyncMock(return_value=[])), \
         patch("asyncio.sleep"):
        result = await discover_company_domains(DiscoverDomainsInput(
            source="companies_house",
            country="GB",
            companies=[
                CompanyLookup(native_id="12345678", name="ACME LIMITED"),
                CompanyLookup(native_id="87654321", name="GLOBEX LTD"),
            ],
        ))

    assert len(result.discoveries) == 1
    assert result.discoveries[0].native_id == "12345678"
    assert result.discoveries[0].domain == "acme.co.uk"


@pytest.mark.asyncio
async def test_discover_company_domains_continues_when_signal_raises():
    """A signal that raises an exception is skipped; other signals still contribute."""
    with patch("activities.discover_company_domains._duckduckgo_signal", new=AsyncMock(side_effect=RuntimeError("network error"))), \
         patch("activities.discover_company_domains._wikidata_signal", new=AsyncMock(return_value=[("acme.co.uk", "wikidata", 85)])), \
         patch("activities.discover_company_domains._certsh_signal", new=AsyncMock(return_value=[])), \
         patch("activities.discover_company_domains._heuristic_signal", new=AsyncMock(return_value=[])):
        result = await discover_company_domains(DiscoverDomainsInput(
            source="companies_house",
            country="GB",
            companies=[CompanyLookup(native_id="12345678", name="ACME LIMITED")],
        ))

    assert len(result.discoveries) == 1
    assert result.discoveries[0].signal == "wikidata"
