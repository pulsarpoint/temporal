from __future__ import annotations

import os

import httpx
from temporalio import activity

from contracts import CompanyDetailResult, FetchCompanyDetailInput

_USER_AGENT = "corpscout-data-pipelines/1.0"
_CH_BASE = "https://api.company-information.service.gov.uk"


@activity.defn(name="fetch_companies_house_detail")
async def fetch_companies_house_detail(input: FetchCompanyDetailInput) -> CompanyDetailResult:
    api_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
    if not api_key:
        raise RuntimeError("COMPANIES_HOUSE_API_KEY is not set")

    url = f"{_CH_BASE}/company/{input.native_id}"
    async with httpx.AsyncClient(timeout=15.0, auth=(api_key, "")) as client:
        resp = await client.get(url, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
        resp.raise_for_status()
        data = resp.json()

    addr = data.get("registered_office_address") or {}

    def _opt(val: str | None) -> str | None:
        return val if val else None

    return CompanyDetailResult(
        native_id=data.get("company_number", input.native_id),
        name=data.get("company_name", ""),
        status=data.get("company_status", ""),
        type=data.get("company_type", ""),
        date_of_creation=data.get("date_of_creation", ""),
        address_line_1=_opt(addr.get("address_line_1")),
        address_line_2=_opt(addr.get("address_line_2")),
        locality=_opt(addr.get("locality")),
        postal_code=_opt(addr.get("postal_code")),
        country=_opt(addr.get("country")),
        region=_opt(addr.get("region")),
        sic_codes=data.get("sic_codes") or [],
    )
