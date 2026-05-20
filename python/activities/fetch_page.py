from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import httpx
from temporalio import activity

from contracts import FetchPageInput, FetchResult, RawRecord

_USER_AGENT = "corpscout-data-pipelines/1.0"
_CH_ENDPOINT = "https://api.company-information.service.gov.uk/advanced-search/companies"
_PAGE_SIZE = 100


@activity.defn
async def fetch_page(input: FetchPageInput) -> FetchResult:
    if input.source != "companies_house":
        raise ValueError(f"unsupported source: {input.source}")
    return await _fetch_companies_house(input)


async def _fetch_companies_house(input: FetchPageInput) -> FetchResult:
    api_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
    if not api_key:
        raise RuntimeError("COMPANIES_HOUSE_API_KEY is not set")

    # cursor format: "YYYY-MM-DD,N" where N is 0-indexed page offset
    date_cursor: str | None = None
    page_offset = 0
    if input.cursor and "," in input.cursor:
        parts = input.cursor.split(",", 1)
        date_cursor = parts[0] or None
        try:
            page_offset = int(parts[1])
        except ValueError:
            page_offset = 0

    start_index = page_offset * _PAGE_SIZE
    params: dict[str, Any] = {
        "size": str(_PAGE_SIZE),
        "start_index": str(start_index),
        "company_status": "active",
    }
    if date_cursor:
        params["incorporated_from"] = date_cursor

    async with httpx.AsyncClient(timeout=30.0, auth=(api_key, "")) as client:
        resp = await client.get(
            _CH_ENDPOINT,
            params=params,
            headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
        )
        resp.raise_for_status()
        data = resp.json()

    items: list[dict] = data.get("items") or []
    records: list[RawRecord] = []
    for item in items:
        raw_bytes = json.dumps(item, sort_keys=True).encode()
        digest = hashlib.sha256(raw_bytes).hexdigest()
        records.append(RawRecord(
            native_id=item.get("company_number", ""),
            name=item.get("company_name", ""),
            status=item.get("company_status", "active"),
            company_type=item.get("company_type", ""),
            raw_json=item,
            hash=digest,
        ))

    total_results: int = data.get("total_results", 0)
    has_more = (page_offset + 1) * _PAGE_SIZE < total_results
    next_cursor = ""
    if has_more:
        date_part = date_cursor or ""
        next_cursor = f"{date_part},{page_offset + 1}"

    return FetchResult(records=records, has_more=has_more, next_cursor=next_cursor)
