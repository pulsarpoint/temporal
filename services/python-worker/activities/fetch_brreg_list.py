from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

import httpx
from temporalio import activity

from contracts import FetchPageInput, FetchResult, RawRecord

_USER_AGENT = "corpscout-data-pipelines/1.0"
_BRREG_ENDPOINT = "https://data.brreg.no/enhetsregisteret/api/enheter"
_PAGE_SIZE = 200
# Brreg API rejects size*(page+1) > 10_000. With size=200 that means pages 0–49 safe.
_BRREG_MAX_PAGE = 49


@activity.defn(name="fetch_brreg_list")
async def fetch_brreg_list(input: FetchPageInput) -> FetchResult:
    # cursor format: "YYYY-MM-DD,N" — registration date bucket + 0-indexed page offset
    date_cursor: str | None = None
    page_offset = 0
    if input.cursor and "," in input.cursor:
        parts = input.cursor.split(",", 1)
        date_cursor = parts[0] or None
        try:
            page_offset = int(parts[1])
        except ValueError:
            page_offset = 0

    params: dict = {
        "page": str(page_offset),
        "size": str(_PAGE_SIZE),
        "sort": "registreringsdatoEnhetsregisteret,asc",
    }
    if date_cursor:
        params["fraRegistreringsdatoEnhetsregisteret"] = date_cursor

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(
            _BRREG_ENDPOINT,
            params=params,
            headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
        )
        resp.raise_for_status()
        data = resp.json()

    embedded = (data.get("_embedded") or {}).get("enheter") or []
    records: list[RawRecord] = []
    for item in embedded:
        org_num = str(item.get("organisasjonsnummer") or "")
        if not org_num:
            continue
        konkurs = bool(item.get("konkurs"))
        under_avvikling = bool(item.get("underAvvikling"))
        status = "dissolved" if (konkurs or under_avvikling) else "active"

        raw_bytes = json.dumps(item, sort_keys=True).encode()
        digest = hashlib.sha256(raw_bytes).hexdigest()
        records.append(RawRecord(
            native_id=org_num,
            name=str(item.get("navn") or ""),
            status=status,
            company_type=str(item.get("organisasjonsform", {}).get("kode") or ""),
            raw_json=item,
            hash=digest,
        ))

    page_info = data.get("page") or {}
    total_pages = int(page_info.get("totalPages") or 0)
    current = int(page_info.get("number") or page_offset)
    has_more = (current + 1) < total_pages

    next_cursor = ""
    if has_more:
        if current < _BRREG_MAX_PAGE:
            next_cursor = f"{date_cursor or ''},{current + 1}"
        else:
            # Exhausted all pages for this date bucket — advance to the next day to avoid
            # re-fetching the same bucket infinitely when many records share the same date.
            last_date = (embedded[-1].get("registreringsdatoEnhetsregisteret") or "") if embedded else ""
            if last_date:
                next_date = (date.fromisoformat(last_date) + timedelta(days=1)).isoformat()
                next_cursor = f"{next_date},0"

    return FetchResult(records=records, has_more=has_more, next_cursor=next_cursor)
