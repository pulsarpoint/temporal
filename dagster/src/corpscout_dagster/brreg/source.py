from __future__ import annotations

import asyncio
import gzip
import json
from collections.abc import Iterator
from typing import Protocol

import dlt
import httpx

from corpscout_dagster.brreg.models import BrregRawRecord

BRREG_API_BASE_URL = "https://data.brreg.no"
BRREG_BULK_PATH = "/enhetsregisteret/api/enheter/lastned"
USER_AGENT = "corpscout-dagster/0.1"


class BrregBulkRecordClient(Protocol):
    async def fetch_records(self) -> list[BrregRawRecord | None]:
        ...


class BrregBulkClient:
    def __init__(self, *, http_client: httpx.AsyncClient | None = None) -> None:
        self._http_client = http_client

    async def fetch_records(self) -> list[BrregRawRecord]:
        if self._http_client is None:
            async with httpx.AsyncClient(base_url=BRREG_API_BASE_URL, timeout=600.0) as client:
                return await self._fetch_records(client)
        return await self._fetch_records(self._http_client)

    async def _fetch_records(self, client: httpx.AsyncClient) -> list[BrregRawRecord]:
        response = await client.get(
            BRREG_BULK_PATH,
            headers={"Accept": "*/*", "User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        response.raise_for_status()
        return parse_brreg_bulk_payload(response.content)


def parse_brreg_bulk_payload(content: bytes) -> list[BrregRawRecord]:
    data = json.loads(gzip.decompress(content).decode("utf-8"))
    if isinstance(data, dict):
        entities = (data.get("_embedded") or {}).get("enheter") or []
    elif isinstance(data, list):
        entities = data
    else:
        entities = []
    return [
        record
        for item in entities
        if isinstance(item, dict) and (record := BrregRawRecord.from_payload(item)) is not None
    ]


def iter_brreg_bulk_records(
    *,
    client: BrregBulkRecordClient | None = None,
) -> Iterator[BrregRawRecord]:
    records = _run_async(_collect_records(client=client or BrregBulkClient()))
    yield from records


@dlt.resource(name="brreg_raw_records", write_disposition="append")
def brreg_raw_records() -> Iterator[dict]:
    for record in iter_brreg_bulk_records():
        yield record.payload


async def _collect_records(*, client: BrregBulkRecordClient) -> list[BrregRawRecord]:
    return [record for record in await client.fetch_records() if record is not None]


def _run_async(awaitable) -> list[BrregRawRecord]:
    return asyncio.run(awaitable)
