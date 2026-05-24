from __future__ import annotations

import gzip
import json

import httpx
import pytest

from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.source import (
    BrregBulkClient,
    iter_brreg_bulk_records,
    parse_brreg_bulk_payload,
)


def test_brreg_raw_record_maps_active_payload_to_corpscout_row() -> None:
    payload = {
        "organisasjonsnummer": "810202572",
        "navn": "BORTIGARD AS",
        "konkurs": False,
        "underAvvikling": False,
        "hjemmeside": "https://bortigard.no",
    }

    record = BrregRawRecord.from_payload(payload)
    assert record is not None

    row = record.to_corpscout_row(run_id="dagster-run-1")

    assert row.source_native_id == "810202572"
    assert row.organization_number == "810202572"
    assert row.organization_name == "BORTIGARD AS"
    assert row.registration_status == "active"
    assert row.website == "https://bortigard.no"
    assert row.country_iso2 == "NO"
    assert row.raw_payload == payload
    assert len(row.payload_hash) == 64
    assert row.run_id == "dagster-run-1"


def test_brreg_raw_record_marks_bankrupt_or_liquidating_as_dissolved() -> None:
    bankrupt = BrregRawRecord.from_payload(
        {
            "organisasjonsnummer": "111111111",
            "navn": "BANKRUPT AS",
            "konkurs": True,
        }
    )
    liquidating = BrregRawRecord.from_payload(
        {
            "organisasjonsnummer": "222222222",
            "navn": "LIQUIDATING AS",
            "underAvvikling": True,
        }
    )

    assert bankrupt is not None
    assert liquidating is not None
    assert bankrupt.to_corpscout_row(run_id="run").registration_status == "dissolved"
    assert liquidating.to_corpscout_row(run_id="run").registration_status == "dissolved"


def test_brreg_raw_record_rejects_payload_without_org_number() -> None:
    record = BrregRawRecord.from_payload({"navn": "NO ORG"})

    assert record is None


@pytest.mark.asyncio
async def test_brreg_bulk_client_downloads_gzipped_bulk_file() -> None:
    requests: list[httpx.Request] = []
    payload = gzip.compress(
        json.dumps(
            {
                "_embedded": {
                    "enheter": [
                        {"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"},
                        {"organisasjonsnummer": "", "navn": "INVALID AS"},
                    ]
                }
            }
        ).encode("utf-8")
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/enhetsregisteret/api/enheter/lastned"
        return httpx.Response(200, content=payload)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://data.brreg.no",
    ) as http:
        client = BrregBulkClient(http_client=http)
        records = await client.fetch_records()

    assert [record.organization_number for record in records] == ["810202572"]
    assert requests[0].headers["User-Agent"] == "corpscout-dagster/0.1"


def test_parse_brreg_bulk_payload_accepts_wrapped_payload() -> None:
    payload = gzip.compress(
        json.dumps(
            {
                "_embedded": {
                    "enheter": [
                        {"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"},
                        {"organisasjonsnummer": "910202572", "navn": "NEXT AS"},
                    ]
                }
            }
        ).encode("utf-8")
    )

    records = parse_brreg_bulk_payload(payload)

    assert [record.organization_number for record in records] == ["810202572", "910202572"]


def test_parse_brreg_bulk_payload_accepts_direct_array_payload() -> None:
    payload = gzip.compress(
        json.dumps(
            [
                {"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"},
                {"navn": "INVALID AS"},
            ]
        ).encode("utf-8")
    )

    records = parse_brreg_bulk_payload(payload)

    assert [record.organization_number for record in records] == ["810202572"]


def test_iter_brreg_bulk_records_sync_wrapper_yields_records() -> None:
    class FakeClient:
        async def fetch_records(self):
            return [
                BrregRawRecord.from_payload({"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"}),
                BrregRawRecord.from_payload({"organisasjonsnummer": "910202572", "navn": "NEXT AS"}),
            ]

    records = list(iter_brreg_bulk_records(client=FakeClient()))

    assert [record.organization_number for record in records] == ["810202572", "910202572"]
