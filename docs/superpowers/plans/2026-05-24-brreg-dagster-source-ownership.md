# BRREG Dagster Source Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first BRREG Dagster slice where Dagster pulls original BRREG data directly from BRREG and writes raw rows directly into Corpscout Postgres.

**Architecture:** Keep Dagster in `data-pipelines/dagster` as the BRREG orchestration owner. The first slice implements a BRREG bulk HTTP/dlt source using `https://data.brreg.no/enhetsregisteret/api/enheter/lastned`, maps records into Corpscout raw input rows, and upserts them into `brreg_company_raw_inputs` using direct Postgres access. Corpscout HTTP APIs are not used in this path.

**Tech Stack:** Python 3.12, Dagster, dlt, httpx, psycopg 3, pytest, pytest-asyncio, uv, Postgres.

---

## File Structure

- Delete `services/python-worker/brreg_enhanced_adapter.py`: obsolete Corpscout API adapter from the abandoned direction.
- Delete `services/python-worker/test_brreg_enhanced_adapter.py`: tests for the obsolete API adapter.
- Modify `dagster/pyproject.toml`: set a useful description, keep Dagster/dlt dependencies, add explicit `httpx` and `psycopg[binary]`.
- Modify `dagster/src/corpscout_dagster/__init__.py`: export Dagster definitions and keep the CLI entry point.
- Create `dagster/src/corpscout_dagster/definitions.py`: top-level Dagster `Definitions`.
- Create `dagster/src/corpscout_dagster/brreg/__init__.py`: BRREG package exports.
- Create `dagster/src/corpscout_dagster/brreg/models.py`: BRREG raw record and Corpscout raw input row models.
- Create `dagster/src/corpscout_dagster/brreg/source.py`: direct BRREG bulk HTTP fetcher and dlt resource.
- Create `dagster/src/corpscout_dagster/brreg/writer.py`: Corpscout Postgres writer for `brreg_company_raw_inputs`.
- Create `dagster/src/corpscout_dagster/brreg/assets.py`: Dagster asset that connects source to writer.
- Create `dagster/tests/brreg/test_source.py`: source model, bulk gzip, and bulk payload parsing tests.
- Create `dagster/tests/brreg/test_writer.py`: SQL shape and idempotency tests with a fake cursor.
- Create `dagster/tests/brreg/test_assets.py`: asset orchestration test with fakes.

## Task 1: Remove Obsolete API Adapter Files

**Files:**
- Delete: `services/python-worker/brreg_enhanced_adapter.py`
- Delete: `services/python-worker/test_brreg_enhanced_adapter.py`

- [ ] **Step 1: Remove the abandoned API adapter files**

Use `apply_patch`:

```patch
*** Begin Patch
*** Delete File: /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/python-worker/brreg_enhanced_adapter.py
*** Delete File: /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/python-worker/test_brreg_enhanced_adapter.py
*** End Patch
```

- [ ] **Step 2: Verify only expected untracked Dagster files remain**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git status --short
```

Expected: no `services/python-worker/brreg_enhanced_adapter.py` or `services/python-worker/test_brreg_enhanced_adapter.py` entries.

- [ ] **Step 3: Confirm no commit is needed for untracked cleanup**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git status --short -- services/python-worker/brreg_enhanced_adapter.py services/python-worker/test_brreg_enhanced_adapter.py
```

Expected: no output. These files were untracked, so removing them does not create a commit.

## Task 2: Configure Dagster Package Dependencies

**Files:**
- Modify: `dagster/pyproject.toml`

- [ ] **Step 1: Update `dagster/pyproject.toml`**

Replace the project metadata and dependencies with:

```toml
[project]
name = "corpscout-dagster"
version = "0.1.0"
description = "Dagster pipelines for Corpscout source ingestion and enrichment"
readme = "README.md"
authors = [
    { name = "Goran Raovic", email = "goran.raovic@pulsarpoint.com" }
]
requires-python = ">=3.12"
dependencies = [
    "dagster>=1.13.6",
    "dagster-dg-cli>=1.13.6",
    "dagster-dlt>=0.29.6",
    "dagster-webserver>=1.13.6",
    "dlt[duckdb,sql-database]>=1.27.0",
    "httpx>=0.28.1",
    "psycopg[binary]>=3.2.13",
    "pytest>=9.0.3",
    "pytest-asyncio>=1.3.0",
]

[project.scripts]
corpscout-dagster = "corpscout_dagster:main"

[build-system]
requires = ["uv_build>=0.11.7,<0.12.0"]
build-backend = "uv_build"
```

- [ ] **Step 2: Sync dependencies**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv sync
```

Expected: uv updates the virtualenv and lockfile without errors.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/pyproject.toml dagster/uv.lock
git commit -m "chore: configure corpscout dagster dependencies"
```

## Task 3: Add BRREG Source Models

**Files:**
- Create: `dagster/src/corpscout_dagster/brreg/__init__.py`
- Create: `dagster/src/corpscout_dagster/brreg/models.py`
- Test: `dagster/tests/brreg/test_source.py`

- [ ] **Step 1: Write failing tests for record mapping**

Create `dagster/tests/brreg/test_source.py`:

```python
from __future__ import annotations

from corpscout_dagster.brreg.models import BrregRawRecord


def test_brreg_raw_record_maps_active_payload_to_corpscout_row() -> None:
    payload = {
        "organisasjonsnummer": "810202572",
        "navn": "BORTIGARD AS",
        "konkurs": False,
        "underAvvikling": False,
        "hjemmeside": "https://bortigard.no",
    }

    record = BrregRawRecord.from_payload(payload)
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

    assert bankrupt.to_corpscout_row(run_id="run").registration_status == "dissolved"
    assert liquidating.to_corpscout_row(run_id="run").registration_status == "dissolved"


def test_brreg_raw_record_rejects_payload_without_org_number() -> None:
    record = BrregRawRecord.from_payload({"navn": "NO ORG"})

    assert record is None
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_source.py -q
```

Expected: FAIL because `corpscout_dagster.brreg.models` does not exist.

- [ ] **Step 3: Implement models**

Create `dagster/src/corpscout_dagster/brreg/__init__.py`:

```python
from corpscout_dagster.brreg.models import BrregRawRecord, CorpscoutBrregRawInputRow

__all__ = ["BrregRawRecord", "CorpscoutBrregRawInputRow"]
```

Create `dagster/src/corpscout_dagster/brreg/models.py`:

```python
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CorpscoutBrregRawInputRow:
    source_native_id: str
    organization_number: str
    organization_name: str
    registration_status: str
    website: str | None
    country_iso2: str
    raw_payload: dict[str, Any]
    payload_hash: str
    run_id: str


@dataclass(frozen=True)
class BrregRawRecord:
    payload: dict[str, Any]
    organization_number: str
    organization_name: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BrregRawRecord | None":
        organization_number = str(payload.get("organisasjonsnummer") or "").strip()
        if not organization_number:
            return None
        organization_name = str(payload.get("navn") or "").strip()
        return cls(
            payload=payload,
            organization_number=organization_number,
            organization_name=organization_name,
        )

    def to_corpscout_row(self, *, run_id: str) -> CorpscoutBrregRawInputRow:
        raw_bytes = json.dumps(self.payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return CorpscoutBrregRawInputRow(
            source_native_id=self.organization_number,
            organization_number=self.organization_number,
            organization_name=self.organization_name,
            registration_status=self._registration_status(),
            website=_blank_to_none(self.payload.get("hjemmeside")),
            country_iso2="NO",
            raw_payload=self.payload,
            payload_hash=hashlib.sha256(raw_bytes).hexdigest(),
            run_id=run_id,
        )

    def _registration_status(self) -> str:
        if bool(self.payload.get("konkurs")) or bool(self.payload.get("underAvvikling")):
            return "dissolved"
        return "active"


def _blank_to_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_source.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/brreg dagster/tests/brreg/test_source.py
git commit -m "feat: model brreg raw input rows"
```

## Task 4: Add Direct BRREG Bulk HTTP and dlt Source

**Files:**
- Create: `dagster/src/corpscout_dagster/brreg/source.py`
- Modify: `dagster/tests/brreg/test_source.py`

- [ ] **Step 1: Extend tests for bulk gzip download and payload parsing**

Append to `dagster/tests/brreg/test_source.py`:

```python
import gzip
import json

import httpx
import pytest

from corpscout_dagster.brreg.source import BrregBulkClient, iter_brreg_bulk_records, parse_brreg_bulk_payload


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

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://data.brreg.no") as http:
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

    records = list(iter_brreg_bulk_records(client=FakeClient(), max_records=1))

    assert [record.organization_number for record in records] == ["810202572"]
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_source.py -q
```

Expected: FAIL because `corpscout_dagster.brreg.source` does not exist.

- [ ] **Step 3: Implement source**

Create `dagster/src/corpscout_dagster/brreg/source.py`:

```python
from __future__ import annotations

import asyncio
import gzip
import json
from typing import Iterator, Protocol

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
    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
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
    max_records: int | None = None,
) -> Iterator[BrregRawRecord]:
    records = _run_async(_collect_records(client=client or BrregBulkClient(), max_records=max_records))
    yield from records


@dlt.resource(name="brreg_raw_records", write_disposition="append")
def brreg_raw_records(max_records: int | None = None) -> Iterator[dict]:
    for record in iter_brreg_bulk_records(max_records=max_records):
        yield record.payload


async def _collect_records(
    *,
    client: BrregBulkRecordClient,
    max_records: int | None,
) -> list[BrregRawRecord]:
    records = [record for record in await client.fetch_records() if record is not None]
    if max_records is None:
        return records
    return records[:max_records]


def _run_async(awaitable) -> list[BrregRawRecord]:
    return asyncio.run(awaitable)
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_source.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/brreg/source.py dagster/tests/brreg/test_source.py
git commit -m "feat: add brreg dagster source"
```

## Task 5: Add Corpscout Postgres Writer

**Files:**
- Create: `dagster/src/corpscout_dagster/brreg/writer.py`
- Test: `dagster/tests/brreg/test_writer.py`

- [ ] **Step 1: Write failing writer tests**

Create `dagster/tests/brreg/test_writer.py`:

```python
from __future__ import annotations

from corpscout_dagster.brreg.models import CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.writer import BrregRawInputWriter, UpsertResult


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))


def test_writer_upserts_raw_rows_with_conflict_preserving_existing_state() -> None:
    cursor = FakeCursor()
    writer = BrregRawInputWriter(cursor)
    row = CorpscoutBrregRawInputRow(
        source_native_id="810202572",
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        registration_status="active",
        website="https://bortigard.no",
        country_iso2="NO",
        raw_payload={"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"},
        payload_hash="a" * 64,
        run_id="dagster-run-1",
    )

    result = writer.upsert_many([row])

    assert result == UpsertResult(rows_seen=1, rows_written=1)
    sql, params = cursor.calls[0]
    assert "INSERT INTO brreg_company_raw_inputs" in sql
    assert "ON CONFLICT (organization_number, payload_hash) DO UPDATE" in sql
    assert "last_seen_at = now()" in sql
    assert "run_id = EXCLUDED.run_id" in sql
    assert "state" not in sql
    assert params["organization_number"] == "810202572"
    assert params["raw_payload"] == '{"navn":"BORTIGARD AS","organisasjonsnummer":"810202572"}'


def test_writer_ignores_empty_batches() -> None:
    cursor = FakeCursor()
    writer = BrregRawInputWriter(cursor)

    result = writer.upsert_many([])

    assert result == UpsertResult(rows_seen=0, rows_written=0)
    assert cursor.calls == []
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_writer.py -q
```

Expected: FAIL because `corpscout_dagster.brreg.writer` does not exist.

- [ ] **Step 3: Implement writer**

Create `dagster/src/corpscout_dagster/brreg/writer.py`:

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from corpscout_dagster.brreg.models import CorpscoutBrregRawInputRow


class Cursor(Protocol):
    def execute(self, sql: str, params: dict) -> object:
        ...


@dataclass(frozen=True)
class UpsertResult:
    rows_seen: int
    rows_written: int


class BrregRawInputWriter:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def upsert_many(self, rows: list[CorpscoutBrregRawInputRow]) -> UpsertResult:
        for row in rows:
            self._cursor.execute(
                BRREG_RAW_INPUT_UPSERT_SQL,
                {
                    "source_native_id": row.source_native_id,
                    "organization_number": row.organization_number,
                    "organization_name": row.organization_name,
                    "registration_status": row.registration_status,
                    "website": row.website,
                    "country_iso2": row.country_iso2,
                    "raw_payload": json.dumps(row.raw_payload, sort_keys=True, separators=(",", ":")),
                    "payload_hash": row.payload_hash,
                    "run_id": row.run_id,
                },
            )
        return UpsertResult(rows_seen=len(rows), rows_written=len(rows))


BRREG_RAW_INPUT_UPSERT_SQL = """
INSERT INTO brreg_company_raw_inputs (
    source_native_id,
    organization_number,
    organization_name,
    registration_status,
    website,
    country_iso2,
    raw_payload,
    payload_hash,
    run_id
) VALUES (
    %(source_native_id)s,
    %(organization_number)s,
    %(organization_name)s,
    %(registration_status)s,
    %(website)s,
    %(country_iso2)s,
    %(raw_payload)s::jsonb,
    %(payload_hash)s,
    %(run_id)s
)
ON CONFLICT (organization_number, payload_hash) DO UPDATE
SET
    last_seen_at = now(),
    organization_name = EXCLUDED.organization_name,
    registration_status = EXCLUDED.registration_status,
    website = EXCLUDED.website,
    country_iso2 = EXCLUDED.country_iso2,
    run_id = EXCLUDED.run_id,
    updated_at = now()
"""
```

- [ ] **Step 4: Run writer tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_writer.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/brreg/writer.py dagster/tests/brreg/test_writer.py
git commit -m "feat: write brreg raw inputs to corpscout postgres"
```

## Task 6: Add Dagster Asset and Definitions

**Files:**
- Modify: `dagster/src/corpscout_dagster/__init__.py`
- Create: `dagster/src/corpscout_dagster/definitions.py`
- Create: `dagster/src/corpscout_dagster/brreg/assets.py`
- Test: `dagster/tests/brreg/test_assets.py`

- [ ] **Step 1: Write failing asset test**

Create `dagster/tests/brreg/test_assets.py`:

```python
from __future__ import annotations

from corpscout_dagster.brreg.assets import build_brreg_raw_input_rows
from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.definitions import defs


def test_build_brreg_raw_input_rows_maps_records_with_run_id() -> None:
    records = [
        BrregRawRecord.from_payload({"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"}),
        None,
        BrregRawRecord.from_payload({"organisasjonsnummer": "910202572", "navn": "NEXT AS"}),
    ]

    rows = build_brreg_raw_input_rows(records=records, run_id="dagster-run-1")

    assert [row.organization_number for row in rows] == ["810202572", "910202572"]
    assert {row.run_id for row in rows} == {"dagster-run-1"}


def test_definitions_include_brreg_raw_inputs_asset() -> None:
    asset_keys = {
        key.to_user_string()
        for definition in defs.assets or []
        for key in definition.keys
    }

    assert "brreg_raw_inputs" in asset_keys
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_assets.py -q
```

Expected: FAIL because asset modules do not exist.

- [ ] **Step 3: Implement asset and definitions**

Replace `dagster/src/corpscout_dagster/__init__.py` with:

```python
from corpscout_dagster.definitions import defs

__all__ = ["defs", "main"]


def main() -> None:
    print("corpscout-dagster")
```

Create `dagster/src/corpscout_dagster/definitions.py`:

```python
from __future__ import annotations

from dagster import Definitions

from corpscout_dagster.brreg.assets import brreg_raw_inputs

defs = Definitions(assets=[brreg_raw_inputs])
```

Create `dagster/src/corpscout_dagster/brreg/assets.py`:

```python
from __future__ import annotations

import os
from collections.abc import Iterable

import psycopg
from dagster import AssetExecutionContext, asset

from corpscout_dagster.brreg.models import BrregRawRecord, CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.source import iter_brreg_bulk_records
from corpscout_dagster.brreg.writer import BrregRawInputWriter


def build_brreg_raw_input_rows(
    *,
    records: Iterable[BrregRawRecord | None],
    run_id: str,
) -> list[CorpscoutBrregRawInputRow]:
    return [record.to_corpscout_row(run_id=run_id) for record in records if record is not None]


@asset(name="brreg_raw_inputs")
def brreg_raw_inputs(context: AssetExecutionContext) -> dict[str, int]:
    connection_url = _corpscout_database_url()
    run_id = context.run_id
    max_records = _optional_int_env("BRREG_MAX_RECORDS")
    rows = build_brreg_raw_input_rows(
        records=iter_brreg_bulk_records(max_records=max_records),
        run_id=run_id,
    )
    with psycopg.connect(connection_url) as conn:
        with conn.cursor() as cursor:
            result = BrregRawInputWriter(cursor).upsert_many(rows)
        conn.commit()
    context.add_output_metadata(
        {
            "rows_seen": result.rows_seen,
            "rows_written": result.rows_written,
            "dagster_run_id": run_id,
        }
    )
    return {"rows_seen": result.rows_seen, "rows_written": result.rows_written}


def _corpscout_database_url() -> str:
    value = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("CORPSCOUT_DB_URL")
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL or CORPSCOUT_DB_URL is required")
    return value


def _optional_int_env(name: str) -> int | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return None
    return int(value)
```

- [ ] **Step 4: Run asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_assets.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/__init__.py dagster/src/corpscout_dagster/definitions.py dagster/src/corpscout_dagster/brreg/assets.py dagster/tests/brreg/test_assets.py
git commit -m "feat: add brreg raw input dagster asset"
```

## Task 7: Add README Usage Notes

**Files:**
- Modify: `dagster/README.md`

- [ ] **Step 1: Replace README with operational notes**

Replace `dagster/README.md` with:

```markdown
# Corpscout Dagster

Dagster pipelines for source ingestion and enrichment.

## BRREG Raw Input Ingestion

The first BRREG asset pulls original company records directly from the BRREG bulk endpoint and upserts them into Corpscout Postgres table `brreg_company_raw_inputs`.

Required environment:

```bash
export CORPSCOUT_DATABASE_URL='postgresql://user:password@localhost:5432/corpscout'
```

Optional local limiter:

```bash
export BRREG_MAX_RECORDS=1000
```

Run tests:

```bash
uv run pytest -q
```

Run Dagster UI:

```bash
uv run dagster-webserver -m corpscout_dagster
```

This package does not call Corpscout HTTP APIs for BRREG ingestion. Dagster writes directly to the Corpscout database contract.
```

- [ ] **Step 2: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/README.md
git commit -m "docs: document brreg dagster ingestion"
```

## Task 8: Full Verification

**Files:**
- Verify all Dagster files.

- [ ] **Step 1: Run Dagster tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q
```

Expected: all Dagster tests pass.

- [ ] **Step 2: Run existing Python worker tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/python-worker
pytest -q
```

Expected: existing Python worker tests pass without the removed API adapter test.

- [ ] **Step 3: Check formatting-sensitive whitespace**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git diff --check
```

Expected: no whitespace errors.

- [ ] **Step 4: Inspect final status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git status --short --branch
```

Expected: branch is ahead of origin with only intentional committed changes and no unexpected untracked source files.

## Scope Deferred After This Plan

- Translation assets.
- Domain enrichment assets.
- Financial enrichment assets.
- Enhanced JSON builder.
- Database unpack function invocation.
- Removing old Temporal BRREG pull workflows.
- Corpscout UI changes to trigger Dagster runs.

Those are separate slices after the raw input ingestion asset works.
