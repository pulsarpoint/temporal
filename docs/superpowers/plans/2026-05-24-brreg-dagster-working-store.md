# BRREG Dagster Working Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor BRREG Dagster raw ingestion so it persists BRREG working data in `dagster_brreg` tables instead of writing raw rows directly to Corpscout source tables.

**Architecture:** Add a Dagster-owned Postgres schema setup script, Python repository methods for the first working-store tables, and a renamed `brreg_working_raw_records` asset. The existing Corpscout raw writer remains in the codebase for the future publish step, but the extraction asset and smoke check move to the working store.

**Tech Stack:** Python 3.12, Dagster, psycopg 3, PostgreSQL, pytest, uv, Docker Compose.

---

## File Structure

- Create `dagster/sql/000001_dagster_brreg_working_store.sql`: idempotent schema/table/index setup for `dagster_brreg`.
- Create `dagster/src/corpscout_dagster/db.py`: shared database URL lookup and SQL script execution helpers.
- Create `dagster/src/corpscout_dagster/brreg/working_store.py`: repository and dataclasses for `enrichment_runs`, `bulk_snapshots`, and `raw_records`.
- Create `dagster/tests/brreg/test_working_store_schema.py`: verifies the schema script contains required tables, constraints, and indexes.
- Create `dagster/tests/brreg/test_working_store.py`: repository unit tests with fake cursor/connection.
- Modify `dagster/src/corpscout_dagster/brreg/assets.py`: rename/write raw asset to `brreg_working_raw_records`.
- Modify `dagster/src/corpscout_dagster/definitions.py`: register `brreg_working_raw_records`.
- Modify `dagster/tests/brreg/test_assets.py`: assert new asset key and working-store calls.
- Modify `dagster/src/corpscout_dagster/brreg/smoke.py`: smoke test writes to `dagster_brreg.raw_records` in a rollback transaction.
- Modify `dagster/tests/brreg/test_smoke.py`: update expected SQL/select behavior for working store.
- Modify `dagster/Makefile`: add `setup-db` and update `smoke-brreg-db`.
- Modify `dagster/README.md`: document setup and working-store smoke.
- Modify `dagster/Dockerfile`: copy `sql/` into the runtime image.

## Task 1: Add Working Store Schema Script

**Files:**
- Create: `dagster/sql/000001_dagster_brreg_working_store.sql`
- Test: `dagster/tests/brreg/test_working_store_schema.py`

- [ ] **Step 1: Write failing schema tests**

Create `dagster/tests/brreg/test_working_store_schema.py`:

```python
from __future__ import annotations

from pathlib import Path


SCHEMA_SQL = Path(__file__).parents[2] / "sql" / "000001_dagster_brreg_working_store.sql"


def test_schema_creates_required_dagster_brreg_tables() -> None:
    sql = SCHEMA_SQL.read_text()

    assert "CREATE SCHEMA IF NOT EXISTS dagster_brreg" in sql
    for table_name in [
        "enrichment_runs",
        "bulk_snapshots",
        "raw_records",
        "task_attempts",
        "translation_results",
        "domain_candidates",
        "financial_results",
        "enhanced_records",
    ]:
        assert f"CREATE TABLE IF NOT EXISTS dagster_brreg.{table_name}" in sql


def test_schema_has_raw_record_idempotency_and_queue_indexes() -> None:
    sql = SCHEMA_SQL.read_text()

    assert "UNIQUE (organization_number, payload_hash)" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dagster_brreg_raw_records_org" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_attempts_queue" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dagster_brreg_enhanced_publish_queue" in sql
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_working_store_schema.py -q
```

Expected: FAIL with `FileNotFoundError` for `000001_dagster_brreg_working_store.sql`.

- [ ] **Step 3: Add schema SQL**

Create `dagster/sql/000001_dagster_brreg_working_store.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS dagster_brreg;

CREATE TABLE IF NOT EXISTS dagster_brreg.enrichment_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dagster_run_id TEXT NOT NULL UNIQUE,
  run_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_completed INTEGER NOT NULL DEFAULT 0,
  records_failed INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_runs_type CHECK (
    run_type IN ('bulk_ingest', 'full_enrichment', 'retry_failed', 'publish')
  ),
  CONSTRAINT chk_brreg_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_brreg_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE IF NOT EXISTS dagster_brreg.bulk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  enrichment_run_id UUID NOT NULL REFERENCES dagster_brreg.enrichment_runs(id) ON DELETE CASCADE,
  source_url TEXT NOT NULL,
  downloaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  content_length_bytes BIGINT,
  compressed_payload_hash TEXT,
  storage_uri TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_bulk_snapshot_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_brreg_bulk_snapshot_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE IF NOT EXISTS dagster_brreg.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES dagster_brreg.bulk_snapshots(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  organization_number TEXT NOT NULL,
  organization_name TEXT,
  registration_status TEXT,
  website TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'NO',
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_working_source_native CHECK (source_native_id = organization_number),
  CONSTRAINT chk_brreg_working_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_brreg_working_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (organization_number, payload_hash)
);

CREATE TABLE IF NOT EXISTS dagster_brreg.task_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  enrichment_run_id UUID NOT NULL REFERENCES dagster_brreg.enrichment_runs(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_type TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  worker_id TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_task_attempt_type CHECK (
    task_type IN ('parse_raw', 'translate', 'discover_domains', 'extract_financials', 'build_enhanced', 'publish')
  ),
  CONSTRAINT chk_brreg_task_attempt_status CHECK (
    status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_brreg_task_attempt_attempt CHECK (attempt > 0),
  CONSTRAINT chk_brreg_task_attempt_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, task_type, attempt)
);

CREATE TABLE IF NOT EXISTS dagster_brreg.translation_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  translated_payload JSONB,
  model TEXT,
  prompt_version TEXT,
  fx_source TEXT,
  fx_rate_date DATE,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_translation_status CHECK (
    status IN ('succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_brreg_translation_payload_object CHECK (
    translated_payload IS NULL OR jsonb_typeof(translated_payload) = 'object'
  ),
  CONSTRAINT chk_brreg_translation_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE IF NOT EXISTS dagster_brreg.domain_candidates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  signal TEXT NOT NULL,
  confidence SMALLINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate',
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_domain_candidate_confidence CHECK (confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_brreg_domain_candidate_status CHECK (
    status IN ('candidate', 'accepted', 'rejected', 'failed')
  ),
  CONSTRAINT chk_brreg_domain_candidate_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_domain_candidate_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, normalized_domain, signal)
);

CREATE TABLE IF NOT EXISTS dagster_brreg.financial_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  fiscal_year INTEGER,
  status TEXT NOT NULL,
  original_currency TEXT,
  financial_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  usd_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_uri TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_financial_status CHECK (
    status IN ('succeeded', 'failed', 'not_available', 'skipped')
  ),
  CONSTRAINT chk_brreg_financial_payload_object CHECK (jsonb_typeof(financial_payload) = 'object'),
  CONSTRAINT chk_brreg_financial_usd_payload_object CHECK (jsonb_typeof(usd_payload) = 'object'),
  CONSTRAINT chk_brreg_financial_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object')
);

CREATE TABLE IF NOT EXISTS dagster_brreg.enhanced_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  schema_version TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'built',
  enhanced_payload JSONB NOT NULL,
  enhanced_payload_hash TEXT NOT NULL,
  corpscout_raw_input_id UUID,
  corpscout_enhanced_raw_input_id UUID,
  corpscout_source_company_id UUID,
  built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_enhanced_status CHECK (
    status IN ('built', 'published', 'publish_failed', 'superseded')
  ),
  CONSTRAINT chk_brreg_enhanced_payload_object CHECK (jsonb_typeof(enhanced_payload) = 'object'),
  CONSTRAINT chk_brreg_enhanced_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, schema_version, enhanced_payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_raw_records_org
  ON dagster_brreg.raw_records (organization_number);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_raw_records_hash
  ON dagster_brreg.raw_records (payload_hash);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_attempts_queue
  ON dagster_brreg.task_attempts (task_type, status, started_at)
  WHERE status IN ('queued', 'running', 'failed');

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_attempts_raw
  ON dagster_brreg.task_attempts (raw_record_id, task_type, attempt DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_translation_latest
  ON dagster_brreg.translation_results (raw_record_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_candidates_raw
  ON dagster_brreg.domain_candidates (raw_record_id, confidence DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_financial_results_raw
  ON dagster_brreg.financial_results (raw_record_id, fiscal_year DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_enhanced_publish_queue
  ON dagster_brreg.enhanced_records (status, built_at)
  WHERE status IN ('built', 'publish_failed');
```

- [ ] **Step 4: Run schema tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_working_store_schema.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/sql/000001_dagster_brreg_working_store.sql dagster/tests/brreg/test_working_store_schema.py
git commit -m "feat: add brreg working store schema"
```

## Task 2: Add Schema Installer

**Files:**
- Create: `dagster/src/corpscout_dagster/db.py`
- Test: `dagster/tests/test_db.py`
- Modify: `dagster/Makefile`
- Modify: `dagster/README.md`
- Modify: `dagster/Dockerfile`

- [ ] **Step 1: Write failing installer tests**

Create `dagster/tests/test_db.py`:

```python
from __future__ import annotations

import pytest

from corpscout_dagster.db import (
    database_url_from_env,
    load_sql_file,
    run_sql_script,
)


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True


def test_database_url_from_env_prefers_corpscout_database_url() -> None:
    value = database_url_from_env(
        {
            "CORPSCOUT_DATABASE_URL": "postgresql://primary",
            "DATABASE_URL": "postgresql://fallback",
        }
    )

    assert value == "postgresql://primary"


def test_database_url_from_env_raises_when_missing() -> None:
    with pytest.raises(RuntimeError, match="CORPSCOUT_DATABASE_URL"):
        database_url_from_env({})


def test_load_sql_file_reads_packaged_schema() -> None:
    sql = load_sql_file("000001_dagster_brreg_working_store.sql")

    assert "CREATE SCHEMA IF NOT EXISTS dagster_brreg" in sql


def test_run_sql_script_executes_and_commits() -> None:
    conn = FakeConnection()

    run_sql_script(conn, "SELECT 1;")

    assert conn.cursor_instance.executed == ["SELECT 1;"]
    assert conn.committed is True
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/test_db.py -q
```

Expected: FAIL because `corpscout_dagster.db` does not exist.

- [ ] **Step 3: Implement installer helpers**

Create `dagster/src/corpscout_dagster/db.py`:

```python
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

import psycopg


SQL_DIR = Path(__file__).parents[2] / "sql"


def database_url_from_env(env: Mapping[str, str] | None = None) -> str:
    values = env if env is not None else os.environ
    value = (
        values.get("CORPSCOUT_DATABASE_URL")
        or values.get("CORPSCOUT_DB_URL")
        or values.get("DATABASE_URL")
    )
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL, CORPSCOUT_DB_URL, or DATABASE_URL is required")
    return value


def load_sql_file(name: str) -> str:
    return (SQL_DIR / name).read_text()


def run_sql_script(conn, sql: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()


def install_schema(database_url: str | None = None) -> None:
    url = database_url or database_url_from_env()
    with psycopg.connect(url) as conn:
        run_sql_script(conn, load_sql_file("000001_dagster_brreg_working_store.sql"))


def main() -> None:
    install_schema()
    print("Dagster BRREG working store schema installed")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Update Makefile and README**

Modify `dagster/Makefile`:

```make
.PHONY: sync test validate setup-db smoke-brreg-db webserver daemon build up run down logs logs-webserver logs-daemon shell

setup-db:
	uv run python -m corpscout_dagster.db
```

Keep the existing targets unchanged except for adding `setup-db` to `.PHONY`.

Modify `dagster/README.md` by adding:

```markdown
Install or update the Dagster BRREG working-store schema:

```bash
make setup-db
```
```

Modify `dagster/Dockerfile` so the image contains the schema SQL:

```dockerfile
COPY sql ./sql
```

Place it after `COPY workspace.yaml ./` and before `COPY src ./src`.

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/test_db.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/db.py dagster/tests/test_db.py dagster/Makefile dagster/README.md
git commit -m "feat: add dagster schema installer"
```

## Task 3: Add BRREG Working Store Repository

**Files:**
- Create: `dagster/src/corpscout_dagster/brreg/working_store.py`
- Test: `dagster/tests/brreg/test_working_store.py`

- [ ] **Step 1: Write failing repository tests**

Create `dagster/tests/brreg/test_working_store.py`:

```python
from __future__ import annotations

from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | tuple | None]] = []
        self.return_values: list[tuple] = []

    def execute(self, sql: str, params=None) -> None:
        self.calls.append((sql, params))

    def fetchone(self):
        return self.return_values.pop(0)


def test_create_enrichment_run_returns_existing_or_inserted_id() -> None:
    cursor = FakeCursor()
    cursor.return_values.append(("run-id",))
    store = BrregWorkingStore(cursor)

    run_id = store.create_enrichment_run(
        CreateEnrichmentRun(
            dagster_run_id="dagster-run-1",
            run_type="bulk_ingest",
            metadata={"source": "test"},
        )
    )

    sql, params = cursor.calls[0]
    assert run_id == "run-id"
    assert "INSERT INTO dagster_brreg.enrichment_runs" in sql
    assert "ON CONFLICT (dagster_run_id) DO UPDATE" in sql
    assert params["dagster_run_id"] == "dagster-run-1"
    assert params["metadata"] == '{"source":"test"}'


def test_create_bulk_snapshot_returns_id() -> None:
    cursor = FakeCursor()
    cursor.return_values.append(("snapshot-id",))
    store = BrregWorkingStore(cursor)

    snapshot_id = store.create_bulk_snapshot(
        CreateBulkSnapshot(
            enrichment_run_id="run-id",
            source_url="https://data.brreg.no/enhetsregisteret/api/enheter/lastned",
            content_length_bytes=123,
            compressed_payload_hash="abc",
            metadata={"download": "mock"},
        )
    )

    sql, params = cursor.calls[0]
    assert snapshot_id == "snapshot-id"
    assert "INSERT INTO dagster_brreg.bulk_snapshots" in sql
    assert params["content_length_bytes"] == 123
    assert params["metadata"] == '{"download":"mock"}'


def test_upsert_raw_records_writes_to_working_store() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)
    record = BrregRawRecord.from_payload(
        {
            "organisasjonsnummer": "810202572",
            "navn": "BORTIGARD AS",
            "hjemmeside": "https://bortigard.no",
        }
    )
    assert record is not None
    row = record.to_corpscout_row(run_id="dagster-run-1")

    result = store.upsert_raw_records(bulk_snapshot_id="snapshot-id", rows=[row])

    assert result.rows_seen == 1
    assert result.rows_written == 1
    sql, params = cursor.calls[0]
    assert "INSERT INTO dagster_brreg.raw_records" in sql
    assert "ON CONFLICT (organization_number, payload_hash) DO UPDATE" in sql
    assert params["bulk_snapshot_id"] == "snapshot-id"
    assert params["organization_number"] == "810202572"
    assert params["raw_payload"] == '{"hjemmeside":"https://bortigard.no","navn":"BORTIGARD AS","organisasjonsnummer":"810202572"}'
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_working_store.py -q
```

Expected: FAIL because `corpscout_dagster.brreg.working_store` does not exist.

- [ ] **Step 3: Implement repository**

Create `dagster/src/corpscout_dagster/brreg/working_store.py`:

```python
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from corpscout_dagster.brreg.models import CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.writer import UpsertResult


class Cursor(Protocol):
    def execute(self, sql: str, params: dict | tuple | None = None) -> object:
        ...

    def fetchone(self):
        ...


@dataclass(frozen=True)
class CreateEnrichmentRun:
    dagster_run_id: str
    run_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CreateBulkSnapshot:
    enrichment_run_id: str
    source_url: str
    content_length_bytes: int | None
    compressed_payload_hash: str | None
    storage_uri: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class BrregWorkingStore:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def create_enrichment_run(self, params: CreateEnrichmentRun) -> str:
        self._cursor.execute(
            """
            INSERT INTO dagster_brreg.enrichment_runs (
                dagster_run_id,
                run_type,
                metadata
            ) VALUES (
                %(dagster_run_id)s,
                %(run_type)s,
                %(metadata)s::jsonb
            )
            ON CONFLICT (dagster_run_id) DO UPDATE
            SET metadata = dagster_brreg.enrichment_runs.metadata || EXCLUDED.metadata
            RETURNING id::text
            """,
            {
                "dagster_run_id": params.dagster_run_id,
                "run_type": params.run_type,
                "metadata": _json(params.metadata),
            },
        )
        return self._cursor.fetchone()[0]

    def create_bulk_snapshot(self, params: CreateBulkSnapshot) -> str:
        self._cursor.execute(
            """
            INSERT INTO dagster_brreg.bulk_snapshots (
                enrichment_run_id,
                source_url,
                content_length_bytes,
                compressed_payload_hash,
                storage_uri,
                metadata
            ) VALUES (
                %(enrichment_run_id)s::uuid,
                %(source_url)s,
                %(content_length_bytes)s,
                %(compressed_payload_hash)s,
                %(storage_uri)s,
                %(metadata)s::jsonb
            )
            RETURNING id::text
            """,
            {
                "enrichment_run_id": params.enrichment_run_id,
                "source_url": params.source_url,
                "content_length_bytes": params.content_length_bytes,
                "compressed_payload_hash": params.compressed_payload_hash,
                "storage_uri": params.storage_uri,
                "metadata": _json(params.metadata),
            },
        )
        return self._cursor.fetchone()[0]

    def upsert_raw_records(
        self,
        *,
        bulk_snapshot_id: str | None,
        rows: list[CorpscoutBrregRawInputRow],
    ) -> UpsertResult:
        for row in rows:
            self._cursor.execute(
                WORKING_RAW_RECORD_UPSERT_SQL,
                {
                    "bulk_snapshot_id": bulk_snapshot_id,
                    "source_native_id": row.source_native_id,
                    "organization_number": row.organization_number,
                    "organization_name": row.organization_name,
                    "registration_status": row.registration_status,
                    "website": row.website,
                    "country_iso2": row.country_iso2,
                    "raw_payload": _json(row.raw_payload),
                    "payload_hash": row.payload_hash,
                    "metadata": _json({"dagster_run_id": row.run_id}),
                },
            )
        return UpsertResult(rows_seen=len(rows), rows_written=len(rows))


WORKING_RAW_RECORD_UPSERT_SQL = """
INSERT INTO dagster_brreg.raw_records (
    bulk_snapshot_id,
    source_native_id,
    organization_number,
    organization_name,
    registration_status,
    website,
    country_iso2,
    raw_payload,
    payload_hash,
    metadata
) VALUES (
    %(bulk_snapshot_id)s::uuid,
    %(source_native_id)s,
    %(organization_number)s,
    %(organization_name)s,
    %(registration_status)s,
    %(website)s,
    %(country_iso2)s,
    %(raw_payload)s::jsonb,
    %(payload_hash)s,
    %(metadata)s::jsonb
)
ON CONFLICT (organization_number, payload_hash) DO UPDATE
SET
    bulk_snapshot_id = COALESCE(EXCLUDED.bulk_snapshot_id, dagster_brreg.raw_records.bulk_snapshot_id),
    last_seen_at = now(),
    organization_name = EXCLUDED.organization_name,
    registration_status = EXCLUDED.registration_status,
    website = EXCLUDED.website,
    country_iso2 = EXCLUDED.country_iso2,
    metadata = dagster_brreg.raw_records.metadata || EXCLUDED.metadata
"""


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
```

- [ ] **Step 4: Run repository tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_working_store.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/brreg/working_store.py dagster/tests/brreg/test_working_store.py
git commit -m "feat: add brreg working store repository"
```

## Task 4: Refactor Raw Asset To Working Store

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/assets.py`
- Modify: `dagster/src/corpscout_dagster/definitions.py`
- Modify: `dagster/tests/brreg/test_assets.py`

- [ ] **Step 1: Replace asset tests**

Update `dagster/tests/brreg/test_assets.py`:

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


def test_definitions_include_brreg_working_raw_records_asset() -> None:
    asset_keys = {
        key.to_user_string()
        for definition in defs.assets or []
        for key in definition.keys
    }

    assert "brreg_working_raw_records" in asset_keys
    assert "brreg_raw_inputs" not in asset_keys
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_assets.py -q
```

Expected: FAIL because definitions still expose `brreg_raw_inputs`.

- [ ] **Step 3: Refactor asset**

Replace `dagster/src/corpscout_dagster/brreg/assets.py` with:

```python
from __future__ import annotations

from collections.abc import Iterable

import psycopg
from dagster import asset

from corpscout_dagster.brreg.models import BrregRawRecord, CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.source import BRREG_API_BASE_URL, BRREG_BULK_PATH, iter_brreg_bulk_records
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
)
from corpscout_dagster.db import database_url_from_env


def build_brreg_raw_input_rows(
    *,
    records: Iterable[BrregRawRecord | None],
    run_id: str,
) -> list[CorpscoutBrregRawInputRow]:
    return [record.to_corpscout_row(run_id=run_id) for record in records if record is not None]


@asset(name="brreg_working_raw_records")
def brreg_working_raw_records(context) -> dict[str, int]:
    connection_url = database_url_from_env()
    rows = build_brreg_raw_input_rows(
        records=iter_brreg_bulk_records(),
        run_id=context.run_id,
    )
    with psycopg.connect(connection_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=context.run_id,
                    run_type="bulk_ingest",
                    metadata={"asset": "brreg_working_raw_records"},
                )
            )
            snapshot_id = store.create_bulk_snapshot(
                CreateBulkSnapshot(
                    enrichment_run_id=run_id,
                    source_url=f"{BRREG_API_BASE_URL}{BRREG_BULK_PATH}",
                    content_length_bytes=None,
                    compressed_payload_hash=None,
                    metadata={"record_count": len(rows)},
                )
            )
            result = store.upsert_raw_records(bulk_snapshot_id=snapshot_id, rows=rows)
        conn.commit()
    context.add_output_metadata(
        {
            "rows_seen": result.rows_seen,
            "rows_written": result.rows_written,
            "dagster_run_id": context.run_id,
        }
    )
    return {"rows_seen": result.rows_seen, "rows_written": result.rows_written}
```

Update `dagster/src/corpscout_dagster/definitions.py`:

```python
from __future__ import annotations

from dagster import Definitions

from corpscout_dagster.brreg.assets import brreg_working_raw_records

defs = Definitions(assets=[brreg_working_raw_records])
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
git add dagster/src/corpscout_dagster/brreg/assets.py dagster/src/corpscout_dagster/definitions.py dagster/tests/brreg/test_assets.py
git commit -m "feat: write brreg raw asset to working store"
```

## Task 5: Move Smoke Check To Working Store

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/smoke.py`
- Modify: `dagster/tests/brreg/test_smoke.py`
- Modify: `dagster/Makefile`
- Modify: `dagster/README.md`

- [ ] **Step 1: Update smoke tests**

Replace `dagster/tests/brreg/test_smoke.py` with:

```python
from __future__ import annotations

from corpscout_dagster.brreg.smoke import SMOKE_ORG_NUMBER, build_smoke_row, run_smoke


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.return_values = [("run-id",), ("snapshot-id",), ("CORPSCOUT DAGSTER SMOKE AS", "dagster-smoke")]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self.calls.append((sql, params))

    def fetchone(self):
        return self.return_values.pop(0)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_build_smoke_row_uses_stable_payload_and_run_id() -> None:
    row = build_smoke_row(run_id="dagster-smoke")

    assert row.organization_number == SMOKE_ORG_NUMBER
    assert row.source_native_id == SMOKE_ORG_NUMBER
    assert row.organization_name == "CORPSCOUT DAGSTER SMOKE AS"
    assert row.run_id == "dagster-smoke"
    assert row.raw_payload["organisasjonsnummer"] == SMOKE_ORG_NUMBER


def test_run_smoke_upserts_working_record_verifies_and_rolls_back() -> None:
    connection = FakeConnection()

    result = run_smoke(
        "postgresql://example.invalid/corpscout",
        connection_factory=lambda _: connection,
    )

    assert result.organization_number == SMOKE_ORG_NUMBER
    assert result.rolled_back is True
    assert connection.committed is True
    assert connection.rolled_back is True
    executed_sql = "\n".join(sql for sql, _ in connection.cursor_instance.calls)
    assert "INSERT INTO dagster_brreg.enrichment_runs" in executed_sql
    assert "INSERT INTO dagster_brreg.bulk_snapshots" in executed_sql
    assert "INSERT INTO dagster_brreg.raw_records" in executed_sql
    assert "SELECT organization_name, metadata ->> 'dagster_run_id'" in executed_sql
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_smoke.py -q
```

Expected: FAIL because smoke still queries `brreg_company_raw_inputs`.

- [ ] **Step 3: Update smoke implementation**

Replace `dagster/src/corpscout_dagster/brreg/smoke.py` with:

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

import psycopg

from corpscout_dagster.brreg.models import BrregRawRecord, CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
)
from corpscout_dagster.db import database_url_from_env, load_sql_file, run_sql_script

SMOKE_ORG_NUMBER = "999999991"
SMOKE_RUN_ID = "dagster-smoke"
SMOKE_NAME = "CORPSCOUT DAGSTER SMOKE AS"


@dataclass(frozen=True)
class SmokeResult:
    organization_number: str
    payload_hash: str
    rolled_back: bool


def build_smoke_row(*, run_id: str = SMOKE_RUN_ID) -> CorpscoutBrregRawInputRow:
    record = BrregRawRecord.from_payload(
        {
            "organisasjonsnummer": SMOKE_ORG_NUMBER,
            "navn": SMOKE_NAME,
            "konkurs": False,
            "underAvvikling": False,
            "corpscout_smoke": True,
        }
    )
    if record is None:
        raise RuntimeError("invalid BRREG smoke payload")
    return record.to_corpscout_row(run_id=run_id)


def run_smoke(
    database_url: str,
    *,
    connection_factory: Callable = psycopg.connect,
) -> SmokeResult:
    row = build_smoke_row()
    with connection_factory(database_url) as conn:
        run_sql_script(conn, load_sql_file("000001_dagster_brreg_working_store.sql"))
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=SMOKE_RUN_ID,
                    run_type="bulk_ingest",
                    metadata={"smoke": True},
                )
            )
            snapshot_id = store.create_bulk_snapshot(
                CreateBulkSnapshot(
                    enrichment_run_id=run_id,
                    source_url="smoke://brreg",
                    content_length_bytes=None,
                    compressed_payload_hash=None,
                    metadata={"smoke": True},
                )
            )
            store.upsert_raw_records(bulk_snapshot_id=snapshot_id, rows=[row])
            cursor.execute(
                """
                SELECT organization_name, metadata ->> 'dagster_run_id'
                FROM dagster_brreg.raw_records
                WHERE organization_number = %s
                  AND payload_hash = %s
                """,
                (row.organization_number, row.payload_hash),
            )
            found = cursor.fetchone()
            if found != (SMOKE_NAME, SMOKE_RUN_ID):
                raise RuntimeError("BRREG working-store smoke row was not readable after upsert")
        conn.rollback()
    return SmokeResult(
        organization_number=row.organization_number,
        payload_hash=row.payload_hash,
        rolled_back=True,
    )


def main() -> None:
    result = run_smoke(database_url_from_env(os.environ))
    print(
        "BRREG working-store DB smoke verified "
        f"organization_number={result.organization_number} rolled_back={str(result.rolled_back).lower()}"
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Update Makefile and README**

Modify `dagster/Makefile` so `smoke-brreg-db` depends on `setup-db`:

```make
smoke-brreg-db: setup-db
	uv run python -m corpscout_dagster.brreg.smoke
```

Modify `dagster/README.md` so the smoke description says it verifies `dagster_brreg.raw_records`, not Corpscout raw input tables.

- [ ] **Step 5: Run smoke tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest tests/brreg/test_smoke.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/brreg/smoke.py dagster/tests/brreg/test_smoke.py dagster/Makefile dagster/README.md
git commit -m "feat: smoke test brreg working store"
```

## Task 6: Full Verification And Real DB Smoke

**Files:**
- Verify Dagster project.

- [ ] **Step 1: Run full test suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
make test
```

Expected: all tests pass.

- [ ] **Step 2: Validate Dagster definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
make validate
```

Expected: Dagster reports all code locations passed validation.

- [ ] **Step 3: Render Docker Compose**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
docker compose -f docker-compose.yml config >/tmp/dagster-compose-config.txt
wc -l /tmp/dagster-compose-config.txt
```

Expected: command exits 0 and prints a positive line count.

- [ ] **Step 4: Run live rollback smoke**

Run with a real Postgres URL in the environment:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
make smoke-brreg-db
```

Expected: output includes:

```text
BRREG working-store DB smoke verified organization_number=999999991 rolled_back=true
```

- [ ] **Step 5: Build and validate Docker image**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
docker build -t corpscout-dagster:local .
docker run --rm -e DAGSTER_HOME=/tmp/dagster_home corpscout-dagster:local \
  sh -c 'mkdir -p "$DAGSTER_HOME" && touch "$DAGSTER_HOME/dagster.yaml" && dagster definitions validate -w /app/workspace.yaml'
```

Expected: image build exits 0 and container validation reports all code locations passed validation.

- [ ] **Step 6: Check whitespace and status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git diff --check
git status --short --branch
```

Expected: no whitespace errors and no unexpected untracked files.

## Deferred Scope

- Real translation storage in `dagster_brreg.translation_results`.
- Domain discovery storage in `dagster_brreg.domain_candidates`.
- Financial extraction storage in `dagster_brreg.financial_results`.
- Enhanced payload build and publish to Corpscout.
- Removing the Corpscout raw writer; it remains needed for the future publish step.
