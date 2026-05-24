from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from corpscout_dagster.brreg.models import BrregWorkingRawRecordRow


class Cursor(Protocol):
    def execute(self, sql: str, params: dict[str, Any]) -> object:
        ...

    def fetchone(self):
        ...


@dataclass(frozen=True)
class CreateEnrichmentRun:
    dagster_run_id: str
    run_type: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class CreateBulkSnapshot:
    enrichment_run_id: str
    source_url: str
    content_length_bytes: int | None
    compressed_payload_hash: str | None
    storage_uri: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class UpsertResult:
    rows_seen: int
    rows_written: int


class BrregWorkingStore:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def create_enrichment_run(self, command: CreateEnrichmentRun) -> str:
        self._cursor.execute(
            CREATE_ENRICHMENT_RUN_SQL,
            {
                "dagster_run_id": command.dagster_run_id,
                "run_type": command.run_type,
                "metadata": _json(command.metadata),
            },
        )
        return _single_value(self._cursor.fetchone())

    def create_bulk_snapshot(self, command: CreateBulkSnapshot) -> str:
        self._cursor.execute(
            CREATE_BULK_SNAPSHOT_SQL,
            {
                "enrichment_run_id": command.enrichment_run_id,
                "source_url": command.source_url,
                "content_length_bytes": command.content_length_bytes,
                "compressed_payload_hash": command.compressed_payload_hash,
                "storage_uri": command.storage_uri,
                "metadata": _json(command.metadata),
            },
        )
        return _single_value(self._cursor.fetchone())

    def upsert_raw_records(
        self,
        rows: list[BrregWorkingRawRecordRow],
        *,
        bulk_snapshot_id: str,
    ) -> UpsertResult:
        for row in rows:
            params = {
                "bulk_snapshot_id": bulk_snapshot_id,
                "source_native_id": row.source_native_id,
                "organization_number": row.organization_number,
                "organization_name": row.organization_name,
                "registration_status": row.registration_status,
                "website": row.website,
                "country_iso2": row.country_iso2,
                "raw_payload": _json(row.raw_payload),
                "payload_hash": row.payload_hash,
                "metadata": _json(row.metadata),
            }
            self._cursor.execute(SUPERSEDE_CURRENT_RAW_RECORD_SQL, params)
            self._cursor.execute(UPSERT_RAW_RECORD_SQL, params)
        return UpsertResult(rows_seen=len(rows), rows_written=len(rows))


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _single_value(row) -> str:
    if row is None:
        raise RuntimeError("expected database statement to return one row")
    return str(row[0])


CREATE_ENRICHMENT_RUN_SQL = """
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
SET
    status = 'running',
    started_at = now(),
    finished_at = NULL,
    error = NULL,
    metadata = EXCLUDED.metadata
RETURNING id
"""

CREATE_BULK_SNAPSHOT_SQL = """
INSERT INTO dagster_brreg.bulk_snapshots (
    enrichment_run_id,
    source_url,
    content_length_bytes,
    compressed_payload_hash,
    storage_uri,
    metadata
) VALUES (
    %(enrichment_run_id)s,
    %(source_url)s,
    %(content_length_bytes)s,
    %(compressed_payload_hash)s,
    %(storage_uri)s,
    %(metadata)s::jsonb
)
RETURNING id
"""

SUPERSEDE_CURRENT_RAW_RECORD_SQL = """
UPDATE dagster_brreg.raw_records
SET
    is_current = false,
    last_seen_at = now()
WHERE organization_number = %(organization_number)s
  AND payload_hash <> %(payload_hash)s
  AND is_current = true
"""

UPSERT_RAW_RECORD_SQL = """
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
    is_current,
    metadata
) VALUES (
    %(bulk_snapshot_id)s,
    %(source_native_id)s,
    %(organization_number)s,
    %(organization_name)s,
    %(registration_status)s,
    %(website)s,
    %(country_iso2)s,
    %(raw_payload)s::jsonb,
    %(payload_hash)s,
    true,
    %(metadata)s::jsonb
)
ON CONFLICT (organization_number, payload_hash) DO UPDATE
SET
    bulk_snapshot_id = EXCLUDED.bulk_snapshot_id,
    organization_name = EXCLUDED.organization_name,
    registration_status = EXCLUDED.registration_status,
    website = EXCLUDED.website,
    country_iso2 = EXCLUDED.country_iso2,
    raw_payload = EXCLUDED.raw_payload,
    is_current = true,
    last_seen_at = now(),
    metadata = EXCLUDED.metadata
"""
