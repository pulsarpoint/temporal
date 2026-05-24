from __future__ import annotations

import os
from collections.abc import Iterable

import psycopg
from dagster import asset

from corpscout_dagster.brreg.models import BrregRawRecord, BrregWorkingRawRecordRow
from corpscout_dagster.brreg.source import BRREG_API_BASE_URL, BRREG_BULK_PATH, iter_brreg_bulk_records
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
)


BRREG_BULK_URL = f"{BRREG_API_BASE_URL}{BRREG_BULK_PATH}"


def build_brreg_working_raw_record_rows(
    *,
    records: Iterable[BrregRawRecord | None],
) -> list[BrregWorkingRawRecordRow]:
    return [record.to_working_row() for record in records if record is not None]


@asset(name="brreg_working_raw_records")
def brreg_working_raw_records(context) -> dict[str, int]:
    connection_url = _corpscout_database_url()
    rows = build_brreg_working_raw_record_rows(records=iter_brreg_bulk_records())
    with psycopg.connect(connection_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            enrichment_run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=context.run_id,
                    run_type="bulk_ingest",
                    metadata={"source": "brreg"},
                )
            )
            bulk_snapshot_id = store.create_bulk_snapshot(
                CreateBulkSnapshot(
                    enrichment_run_id=enrichment_run_id,
                    source_url=BRREG_BULK_URL,
                    content_length_bytes=None,
                    compressed_payload_hash=None,
                    storage_uri=None,
                    metadata={"format": "gzip-json"},
                )
            )
            result = store.upsert_raw_records(rows, bulk_snapshot_id=bulk_snapshot_id)
        conn.commit()
    context.add_output_metadata(
        {
            "rows_seen": result.rows_seen,
            "rows_written": result.rows_written,
            "dagster_run_id": context.run_id,
        }
    )
    return {"rows_seen": result.rows_seen, "rows_written": result.rows_written}


def _corpscout_database_url() -> str:
    value = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("CORPSCOUT_DB_URL")
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL or CORPSCOUT_DB_URL is required")
    return value
