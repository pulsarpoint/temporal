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
    FinishEnrichmentRun,
    IncrementEnrichmentRunProgress,
)


BRREG_BULK_URL = f"{BRREG_API_BASE_URL}{BRREG_BULK_PATH}"
DEFAULT_RAW_RECORD_BATCH_SIZE = 5000


def build_brreg_working_raw_record_rows(
    *,
    records: Iterable[BrregRawRecord | None],
) -> list[BrregWorkingRawRecordRow]:
    return [record.to_working_row() for record in records if record is not None]


@asset(name="brreg_working_raw_records")
def brreg_working_raw_records(context) -> dict[str, int]:
    return materialize_brreg_working_raw_records(
        context,
        records=iter_brreg_bulk_records(),
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=DEFAULT_RAW_RECORD_BATCH_SIZE,
    )


def materialize_brreg_working_raw_records(
    context,
    *,
    records: Iterable[BrregRawRecord | None],
    connection_factory,
    database_url: str,
    batch_size: int,
) -> dict[str, int]:
    rows_seen = 0
    rows_written = 0
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
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
        conn.commit()

        try:
            for batch in _iter_working_row_batches(records, batch_size=batch_size):
                with conn.cursor() as cursor:
                    store = BrregWorkingStore(cursor)
                    result = store.upsert_raw_records(batch, bulk_snapshot_id=bulk_snapshot_id)
                    store.increment_enrichment_run_progress(
                        IncrementEnrichmentRunProgress(
                            enrichment_run_id=enrichment_run_id,
                            records_seen=result.rows_seen,
                            records_completed=result.rows_written,
                        )
                    )
                conn.commit()
                rows_seen += result.rows_seen
                rows_written += result.rows_written
                context.log.info(
                    "BRREG raw ingest batch committed rows_seen=%s rows_written=%s total_rows_seen=%s",
                    result.rows_seen,
                    result.rows_written,
                    rows_seen,
                )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded",
                        error=None,
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    context.add_output_metadata(
        {
            "rows_seen": rows_seen,
            "rows_written": rows_written,
            "dagster_run_id": context.run_id,
        }
    )
    return {"rows_seen": rows_seen, "rows_written": rows_written}


def _iter_working_row_batches(
    records: Iterable[BrregRawRecord | None],
    *,
    batch_size: int,
) -> Iterable[list[BrregWorkingRawRecordRow]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    batch: list[BrregWorkingRawRecordRow] = []
    for record in records:
        if record is None:
            continue
        batch.append(record.to_working_row())
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _corpscout_database_url() -> str:
    value = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("CORPSCOUT_DB_URL")
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL or CORPSCOUT_DB_URL is required")
    return value
