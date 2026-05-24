from __future__ import annotations

from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    FinishEnrichmentRun,
    IncrementEnrichmentRunProgress,
    UpsertResult,
)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.many_calls: list[tuple[str, list[dict]]] = []
        self.fetchone_values = [
            ("00000000-0000-0000-0000-000000000001",),
            ("00000000-0000-0000-0000-000000000002",),
        ]

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))

    def executemany(self, sql: str, params_seq: list[dict]) -> None:
        self.many_calls.append((sql, params_seq))

    def fetchone(self):
        return self.fetchone_values.pop(0)


def test_working_store_creates_enrichment_run_and_snapshot() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    run_id = store.create_enrichment_run(
        CreateEnrichmentRun(
            dagster_run_id="dagster-run-1",
            run_type="bulk_ingest",
            metadata={"source": "brreg"},
        )
    )
    snapshot_id = store.create_bulk_snapshot(
        CreateBulkSnapshot(
            enrichment_run_id=run_id,
            source_url="https://data.brreg.no/enhetsregisteret/api/enheter/lastned",
            content_length_bytes=None,
            compressed_payload_hash=None,
            storage_uri=None,
            metadata={"format": "gzip-json"},
        )
    )

    assert run_id == "00000000-0000-0000-0000-000000000001"
    assert snapshot_id == "00000000-0000-0000-0000-000000000002"
    assert "INSERT INTO dagster_brreg.enrichment_runs" in cursor.calls[0][0]
    assert cursor.calls[0][1]["dagster_run_id"] == "dagster-run-1"
    assert cursor.calls[0][1]["metadata"] == '{"source":"brreg"}'
    assert "INSERT INTO dagster_brreg.bulk_snapshots" in cursor.calls[1][0]
    assert cursor.calls[1][1]["enrichment_run_id"] == run_id


def test_working_store_upserts_raw_records_as_current_working_rows() -> None:
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

    result = store.upsert_raw_records(
        [record.to_working_row()],
        bulk_snapshot_id="00000000-0000-0000-0000-000000000002",
    )

    assert result == UpsertResult(rows_seen=1, rows_written=1)
    assert len(cursor.many_calls) == 2
    supersede_sql, supersede_params_seq = cursor.many_calls[0]
    assert "UPDATE dagster_brreg.raw_records" in supersede_sql
    assert "is_current = false" in supersede_sql
    supersede_params = supersede_params_seq[0]
    assert supersede_params["organization_number"] == "810202572"

    upsert_sql, upsert_params_seq = cursor.many_calls[1]
    assert "INSERT INTO dagster_brreg.raw_records" in upsert_sql
    assert "ON CONFLICT (organization_number, payload_hash) DO UPDATE" in upsert_sql
    assert "is_current = true" in upsert_sql
    upsert_params = upsert_params_seq[0]
    assert upsert_params["bulk_snapshot_id"] == "00000000-0000-0000-0000-000000000002"
    assert upsert_params["raw_payload"] == (
        '{"hjemmeside":"https://bortigard.no","navn":"BORTIGARD AS",'
        '"organisasjonsnummer":"810202572"}'
    )


def test_working_store_ignores_empty_raw_record_batches() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    result = store.upsert_raw_records([], bulk_snapshot_id="00000000-0000-0000-0000-000000000002")

    assert result == UpsertResult(rows_seen=0, rows_written=0)
    assert cursor.calls == []
    assert cursor.many_calls == []


def test_working_store_updates_enrichment_run_progress_and_completion() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    store.increment_enrichment_run_progress(
        IncrementEnrichmentRunProgress(
            enrichment_run_id="00000000-0000-0000-0000-000000000001",
            records_seen=5000,
            records_completed=4998,
            records_failed=2,
        )
    )
    store.finish_enrichment_run(
        FinishEnrichmentRun(
            enrichment_run_id="00000000-0000-0000-0000-000000000001",
            status="succeeded",
            error=None,
        )
    )

    progress_sql, progress_params = cursor.calls[0]
    assert "UPDATE dagster_brreg.enrichment_runs" in progress_sql
    assert "records_seen = records_seen + %(records_seen)s" in progress_sql
    assert progress_params["records_seen"] == 5000
    assert progress_params["records_completed"] == 4998
    assert progress_params["records_failed"] == 2

    finish_sql, finish_params = cursor.calls[1]
    assert "finished_at = now()" in finish_sql
    assert finish_params["status"] == "succeeded"
