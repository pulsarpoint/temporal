from __future__ import annotations

from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    UpsertResult,
)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fetchone_values = [
            ("00000000-0000-0000-0000-000000000001",),
            ("00000000-0000-0000-0000-000000000002",),
        ]

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))

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
    assert len(cursor.calls) == 2
    supersede_sql, supersede_params = cursor.calls[0]
    assert "UPDATE dagster_brreg.raw_records" in supersede_sql
    assert "is_current = false" in supersede_sql
    assert supersede_params["organization_number"] == "810202572"

    upsert_sql, upsert_params = cursor.calls[1]
    assert "INSERT INTO dagster_brreg.raw_records" in upsert_sql
    assert "ON CONFLICT (organization_number, payload_hash) DO UPDATE" in upsert_sql
    assert "is_current = true" in upsert_sql
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
