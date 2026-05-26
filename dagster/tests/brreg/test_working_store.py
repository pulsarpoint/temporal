from __future__ import annotations

from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    CreateTaskAttempt,
    DomainProposalRow,
    EnhancedBuildRecord,
    FinishEnrichmentRun,
    IncrementEnrichmentRunProgress,
    InsertDomainResult,
    InsertEnhancedRecord,
    InsertFinancialResult,
    InsertTranslationResult,
    RawTaskRecord,
    TaskAttempt,
    UpsertCachedTranslation,
    UpsertResult,
)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.many_calls: list[tuple[str, list[dict]]] = []
        self.last_sql = ""
        self.seed_pending_count = 0
        self.fetchone_values = [
            ("00000000-0000-0000-0000-000000000001",),
            ("00000000-0000-0000-0000-000000000002",),
        ]
        self.fetchall_values = []

    def execute(self, sql: str, params: dict) -> None:
        self.last_sql = sql
        self.calls.append((sql, params))

    def executemany(self, sql: str, params_seq: list[dict]) -> None:
        self.many_calls.append((sql, params_seq))

    def fetchone(self):
        if "seeded_raw_records" in self.last_sql:
            return (self.seed_pending_count,)
        return self.fetchone_values.pop(0)

    def fetchall(self):
        return self.fetchall_values


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


def test_working_store_fetches_pending_task_records_with_indexed_candidate_branches() -> None:
    cursor = FakeCursor()
    cursor.fetchone_values = []
    cursor.fetchall_values = [
        (
            "00000000-0000-0000-0000-000000000002",
            "810202572",
            "BORTIGARD AS",
            "https://bortigard.no",
            {"organisasjonsnummer": "810202572"},
        )
    ]
    store = BrregWorkingStore(cursor)

    rows = store.fetch_pending_raw_task_records(
        task_type="translate",
        limit=100,
        max_parallel_tasks=25,
        lease_seconds=900,
    )

    assert rows == [
        RawTaskRecord(
            id="00000000-0000-0000-0000-000000000002",
            organization_number="810202572",
            organization_name="BORTIGARD AS",
            website="https://bortigard.no",
            raw_payload={"organisasjonsnummer": "810202572"},
        )
    ]
    sql, params = cursor.calls[0]
    assert "WITH lock_task AS" in sql
    assert "pg_advisory_xact_lock" in sql
    assert "active_slots AS" in sql
    assert "pending_task_ids AS" in sql
    assert "failed_task_ids AS" in sql
    assert "stale_running_task_ids AS" in sql
    assert "new_task_ids AS" in sql
    assert "available_slots" in sql
    assert "LEAST(%(limit)s, active_slots.available_slots)" in sql
    assert "UNION ALL" in sql
    assert "NOT EXISTS" in sql
    assert "%(include_new_records)s" in sql
    assert "LEFT JOIN dagster_brreg.raw_record_task_states" not in sql
    assert "ON CONFLICT (raw_record_id, task_type) DO UPDATE" in sql
    assert "status = 'running'" in sql
    assert "lease_until = now() + (%(lease_seconds)s::text || ' seconds')::interval" in sql
    assert "ts.status = 'pending'" in sql
    assert "ts.status = 'failed_retryable'" in sql
    assert "ts.status = 'running'" in sql
    assert "next_retry_at <= now()" in sql
    assert params == {
        "task_type": "translate",
        "limit": 100,
        "include_new_records": True,
        "max_parallel_tasks": 25,
        "lease_seconds": 900,
    }


def test_working_store_creates_task_attempts_with_next_attempt_number() -> None:
    cursor = FakeCursor()
    cursor.fetchone_values = [
        (
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            1,
        )
    ]
    store = BrregWorkingStore(cursor)

    attempt = store.create_task_attempt(
        CreateTaskAttempt(
            enrichment_run_id="00000000-0000-0000-0000-000000000001",
            raw_record_id="00000000-0000-0000-0000-000000000002",
            task_type="translate",
            metadata={"source": "brreg"},
        )
    )

    assert attempt == TaskAttempt(
        id="00000000-0000-0000-0000-000000000001",
        raw_record_id="00000000-0000-0000-0000-000000000002",
        attempt=1,
    )
    sql, params = cursor.calls[0]
    assert "INSERT INTO dagster_brreg.task_attempts" in sql
    assert "coalesce(max(attempt), 0) + 1" in sql
    assert params["task_type"] == "translate"
    state_sql, state_params = cursor.calls[1]
    assert "INSERT INTO dagster_brreg.raw_record_task_states" in state_sql
    assert state_params["status"] == "running"
    assert state_params["attempt_count"] == 1


def test_working_store_upserts_translation_cache_and_results() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    store.upsert_cached_translations(
        [
            UpsertCachedTranslation(
                category="activity",
                source_lang="no",
                target_lang="en",
                original_hash="hash-1",
                original_text="Drive utleie",
                translated_text="Rental activity",
                model="qwen3:6b",
                prompt_version="v1",
                metadata={"source": "llm"},
            )
        ]
    )
    store.insert_translation_result(
        InsertTranslationResult(
            raw_record_id="00000000-0000-0000-0000-000000000002",
            task_attempt_id="00000000-0000-0000-0000-000000000001",
            status="succeeded",
            translated_payload={"terms": []},
            model="qwen3:6b",
            prompt_version="v1",
            error=None,
            metadata={},
        )
    )

    cache_sql, cache_params_seq = cursor.many_calls[0]
    assert "INSERT INTO dagster_brreg.translation_cache" in cache_sql
    assert "ON CONFLICT" in cache_sql
    assert cache_params_seq[0]["translated_text"] == "Rental activity"

    result_sql, result_params = cursor.calls[0]
    assert "INSERT INTO dagster_brreg.translation_results" in result_sql
    assert result_params["status"] == "succeeded"
    assert result_params["translated_payload"] == '{"terms":[]}'


def test_working_store_fetches_records_ready_for_enhanced_build() -> None:
    cursor = FakeCursor()
    cursor.fetchone_values = []
    cursor.fetchall_values = [
        (
            "00000000-0000-0000-0000-000000000002",
            "810202572",
            "BORTIGARD AS",
            "active",
            None,
            "NO",
            {"organisasjonsnummer": "810202572"},
            "payload-hash",
            "succeeded",
            {"terms": []},
            "succeeded",
            [
                {
                    "domain": "www.example.no",
                    "normalized_domain": "example.no",
                    "score": 95,
                    "signals": ["existing_website"],
                    "status": "accepted",
                    "evidence": {"url": "https://www.example.no"},
                    "metadata": {"source": "crawl-service"},
                }
            ],
            "succeeded",
            {"capital": {"original_amount": 81870.0, "original_currency": "NOK"}},
            {"capital": {"amount_usd": 8868.64, "amount_usd_cents": 886864}},
            {
                "source": "ECB",
                "rate_date": "2026-05-21",
                "capital": {
                    "source_currency": "NOK",
                    "target_currency": "USD",
                    "source_rate_per_eur": 10.7075,
                    "target_rate_per_eur": 1.1599,
                },
            },
            {"translate": "succeeded", "domain_results": "succeeded", "currency_conversion": "succeeded"},
        )
    ]
    store = BrregWorkingStore(cursor)

    rows = store.fetch_pending_enhanced_build_records(limit=50)

    assert rows == [
        EnhancedBuildRecord(
            record=RawTaskRecord(
                id="00000000-0000-0000-0000-000000000002",
                organization_number="810202572",
                organization_name="BORTIGARD AS",
                website=None,
                raw_payload={"organisasjonsnummer": "810202572"},
            ),
            registration_status="active",
            country_iso2="NO",
            payload_hash="payload-hash",
            translation_status="succeeded",
            translation_payload={"terms": []},
            domain_status="succeeded",
            domain_proposals=[
                DomainProposalRow(
                    domain="www.example.no",
                    normalized_domain="example.no",
                    score=95,
                    signals=["existing_website"],
                    status="accepted",
                    evidence={"url": "https://www.example.no"},
                    metadata={"source": "crawl-service"},
                )
            ],
            currency_status="succeeded",
            financial_payload={"capital": {"original_amount": 81870.0, "original_currency": "NOK"}},
            usd_payload={"capital": {"amount_usd": 8868.64, "amount_usd_cents": 886864}},
            fx_metadata={
                "source": "ECB",
                "rate_date": "2026-05-21",
                "capital": {
                    "source_currency": "NOK",
                    "target_currency": "USD",
                    "source_rate_per_eur": 10.7075,
                    "target_rate_per_eur": 1.1599,
                },
            },
            task_statuses={"translate": "succeeded", "domain_results": "succeeded", "currency_conversion": "succeeded"},
        )
    ]
    sql, params = cursor.calls[0]
    assert "FROM dagster_brreg.raw_records rr" in sql
    assert "dagster_brreg.translation_results" in sql
    assert "dagster_brreg.domain_results" in sql
    assert "dagster_brreg.financial_results" in sql
    assert "domain_payload->'candidates'" in sql
    assert "dts.task_type = 'domain_results'" in sql
    assert "fts.task_type = 'currency_conversion'" in sql
    assert "merge_domain_proposals" not in sql
    assert "dagster_brreg.enhanced_records" in sql
    assert params == {"limit": 50}


def test_working_store_upserts_enhanced_records_and_returns_id() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    enhanced_id = store.upsert_enhanced_record(
        InsertEnhancedRecord(
            raw_record_id="00000000-0000-0000-0000-000000000002",
            task_attempt_id="00000000-0000-0000-0000-000000000001",
            schema_version="brreg.enhanced.v1",
            enhanced_payload={"schema_version": "brreg.enhanced.v1"},
            enhanced_payload_hash="enhanced-hash",
            metadata={"source": "test"},
        )
    )

    assert enhanced_id == "00000000-0000-0000-0000-000000000001"
    sql, params = cursor.calls[0]
    assert "INSERT INTO dagster_brreg.enhanced_records" in sql
    assert "ON CONFLICT (raw_record_id, schema_version, enhanced_payload_hash) DO UPDATE" in sql
    assert params["enhanced_payload"] == '{"schema_version":"brreg.enhanced.v1"}'
    assert params["enhanced_payload_hash"] == "enhanced-hash"


def test_working_store_inserts_domain_result_artifact() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    store.insert_domain_result(
        InsertDomainResult(
            raw_record_id="00000000-0000-0000-0000-000000000002",
            task_attempt_id="00000000-0000-0000-0000-000000000001",
            status="succeeded",
            best_domain="bortigard.no",
            domain_payload={"status": "succeeded", "best_domain": "bortigard.no"},
            error=None,
            metadata={"source": "crawl-service"},
        )
    )

    sql, params = cursor.calls[0]
    assert "INSERT INTO dagster_brreg.domain_results" in sql
    assert params["best_domain"] == "bortigard.no"
    assert params["domain_payload"] == '{"best_domain":"bortigard.no","status":"succeeded"}'


def test_working_store_inserts_currency_result_artifact() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    store.insert_financial_result(
        InsertFinancialResult(
            raw_record_id="00000000-0000-0000-0000-000000000002",
            task_attempt_id="00000000-0000-0000-0000-000000000001",
            status="succeeded",
            original_currency="NOK",
            financial_payload={"capital": {"original_amount": 81870.0, "original_currency": "NOK"}},
            usd_payload={"capital": {"amount_usd": 8868.64, "amount_usd_cents": 886864}},
            fx_metadata={"source": "ECB", "rate_date": "2026-05-21"},
            source_uri=None,
            error=None,
            metadata={"source": "dagster"},
        )
    )

    sql, params = cursor.calls[0]
    assert "INSERT INTO dagster_brreg.financial_results" in sql
    assert params["status"] == "succeeded"
    assert params["original_currency"] == "NOK"
    assert params["usd_payload"] == '{"capital":{"amount_usd":8868.64,"amount_usd_cents":886864}}'
