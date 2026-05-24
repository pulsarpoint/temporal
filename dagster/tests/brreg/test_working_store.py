from __future__ import annotations

from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    CreateTaskAttempt,
    FinishEnrichmentRun,
    IncrementEnrichmentRunProgress,
    InsertDomainProposal,
    InsertDomainCandidate,
    InsertTranslationResult,
    DomainCandidateRow,
    RawTaskRecord,
    TaskAttempt,
    UpsertCachedTranslation,
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
        self.fetchall_values = []

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))

    def executemany(self, sql: str, params_seq: list[dict]) -> None:
        self.many_calls.append((sql, params_seq))

    def fetchone(self):
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


def test_working_store_fetches_pending_task_records_without_coupling_tasks() -> None:
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

    rows = store.fetch_pending_raw_task_records(task_type="translate", limit=100)

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
    assert "FROM dagster_brreg.raw_records rr" in sql
    assert "ta.task_type = %(task_type)s" in sql
    assert "ta.status IN" not in sql
    assert params == {"task_type": "translate", "limit": 100}


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


def test_working_store_inserts_domain_candidates_and_marks_attempts() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    store.insert_domain_candidates(
        [
            InsertDomainCandidate(
                raw_record_id="00000000-0000-0000-0000-000000000002",
                task_attempt_id="00000000-0000-0000-0000-000000000001",
                domain="www.example.no",
                normalized_domain="example.no",
                signal="website_field",
                confidence=95,
                evidence={"website": "https://www.example.no"},
                metadata={"source_field": "hjemmeside"},
            )
        ]
    )
    store.finish_task_attempt(
        task_attempt_id="00000000-0000-0000-0000-000000000001",
        status="succeeded",
        error=None,
    )

    domain_sql, domain_params_seq = cursor.many_calls[0]
    assert "INSERT INTO dagster_brreg.domain_candidates" in domain_sql
    assert "ON CONFLICT (raw_record_id, normalized_domain, signal) DO UPDATE" in domain_sql
    assert domain_params_seq[0]["normalized_domain"] == "example.no"

    attempt_sql, attempt_params = cursor.calls[0]
    assert "UPDATE dagster_brreg.task_attempts" in attempt_sql
    assert "finished_at = now()" in attempt_sql
    assert attempt_params["status"] == "succeeded"


def test_working_store_fetches_domain_candidates_and_upserts_proposals() -> None:
    cursor = FakeCursor()
    cursor.fetchall_values = [
        ("example.no", "www.example.no", "website_field", 95, {"website": "https://www.example.no"}, {}),
        ("example.no", "example.no", "wikidata", 85, {"url": "https://www.example.no"}, {}),
    ]
    store = BrregWorkingStore(cursor)

    rows = store.fetch_domain_candidates_for_raw_record(raw_record_id="00000000-0000-0000-0000-000000000002")
    store.upsert_domain_proposals(
        [
            InsertDomainProposal(
                raw_record_id="00000000-0000-0000-0000-000000000002",
                task_attempt_id="00000000-0000-0000-0000-000000000001",
                domain="www.example.no",
                normalized_domain="example.no",
                score=100,
                signals=["website_field", "wikidata"],
                evidence={"signals": ["website_field", "wikidata"]},
                metadata={"source": "dagster"},
            )
        ]
    )

    assert rows == [
        DomainCandidateRow(
            normalized_domain="example.no",
            domain="www.example.no",
            signal="website_field",
            confidence=95,
            evidence={"website": "https://www.example.no"},
            metadata={},
        ),
        DomainCandidateRow(
            normalized_domain="example.no",
            domain="example.no",
            signal="wikidata",
            confidence=85,
            evidence={"url": "https://www.example.no"},
            metadata={},
        ),
    ]
    fetch_sql, fetch_params = cursor.calls[0]
    assert "FROM dagster_brreg.domain_candidates" in fetch_sql
    assert fetch_params["raw_record_id"] == "00000000-0000-0000-0000-000000000002"

    proposal_sql, proposal_params_seq = cursor.many_calls[0]
    assert "INSERT INTO dagster_brreg.domain_proposals" in proposal_sql
    assert "ON CONFLICT (raw_record_id, normalized_domain) DO UPDATE" in proposal_sql
    assert proposal_params_seq[0]["score"] == 100
