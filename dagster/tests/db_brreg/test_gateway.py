from __future__ import annotations

import json

import pytest

from corpscout_dagster.db_brreg.gateway import (
    BrregAssetGateway,
    BrregAssetName,
    BrregTaskStatus,
    ClaimEnhancedBatchCommand,
    ClaimTaskBatchCommand,
    SubmitCurrencyResultCommand,
    SubmitDomainResultCommand,
    SubmitEnhancedRecordCommand,
    SubmitTaskFailureCommand,
    SubmitTranslationResultCommand,
    FetchCachedTranslationsCommand,
    RetryTaskFailuresCommand,
    UpsertCachedTranslationsCommand,
)
from corpscout_dagster.db_brreg.store import EnhancedBuildRecord, RawTaskRecord, UpsertCachedTranslation
from corpscout_dagster.brreg.translation_terms import TranslationCacheKey


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fetchone_values: list[tuple] = []
        self.fetchall_values: list[list[tuple]] = []
        self.last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: dict) -> None:
        self.last_sql = sql
        self.calls.append((sql, params))

    def executemany(self, sql: str, params_seq: list[dict]) -> None:
        self.calls.append((sql, {"many": params_seq}))

    def fetchone(self):
        if "retried AS" in self.last_sql:
            return (1,)
        if "fetch_raw_task_state_summary" in self.last_sql:
            return (1000, 1000, 0, 706, 0, 1, 1, 0, 0, 0, 293, 0, 0, 706)
        if "fetch_domain_result_summary" in self.last_sql:
            return (221, 0, 72, 0, 707)
        if "fetch_currency_result_summary" in self.last_sql:
            return (430, 570, 0, 0, 0)
        if "fetch_translation_artifact_summary" in self.last_sql:
            return (900, 100, 0, 0, 0)
        if "fetch_enhanced_record_summary" in self.last_sql:
            return (1000, 0, 0, 0, 0)
        if self.fetchone_values:
            return self.fetchone_values.pop(0)
        return ("00000000-0000-0000-0000-000000000001",)

    def fetchall(self):
        if self.fetchall_values:
            return self.fetchall_values.pop(0)
        return []


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_gateway_claims_domain_batch_with_typed_asset_api() -> None:
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                "https://bortigard.no",
                {"organisasjonsnummer": "810202572"},
            )
        ]
    ]
    connection.cursor_instance.fetchone_values = [("00000000-0000-0000-0000-000000000011", raw_record_id, 2)]
    gateway = BrregAssetGateway(connection)

    batch = gateway.claim_domain_batch(
        ClaimTaskBatchCommand(
            run_id="run-1",
            batch_size=10,
            max_parallel_tasks=1,
            lease_seconds=1800,
            metadata={"source": "test"},
        )
    )

    assert len(batch.records) == 1
    record = batch.records[0]
    assert record.raw_record_id == raw_record_id
    assert record.task_attempt_id == "00000000-0000-0000-0000-000000000011"
    assert record.attempt == 2
    assert connection.commits == 1
    assert any(
        "pending_task_ids AS" in sql and params["task_type"] == "domain_results"
        for sql, params in connection.cursor_instance.calls
    )
    assert any(
        "active_slots AS" in sql
        and "stale_running_task_ids AS" in sql
        and params["max_parallel_tasks"] == 1
        and params["lease_seconds"] == 1800
        for sql, params in connection.cursor_instance.calls
    )
    assert any(
        "INSERT INTO dagster_brreg.task_attempts" in sql and params["task_type"] == "domain_results"
        for sql, params in connection.cursor_instance.calls
    )


def test_gateway_submit_translation_result_writes_artifact_and_success_state() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    result = gateway.submit_translation_result(
        SubmitTranslationResultCommand(
            raw_record_id="00000000-0000-0000-0000-000000000010",
            task_attempt_id="00000000-0000-0000-0000-000000000011",
            status="succeeded",
            translated_payload={"name": "BORTIGARD AS"},
            model="qwen3:6b",
            prompt_version="v1",
            metadata={"source": "test"},
            enrichment_run_id="00000000-0000-0000-0000-000000000001",
        )
    )

    assert result.asset is BrregAssetName.TRANSLATION_RESULTS
    assert result.status is BrregTaskStatus.SUCCEEDED
    translation_insert_params = [
        params
        for sql, params in connection.cursor_instance.calls
        if "INSERT INTO dagster_brreg.translation_results" in sql
    ]
    assert translation_insert_params
    assert translation_insert_params[0]["status"] == "succeeded"
    assert json.loads(translation_insert_params[0]["translated_payload"]) == {"name": "BORTIGARD AS"}
    assert any("UPDATE dagster_brreg.raw_record_task_states" in sql for sql, _ in connection.cursor_instance.calls)


def test_gateway_submit_domain_result_writes_artifact_and_task_state_in_one_transaction() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    result = gateway.submit_domain_result(
        SubmitDomainResultCommand(
            raw_record_id="00000000-0000-0000-0000-000000000010",
            task_attempt_id="00000000-0000-0000-0000-000000000011",
            status="succeeded",
            best_domain="bortigard.no",
            domain_payload={"status": "succeeded", "best_domain": "bortigard.no"},
            metadata={"source": "test"},
            enrichment_run_id="00000000-0000-0000-0000-000000000001",
        )
    )

    assert result.asset is BrregAssetName.DOMAIN_RESULTS
    assert result.status is BrregTaskStatus.SUCCEEDED
    assert connection.commits == 1
    sql_calls = connection.cursor_instance.calls
    assert any("INSERT INTO dagster_brreg.domain_results" in sql for sql, _ in sql_calls)
    assert any("UPDATE dagster_brreg.task_attempts" in sql for sql, _ in sql_calls)
    assert any("UPDATE dagster_brreg.raw_record_task_states" in sql for sql, _ in sql_calls)
    assert any("records_completed = records_completed + %(records_completed)s" in sql for sql, _ in sql_calls)


def test_gateway_submit_currency_result_writes_artifact_and_skipped_state() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    result = gateway.submit_currency_result(
        SubmitCurrencyResultCommand(
            raw_record_id="00000000-0000-0000-0000-000000000010",
            task_attempt_id="00000000-0000-0000-0000-000000000011",
            status="skipped",
            original_currency=None,
            original_payload={},
            usd_payload={},
            fx_metadata={},
            metadata={"reason": "no_capital"},
            enrichment_run_id="00000000-0000-0000-0000-000000000001",
        )
    )

    assert result.asset is BrregAssetName.CURRENCY_RESULTS
    assert result.status is BrregTaskStatus.SKIPPED
    assert any(
        "INSERT INTO dagster_brreg.currency_results" in sql
        and params["status"] == "skipped"
        for sql, params in connection.cursor_instance.calls
    )
    assert any(
        "UPDATE dagster_brreg.raw_record_task_states" in sql
        and params["status"] == "skipped"
        for sql, params in connection.cursor_instance.calls
    )


def test_gateway_submit_failure_writes_translation_failure_artifact_and_retry_state() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    result = gateway.submit_translation_failure(
        SubmitTaskFailureCommand(
            asset=BrregAssetName.TRANSLATION_RESULTS,
            raw_record_id="00000000-0000-0000-0000-000000000010",
            task_attempt_id="00000000-0000-0000-0000-000000000011",
            error="missing translations",
            error_category="invalid_llm_output",
            error_code="missing_translation_terms",
            retry_strategy="change_model_or_prompt",
            metadata={"source": "test"},
            enrichment_run_id="00000000-0000-0000-0000-000000000001",
            model="qwen3:6b",
            prompt_version="v1",
        )
    )

    assert result.status is BrregTaskStatus.FAILED_TERMINAL
    assert connection.commits == 1
    assert any(
        "INSERT INTO dagster_brreg.translation_results" in sql
        and params["status"] == "failed"
        and params["model"] == "qwen3:6b"
        for sql, params in connection.cursor_instance.calls
    )
    assert any(
        "UPDATE dagster_brreg.raw_record_task_states" in sql
        and params["error_category"] == "invalid_llm_output"
        for sql, params in connection.cursor_instance.calls
    )


def test_gateway_retries_task_failures() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    result = gateway.retry_task_failures(
        RetryTaskFailuresCommand(task_type="translate", error_category="invalid_llm_output", limit=5000)
    )

    assert result.retried_rows == 1
    assert connection.commits == 1
    assert any("retried AS" in sql for sql, _ in connection.cursor_instance.calls)


def test_gateway_fetches_and_upserts_translation_cache() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    gateway.upsert_cached_translations(
        UpsertCachedTranslationsCommand(
            rows=[
                UpsertCachedTranslation(
                    category="activity",
                    source_lang="no",
                    target_lang="en",
                    original_hash="hash",
                    original_text="Aksjer",
                    translated_text="Shares",
                    model="qwen3:6b",
                    prompt_version="v1",
                    metadata={},
                )
            ]
        )
    )
    result = gateway.fetch_cached_translations(
        FetchCachedTranslationsCommand(
            keys=[
                TranslationCacheKey(
                    category="activity",
                    source_lang="no",
                    target_lang="en",
                    original_hash="hash",
                )
            ],
            model="qwen3:6b",
            prompt_version="v1",
        )
    )

    assert result == {}
    assert connection.commits == 1
    assert any("translation_cache" in sql for sql, _ in connection.cursor_instance.calls)


def test_gateway_claims_and_submits_enhanced_records() -> None:
    connection = FakeConnection()
    raw_record = RawTaskRecord(
        id="00000000-0000-0000-0000-000000000010",
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        website=None,
        raw_payload={"organisasjonsnummer": "810202572"},
    )
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record.id,
                raw_record.organization_number,
                raw_record.organization_name,
                "active",
                None,
                "NO",
                raw_record.raw_payload,
                "payload-hash",
                "succeeded",
                {"translated": True},
                "not_found",
                [],
                "skipped",
                {},
                {},
                {},
                {"translate": "succeeded", "domain_results": "succeeded", "currency_conversion": "skipped"},
            )
        ]
    ]
    connection.cursor_instance.fetchone_values = [("00000000-0000-0000-0000-000000000011", raw_record.id, 1)]
    gateway = BrregAssetGateway(connection)

    batch = gateway.claim_enhanced_batch(ClaimEnhancedBatchCommand(run_id="run-1", batch_size=10, metadata={}))

    assert len(batch.records) == 1
    assert isinstance(batch.records[0].build_record, EnhancedBuildRecord)
    gateway.submit_enhanced_record(
        SubmitEnhancedRecordCommand(
            raw_record_id=raw_record.id,
            task_attempt_id=batch.records[0].task_attempt_id,
            schema_version="brreg.enhanced.v1",
            enhanced_payload={"schema_version": "brreg.enhanced.v1"},
            enhanced_payload_hash="hash",
            metadata={"source": "test"},
        )
    )

    assert any("REFRESH MATERIALIZED VIEW" in sql for sql, _ in connection.cursor_instance.calls)
    assert any("INSERT INTO dagster_brreg.enhanced_records" in sql for sql, _ in connection.cursor_instance.calls)


def test_gateway_rejects_asset_specific_submit_mismatch() -> None:
    gateway = BrregAssetGateway(FakeConnection())

    with pytest.raises(ValueError, match="translation failure"):
        gateway.submit_translation_failure(
            SubmitTaskFailureCommand(
                asset=BrregAssetName.DOMAIN_RESULTS,
                raw_record_id="00000000-0000-0000-0000-000000000010",
                task_attempt_id="00000000-0000-0000-0000-000000000011",
                error="wrong asset",
                error_category="unknown",
                error_code="task_failed",
                retry_strategy="automatic",
                metadata={},
            )
        )
