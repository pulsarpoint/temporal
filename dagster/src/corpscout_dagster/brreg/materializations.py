from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date

from corpscout_dagster.brreg.crawl_service import CrawlServiceClient
from corpscout_dagster.db_brreg.gateway import (
    BrregAssetGateway,
    BrregAssetName,
    ClaimEnhancedBatchCommand,
    ClaimTaskBatchCommand,
    ClaimedRawRecord,
    IngestRawRecordsCommand,
    SubmitCurrencyResultCommand,
    SubmitDomainResultCommand,
    SubmitEnhancedRecordCommand,
    SubmitTaskFailureCommand,
    SubmitTranslationResultCommand,
)
from corpscout_dagster.brreg.enhanced_payload import (
    BRREG_ENHANCED_SCHEMA_VERSION,
    build_brreg_enhanced_payload,
    enhanced_payload_hash,
)
from corpscout_dagster.brreg.fx_rates import FxRateSet, load_ecb_rates_for_date, load_latest_ecb_rates
from corpscout_dagster.brreg.source import (
    BRREG_API_BASE_URL,
    BRREG_BULK_PATH,
    BrregBulkRecordClient,
    iter_brreg_bulk_records,
)
from corpscout_dagster.brreg.translation_terms import (
    CachedTermTranslation,
    TermTranslator,
    TranslationCacheKey,
    TranslationItem,
    build_translation_payload,
    extract_translation_items,
    translation_cache_key,
    translation_item_id,
)
from corpscout_dagster.db_brreg.store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    FinishEnrichmentRun,
    InsertCurrencyResult,
    RawTaskRecord,
    TaskAttempt,
    UpsertCachedTranslation,
)


DEFAULT_RAW_RECORD_BATCH_SIZE = 5000
DEFAULT_RAW_RECORD_LIMIT = 1000
DEFAULT_TRANSLATION_RECORD_BATCH_SIZE = 50
DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN = 0
DEFAULT_DOMAIN_RESULT_BATCH_SIZE = 10
DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN = 0
DEFAULT_CURRENCY_RESULT_BATCH_SIZE = 500
DEFAULT_CURRENCY_MAX_BATCHES_PER_RUN = 0
DEFAULT_ENHANCED_RECORD_BATCH_SIZE = 500
DEFAULT_TASK_LEASE_SECONDS = 1800
DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS = DEFAULT_TRANSLATION_RECORD_BATCH_SIZE
DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS = 1
DEFAULT_CURRENCY_RESULT_MAX_PARALLEL_TASKS = 100
ERROR_CATEGORIES = (
    "transient_external",
    "rate_limited",
    "invalid_llm_output",
    "invalid_input",
    "blocked_by_config",
    "not_found",
    "internal_error",
    "interrupted",
    "unknown",
)


@dataclass(frozen=True)
class TaskFailureClassification:
    error_category: str
    error_code: str
    retry_strategy: str


@dataclass(frozen=True)
class TaskFailureLogKey:
    error_category: str
    error_code: str
    retry_strategy: str
    sample_error: str


TaskFailureLogSummary = dict[TaskFailureLogKey, int]


class StructuredTaskError(RuntimeError):
    def __init__(self, message: str, classification: TaskFailureClassification) -> None:
        super().__init__(message)
        self.error_category = classification.error_category
        self.error_code = classification.error_code
        self.retry_strategy = classification.retry_strategy


def materialize_brreg_raw_records(
    context,
    *,
    connection_factory,
    database_url: str,
    bulk_client: BrregBulkRecordClient,
    batch_size: int = DEFAULT_RAW_RECORD_BATCH_SIZE,
    limit: int = DEFAULT_RAW_RECORD_LIMIT,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if limit < 0:
        raise ValueError("limit must be zero or positive")
    rows_seen = 0
    rows_written = 0
    rows_inserted_new = 0
    rows_existing_unchanged = 0
    rows_new_versions = 0
    batches_processed = 0
    source_url = f"{BRREG_API_BASE_URL}{BRREG_BULK_PATH}"
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            enrichment_run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, "bulk_ingest"),
                    run_type="bulk_ingest",
                    metadata={
                        "source": "brreg",
                        "dagster_run_id": context.run_id,
                        "source_mode": "bulk",
                        "limit": limit or None,
                    },
                )
            )
            bulk_snapshot_id = store.create_bulk_snapshot(
                CreateBulkSnapshot(
                    enrichment_run_id=enrichment_run_id,
                    source_url=source_url,
                    content_length_bytes=None,
                    compressed_payload_hash=None,
                    storage_uri=None,
                    metadata={
                        "format": "gzip-json",
                        "source": "brreg",
                        "source_mode": "bulk",
                        "limit": limit or None,
                    },
                )
            )
        conn.commit()
        context.log.info(
            "BRREG raw ingest run started source_url=%s batch_size=%s limit=%s",
            source_url,
            batch_size,
            limit,
        )

        try:
            batch = []
            source_rows_seen = 0
            for record in iter_brreg_bulk_records(client=bulk_client):
                if limit and source_rows_seen >= limit:
                    break
                source_rows_seen += 1
                batch.append(record.to_working_row())
                if len(batch) < batch_size:
                    continue
                next_batch = batches_processed + 1
                context.log.info(
                    "BRREG raw ingest batch writing batch=%s records=%s source_rows_seen=%s total_rows_seen_before_batch=%s total_rows_written_before_batch=%s",
                    next_batch,
                    len(batch),
                    source_rows_seen,
                    rows_seen,
                    rows_written,
                )
                result = _write_raw_record_batch(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    bulk_snapshot_id=bulk_snapshot_id,
                    rows=batch,
                )
                rows_seen += result.rows_seen
                rows_written += result.rows_written
                rows_inserted_new += result.rows_inserted_new
                rows_existing_unchanged += result.rows_existing_unchanged
                rows_new_versions += result.rows_new_versions
                batches_processed += 1
                context.log.info(
                    "BRREG raw ingest batch committed batch=%s rows_seen=%s rows_written=%s rows_inserted_new=%s rows_existing_unchanged=%s rows_new_versions=%s total_rows_seen=%s total_rows_written=%s",
                    batches_processed,
                    result.rows_seen,
                    result.rows_written,
                    result.rows_inserted_new,
                    result.rows_existing_unchanged,
                    result.rows_new_versions,
                    rows_seen,
                    rows_written,
                )
                batch = []

            if batch:
                next_batch = batches_processed + 1
                context.log.info(
                    "BRREG raw ingest batch writing batch=%s records=%s source_rows_seen=%s total_rows_seen_before_batch=%s total_rows_written_before_batch=%s",
                    next_batch,
                    len(batch),
                    source_rows_seen,
                    rows_seen,
                    rows_written,
                )
                result = _write_raw_record_batch(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    bulk_snapshot_id=bulk_snapshot_id,
                    rows=batch,
                )
                rows_seen += result.rows_seen
                rows_written += result.rows_written
                rows_inserted_new += result.rows_inserted_new
                rows_existing_unchanged += result.rows_existing_unchanged
                rows_new_versions += result.rows_new_versions
                batches_processed += 1
                context.log.info(
                    "BRREG raw ingest batch committed batch=%s rows_seen=%s rows_written=%s rows_inserted_new=%s rows_existing_unchanged=%s rows_new_versions=%s total_rows_seen=%s total_rows_written=%s",
                    batches_processed,
                    result.rows_seen,
                    result.rows_written,
                    result.rows_inserted_new,
                    result.rows_existing_unchanged,
                    result.rows_new_versions,
                    rows_seen,
                    rows_written,
                )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(enrichment_run_id=enrichment_run_id, status="succeeded", error=None)
                )
            conn.commit()
            context.log.info(
                "BRREG raw ingest run committed rows_seen=%s rows_written=%s rows_inserted_new=%s rows_existing_unchanged=%s rows_new_versions=%s batches_processed=%s source_rows_seen=%s",
                rows_seen,
                rows_written,
                rows_inserted_new,
                rows_existing_unchanged,
                rows_new_versions,
                batches_processed,
                source_rows_seen,
            )
        except Exception as exc:
            conn.rollback()
            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(enrichment_run_id=enrichment_run_id, status="failed", error=str(exc))
                )
            conn.commit()
            context.log.info(
                "BRREG raw ingest run failed rows_seen=%s rows_written=%s batches_processed=%s error=%s",
                rows_seen,
                rows_written,
                batches_processed,
                _task_error_message(exc),
            )
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_written": rows_written,
        "rows_inserted_new": rows_inserted_new,
        "rows_existing_unchanged": rows_existing_unchanged,
        "rows_new_versions": rows_new_versions,
        "batches_processed": batches_processed,
        "source_limit": limit,
    }
    context.add_output_metadata({**result, "dagster_run_id": context.run_id, "source_url": source_url})
    return result


def materialize_brreg_translation_results(
    context,
    *,
    connection_factory,
    database_url: str,
    translator: TermTranslator,
    batch_size: int,
    max_batches_per_run: int = DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
    max_parallel_tasks: int = DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
    model: str,
    prompt_version: str,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    reconciled_translation_tasks = 0
    task_summary: dict[str, int] = {}
    artifact_summary: dict[str, int] = {}
    failure_summary: dict[str, int] = {}
    enrichment_run_id: str | None = None
    claimed_record_ids: set[str] = set()
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            enrichment_run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, "translate"),
                    run_type="translate",
                    metadata={
                        "source": "brreg",
                        "dagster_run_id": context.run_id,
                        "model": model,
                        "prompt_version": prompt_version,
                    },
                )
            )
            reconciled_translation_tasks = store.reconcile_translation_task_states(
                model=model,
                prompt_version=prompt_version,
            )
        conn.commit()
        context.log.info(
            "BRREG translation run started model=%s prompt_version=%s batch_size=%s max_batches_per_run=%s max_parallel_tasks=%s reconciled_translation_tasks=%s",
            model,
            prompt_version,
            batch_size,
            max_batches_per_run,
            max_parallel_tasks,
            reconciled_translation_tasks,
        )

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                claimed_batch = BrregAssetGateway(
                    conn,
                    translation_model=model,
                    translation_prompt_version=prompt_version,
                ).claim_translation_batch(
                    ClaimTaskBatchCommand(
                        run_id=context.run_id,
                        batch_size=batch_size,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                        metadata={"model": model, "prompt_version": prompt_version},
                        enrichment_run_id=enrichment_run_id,
                    )
                )
                records = [claimed.record for claimed in claimed_batch.records]
                claimed_record_ids.update(claimed.raw_record_id for claimed in claimed_batch.records)

                if not records:
                    stopped_reason = "no_claimable_records"
                    context.log.info(
                        "BRREG translation run has no claimable records rows_seen=%s rows_completed=%s rows_failed=%s batches_processed=%s",
                        rows_seen,
                        rows_completed,
                        rows_failed,
                        batches_processed,
                    )
                    break

                batches_processed += 1
                context.log.info(
                    "BRREG translation batch claimed batch=%s records=%s total_rows_seen_before_batch=%s total_rows_completed_before_batch=%s total_rows_failed_before_batch=%s",
                    batches_processed,
                    len(records),
                    rows_seen,
                    rows_completed,
                    rows_failed,
                )
                completed, failed, batch_failure_summary = _translate_record_batch(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    claimed_records=claimed_batch.records,
                    translator=translator,
                    model=model,
                    prompt_version=prompt_version,
                )
                rows_seen += len(records)
                rows_completed += completed
                rows_failed += failed
                context.log.info(
                    "BRREG translation batch completed batch=%s records=%s batch_completed=%s batch_failed=%s total_rows_seen=%s total_rows_completed=%s total_rows_failed=%s",
                    batches_processed,
                    len(records),
                    completed,
                    failed,
                    rows_seen,
                    rows_completed,
                    rows_failed,
                )
                _log_batch_failure_summary(
                    context,
                    task_label="translation",
                    batch=batches_processed,
                    failure_summary=batch_failure_summary,
                )

            context.log.info(
                "BRREG translation batches committed rows_seen=%s rows_completed=%s rows_failed=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                store = BrregWorkingStore(cursor)
                task_summary = store.fetch_raw_task_state_summary(task_type="translate")
                artifact_summary = store.fetch_translation_artifact_summary(model=model, prompt_version=prompt_version)
                failure_summary = store.fetch_task_failure_summary(task_type="translate")
                store.finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} translation rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    store = BrregWorkingStore(cursor)
                    store.reset_unstarted_running_task_records(
                        task_type="translate",
                        raw_record_ids=sorted(claimed_record_ids),
                    )
                    store.finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_claimed_this_run": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "batches_processed": batches_processed,
        "reconciled_translation_tasks": reconciled_translation_tasks,
    }
    context.add_output_metadata(
        _build_translation_asset_metadata(
            run_id=context.run_id,
            model=model,
            prompt_version=prompt_version,
            rows_claimed=rows_seen,
            rows_succeeded=rows_completed,
            rows_failed=rows_failed,
            batches_processed=batches_processed,
            reconciled_translation_tasks=reconciled_translation_tasks,
            max_batches_per_run=max_batches_per_run,
            max_parallel_tasks=max_parallel_tasks,
            stopped_reason=stopped_reason,
            task_summary=task_summary,
            artifact_summary=artifact_summary,
            failure_summary=failure_summary,
        )
    )
    _raise_if_materialization_rows_failed(task_label="translation", rows_failed=rows_failed)
    with connection_factory(database_url) as state_conn:
        BrregAssetGateway(
            state_conn,
            translation_model=model,
            translation_prompt_version=prompt_version,
        ).assert_asset_complete(BrregAssetName.TRANSLATION_RESULTS, max_parallel_tasks=max_parallel_tasks)
    return result


def _build_translation_asset_metadata(
    *,
    run_id: str,
    model: str,
    prompt_version: str,
    rows_claimed: int,
    rows_succeeded: int,
    rows_failed: int,
    batches_processed: int,
    reconciled_translation_tasks: int,
    max_batches_per_run: int,
    max_parallel_tasks: int,
    stopped_reason: str,
    task_summary: dict[str, int],
    artifact_summary: dict[str, int],
    failure_summary: dict[str, int],
) -> dict[str, int | str]:
    live_translation_succeeded = artifact_summary.get("translation_result_succeeded", 0)
    live_translation_skipped = artifact_summary.get("translation_result_skipped", 0)
    live_translation_failed = artifact_summary.get("translation_result_failed", 0)
    live_failure_metadata = {
        f"live_translate_failures_{category}": failure_summary.get(category, 0)
        for category in ERROR_CATEGORIES
    }
    return {
        "run_dagster_run_id": run_id,
        "run_rows_claimed": rows_claimed,
        "run_rows_succeeded": rows_succeeded,
        "run_rows_failed": rows_failed,
        "run_batches_processed": batches_processed,
        "run_reconciled_translation_tasks": reconciled_translation_tasks,
        "run_max_batches_per_run": max_batches_per_run,
        "run_max_parallel_tasks": max_parallel_tasks,
        "run_stopped_reason": stopped_reason,
        "live_translation_model": model,
        "live_translation_prompt_version": prompt_version,
        "live_raw_records_total": task_summary.get("raw_records_total", 0),
        "live_raw_records_current": task_summary.get("raw_records_current", 0),
        "live_raw_records_not_current": task_summary.get("raw_records_not_current", 0),
        "live_translate_task_no_state": task_summary.get("task_no_state", 0),
        "live_translate_task_pending": task_summary.get("task_pending", 0),
        "live_translate_task_running": task_summary.get("task_running", 0),
        "live_translate_task_running_active": task_summary.get("task_running_active", 0),
        "live_translate_task_running_stale": task_summary.get("task_running_stale", 0),
        "live_translate_task_failed_retryable": task_summary.get("task_failed_retryable", 0),
        "live_translate_task_failed_terminal": task_summary.get("task_failed_terminal", 0),
        "live_translate_task_succeeded": task_summary.get("task_succeeded", 0),
        "live_translate_task_skipped": task_summary.get("task_skipped", 0),
        "live_translate_task_cancelled": task_summary.get("task_cancelled", 0),
        "live_translate_task_eligible_now": task_summary.get("task_eligible_now", 0),
        "live_translation_results_current_model_total": (
            live_translation_succeeded + live_translation_skipped + live_translation_failed
        ),
        "live_translation_results_current_model_succeeded": live_translation_succeeded,
        "live_translation_results_current_model_skipped": live_translation_skipped,
        "live_translation_results_current_model_failed": live_translation_failed,
        "live_translation_results_current_model_missing": artifact_summary.get("translation_result_missing", 0),
        "live_translation_artifacts_current_model_missing": artifact_summary.get(
            "translation_artifact_missing",
            0,
        ),
        "live_translate_failures_total": sum(failure_summary.values()),
        **live_failure_metadata,
    }


def _build_standard_task_asset_metadata(
    *,
    run_id: str,
    task_type: str,
    rows_claimed: int,
    rows_succeeded: int,
    rows_failed: int,
    batches_processed: int,
    result_counter_name: str,
    result_counter_value: int,
    live_prefix: str,
    max_batches_per_run: int,
    max_parallel_tasks: int,
    stopped_reason: str,
    task_summary: dict[str, int],
    failure_summary: dict[str, int],
    live_metadata: dict[str, int],
) -> dict[str, int | str]:
    live_failure_metadata = {
        f"live_{live_prefix}_failures_{category}": failure_summary.get(category, 0)
        for category in ERROR_CATEGORIES
    }
    return {
        "run_dagster_run_id": run_id,
        "run_task_type": task_type,
        "run_rows_claimed": rows_claimed,
        "run_rows_succeeded": rows_succeeded,
        "run_rows_failed": rows_failed,
        "run_batches_processed": batches_processed,
        f"run_{result_counter_name}": result_counter_value,
        "run_max_batches_per_run": max_batches_per_run,
        "run_max_parallel_tasks": max_parallel_tasks,
        "run_stopped_reason": stopped_reason,
        f"live_{task_type}_task_no_state": task_summary.get("task_no_state", 0),
        f"live_{task_type}_task_pending": task_summary.get("task_pending", 0),
        f"live_{task_type}_task_running": task_summary.get("task_running", 0),
        f"live_{task_type}_task_running_active": task_summary.get("task_running_active", 0),
        f"live_{task_type}_task_running_stale": task_summary.get("task_running_stale", 0),
        f"live_{task_type}_task_failed_retryable": task_summary.get("task_failed_retryable", 0),
        f"live_{task_type}_task_failed_terminal": task_summary.get("task_failed_terminal", 0),
        f"live_{task_type}_task_succeeded": task_summary.get("task_succeeded", 0),
        f"live_{task_type}_task_skipped": task_summary.get("task_skipped", 0),
        f"live_{task_type}_task_cancelled": task_summary.get("task_cancelled", 0),
        f"live_{task_type}_task_eligible_now": task_summary.get("task_eligible_now", 0),
        f"live_{live_prefix}_failures_total": sum(failure_summary.values()),
        **live_failure_metadata,
        **live_metadata,
    }


def _raise_if_materialization_rows_failed(*, task_label: str, rows_failed: int) -> None:
    if rows_failed > 0:
        raise RuntimeError(f"BRREG {task_label} materialization failed with {rows_failed} failed rows")


def materialize_brreg_domain_results(
    context,
    *,
    connection_factory,
    database_url: str,
    crawl_service_client: CrawlServiceClient,
    batch_size: int,
    max_batches_per_run: int = DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
    max_parallel_tasks: int = DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    domain_results_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    task_type = "domain_results"
    task_summary: dict[str, int] = {}
    artifact_summary: dict[str, int] = {}
    failure_summary: dict[str, int] = {}
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id, "service": "crawl-service"},
                )
            )
        conn.commit()
        context.log.info(
            "BRREG domain result run started batch_size=%s max_batches_per_run=%s max_parallel_tasks=%s",
            batch_size,
            max_batches_per_run,
            max_parallel_tasks,
        )

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                claimed_batch = BrregAssetGateway(conn).claim_domain_batch(
                    ClaimTaskBatchCommand(
                        run_id=context.run_id,
                        batch_size=batch_size,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                        metadata={"service": "crawl-service"},
                        enrichment_run_id=enrichment_run_id,
                    )
                )
                records = [claimed.record for claimed in claimed_batch.records]
                if not records:
                    stopped_reason = "no_claimable_records"
                    context.log.info(
                        "BRREG domain result run has no claimable records rows_seen=%s rows_completed=%s rows_failed=%s batches_processed=%s",
                        rows_seen,
                        rows_completed,
                        rows_failed,
                        batches_processed,
                    )
                    break

                batches_processed += 1
                rows_seen += len(records)
                context.log.info(
                    "BRREG domain result batch claimed batch=%s records=%s total_rows_seen=%s total_rows_completed_before_batch=%s total_rows_failed_before_batch=%s",
                    batches_processed,
                    len(records),
                    rows_seen,
                    rows_completed,
                    rows_failed,
                )
                for index, claimed in enumerate(claimed_batch.records, start=1):
                    record = claimed.record
                    context.log.info(
                        "BRREG domain result record started batch=%s batch_index=%s records_in_batch=%s organization_number=%s",
                        batches_processed,
                        index,
                        len(records),
                        record.organization_number,
                    )
                    attempt = TaskAttempt(
                        id=claimed.task_attempt_id,
                        raw_record_id=claimed.raw_record_id,
                        attempt=claimed.attempt,
                    )
                    try:
                        payload = crawl_service_client.discover_brreg_domain(record)
                        task_succeeded = _write_domain_result(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            payload=payload,
                        )
                        domain_results_written += 1
                        if task_succeeded:
                            rows_completed += 1
                            record_status = "succeeded"
                        else:
                            rows_failed += 1
                            record_status = "failed"
                        context.log.info(
                            "BRREG domain result record completed batch=%s batch_index=%s organization_number=%s status=%s total_rows_completed=%s total_rows_failed=%s",
                            batches_processed,
                            index,
                            record.organization_number,
                            record_status,
                            rows_completed,
                            rows_failed,
                        )
                    except Exception as exc:
                        conn.rollback()
                        _mark_domain_result_failed(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            error=str(exc),
                        )
                        rows_failed += 1
                        domain_results_written += 1
                        context.log.info(
                            "BRREG domain result record failed batch=%s batch_index=%s organization_number=%s total_rows_completed=%s total_rows_failed=%s error=%s",
                            batches_processed,
                            index,
                            record.organization_number,
                            rows_completed,
                            rows_failed,
                            _task_error_message(exc),
                        )
                context.log.info(
                    "BRREG domain result batch completed batch=%s records=%s total_rows_seen=%s total_rows_completed=%s total_rows_failed=%s domain_results_written=%s",
                    batches_processed,
                    len(records),
                    rows_seen,
                    rows_completed,
                    rows_failed,
                    domain_results_written,
                )

            context.log.info(
                "BRREG domain result batches committed rows_seen=%s rows_completed=%s rows_failed=%s domain_results_written=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                domain_results_written,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                store = BrregWorkingStore(cursor)
                task_summary = store.fetch_raw_task_state_summary(task_type=task_type)
                artifact_summary = store.fetch_domain_result_summary()
                failure_summary = store.fetch_task_failure_summary(task_type=task_type)
                store.finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} domain result rows failed",
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

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "domain_results_written": domain_results_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        _build_standard_task_asset_metadata(
            run_id=context.run_id,
            task_type=task_type,
            rows_claimed=rows_seen,
            rows_succeeded=rows_completed,
            rows_failed=rows_failed,
            batches_processed=batches_processed,
            result_counter_name="domain_results_written",
            result_counter_value=domain_results_written,
            live_prefix="domain",
            max_batches_per_run=max_batches_per_run,
            max_parallel_tasks=max_parallel_tasks,
            stopped_reason=stopped_reason,
            task_summary=task_summary,
            failure_summary=failure_summary,
            live_metadata={
                "live_domain_results_succeeded": artifact_summary.get("domain_result_succeeded", 0),
                "live_domain_results_partial": artifact_summary.get("domain_result_partial", 0),
                "live_domain_results_not_found": artifact_summary.get("domain_result_not_found", 0),
                "live_domain_results_failed": artifact_summary.get("domain_result_failed", 0),
                "live_domain_results_missing": artifact_summary.get("domain_result_missing", 0),
            },
        )
    )
    _raise_if_materialization_rows_failed(task_label="domain result", rows_failed=rows_failed)
    with connection_factory(database_url) as state_conn:
        BrregAssetGateway(state_conn).assert_asset_complete(
            BrregAssetName.DOMAIN_RESULTS,
            max_parallel_tasks=max_parallel_tasks,
        )
    return result


def materialize_brreg_currency_results(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
    max_batches_per_run: int = DEFAULT_CURRENCY_MAX_BATCHES_PER_RUN,
    max_parallel_tasks: int = DEFAULT_CURRENCY_RESULT_MAX_PARALLEL_TASKS,
    fx_rate_loader: Callable[[str | None], FxRateSet] | None = None,
    fx_rate_date: str | None = None,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    currency_results_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    task_type = "currency_conversion"
    task_summary: dict[str, int] = {}
    artifact_summary: dict[str, int] = {}
    failure_summary: dict[str, int] = {}
    enrichment_run_id: str | None = None
    fx_rates: FxRateSet | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id, "fx_source": "ECB"},
                )
            )
        conn.commit()
        context.log.info(
            "BRREG currency run started batch_size=%s max_batches_per_run=%s max_parallel_tasks=%s fx_rate_date=%s",
            batch_size,
            max_batches_per_run,
            max_parallel_tasks,
            fx_rate_date or "",
        )

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                claimed_batch = BrregAssetGateway(conn).claim_currency_batch(
                    ClaimTaskBatchCommand(
                        run_id=context.run_id,
                        batch_size=batch_size,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                        metadata={"fx_source": "ECB"},
                        enrichment_run_id=enrichment_run_id,
                    )
                )
                records = [claimed.record for claimed in claimed_batch.records]
                if not records:
                    stopped_reason = "no_claimable_records"
                    context.log.info(
                        "BRREG currency run has no claimable records rows_seen=%s rows_completed=%s rows_failed=%s batches_processed=%s",
                        rows_seen,
                        rows_completed,
                        rows_failed,
                        batches_processed,
                    )
                    break

                batches_processed += 1
                rows_seen += len(records)
                context.log.info(
                    "BRREG currency batch claimed batch=%s records=%s total_rows_seen=%s total_rows_completed_before_batch=%s total_rows_failed_before_batch=%s",
                    batches_processed,
                    len(records),
                    rows_seen,
                    rows_completed,
                    rows_failed,
                )
                for claimed in claimed_batch.records:
                    record = claimed.record
                    attempt = TaskAttempt(
                        id=claimed.task_attempt_id,
                        raw_record_id=claimed.raw_record_id,
                        attempt=claimed.attempt,
                    )
                    try:
                        if _record_needs_currency_conversion(record) and fx_rates is None:
                            loader = fx_rate_loader or _load_brreg_fx_rates
                            fx_rates = loader(fx_rate_date)
                        _write_currency_result(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            fx_rates=fx_rates,
                        )
                        currency_results_written += 1
                        rows_completed += 1
                    except Exception as exc:
                        conn.rollback()
                        _mark_currency_result_failed(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            error=str(exc),
                        )
                        rows_failed += 1
                        currency_results_written += 1
                context.log.info(
                    "BRREG currency batch completed batch=%s records=%s total_rows_seen=%s total_rows_completed=%s total_rows_failed=%s currency_results_written=%s",
                    batches_processed,
                    len(records),
                    rows_seen,
                    rows_completed,
                    rows_failed,
                    currency_results_written,
                )

            context.log.info(
                "BRREG currency batches committed rows_seen=%s rows_completed=%s rows_failed=%s currency_results_written=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                currency_results_written,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                store = BrregWorkingStore(cursor)
                task_summary = store.fetch_raw_task_state_summary(task_type=task_type)
                artifact_summary = store.fetch_currency_result_summary()
                failure_summary = store.fetch_task_failure_summary(task_type=task_type)
                store.finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} currency rows failed",
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

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "currency_results_written": currency_results_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        _build_standard_task_asset_metadata(
            run_id=context.run_id,
            task_type=task_type,
            rows_claimed=rows_seen,
            rows_succeeded=rows_completed,
            rows_failed=rows_failed,
            batches_processed=batches_processed,
            result_counter_name="currency_results_written",
            result_counter_value=currency_results_written,
            live_prefix="currency",
            max_batches_per_run=max_batches_per_run,
            max_parallel_tasks=max_parallel_tasks,
            stopped_reason=stopped_reason,
            task_summary=task_summary,
            failure_summary=failure_summary,
            live_metadata={
                "live_currency_results_succeeded": artifact_summary.get("currency_result_succeeded", 0),
                "live_currency_results_skipped": artifact_summary.get("currency_result_skipped", 0),
                "live_currency_results_not_available": artifact_summary.get("currency_result_not_available", 0),
                "live_currency_results_failed": artifact_summary.get("currency_result_failed", 0),
                "live_currency_results_missing": artifact_summary.get("currency_result_missing", 0),
            },
        )
    )
    _raise_if_materialization_rows_failed(task_label="currency", rows_failed=rows_failed)
    with connection_factory(database_url) as state_conn:
        BrregAssetGateway(state_conn).assert_asset_complete(
            BrregAssetName.CURRENCY_RESULTS,
            max_parallel_tasks=max_parallel_tasks,
        )
    return result


def materialize_brreg_enhanced_records(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    enhanced_records_built = 0
    enrichment_run_id: str | None = None
    task_type = "build_enhanced"
    task_summary: dict[str, int] = {}
    artifact_summary: dict[str, int] = {}
    failure_summary: dict[str, int] = {}
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id},
                )
            )
        conn.commit()
        context.log.info(
            "BRREG enhanced record run started batch_size=%s",
            batch_size,
        )

        try:
            claimed_batch = BrregAssetGateway(conn).claim_enhanced_batch(
                ClaimEnhancedBatchCommand(
                    run_id=context.run_id,
                    batch_size=batch_size,
                    metadata={"source": "dagster"},
                    enrichment_run_id=enrichment_run_id,
                )
            )
            records = [claimed.build_record for claimed in claimed_batch.records]
            context.log.info(
                "BRREG enhanced record batch claimed records=%s",
                len(records),
            )

            for claimed in claimed_batch.records:
                build_record = claimed.build_record
                rows_seen += 1
                attempt = TaskAttempt(
                    id=claimed.task_attempt_id,
                    raw_record_id=build_record.record.id,
                    attempt=claimed.attempt,
                )
                try:
                    _build_record_enhanced_payload(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        build_record=build_record,
                        dagster_run_id=context.run_id,
                    )
                    rows_completed += 1
                    enhanced_records_built += 1
                except Exception as exc:
                    conn.rollback()
                    _mark_record_task_failed(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        record=build_record.record,
                        task_type=task_type,
                        error=exc,
                    )
                    rows_failed += 1
            context.log.info(
                "BRREG enhanced record batch completed rows_seen=%s rows_completed=%s rows_failed=%s enhanced_records_built=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                enhanced_records_built,
            )

            context.log.info(
                "BRREG enhanced record batch committed rows_seen=%s rows_completed=%s rows_failed=%s enhanced_records_built=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                enhanced_records_built,
            )

            with conn.cursor() as cursor:
                store = BrregWorkingStore(cursor)
                store.finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} enhanced record rows failed",
                    )
                )
                task_summary = store.fetch_raw_task_state_summary(task_type=task_type)
                artifact_summary = store.fetch_enhanced_record_summary()
                failure_summary = store.fetch_task_failure_summary(task_type=task_type)
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

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "enhanced_records_built": enhanced_records_built,
    }
    context.add_output_metadata(
        _build_standard_task_asset_metadata(
            run_id=context.run_id,
            task_type=task_type,
            rows_claimed=rows_seen,
            rows_succeeded=rows_completed,
            rows_failed=rows_failed,
            batches_processed=1 if rows_seen else 0,
            result_counter_name="enhanced_records_built",
            result_counter_value=enhanced_records_built,
            live_prefix="enhanced",
            max_batches_per_run=1,
            max_parallel_tasks=1,
            stopped_reason="completed",
            task_summary=task_summary,
            failure_summary=failure_summary,
            live_metadata={
                "live_enhanced_records_built": artifact_summary.get("enhanced_record_built", 0),
                "live_enhanced_records_published": artifact_summary.get("enhanced_record_published", 0),
                "live_enhanced_records_publish_failed": artifact_summary.get("enhanced_record_publish_failed", 0),
                "live_enhanced_records_superseded": artifact_summary.get("enhanced_record_superseded", 0),
                "live_enhanced_records_missing": artifact_summary.get("enhanced_record_missing", 0),
            },
        )
    )
    _raise_if_materialization_rows_failed(task_label="enhanced record", rows_failed=rows_failed)
    return result


def _write_raw_record_batch(
    *,
    conn,
    enrichment_run_id: str,
    bulk_snapshot_id: str,
    rows,
):
    return BrregAssetGateway(conn).ingest_raw_records(
        IngestRawRecordsCommand(
            bulk_snapshot_id=bulk_snapshot_id,
            rows=rows,
            enrichment_run_id=enrichment_run_id,
        )
    )


def _translate_record_batch(
    *,
    conn,
    enrichment_run_id: str,
    claimed_records: list[ClaimedRawRecord],
    translator: TermTranslator,
    model: str,
    prompt_version: str,
) -> tuple[int, int, TaskFailureLogSummary]:
    attempts_by_record_id: dict[str, TaskAttempt] = {}
    items_by_record_id: dict[str, list[TranslationItem]] = {}
    failure_log_summary: TaskFailureLogSummary = {}
    records = [claimed.record for claimed in claimed_records]
    for claimed in claimed_records:
        attempts_by_record_id[claimed.raw_record_id] = TaskAttempt(
            id=claimed.task_attempt_id,
            raw_record_id=claimed.raw_record_id,
            attempt=claimed.attempt,
        )
        record = claimed.record
        items_by_record_id[record.id] = extract_translation_items(record.raw_payload)

    unique_items = _unique_translation_items(
        item
        for items in items_by_record_id.values()
        for item in items
    )
    try:
        keys = [translation_cache_key(item) for item in unique_items]
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            cached = store.fetch_cached_translations(keys, model=model, prompt_version=prompt_version)

        missing_items = [
            item
            for item in unique_items
            if translation_cache_key(item) not in cached
        ]
        new_cache_rows = _translate_missing_terms(
            translator=translator,
            missing_items=missing_items,
            model=model,
            prompt_version=prompt_version,
        )
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.upsert_cached_translations(new_cache_rows)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        for record in records:
            classification = _mark_record_task_failed(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                task_type="translate",
                error=exc,
                translation_model=model,
                translation_prompt_version=prompt_version,
            )
            _add_failure_log_entry(
                failure_log_summary,
                classification=classification,
                error=exc,
            )
        return 0, len(records), failure_log_summary

    for row in new_cache_rows:
        key = TranslationCacheKey(
            category=row.category,
            source_lang=row.source_lang,
            target_lang=row.target_lang,
            original_hash=row.original_hash,
        )
        cached[key] = CachedTermTranslation(
            category=row.category,
            original_text=row.original_text,
            translated_text=row.translated_text,
            model=row.model,
            prompt_version=row.prompt_version,
        )

    rows_completed = 0
    rows_failed = 0
    for record in records:
        try:
            _write_translation_record_result(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                items=items_by_record_id[record.id],
                cached_translations=cached,
                model=model,
                prompt_version=prompt_version,
            )
            rows_completed += 1
        except Exception as exc:
            conn.rollback()
            classification = _mark_record_task_failed(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                task_type="translate",
                error=exc,
                translation_model=model,
                translation_prompt_version=prompt_version,
            )
            _add_failure_log_entry(
                failure_log_summary,
                classification=classification,
                error=exc,
            )
            rows_failed += 1
    return rows_completed, rows_failed, failure_log_summary


def _write_translation_record_result(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    items: list[TranslationItem],
    cached_translations: dict[TranslationCacheKey, CachedTermTranslation],
    model: str,
    prompt_version: str,
) -> None:
    if not items:
        payload = build_translation_payload(
            raw_payload=record.raw_payload,
            items=[],
            cached_translations={},
            model=model,
            prompt_version=prompt_version,
        )
        status = "skipped"
        metadata = {"reason": "no_translatable_terms"}
    else:
        _raise_if_missing_translation_terms(
            items=items,
            cached_translations=cached_translations,
        )
        payload = build_translation_payload(
            raw_payload=record.raw_payload,
            items=items,
            cached_translations=cached_translations,
            model=model,
            prompt_version=prompt_version,
        )
        status = "succeeded"
        metadata = {}

    BrregAssetGateway(
        conn,
        translation_model=model,
        translation_prompt_version=prompt_version,
    ).submit_translation_result(
        SubmitTranslationResultCommand(
            raw_record_id=record.id,
            task_attempt_id=attempt.id,
            status=status,
            translated_payload=payload,
            model=model,
            prompt_version=prompt_version,
            metadata=metadata,
            enrichment_run_id=enrichment_run_id,
        )
    )


def _raise_if_missing_translation_terms(
    *,
    items: list[TranslationItem],
    cached_translations: dict[TranslationCacheKey, CachedTermTranslation],
) -> None:
    missing_items = [
        item
        for item in items
        if translation_cache_key(item) not in cached_translations
    ]
    if missing_items:
        raise RuntimeError("translation service did not return translations for all requested terms")


def _write_domain_result(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    payload: dict,
) -> bool:
    status = str(payload.get("status") or "failed")
    task_status = "failed" if status == "failed" else "succeeded"
    error = _domain_result_error(payload)
    metadata = {
        "source": "crawl-service",
        "service_version": payload.get("service_version"),
        "model": payload.get("model"),
        "provider": payload.get("provider"),
    }
    gateway = BrregAssetGateway(conn)
    if task_status == "failed":
        structured_error = _structured_error_from_payload(
            payload,
            fallback_message=error or "domain service returned failed status",
        )
        classification = classify_task_error(task_type="domain_results", error=structured_error)
        gateway.submit_domain_failure(
            SubmitTaskFailureCommand(
                asset=BrregAssetName.DOMAIN_RESULTS,
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                error=_task_error_message(structured_error),
                error_category=classification.error_category,
                error_code=classification.error_code,
                retry_strategy=classification.retry_strategy,
                metadata=metadata,
                enrichment_run_id=enrichment_run_id,
                artifact_payload=payload,
            )
        )
    else:
        gateway.submit_domain_result(
            SubmitDomainResultCommand(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status=status,
                best_domain=payload.get("best_domain"),
                domain_payload=payload,
                error=error,
                metadata=metadata,
                enrichment_run_id=enrichment_run_id,
            )
        )
    return task_status == "succeeded"


def _mark_domain_result_failed(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    error: str,
) -> None:
    payload = {
        "schema_version": "crawl-service.brreg.v1",
        "status": "failed",
        "record_id": record.id,
        "organization_number": record.organization_number,
        "best_domain": None,
        "candidates": [],
        "search_artifacts": [],
        "crawl_artifacts": [],
        "errors": [{"code": "dagster_crawl_service_call_failed", "message": "Crawl service call failed.", "detail": {"error": error}}],
        "warnings": [],
    }
    classification = classify_task_error(task_type="domain_results", error=error or "domain service returned failed status")
    BrregAssetGateway(conn).submit_domain_failure(
        SubmitTaskFailureCommand(
            asset=BrregAssetName.DOMAIN_RESULTS,
            raw_record_id=record.id,
            task_attempt_id=attempt.id,
            error=error,
            error_category=classification.error_category,
            error_code=classification.error_code,
            retry_strategy=classification.retry_strategy,
            metadata={"source": "dagster"},
            enrichment_run_id=enrichment_run_id,
            artifact_payload=payload,
        )
    )


def _domain_result_error(payload: dict) -> str | None:
    errors = payload.get("errors")
    if not isinstance(errors, list) or not errors:
        return None
    first = errors[0]
    if not isinstance(first, dict):
        return str(first)
    message = first.get("message") or first.get("code")
    return str(message) if message else None


def _structured_error_from_payload(payload: dict, *, fallback_message: str) -> object:
    errors = payload.get("errors")
    if not isinstance(errors, list) or not errors or not isinstance(errors[0], dict):
        return fallback_message
    first = errors[0]
    category = _optional_payload_text(first.get("error_category") or first.get("category"))
    code = _optional_payload_text(first.get("error_code") or first.get("code"))
    retry_strategy = _optional_payload_text(first.get("retry_strategy"))
    if not category and not code and not retry_strategy:
        return fallback_message
    message = _optional_payload_text(first.get("message")) or fallback_message
    return StructuredTaskError(
        message,
        TaskFailureClassification(
            category or "unknown",
            code or "task_failed",
            retry_strategy or "automatic",
        ),
    )


def _optional_payload_text(value) -> str | None:
    text = str(value or "").strip()
    return text or None


def _write_currency_result(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    fx_rates: FxRateSet | None,
) -> None:
    command = _build_currency_result(record=record, attempt=attempt, fx_rates=fx_rates)
    BrregAssetGateway(conn).submit_currency_result(
        SubmitCurrencyResultCommand(
            raw_record_id=command.raw_record_id,
            task_attempt_id=command.task_attempt_id,
            status=command.status,
            original_currency=command.original_currency,
            original_payload=command.original_payload,
            usd_payload=command.usd_payload,
            fx_metadata=command.fx_metadata,
            source_uri=command.source_uri,
            error=command.error,
            metadata=command.metadata,
            enrichment_run_id=enrichment_run_id,
        )
    )


def _build_currency_result(
    *,
    record: RawTaskRecord,
    attempt: TaskAttempt,
    fx_rates: FxRateSet | None,
) -> InsertCurrencyResult:
    capital = record.raw_payload.get("kapital")
    if not isinstance(capital, dict) or not capital:
        return InsertCurrencyResult(
            raw_record_id=record.id,
            task_attempt_id=attempt.id,
            status="skipped",
            original_currency=None,
            original_payload={},
            usd_payload={},
            fx_metadata={},
            source_uri=None,
            error=None,
            metadata={"reason": "no_capital"},
        )

    original_amount = capital.get("belop")
    original_currency = _optional_currency(capital.get("valuta"))
    if original_amount is None or original_currency is None:
        raise ValueError("incomplete capital currency data")
    if fx_rates is None:
        raise ValueError("FX rates are required for BRREG currency conversion")

    amount_usd_cents = fx_rates.to_usd_cents(original_amount, original_currency)
    amount_usd = amount_usd_cents / 100
    return InsertCurrencyResult(
        raw_record_id=record.id,
        task_attempt_id=attempt.id,
        status="succeeded",
        original_currency=original_currency,
        original_payload={
            "capital": {
                "original_amount": float(original_amount),
                "original_currency": original_currency,
            }
        },
        usd_payload={
            "capital": {
                "amount_usd": amount_usd,
                "amount_usd_cents": amount_usd_cents,
            }
        },
        fx_metadata={
            "source": fx_rates.source,
            "rate_date": fx_rates.rate_date,
            "capital": fx_rates.exchange_metadata(original_currency),
        },
        source_uri=None,
        error=None,
        metadata={"source": "dagster"},
    )


def classify_task_error(*, task_type: str, error: object | None) -> TaskFailureClassification:
    structured_category = _optional_error_attr(error, "error_category")
    structured_code = _optional_error_attr(error, "error_code")
    structured_retry_strategy = _optional_error_attr(error, "retry_strategy")
    if structured_category or structured_code or structured_retry_strategy:
        return TaskFailureClassification(
            structured_category or "unknown",
            structured_code or "task_failed",
            structured_retry_strategy or "automatic",
        )
    text = _task_error_message(error)
    normalized = text.lower()
    if not normalized:
        return TaskFailureClassification("unknown", "unknown_error", "automatic")
    if "rate limit" in normalized or "rate_limited" in normalized or "429" in normalized:
        return TaskFailureClassification("rate_limited", "rate_limited", "automatic")
    if "missing translations" in normalized or "did not return translations" in normalized:
        return TaskFailureClassification(
            "invalid_llm_output",
            "missing_translation_terms",
            "change_model_or_prompt",
        )
    if "non-object response" in normalized or "invalid json" in normalized or "malformed" in normalized:
        return TaskFailureClassification(
            "invalid_llm_output",
            "malformed_llm_response",
            "change_model_or_prompt",
        )
    if "api key" in normalized or "unauthorized" in normalized or "401" in normalized or "403" in normalized:
        return TaskFailureClassification("blocked_by_config", "auth_or_config_error", "manual_config")
    if "timeout" in normalized or "timed out" in normalized or "temporarily unavailable" in normalized:
        return TaskFailureClassification("transient_external", "external_timeout", "automatic")
    if "connection refused" in normalized or "connection reset" in normalized or "502" in normalized or "503" in normalized:
        return TaskFailureClassification("transient_external", "external_service_unavailable", "automatic")
    if "not found" in normalized:
        return TaskFailureClassification("not_found", "not_found", "not_retryable")
    if "invalid input" in normalized or "missing required" in normalized:
        return TaskFailureClassification("invalid_input", "invalid_input", "manual_input")
    if task_type == "translate":
        return TaskFailureClassification("unknown", "translation_failed", "automatic")
    return TaskFailureClassification("unknown", "task_failed", "automatic")


def _optional_error_attr(error: object | None, name: str) -> str | None:
    text = str(getattr(error, name, "") or "").strip()
    return text or None


def _task_error_message(error: object | None) -> str:
    return str(error or "").strip()


def _add_failure_log_entry(
    failure_summary: TaskFailureLogSummary,
    *,
    classification: TaskFailureClassification,
    error: object,
) -> None:
    key = TaskFailureLogKey(
        error_category=classification.error_category,
        error_code=classification.error_code,
        retry_strategy=classification.retry_strategy,
        sample_error=_safe_log_error_message(error),
    )
    failure_summary[key] = failure_summary.get(key, 0) + 1


def _log_batch_failure_summary(
    context,
    *,
    task_label: str,
    batch: int,
    failure_summary: TaskFailureLogSummary,
) -> None:
    for failure, count in sorted(
        failure_summary.items(),
        key=lambda item: (-item[1], item[0].error_category, item[0].error_code, item[0].retry_strategy),
    ):
        context.log.info(
            "BRREG %s batch failure reason batch=%s error_category=%s error_code=%s retry_strategy=%s count=%s sample_error=%s",
            task_label,
            batch,
            failure.error_category,
            failure.error_code,
            failure.retry_strategy,
            count,
            failure.sample_error,
        )


def _safe_log_error_message(error: object | None, *, max_length: int = 240) -> str:
    message = " ".join(_task_error_message(error).split())
    if len(message) <= max_length:
        return message
    return message[: max_length - 3] + "..."


def _mark_currency_result_failed(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    error: str,
) -> None:
    classification = classify_task_error(task_type="currency_conversion", error=error)
    BrregAssetGateway(conn).submit_currency_failure(
        SubmitTaskFailureCommand(
            asset=BrregAssetName.CURRENCY_RESULTS,
            raw_record_id=record.id,
            task_attempt_id=attempt.id,
            error=error,
            error_category=classification.error_category,
            error_code=classification.error_code,
            retry_strategy=classification.retry_strategy,
            metadata={
                "source": "dagster",
                "original_currency": _capital_original_currency(record),
            },
            enrichment_run_id=enrichment_run_id,
        )
    )


def _record_needs_currency_conversion(record: RawTaskRecord) -> bool:
    capital = record.raw_payload.get("kapital")
    return isinstance(capital, dict) and ("belop" in capital or "valuta" in capital)


def _capital_original_currency(record: RawTaskRecord) -> str | None:
    capital = record.raw_payload.get("kapital")
    if not isinstance(capital, dict):
        return None
    return _optional_currency(capital.get("valuta"))


def _optional_currency(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _translate_missing_terms(
    *,
    translator: TermTranslator,
    missing_items: list[TranslationItem],
    model: str,
    prompt_version: str,
) -> list[UpsertCachedTranslation]:
    rows: list[UpsertCachedTranslation] = []
    unique_items = _unique_translation_items(missing_items)
    if not unique_items:
        return rows
    translated_by_id = translator.translate_terms(
        category="mixed",
        items=unique_items,
        source_lang="no",
        target_lang="en",
        model=model,
        prompt_version=prompt_version,
    )
    for item in unique_items:
        translated_text = translated_by_id.get(translation_item_id(item), "").strip()
        if not translated_text:
            continue
        key = translation_cache_key(item)
        rows.append(
            UpsertCachedTranslation(
                category=item.category,
                source_lang=key.source_lang,
                target_lang=key.target_lang,
                original_hash=key.original_hash,
                original_text=item.text,
                translated_text=translated_text,
                model=model,
                prompt_version=prompt_version,
                metadata={"source": "dagster"},
            )
        )
    return rows


def _unique_translation_items(items: Iterable[TranslationItem]) -> list[TranslationItem]:
    seen: set[TranslationCacheKey] = set()
    unique: list[TranslationItem] = []
    for item in sorted(items, key=lambda value: (value.category, value.text.strip().lower())):
        key = translation_cache_key(item)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _build_record_enhanced_payload(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    build_record,
    dagster_run_id: str,
) -> None:
    payload = build_brreg_enhanced_payload(
        record=build_record.record,
        payload_hash=build_record.payload_hash,
        translation_status=build_record.translation_status,
        translation_payload=build_record.translation_payload,
        domain_status=build_record.domain_status,
        domain_candidates=build_record.domain_candidates,
        currency_status=build_record.currency_status,
        original_payload=build_record.original_payload,
        usd_payload=build_record.usd_payload,
        fx_metadata=build_record.fx_metadata,
        task_statuses=build_record.task_statuses,
        dagster_run_id=dagster_run_id,
    )
    BrregAssetGateway(conn).submit_enhanced_record(
        SubmitEnhancedRecordCommand(
            raw_record_id=build_record.record.id,
            task_attempt_id=attempt.id,
            schema_version=BRREG_ENHANCED_SCHEMA_VERSION,
            enhanced_payload=payload,
            enhanced_payload_hash=enhanced_payload_hash(payload),
            metadata={
                "source": "dagster",
                "task_statuses": build_record.task_statuses,
                "raw_payload_hash": build_record.payload_hash,
            },
            enrichment_run_id=enrichment_run_id,
        )
    )


def _load_brreg_fx_rates(rate_date: str | None) -> FxRateSet:
    if not rate_date:
        return load_latest_ecb_rates()
    return load_ecb_rates_for_date(date.fromisoformat(rate_date))


def _mark_record_task_failed(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    task_type: str,
    error: object,
    translation_model: str | None = None,
    translation_prompt_version: str | None = None,
) -> TaskFailureClassification:
    classification = classify_task_error(task_type=task_type, error=error)
    error_message = _task_error_message(error)
    if task_type == "translate":
        BrregAssetGateway(
            conn,
            translation_model=translation_model,
            translation_prompt_version=translation_prompt_version,
        ).submit_translation_failure(
            SubmitTaskFailureCommand(
                asset=BrregAssetName.TRANSLATION_RESULTS,
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                error=error_message,
                error_category=classification.error_category,
                error_code=classification.error_code,
                retry_strategy=classification.retry_strategy,
                metadata={},
                enrichment_run_id=enrichment_run_id,
                model=translation_model,
                prompt_version=translation_prompt_version,
            )
        )
    else:
        BrregAssetGateway(conn).submit_enhanced_failure(
            SubmitTaskFailureCommand(
                asset=BrregAssetName.ENHANCED_RECORDS,
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                error=error_message,
                error_category=classification.error_category,
                error_code=classification.error_code,
                retry_strategy=classification.retry_strategy,
                metadata={},
                enrichment_run_id=enrichment_run_id,
            )
        )
    return classification


def _enrichment_run_key(context, run_type: str) -> str:
    return f"{context.run_id}:{run_type}"
