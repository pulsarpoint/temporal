from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date

from corpscout_dagster.brreg.crawl_service import CrawlServiceClient
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
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    CreateTaskAttempt,
    FinishEnrichmentRun,
    IncrementEnrichmentRunProgress,
    InsertDomainResult,
    InsertEnhancedRecord,
    InsertCurrencyResult,
    InsertTranslationResult,
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
                batch = []

            if batch:
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

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(enrichment_run_id=enrichment_run_id, status="succeeded", error=None)
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(enrichment_run_id=enrichment_run_id, status="failed", error=str(exc))
                )
            conn.commit()
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

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                        task_type="translate",
                        limit=batch_size,
                        include_new_records=True,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()

                if not records:
                    stopped_reason = "no_claimable_records"
                    break

                batches_processed += 1
                completed, failed = _translate_record_batch(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    records=records,
                    translator=translator,
                    model=model,
                    prompt_version=prompt_version,
                )
                rows_seen += len(records)
                rows_completed += completed
                rows_failed += failed

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
                store.finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} translation rows failed",
                    )
                )
                task_summary = store.fetch_raw_task_state_summary(task_type="translate")
                artifact_summary = store.fetch_translation_artifact_summary(model=model, prompt_version=prompt_version)
                failure_summary = store.fetch_task_failure_summary(task_type="translate")
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

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                        task_type=task_type,
                        limit=batch_size,
                        include_new_records=True,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()
                if not records:
                    stopped_reason = "no_claimable_records"
                    break

                batches_processed += 1
                rows_seen += len(records)
                for record in records:
                    attempt = _create_task_attempt(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        record=record,
                        task_type=task_type,
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
                        else:
                            rows_failed += 1
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
                BrregWorkingStore(cursor).finish_enrichment_run(
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
        {
            **result,
            "dagster_run_id": context.run_id,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
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

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                        task_type=task_type,
                        limit=batch_size,
                        include_new_records=True,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()
                if not records:
                    stopped_reason = "no_claimable_records"
                    break

                batches_processed += 1
                rows_seen += len(records)
                for record in records:
                    attempt = _create_task_attempt(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        record=record,
                        task_type=task_type,
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
                BrregWorkingStore(cursor).finish_enrichment_run(
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
        {
            **result,
            "dagster_run_id": context.run_id,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
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

        try:
            with conn.cursor() as cursor:
                records = BrregWorkingStore(cursor).fetch_pending_enhanced_build_records(limit=batch_size)

            for build_record in records:
                rows_seen += 1
                attempt = _create_task_attempt(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    record=build_record.record,
                    task_type=task_type,
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
                        error=str(exc),
                    )
                    rows_failed += 1

            context.log.info(
                "BRREG enhanced record batch committed rows_seen=%s rows_completed=%s rows_failed=%s enhanced_records_built=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                enhanced_records_built,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} enhanced record rows failed",
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
        "enhanced_records_built": enhanced_records_built,
    }
    context.add_output_metadata({**result, "dagster_run_id": context.run_id, "task_type": task_type})
    return result


def _create_task_attempt(
    *,
    conn,
    enrichment_run_id: str,
    record: RawTaskRecord,
    task_type: str,
) -> TaskAttempt:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        attempt = store.create_task_attempt(
            CreateTaskAttempt(
                enrichment_run_id=enrichment_run_id,
                raw_record_id=record.id,
                task_type=task_type,
                metadata={"organization_number": record.organization_number},
            )
        )
    conn.commit()
    return attempt


def _write_raw_record_batch(
    *,
    conn,
    enrichment_run_id: str,
    bulk_snapshot_id: str,
    rows,
):
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        result = store.upsert_raw_records(rows, bulk_snapshot_id=bulk_snapshot_id)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=result.rows_seen,
                records_completed=result.rows_written,
            )
        )
    conn.commit()
    return result


def _translate_record_batch(
    *,
    conn,
    enrichment_run_id: str,
    records: list[RawTaskRecord],
    translator: TermTranslator,
    model: str,
    prompt_version: str,
) -> tuple[int, int]:
    attempts_by_record_id: dict[str, TaskAttempt] = {}
    items_by_record_id: dict[str, list[TranslationItem]] = {}
    for record in records:
        attempts_by_record_id[record.id] = _create_task_attempt(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            record=record,
            task_type="translate",
        )
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
            _mark_record_task_failed(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                task_type="translate",
                error=str(exc),
            )
        return 0, len(records)

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
            _mark_record_task_failed(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                task_type="translate",
                error=str(exc),
            )
            rows_failed += 1
    return rows_completed, rows_failed


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

    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_translation_result(
            InsertTranslationResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status=status,
                translated_payload=payload,
                model=model,
                prompt_version=prompt_version,
                error=None,
                metadata=metadata,
            )
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status=status, error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()


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
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_domain_result(
            InsertDomainResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status=status,
                best_domain=payload.get("best_domain"),
                domain_payload=payload,
                error=error,
                metadata={
                    "source": "crawl-service",
                    "service_version": payload.get("service_version"),
                    "model": payload.get("model"),
                    "provider": payload.get("provider"),
                },
            )
        )
        if task_status == "failed":
            _finish_failed_task_attempt(
                store=store,
                attempt=attempt,
                task_type="domain_results",
                error=error or "domain service returned failed status",
            )
        else:
            store.finish_task_attempt(task_attempt_id=attempt.id, status=task_status, error=error)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=1,
                records_completed=1 if task_status == "succeeded" else 0,
                records_failed=0 if task_status == "succeeded" else 1,
            )
        )
    conn.commit()
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
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_domain_result(
            InsertDomainResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status="failed",
                best_domain=None,
                domain_payload=payload,
                error=error,
                metadata={"source": "dagster"},
            )
        )
        _finish_failed_task_attempt(
            store=store,
            attempt=attempt,
            task_type="domain_results",
            error=error or "domain service returned failed status",
        )
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=1,
                records_completed=0,
                records_failed=1,
            )
        )
    conn.commit()


def _domain_result_error(payload: dict) -> str | None:
    errors = payload.get("errors")
    if not isinstance(errors, list) or not errors:
        return None
    first = errors[0]
    if not isinstance(first, dict):
        return str(first)
    message = first.get("message") or first.get("code")
    return str(message) if message else None


def _write_currency_result(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    fx_rates: FxRateSet | None,
) -> None:
    command = _build_currency_result(record=record, attempt=attempt, fx_rates=fx_rates)
    task_status = "skipped" if command.status in {"skipped", "not_available"} else "succeeded"
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_currency_result(command)
        store.finish_task_attempt(task_attempt_id=attempt.id, status=task_status, error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()


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


def classify_task_error(*, task_type: str, error: str | None) -> TaskFailureClassification:
    text = str(error or "").strip()
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


def _finish_failed_task_attempt(
    *,
    store: BrregWorkingStore,
    attempt: TaskAttempt,
    task_type: str,
    error: str,
) -> TaskFailureClassification:
    classification = classify_task_error(task_type=task_type, error=error)
    store.finish_task_attempt(
        task_attempt_id=attempt.id,
        status="failed",
        error=error,
        error_category=classification.error_category,
        error_code=classification.error_code,
        retry_strategy=classification.retry_strategy,
    )
    return classification


def _mark_currency_result_failed(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    error: str,
) -> None:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_currency_result(
            InsertCurrencyResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status="failed",
                original_currency=_capital_original_currency(record),
                original_payload={},
                usd_payload={},
                fx_metadata={},
                source_uri=None,
                error=error,
                metadata={"source": "dagster"},
            )
        )
        _finish_failed_task_attempt(
            store=store,
            attempt=attempt,
            task_type="currency_conversion",
            error=error,
        )
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=1,
                records_completed=0,
                records_failed=1,
            )
        )
    conn.commit()


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
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.upsert_enhanced_record(
            InsertEnhancedRecord(
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
            )
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()


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
    error: str,
) -> None:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        classification = _finish_failed_task_attempt(
            store=store,
            attempt=attempt,
            task_type=task_type,
            error=error,
        )
        if task_type == "translate":
            store.insert_translation_result(
                InsertTranslationResult(
                    raw_record_id=record.id,
                    task_attempt_id=attempt.id,
                    status="failed",
                    translated_payload=None,
                    model=None,
                    prompt_version=None,
                    error=error,
                    metadata={
                        "error_category": classification.error_category,
                        "error_code": classification.error_code,
                        "retry_strategy": classification.retry_strategy,
                    },
                )
            )
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=1,
                records_completed=0,
                records_failed=1,
            )
        )
    conn.commit()


def _enrichment_run_key(context, run_type: str) -> str:
    return f"{context.run_id}:{run_type}"
