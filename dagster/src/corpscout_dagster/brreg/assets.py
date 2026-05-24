from __future__ import annotations

import asyncio
import os
from collections.abc import Iterable
from itertools import groupby

import psycopg
from dagster import AssetKey, asset

from corpscout_dagster.brreg.domain_enrichment import build_domain_proposals, discover_domain_candidates_for_signal
from corpscout_dagster.brreg.models import BrregRawRecord, BrregWorkingRawRecordRow
from corpscout_dagster.brreg.source import BRREG_API_BASE_URL, BRREG_BULK_PATH, iter_brreg_bulk_records
from corpscout_dagster.brreg.translation import (
    DEFAULT_LLM_MODEL,
    DEFAULT_PROMPT_VERSION,
    CachedTermTranslation,
    DirectLLMTermTranslator,
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
    InsertDomainCandidate,
    InsertDomainProposal,
    InsertTranslationResult,
    RawTaskRecord,
    TaskAttempt,
    UpsertCachedTranslation,
)


BRREG_BULK_URL = f"{BRREG_API_BASE_URL}{BRREG_BULK_PATH}"
DEFAULT_RAW_RECORD_BATCH_SIZE = 5000
DEFAULT_TRANSLATION_RECORD_BATCH_SIZE = 50
DEFAULT_DOMAIN_WEBSITE_FIELD_BATCH_SIZE = 5000
DEFAULT_DOMAIN_DUCKDUCKGO_BATCH_SIZE = 10
DEFAULT_DOMAIN_CRTSH_BATCH_SIZE = 10
DEFAULT_DOMAIN_WIKIDATA_BATCH_SIZE = 25
DEFAULT_DOMAIN_DNS_HEURISTIC_BATCH_SIZE = 100
DEFAULT_DOMAIN_PROPOSAL_BATCH_SIZE = 500
DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN = 20

DOMAIN_SIGNAL_ASSET_KEYS = [
    AssetKey("brreg_domain_website_field_candidates"),
    AssetKey("brreg_domain_duckduckgo_candidates"),
    AssetKey("brreg_domain_crtsh_candidates"),
    AssetKey("brreg_domain_wikidata_candidates"),
    AssetKey("brreg_domain_dns_heuristic_candidates"),
]


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


@asset(name="brreg_translation_results")
def brreg_translation_results(context) -> dict[str, int]:
    return materialize_brreg_translation_results(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        translator=DirectLLMTermTranslator(),
        batch_size=_env_int("BRREG_TRANSLATION_BATCH_SIZE", DEFAULT_TRANSLATION_RECORD_BATCH_SIZE),
        model=os.environ.get("BRREG_TRANSLATION_MODEL") or DEFAULT_LLM_MODEL,
        prompt_version=os.environ.get("BRREG_TRANSLATION_PROMPT_VERSION") or DEFAULT_PROMPT_VERSION,
    )


@asset(name="brreg_domain_website_field_candidates")
def brreg_domain_website_field_candidates(context) -> dict[str, int]:
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="website_field",
        task_type="domain_website_field",
        batch_size=_env_int("BRREG_DOMAIN_WEBSITE_FIELD_BATCH_SIZE", DEFAULT_DOMAIN_WEBSITE_FIELD_BATCH_SIZE),
        max_batches_per_run=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
    )


@asset(name="brreg_domain_duckduckgo_candidates")
def brreg_domain_duckduckgo_candidates(context) -> dict[str, int]:
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="duckduckgo",
        task_type="domain_duckduckgo",
        batch_size=_env_int("BRREG_DOMAIN_DUCKDUCKGO_BATCH_SIZE", DEFAULT_DOMAIN_DUCKDUCKGO_BATCH_SIZE),
        max_batches_per_run=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
    )


@asset(name="brreg_domain_crtsh_candidates")
def brreg_domain_crtsh_candidates(context) -> dict[str, int]:
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="crtsh",
        task_type="domain_crtsh",
        batch_size=_env_int("BRREG_DOMAIN_CRTSH_BATCH_SIZE", DEFAULT_DOMAIN_CRTSH_BATCH_SIZE),
        max_batches_per_run=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
    )


@asset(name="brreg_domain_wikidata_candidates")
def brreg_domain_wikidata_candidates(context) -> dict[str, int]:
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="wikidata",
        task_type="domain_wikidata",
        batch_size=_env_int("BRREG_DOMAIN_WIKIDATA_BATCH_SIZE", DEFAULT_DOMAIN_WIKIDATA_BATCH_SIZE),
        max_batches_per_run=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
    )


@asset(name="brreg_domain_dns_heuristic_candidates")
def brreg_domain_dns_heuristic_candidates(context) -> dict[str, int]:
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="heuristic",
        task_type="domain_dns_heuristic",
        batch_size=_env_int("BRREG_DOMAIN_DNS_HEURISTIC_BATCH_SIZE", DEFAULT_DOMAIN_DNS_HEURISTIC_BATCH_SIZE),
        max_batches_per_run=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
    )


@asset(name="brreg_domain_proposals", deps=DOMAIN_SIGNAL_ASSET_KEYS)
def brreg_domain_proposals(context) -> dict[str, int]:
    return materialize_brreg_domain_proposals(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=_env_int("BRREG_DOMAIN_PROPOSAL_BATCH_SIZE", DEFAULT_DOMAIN_PROPOSAL_BATCH_SIZE),
        max_batches_per_run=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
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
                    dagster_run_id=_enrichment_run_key(context, "bulk_ingest"),
                    run_type="bulk_ingest",
                    metadata={"source": "brreg", "dagster_run_id": context.run_id},
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


def materialize_brreg_translation_results(
    context,
    *,
    connection_factory,
    database_url: str,
    translator: TermTranslator,
    batch_size: int,
    model: str,
    prompt_version: str,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
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
        conn.commit()

        try:
            with conn.cursor() as cursor:
                records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                    task_type="translate",
                    limit=batch_size,
                )

            for record in records:
                rows_seen += 1
                attempt = _create_task_attempt(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    record=record,
                    task_type="translate",
                )
                try:
                    _translate_record(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        record=record,
                        translator=translator,
                        model=model,
                        prompt_version=prompt_version,
                    )
                    rows_completed += 1
                except Exception as exc:
                    conn.rollback()
                    _mark_record_task_failed(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        record=record,
                        task_type="translate",
                        error=str(exc),
                    )
                    rows_failed += 1

            context.log.info(
                "BRREG translation batch committed rows_seen=%s rows_completed=%s rows_failed=%s",
                rows_seen,
                rows_completed,
                rows_failed,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
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
    }
    context.add_output_metadata({**result, "dagster_run_id": context.run_id})
    return result


def materialize_brreg_domain_signal_candidates(
    context,
    *,
    connection_factory,
    database_url: str,
    signal: str,
    task_type: str,
    batch_size: int,
    max_batches_per_run: int = 1,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run <= 0:
        raise ValueError("max_batches_per_run must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    domains_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id, "signal": signal},
                )
            )
        conn.commit()

        try:
            while batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                        task_type=task_type,
                        limit=batch_size,
                    )
                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                for record in records:
                    rows_seen += 1
                    attempt = _create_task_attempt(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        record=record,
                        task_type=task_type,
                    )
                    try:
                        domains_written += _discover_record_domain_signal(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            signal=signal,
                        )
                        rows_completed += 1
                    except Exception as exc:
                        conn.rollback()
                        _mark_record_task_failed(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            task_type=task_type,
                            error=str(exc),
                        )
                        rows_failed += 1

            context.log.info(
                "BRREG domain signal batches committed signal=%s rows_seen=%s rows_completed=%s rows_failed=%s domains_written=%s batches_processed=%s max_batches_per_run=%s stopped_reason=%s",
                signal,
                rows_seen,
                rows_completed,
                rows_failed,
                domains_written,
                batches_processed,
                max_batches_per_run,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} domain enrichment rows failed",
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
        "domains_written": domains_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "signal": signal,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "stopped_reason": stopped_reason,
        }
    )
    return result


def materialize_brreg_domain_proposals(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
    max_batches_per_run: int = 1,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run <= 0:
        raise ValueError("max_batches_per_run must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    proposals_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    enrichment_run_id: str | None = None
    task_type = "merge_domain_proposals"
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
            while batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_domain_proposal_records(
                        task_type=task_type,
                        limit=batch_size,
                    )
                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                for record in records:
                    rows_seen += 1
                    attempt = _create_task_attempt(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        record=record,
                        task_type=task_type,
                    )
                    try:
                        proposals_written += _merge_record_domain_proposals(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                        )
                        rows_completed += 1
                    except Exception as exc:
                        conn.rollback()
                        _mark_record_task_failed(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            task_type=task_type,
                            error=str(exc),
                        )
                        rows_failed += 1

            context.log.info(
                "BRREG domain proposal batches committed rows_seen=%s rows_completed=%s rows_failed=%s proposals_written=%s batches_processed=%s max_batches_per_run=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                proposals_written,
                batches_processed,
                max_batches_per_run,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} domain proposal rows failed",
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
        "proposals_written": proposals_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "stopped_reason": stopped_reason,
        }
    )
    return result


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


def _translate_record(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    translator: TermTranslator,
    model: str,
    prompt_version: str,
) -> None:
    items = extract_translation_items(record.raw_payload)
    if not items:
        payload = build_translation_payload(
            raw_payload=record.raw_payload,
            items=[],
            cached_translations={},
            model=model,
            prompt_version=prompt_version,
        )
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.insert_translation_result(
                InsertTranslationResult(
                    raw_record_id=record.id,
                    task_attempt_id=attempt.id,
                    status="skipped",
                    translated_payload=payload,
                    model=model,
                    prompt_version=prompt_version,
                    error=None,
                    metadata={"reason": "no_translatable_terms"},
                )
            )
            store.finish_task_attempt(task_attempt_id=attempt.id, status="skipped", error=None)
            store.increment_enrichment_run_progress(
                IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
            )
        conn.commit()
        return

    keys = [translation_cache_key(item) for item in items]
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        cached = store.fetch_cached_translations(keys, model=model, prompt_version=prompt_version)
        missing_items = [
            item
            for item in items
            if translation_cache_key(item) not in cached
        ]
        new_cache_rows = _translate_missing_terms(
            translator=translator,
            missing_items=missing_items,
            model=model,
            prompt_version=prompt_version,
        )
        store.upsert_cached_translations(new_cache_rows)
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
        payload = build_translation_payload(
            raw_payload=record.raw_payload,
            items=items,
            cached_translations=cached,
            model=model,
            prompt_version=prompt_version,
        )
        store.insert_translation_result(
            InsertTranslationResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status="succeeded",
                translated_payload=payload,
                model=model,
                prompt_version=prompt_version,
                error=None,
                metadata={},
            )
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()


def _translate_missing_terms(
    *,
    translator: TermTranslator,
    missing_items: list[TranslationItem],
    model: str,
    prompt_version: str,
) -> list[UpsertCachedTranslation]:
    rows: list[UpsertCachedTranslation] = []
    sorted_items = sorted(missing_items, key=lambda item: item.category)
    for category, grouped in groupby(sorted_items, key=lambda item: item.category):
        items = list(grouped)
        translated_by_id = translator.translate_terms(
            category=category,
            items=items,
            source_lang="no",
            target_lang="en",
            model=model,
            prompt_version=prompt_version,
        )
        for item in items:
            translated_text = translated_by_id.get(translation_item_id(item), "").strip()
            if not translated_text:
                raise RuntimeError(f"missing translation for {item.category}: {item.text}")
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


def _discover_record_domain_signal(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    signal: str,
) -> int:
    candidates = asyncio.run(
        discover_domain_candidates_for_signal(
            signal=signal,
            raw_payload=record.raw_payload,
            organization_number=record.organization_number,
            organization_name=record.organization_name,
            website=record.website,
            country="NO",
        )
    )
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_domain_candidates(
            [
                InsertDomainCandidate(
                    raw_record_id=record.id,
                    task_attempt_id=attempt.id,
                    domain=candidate.domain,
                    normalized_domain=candidate.normalized_domain,
                    signal=candidate.signal,
                    confidence=candidate.confidence,
                    evidence=candidate.evidence,
                    metadata=candidate.metadata,
                )
                for candidate in candidates
            ]
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()
    return len(candidates)


def _merge_record_domain_proposals(*, conn, enrichment_run_id: str, attempt: TaskAttempt, record: RawTaskRecord) -> int:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        candidates = store.fetch_domain_candidates_for_raw_record(raw_record_id=record.id)
        proposals = build_domain_proposals(candidates)
        store.upsert_domain_proposals(
            [
                InsertDomainProposal(
                    raw_record_id=record.id,
                    task_attempt_id=attempt.id,
                    domain=proposal.domain,
                    normalized_domain=proposal.normalized_domain,
                    score=proposal.score,
                    signals=proposal.signals,
                    evidence=proposal.evidence,
                    metadata=proposal.metadata,
                )
                for proposal in proposals
            ]
        )
        store.finish_task_attempt(
            task_attempt_id=attempt.id,
            status="succeeded" if proposals else "skipped",
            error=None,
        )
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()
    return len(proposals)


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
        store.finish_task_attempt(task_attempt_id=attempt.id, status="failed", error=error)
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
                    metadata={},
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


def _corpscout_database_url() -> str:
    value = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("CORPSCOUT_DB_URL")
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL or CORPSCOUT_DB_URL is required")
    return value


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


def _enrichment_run_key(context, run_type: str) -> str:
    return f"{context.run_id}:{run_type}"
