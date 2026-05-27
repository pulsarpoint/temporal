from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow
from corpscout_dagster.brreg.translation_terms import CachedTermTranslation, TranslationCacheKey


class Cursor(Protocol):
    def execute(self, sql: str, params: dict[str, Any]) -> object:
        ...

    def executemany(self, sql: str, params_seq: list[dict[str, Any]]) -> object:
        ...

    def fetchone(self):
        ...

    def fetchall(self):
        ...


@dataclass(frozen=True)
class CreateEnrichmentRun:
    dagster_run_id: str
    run_type: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class CreateBulkSnapshot:
    enrichment_run_id: str
    source_url: str
    content_length_bytes: int | None
    compressed_payload_hash: str | None
    storage_uri: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class UpsertResult:
    rows_seen: int
    rows_written: int
    rows_inserted_new: int = 0
    rows_existing_unchanged: int = 0
    rows_new_versions: int = 0


@dataclass(frozen=True)
class IncrementEnrichmentRunProgress:
    enrichment_run_id: str
    records_seen: int
    records_completed: int
    records_failed: int = 0


@dataclass(frozen=True)
class FinishEnrichmentRun:
    enrichment_run_id: str
    status: str
    error: str | None


@dataclass(frozen=True)
class RawTaskRecord:
    id: str
    organization_number: str
    organization_name: str | None
    website: str | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class TaskAttempt:
    id: str
    raw_record_id: str
    attempt: int


@dataclass(frozen=True)
class CreateTaskAttempt:
    enrichment_run_id: str
    raw_record_id: str
    task_type: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class UpsertCachedTranslation:
    category: str
    source_lang: str
    target_lang: str
    original_hash: str
    original_text: str
    translated_text: str
    model: str
    prompt_version: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class InsertTranslationResult:
    raw_record_id: str
    task_attempt_id: str
    status: str
    translated_payload: dict[str, Any] | None
    model: str | None
    prompt_version: str | None
    error: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class InsertDomainResult:
    raw_record_id: str
    task_attempt_id: str
    status: str
    best_domain: str | None
    domain_payload: dict[str, Any]
    error: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class InsertCurrencyResult:
    raw_record_id: str
    task_attempt_id: str
    status: str
    original_currency: str | None
    original_payload: dict[str, Any]
    usd_payload: dict[str, Any]
    fx_metadata: dict[str, Any]
    source_uri: str | None
    error: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class DomainResultCandidateRow:
    domain: str
    normalized_domain: str
    score: int
    signals: list[str]
    status: str
    evidence: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class EnhancedBuildRecord:
    record: RawTaskRecord
    registration_status: str | None
    country_iso2: str
    payload_hash: str
    translation_status: str
    translation_payload: dict[str, Any]
    domain_status: str
    domain_candidates: list[DomainResultCandidateRow]
    currency_status: str
    original_payload: dict[str, Any]
    usd_payload: dict[str, Any]
    fx_metadata: dict[str, Any]
    task_statuses: dict[str, str]


@dataclass(frozen=True)
class InsertEnhancedRecord:
    raw_record_id: str
    task_attempt_id: str
    schema_version: str
    enhanced_payload: dict[str, Any]
    enhanced_payload_hash: str
    metadata: dict[str, Any]


class BrregWorkingStore:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def create_enrichment_run(self, command: CreateEnrichmentRun) -> str:
        self._cursor.execute(
            CREATE_ENRICHMENT_RUN_SQL,
            {
                "dagster_run_id": command.dagster_run_id,
                "run_type": command.run_type,
                "metadata": _json(command.metadata),
            },
        )
        return _single_value(self._cursor.fetchone())

    def create_bulk_snapshot(self, command: CreateBulkSnapshot) -> str:
        self._cursor.execute(
            CREATE_BULK_SNAPSHOT_SQL,
            {
                "enrichment_run_id": command.enrichment_run_id,
                "source_url": command.source_url,
                "content_length_bytes": command.content_length_bytes,
                "compressed_payload_hash": command.compressed_payload_hash,
                "storage_uri": command.storage_uri,
                "metadata": _json(command.metadata),
            },
        )
        return _single_value(self._cursor.fetchone())

    def upsert_raw_records(
        self,
        rows: list[BrregWorkingRawRecordRow],
        *,
        bulk_snapshot_id: str,
    ) -> UpsertResult:
        params_seq = [
            {
                "bulk_snapshot_id": bulk_snapshot_id,
                "source_native_id": row.source_native_id,
                "organization_number": row.organization_number,
                "organization_name": row.organization_name,
                "registration_status": row.registration_status,
                "website": row.website,
                "country_iso2": row.country_iso2,
                "raw_payload": _json(row.raw_payload),
                "payload_hash": row.payload_hash,
                "metadata": _json(row.metadata),
            }
            for row in rows
        ]
        rows_written = 0
        rows_inserted_new = 0
        rows_existing_unchanged = 0
        rows_new_versions = 0
        for params in params_seq:
            self._cursor.execute(UPSERT_RAW_RECORD_SQL, params)
            row = self._cursor.fetchone()
            if row is None:
                rows_written += 1
                continue
            rows_written += int(row[0] or 0)
            rows_inserted_new += int(row[1] or 0)
            rows_existing_unchanged += int(row[2] or 0)
            rows_new_versions += int(row[3] or 0)
        return UpsertResult(
            rows_seen=len(rows),
            rows_written=rows_written,
            rows_inserted_new=rows_inserted_new,
            rows_existing_unchanged=rows_existing_unchanged,
            rows_new_versions=rows_new_versions,
        )

    def increment_enrichment_run_progress(self, command: IncrementEnrichmentRunProgress) -> None:
        self._cursor.execute(
            INCREMENT_ENRICHMENT_RUN_PROGRESS_SQL,
            {
                "enrichment_run_id": command.enrichment_run_id,
                "records_seen": command.records_seen,
                "records_completed": command.records_completed,
                "records_failed": command.records_failed,
            },
        )

    def finish_enrichment_run(self, command: FinishEnrichmentRun) -> None:
        self._cursor.execute(
            FINISH_ENRICHMENT_RUN_SQL,
            {
                "enrichment_run_id": command.enrichment_run_id,
                "status": command.status,
                "error": command.error,
            },
        )

    def reconcile_translation_task_states(self, *, model: str, prompt_version: str) -> int:
        self._cursor.execute(
            RECONCILE_TRANSLATION_TASK_STATES_SQL,
            {
                "model": model,
                "prompt_version": prompt_version,
            },
        )
        return int(_single_value(self._cursor.fetchone()))

    def fetch_raw_task_state_summary(self, *, task_type: str) -> dict[str, int]:
        self._cursor.execute(FETCH_RAW_TASK_STATE_SUMMARY_SQL, {"task_type": task_type})
        row = self._cursor.fetchone()
        if row is None:
            return {
                "raw_records_total": 0,
                "raw_records_current": 0,
                "raw_records_not_current": 0,
                "task_no_state": 0,
                "task_pending": 0,
                "task_running": 0,
                "task_running_active": 0,
                "task_running_stale": 0,
                "task_failed_retryable": 0,
                "task_failed_terminal": 0,
                "task_succeeded": 0,
                "task_skipped": 0,
                "task_cancelled": 0,
                "task_eligible_now": 0,
            }
        keys = [
            "raw_records_total",
            "raw_records_current",
            "raw_records_not_current",
            "task_no_state",
            "task_pending",
            "task_running",
            "task_running_active",
            "task_running_stale",
            "task_failed_retryable",
            "task_failed_terminal",
            "task_succeeded",
            "task_skipped",
            "task_cancelled",
            "task_eligible_now",
        ]
        return {key: int(value or 0) for key, value in zip(keys, row, strict=True)}

    def fetch_translation_artifact_summary(self, *, model: str, prompt_version: str) -> dict[str, int]:
        self._cursor.execute(
            FETCH_TRANSLATION_ARTIFACT_SUMMARY_SQL,
            {
                "model": model,
                "prompt_version": prompt_version,
            },
        )
        row = self._cursor.fetchone()
        if row is None:
            return {
                "translation_result_succeeded": 0,
                "translation_result_skipped": 0,
                "translation_result_failed": 0,
                "translation_result_missing": 0,
                "translation_artifact_missing": 0,
            }
        keys = [
            "translation_result_succeeded",
            "translation_result_skipped",
            "translation_result_failed",
            "translation_result_missing",
            "translation_artifact_missing",
        ]
        return {key: int(value or 0) for key, value in zip(keys, row, strict=True)}

    def fetch_domain_result_summary(self) -> dict[str, int]:
        self._cursor.execute(FETCH_DOMAIN_RESULT_SUMMARY_SQL, {})
        row = self._cursor.fetchone()
        keys = [
            "domain_result_succeeded",
            "domain_result_partial",
            "domain_result_not_found",
            "domain_result_failed",
            "domain_result_missing",
        ]
        if row is None:
            return {key: 0 for key in keys}
        return {key: int(value or 0) for key, value in zip(keys, row, strict=True)}

    def fetch_currency_result_summary(self) -> dict[str, int]:
        self._cursor.execute(FETCH_CURRENCY_RESULT_SUMMARY_SQL, {})
        row = self._cursor.fetchone()
        keys = [
            "currency_result_succeeded",
            "currency_result_skipped",
            "currency_result_not_available",
            "currency_result_failed",
            "currency_result_missing",
        ]
        if row is None:
            return {key: 0 for key in keys}
        return {key: int(value or 0) for key, value in zip(keys, row, strict=True)}

    def fetch_enhanced_record_summary(self) -> dict[str, int]:
        self._cursor.execute(FETCH_ENHANCED_RECORD_SUMMARY_SQL, {})
        row = self._cursor.fetchone()
        keys = [
            "enhanced_record_built",
            "enhanced_record_published",
            "enhanced_record_publish_failed",
            "enhanced_record_superseded",
            "enhanced_record_missing",
        ]
        if row is None:
            return {key: 0 for key in keys}
        return {key: int(value or 0) for key, value in zip(keys, row, strict=True)}

    def fetch_pending_raw_task_records(
        self,
        *,
        task_type: str,
        limit: int,
        max_parallel_tasks: int,
        lease_seconds: int,
        include_new_records: bool = True,
    ) -> list[RawTaskRecord]:
        if max_parallel_tasks <= 0:
            raise ValueError("max_parallel_tasks must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._cursor.execute(
            FETCH_PENDING_RAW_TASK_RECORDS_SQL,
            {
                "task_type": task_type,
                "limit": limit,
                "max_parallel_tasks": max_parallel_tasks,
                "lease_seconds": lease_seconds,
                "include_new_records": include_new_records,
            },
        )
        return [_raw_task_record_from_row(row) for row in self._cursor.fetchall()]

    def reset_unstarted_running_task_records(self, *, task_type: str, raw_record_ids: list[str]) -> int:
        if not raw_record_ids:
            return 0
        self._cursor.execute(
            RESET_UNSTARTED_RUNNING_TASK_RECORDS_SQL,
            {
                "task_type": task_type,
                "raw_record_ids": raw_record_ids,
            },
        )
        return int(_single_value(self._cursor.fetchone()))

    def create_task_attempt(self, command: CreateTaskAttempt) -> TaskAttempt:
        self._cursor.execute(
            CREATE_TASK_ATTEMPT_SQL,
            {
                "enrichment_run_id": command.enrichment_run_id,
                "raw_record_id": command.raw_record_id,
                "task_type": command.task_type,
                "metadata": _json(command.metadata),
            },
        )
        row = self._cursor.fetchone()
        if row is None:
            raise RuntimeError("expected task attempt insert to return one row")
        attempt = TaskAttempt(id=str(row[0]), raw_record_id=str(row[1]), attempt=int(row[2]))
        self._cursor.execute(
            UPSERT_TASK_STATE_RUNNING_SQL,
            {
                "raw_record_id": attempt.raw_record_id,
                "task_type": command.task_type,
                "status": "running",
                "attempt_count": attempt.attempt,
                "last_attempt_id": attempt.id,
            },
        )
        return attempt

    def finish_task_attempt(
        self,
        *,
        task_attempt_id: str,
        status: str,
        error: str | None,
        error_category: str | None = None,
        error_code: str | None = None,
        retry_strategy: str | None = None,
    ) -> None:
        self._cursor.execute(
            FINISH_TASK_ATTEMPT_SQL,
            {
                "task_attempt_id": task_attempt_id,
                "status": status,
                "error": error,
                "error_category": error_category,
                "error_code": error_code,
                "retry_strategy": retry_strategy,
            },
        )
        self._cursor.execute(
            UPDATE_TASK_STATE_FINISHED_SQL,
            {
                "task_attempt_id": task_attempt_id,
                "status": status,
                "error": error,
                "error_category": error_category,
                "error_code": error_code,
                "retry_strategy": retry_strategy,
            },
        )

    def fetch_task_failure_summary(self, *, task_type: str) -> dict[str, int]:
        self._cursor.execute(FETCH_TASK_FAILURE_SUMMARY_SQL, {"task_type": task_type})
        return {
            str(row[0] or "unknown"): int(row[1] or 0)
            for row in self._cursor.fetchall()
        }

    def retry_task_failures(self, *, task_type: str | None, error_category: str, limit: int) -> int:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._cursor.execute(
            RETRY_TASK_FAILURES_SQL,
            {
                "task_type": task_type,
                "error_category": error_category,
                "limit": limit,
            },
        )
        return int(_single_value(self._cursor.fetchone()))

    def fetch_cached_translations(
        self,
        keys: list[TranslationCacheKey],
        *,
        model: str,
        prompt_version: str,
    ) -> dict[TranslationCacheKey, CachedTermTranslation]:
        unique_keys = _unique_translation_keys(keys)
        if not unique_keys:
            return {}
        conditions: list[str] = []
        params: dict[str, Any] = {
            "model": model,
            "prompt_version": prompt_version,
        }
        for index, key in enumerate(unique_keys):
            conditions.append(
                "("
                f"category = %(category_{index})s "
                f"AND source_lang = %(source_lang_{index})s "
                f"AND target_lang = %(target_lang_{index})s "
                f"AND original_hash = %(original_hash_{index})s"
                ")"
            )
            params[f"category_{index}"] = key.category
            params[f"source_lang_{index}"] = key.source_lang
            params[f"target_lang_{index}"] = key.target_lang
            params[f"original_hash_{index}"] = key.original_hash
        self._cursor.execute(
            FETCH_CACHED_TRANSLATIONS_SQL.format(conditions=" OR ".join(conditions)),
            params,
        )
        cached: dict[TranslationCacheKey, CachedTermTranslation] = {}
        for row in self._cursor.fetchall():
            key = TranslationCacheKey(
                category=str(row[0]),
                source_lang=str(row[1]),
                target_lang=str(row[2]),
                original_hash=str(row[3]),
            )
            cached[key] = CachedTermTranslation(
                category=str(row[0]),
                original_text=str(row[4]),
                translated_text=str(row[5]),
                model=str(row[6]),
                prompt_version=str(row[7]),
            )
        return cached

    def upsert_cached_translations(self, rows: list[UpsertCachedTranslation]) -> None:
        params_seq = [
            {
                "category": row.category,
                "source_lang": row.source_lang,
                "target_lang": row.target_lang,
                "original_hash": row.original_hash,
                "original_text": row.original_text,
                "translated_text": row.translated_text,
                "model": row.model,
                "prompt_version": row.prompt_version,
                "metadata": _json(row.metadata),
            }
            for row in rows
        ]
        if params_seq:
            self._cursor.executemany(UPSERT_CACHED_TRANSLATION_SQL, params_seq)

    def insert_translation_result(self, command: InsertTranslationResult) -> None:
        self._cursor.execute(
            INSERT_TRANSLATION_RESULT_SQL,
            {
                "raw_record_id": command.raw_record_id,
                "task_attempt_id": command.task_attempt_id,
                "status": command.status,
                "translated_payload": _json(command.translated_payload) if command.translated_payload is not None else None,
                "model": command.model,
                "prompt_version": command.prompt_version,
                "error": command.error,
                "metadata": _json(command.metadata),
            },
        )

    def insert_domain_result(self, command: InsertDomainResult) -> None:
        self._cursor.execute(
            INSERT_DOMAIN_RESULT_SQL,
            {
                "raw_record_id": command.raw_record_id,
                "task_attempt_id": command.task_attempt_id,
                "status": command.status,
                "best_domain": command.best_domain,
                "domain_payload": _json(command.domain_payload),
                "error": command.error,
                "metadata": _json(command.metadata),
            },
        )

    def insert_currency_result(self, command: InsertCurrencyResult) -> None:
        self._cursor.execute(
            INSERT_CURRENCY_RESULT_SQL,
            {
                "raw_record_id": command.raw_record_id,
                "task_attempt_id": command.task_attempt_id,
                "status": command.status,
                "original_currency": command.original_currency,
                "original_payload": _json(command.original_payload),
                "usd_payload": _json(command.usd_payload),
                "fx_metadata": _json(command.fx_metadata),
                "source_uri": command.source_uri,
                "error": command.error,
                "metadata": _json(command.metadata),
            },
        )

    def fetch_pending_enhanced_build_records(self, *, limit: int) -> list[EnhancedBuildRecord]:
        self._cursor.execute(FETCH_PENDING_ENHANCED_BUILD_RECORDS_SQL, {"limit": limit})
        return [_enhanced_build_record_from_row(row) for row in self._cursor.fetchall()]

    def refresh_enhanced_ready_records(self) -> None:
        self._cursor.execute(REFRESH_ENHANCED_READY_RECORDS_SQL, {})

    def upsert_enhanced_record(self, command: InsertEnhancedRecord) -> str:
        self._cursor.execute(
            UPSERT_ENHANCED_RECORD_SQL,
            {
                "raw_record_id": command.raw_record_id,
                "task_attempt_id": command.task_attempt_id,
                "schema_version": command.schema_version,
                "enhanced_payload": _json(command.enhanced_payload),
                "enhanced_payload_hash": command.enhanced_payload_hash,
                "metadata": _json(command.metadata),
            },
        )
        return _single_value(self._cursor.fetchone())


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _single_value(row) -> str:
    if row is None:
        raise RuntimeError("expected database statement to return one row")
    return str(row[0])


def _raw_task_record_from_row(row) -> RawTaskRecord:
    raw_payload = row[4]
    if isinstance(raw_payload, str):
        raw_payload = json.loads(raw_payload)
    return RawTaskRecord(
        id=str(row[0]),
        organization_number=str(row[1]),
        organization_name=str(row[2]) if row[2] is not None else None,
        website=str(row[3]) if row[3] is not None else None,
        raw_payload=raw_payload,
    )


def _enhanced_build_record_from_row(row) -> EnhancedBuildRecord:
    raw_payload = _json_value(row[6], {})
    translation_payload = _json_value(row[9], {})
    domain_candidates = _json_value(row[11], [])
    original_payload = _json_value(row[13], {})
    usd_payload = _json_value(row[14], {})
    fx_metadata = _json_value(row[15], {})
    task_statuses = _json_value(row[16], {})
    return EnhancedBuildRecord(
        record=RawTaskRecord(
            id=str(row[0]),
            organization_number=str(row[1]),
            organization_name=str(row[2]) if row[2] is not None else None,
            website=str(row[4]) if row[4] is not None else None,
            raw_payload=raw_payload,
        ),
        registration_status=str(row[3]) if row[3] is not None else None,
        country_iso2=str(row[5]),
        payload_hash=str(row[7]),
        translation_status=str(row[8]),
        translation_payload=translation_payload,
        domain_status=str(row[10]) if row[10] is not None else "skipped",
        domain_candidates=[_domain_result_candidate_row_from_mapping(item) for item in domain_candidates],
        currency_status=str(row[12]) if row[12] is not None else "skipped",
        original_payload=_dict(original_payload),
        usd_payload=_dict(usd_payload),
        fx_metadata=_dict(fx_metadata),
        task_statuses={str(key): str(value) for key, value in task_statuses.items()},
    )


def _domain_result_candidate_row_from_mapping(value: dict[str, Any]) -> DomainResultCandidateRow:
    return DomainResultCandidateRow(
        domain=str(value.get("domain") or ""),
        normalized_domain=str(value.get("normalized_domain") or ""),
        score=int(value.get("score") or 0),
        signals=[str(signal) for signal in value.get("signals") or []],
        status=str(value.get("status") or "proposed"),
        evidence=_dict(value.get("evidence")),
        metadata=_dict(value.get("metadata")),
    )


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        return json.loads(value)
    return value


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _unique_translation_keys(keys: list[TranslationCacheKey]) -> list[TranslationCacheKey]:
    seen: set[TranslationCacheKey] = set()
    unique: list[TranslationCacheKey] = []
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        unique.append(key)
    return unique


CREATE_ENRICHMENT_RUN_SQL = """
INSERT INTO dagster_brreg.enrichment_runs (
    dagster_run_id,
    run_type,
    metadata
) VALUES (
    %(dagster_run_id)s,
    %(run_type)s,
    %(metadata)s::jsonb
)
ON CONFLICT (dagster_run_id) DO UPDATE
SET
    status = 'running',
    started_at = now(),
    finished_at = NULL,
    error = NULL,
    metadata = EXCLUDED.metadata
RETURNING id
"""

CREATE_BULK_SNAPSHOT_SQL = """
INSERT INTO dagster_brreg.bulk_snapshots (
    enrichment_run_id,
    source_url,
    content_length_bytes,
    compressed_payload_hash,
    storage_uri,
    metadata
) VALUES (
    %(enrichment_run_id)s,
    %(source_url)s,
    %(content_length_bytes)s,
    %(compressed_payload_hash)s,
    %(storage_uri)s,
    %(metadata)s::jsonb
)
RETURNING id
"""

UPSERT_RAW_RECORD_SQL = """
WITH existing_current AS (
    SELECT payload_hash
    FROM dagster_brreg.raw_records
    WHERE organization_number = %(organization_number)s
      AND is_current = true
),
superseded_current AS (
    UPDATE dagster_brreg.raw_records
    SET
        is_current = false,
        last_seen_at = now()
    WHERE organization_number = %(organization_number)s
      AND payload_hash <> %(payload_hash)s
      AND is_current = true
    RETURNING id
),
supersede_done AS (
    SELECT count(*) AS rows_superseded
    FROM superseded_current
),
upserted AS (
    INSERT INTO dagster_brreg.raw_records (
        bulk_snapshot_id,
        source_native_id,
        organization_number,
        organization_name,
        registration_status,
        website,
        country_iso2,
        raw_payload,
        payload_hash,
        is_current,
        metadata
    )
    SELECT
        %(bulk_snapshot_id)s,
        %(source_native_id)s,
        %(organization_number)s,
        %(organization_name)s,
        %(registration_status)s,
        %(website)s,
        %(country_iso2)s,
        %(raw_payload)s::jsonb,
        %(payload_hash)s,
        true,
        %(metadata)s::jsonb
    FROM supersede_done
    ON CONFLICT (organization_number, payload_hash) DO UPDATE
    SET
        bulk_snapshot_id = EXCLUDED.bulk_snapshot_id,
        organization_name = EXCLUDED.organization_name,
        registration_status = EXCLUDED.registration_status,
        website = EXCLUDED.website,
        country_iso2 = EXCLUDED.country_iso2,
        raw_payload = EXCLUDED.raw_payload,
        is_current = true,
        last_seen_at = now(),
        metadata = EXCLUDED.metadata
    RETURNING 1
)
SELECT
    (SELECT count(*)::int FROM upserted) AS rows_written,
    CASE WHEN NOT EXISTS (SELECT 1 FROM existing_current) THEN 1 ELSE 0 END AS rows_inserted_new,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM existing_current WHERE payload_hash = %(payload_hash)s
        ) THEN 1
        ELSE 0
    END AS rows_existing_unchanged,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM existing_current WHERE payload_hash <> %(payload_hash)s
        ) THEN 1
        ELSE 0
    END AS rows_new_versions
"""

INCREMENT_ENRICHMENT_RUN_PROGRESS_SQL = """
UPDATE dagster_brreg.enrichment_runs
SET
    records_seen = records_seen + %(records_seen)s,
    records_completed = records_completed + %(records_completed)s,
    records_failed = records_failed + %(records_failed)s
WHERE id = %(enrichment_run_id)s
"""

FINISH_ENRICHMENT_RUN_SQL = """
UPDATE dagster_brreg.enrichment_runs
SET
    status = %(status)s,
    finished_at = now(),
    error = %(error)s
WHERE id = %(enrichment_run_id)s
"""

RECONCILE_TRANSLATION_TASK_STATES_SQL = """
-- reconcile_translation_task_states
WITH latest_usable_translation AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id
    FROM dagster_brreg.translation_results
    WHERE status IN ('succeeded', 'skipped')
      AND model = %(model)s
      AND prompt_version = %(prompt_version)s
    ORDER BY raw_record_id, created_at DESC
),
missing_usable_translation AS (
    SELECT rr.id AS raw_record_id
    FROM dagster_brreg.raw_records rr
    LEFT JOIN latest_usable_translation tr ON tr.raw_record_id = rr.id
    WHERE rr.is_current = true
      AND tr.raw_record_id IS NULL
),
reconciled AS (
    INSERT INTO dagster_brreg.raw_record_task_states (
        raw_record_id,
        task_type,
        status,
        attempt_count,
        last_attempt_id,
        last_started_at,
        last_finished_at,
        next_retry_at,
        lease_until,
        last_error,
        result_summary
    )
    SELECT
        raw_record_id,
        'translate',
        'pending',
        0,
        NULL,
        NULL,
        NULL,
        now(),
        NULL,
        NULL,
        '{}'::jsonb
    FROM missing_usable_translation
    ON CONFLICT (raw_record_id, task_type) DO UPDATE
    SET
        status = 'pending',
        next_retry_at = now(),
        lease_until = NULL,
        updated_at = now()
    WHERE dagster_brreg.raw_record_task_states.task_type = 'translate'
      AND NOT (
          dagster_brreg.raw_record_task_states.status = 'running'
          AND coalesce(
              dagster_brreg.raw_record_task_states.lease_until,
              dagster_brreg.raw_record_task_states.last_started_at + interval '30 minutes'
          ) > now()
      )
    RETURNING raw_record_id
)
SELECT count(*) FROM reconciled
"""

FETCH_RAW_TASK_STATE_SUMMARY_SQL = """
-- fetch_raw_task_state_summary
WITH raw_counts AS (
    SELECT
        count(*)::int AS raw_records_total,
        count(*) FILTER (WHERE is_current)::int AS raw_records_current,
        count(*) FILTER (WHERE NOT is_current)::int AS raw_records_not_current
    FROM dagster_brreg.raw_records
),
current_task_rows AS (
    SELECT
        rr.id,
        ts.status,
        ts.next_retry_at,
        ts.lease_until,
        ts.last_started_at
    FROM dagster_brreg.raw_records rr
    LEFT JOIN dagster_brreg.raw_record_task_states ts
      ON ts.raw_record_id = rr.id
     AND ts.task_type = %(task_type)s
    WHERE rr.is_current = true
),
task_counts AS (
    SELECT
        count(*) FILTER (WHERE status IS NULL)::int AS task_no_state,
        count(*) FILTER (WHERE status = 'pending')::int AS task_pending,
        count(*) FILTER (WHERE status = 'running')::int AS task_running,
        count(*) FILTER (
            WHERE status = 'running'
              AND coalesce(lease_until, last_started_at + interval '30 minutes') > now()
        )::int AS task_running_active,
        count(*) FILTER (
            WHERE status = 'running'
              AND coalesce(lease_until, last_started_at + interval '30 minutes') <= now()
        )::int AS task_running_stale,
        count(*) FILTER (WHERE status = 'failed_retryable')::int AS task_failed_retryable,
        count(*) FILTER (WHERE status = 'failed_terminal')::int AS task_failed_terminal,
        count(*) FILTER (WHERE status = 'succeeded')::int AS task_succeeded,
        count(*) FILTER (WHERE status = 'skipped')::int AS task_skipped,
        count(*) FILTER (WHERE status = 'cancelled')::int AS task_cancelled,
        count(*) FILTER (
            WHERE status IS NULL
               OR status = 'pending'
               OR (status = 'failed_retryable' AND next_retry_at <= now())
               OR (
                   status = 'running'
                   AND coalesce(lease_until, last_started_at + interval '30 minutes') <= now()
               )
        )::int AS task_eligible_now
    FROM current_task_rows
)
SELECT
    raw_counts.raw_records_total,
    raw_counts.raw_records_current,
    raw_counts.raw_records_not_current,
    task_counts.task_no_state,
    task_counts.task_pending,
    task_counts.task_running,
    task_counts.task_running_active,
    task_counts.task_running_stale,
    task_counts.task_failed_retryable,
    task_counts.task_failed_terminal,
    task_counts.task_succeeded,
    task_counts.task_skipped,
    task_counts.task_cancelled,
    task_counts.task_eligible_now
FROM raw_counts
CROSS JOIN task_counts
"""

FETCH_TASK_FAILURE_SUMMARY_SQL = """
SELECT
    coalesce(rts.error_category, 'unknown') AS error_category,
    count(*)::int AS failure_count
FROM dagster_brreg.raw_record_task_states rts
JOIN dagster_brreg.raw_records rr ON rr.id = rts.raw_record_id
WHERE rts.task_type = %(task_type)s
  AND rr.is_current = true
  AND rts.status IN ('failed_retryable', 'failed_terminal')
GROUP BY coalesce(rts.error_category, 'unknown')
ORDER BY error_category
"""

RETRY_TASK_FAILURES_SQL = """
WITH retry_candidates AS (
    SELECT rts.raw_record_id, rts.task_type
    FROM dagster_brreg.raw_record_task_states rts
    JOIN dagster_brreg.raw_records rr ON rr.id = rts.raw_record_id
    WHERE rr.is_current = true
      AND rts.status IN ('failed_retryable', 'failed_terminal')
      AND rts.error_category = %(error_category)s
      AND (%(task_type)s::text IS NULL OR rts.task_type = %(task_type)s::text)
    ORDER BY rts.next_retry_at NULLS FIRST, rts.updated_at ASC, rts.raw_record_id ASC
    LIMIT %(limit)s
),
retried AS (
    UPDATE dagster_brreg.raw_record_task_states rts
    SET
        status = 'pending',
        last_started_at = NULL,
        last_finished_at = NULL,
        next_retry_at = now(),
        lease_until = NULL,
        last_error = NULL,
        error_category = NULL,
        error_code = NULL,
        retry_strategy = NULL,
        updated_at = now()
    FROM retry_candidates rc
    WHERE rts.raw_record_id = rc.raw_record_id
      AND rts.task_type = rc.task_type
    RETURNING rts.raw_record_id
)
SELECT count(*)::int FROM retried
"""

FETCH_TRANSLATION_ARTIFACT_SUMMARY_SQL = """
-- fetch_translation_artifact_summary
WITH current_raw AS (
    SELECT id
    FROM dagster_brreg.raw_records
    WHERE is_current = true
),
latest_translation_result AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status
    FROM dagster_brreg.translation_results
    WHERE model = %(model)s
      AND prompt_version = %(prompt_version)s
    ORDER BY raw_record_id, created_at DESC
),
translation_counts AS (
    SELECT
        count(*) FILTER (WHERE ltr.status = 'succeeded')::int AS translation_result_succeeded,
        count(*) FILTER (WHERE ltr.status = 'skipped')::int AS translation_result_skipped,
        count(*) FILTER (WHERE ltr.status = 'failed')::int AS translation_result_failed,
        count(*) FILTER (WHERE ltr.raw_record_id IS NULL)::int AS translation_result_missing
    FROM current_raw cr
    LEFT JOIN latest_translation_result ltr ON ltr.raw_record_id = cr.id
)
SELECT
    translation_result_succeeded,
    translation_result_skipped,
    translation_result_failed,
    translation_result_missing,
    translation_result_failed + translation_result_missing AS translation_artifact_missing
FROM translation_counts
"""

FETCH_DOMAIN_RESULT_SUMMARY_SQL = """
-- fetch_domain_result_summary
WITH current_raw AS (
    SELECT id
    FROM dagster_brreg.raw_records
    WHERE is_current = true
),
latest_domain_result AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status
    FROM dagster_brreg.domain_results
    ORDER BY raw_record_id, created_at DESC
),
domain_counts AS (
    SELECT
        count(*) FILTER (WHERE ldr.status = 'succeeded')::int AS domain_result_succeeded,
        count(*) FILTER (WHERE ldr.status = 'partial')::int AS domain_result_partial,
        count(*) FILTER (WHERE ldr.status = 'not_found')::int AS domain_result_not_found,
        count(*) FILTER (WHERE ldr.status = 'failed')::int AS domain_result_failed,
        count(*) FILTER (WHERE ldr.raw_record_id IS NULL)::int AS domain_result_missing
    FROM current_raw cr
    LEFT JOIN latest_domain_result ldr ON ldr.raw_record_id = cr.id
)
SELECT
    domain_result_succeeded,
    domain_result_partial,
    domain_result_not_found,
    domain_result_failed,
    domain_result_missing
FROM domain_counts
"""

FETCH_CURRENCY_RESULT_SUMMARY_SQL = """
-- fetch_currency_result_summary
WITH current_raw AS (
    SELECT id
    FROM dagster_brreg.raw_records
    WHERE is_current = true
),
latest_currency_result AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status
    FROM dagster_brreg.currency_results
    ORDER BY raw_record_id, created_at DESC
),
currency_counts AS (
    SELECT
        count(*) FILTER (WHERE lcr.status = 'succeeded')::int AS currency_result_succeeded,
        count(*) FILTER (WHERE lcr.status = 'skipped')::int AS currency_result_skipped,
        count(*) FILTER (WHERE lcr.status = 'not_available')::int AS currency_result_not_available,
        count(*) FILTER (WHERE lcr.status = 'failed')::int AS currency_result_failed,
        count(*) FILTER (WHERE lcr.raw_record_id IS NULL)::int AS currency_result_missing
    FROM current_raw cr
    LEFT JOIN latest_currency_result lcr ON lcr.raw_record_id = cr.id
)
SELECT
    currency_result_succeeded,
    currency_result_skipped,
    currency_result_not_available,
    currency_result_failed,
    currency_result_missing
FROM currency_counts
"""

FETCH_ENHANCED_RECORD_SUMMARY_SQL = """
-- fetch_enhanced_record_summary
WITH current_raw AS (
    SELECT id
    FROM dagster_brreg.raw_records
    WHERE is_current = true
),
latest_enhanced_record AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status
    FROM dagster_brreg.enhanced_records
    ORDER BY raw_record_id, built_at DESC
),
enhanced_counts AS (
    SELECT
        count(*) FILTER (WHERE ler.status = 'built')::int AS enhanced_record_built,
        count(*) FILTER (WHERE ler.status = 'published')::int AS enhanced_record_published,
        count(*) FILTER (WHERE ler.status = 'publish_failed')::int AS enhanced_record_publish_failed,
        count(*) FILTER (WHERE ler.status = 'superseded')::int AS enhanced_record_superseded,
        count(*) FILTER (WHERE ler.raw_record_id IS NULL)::int AS enhanced_record_missing
    FROM current_raw cr
    LEFT JOIN latest_enhanced_record ler ON ler.raw_record_id = cr.id
)
SELECT
    enhanced_record_built,
    enhanced_record_published,
    enhanced_record_publish_failed,
    enhanced_record_superseded,
    enhanced_record_missing
FROM enhanced_counts
"""

FETCH_PENDING_RAW_TASK_RECORDS_SQL = """
WITH lock_task AS (
    SELECT pg_advisory_xact_lock(hashtext('dagster_brreg.raw_record_task_states:' || %(task_type)s))
),
active_slots AS (
    SELECT GREATEST(%(max_parallel_tasks)s - count(*)::int, 0) AS available_slots
    FROM dagster_brreg.raw_record_task_states ts
    CROSS JOIN lock_task
    WHERE ts.task_type = %(task_type)s
      AND ts.status = 'running'
      AND coalesce(ts.lease_until, ts.last_started_at + interval '30 minutes') > now()
),
pending_task_ids AS (
    SELECT
        ts.raw_record_id AS id,
        ts.next_retry_at AS sort_at
    FROM dagster_brreg.raw_record_task_states ts
    WHERE ts.task_type = %(task_type)s
      AND ts.status = 'pending'
    ORDER BY ts.next_retry_at ASC, ts.raw_record_id ASC
    LIMIT %(limit)s
),
failed_task_ids AS (
    SELECT
        ts.raw_record_id AS id,
        ts.next_retry_at AS sort_at
    FROM dagster_brreg.raw_record_task_states ts
    WHERE ts.task_type = %(task_type)s
      AND ts.status = 'failed_retryable'
      AND ts.next_retry_at <= now()
    ORDER BY ts.next_retry_at ASC, ts.raw_record_id ASC
    LIMIT %(limit)s
),
stale_running_task_ids AS (
    SELECT
        ts.raw_record_id AS id,
        ts.last_started_at AS sort_at
    FROM dagster_brreg.raw_record_task_states ts
    WHERE ts.task_type = %(task_type)s
      AND ts.status = 'running'
      AND coalesce(ts.lease_until, ts.last_started_at + interval '30 minutes') <= now()
    ORDER BY ts.last_started_at ASC, ts.raw_record_id ASC
    LIMIT %(limit)s
),
retryable_task_ids AS (
    SELECT id, sort_at FROM pending_task_ids
    UNION ALL
    SELECT id, sort_at FROM failed_task_ids
    UNION ALL
    SELECT id, sort_at FROM stale_running_task_ids
    ORDER BY sort_at ASC, id ASC
    LIMIT %(limit)s
),
new_task_ids AS (
    SELECT
        rr.id,
        rr.last_seen_at AS sort_at
    FROM dagster_brreg.raw_records rr
    WHERE %(include_new_records)s
      AND rr.is_current = true
      AND NOT EXISTS (
          SELECT 1
          FROM dagster_brreg.raw_record_task_states ts
          WHERE ts.raw_record_id = rr.id
            AND ts.task_type = %(task_type)s
      )
    ORDER BY rr.last_seen_at ASC, rr.id ASC
    LIMIT %(limit)s
),
candidate_ids AS (
    SELECT id, sort_at FROM retryable_task_ids
    UNION ALL
    SELECT id, sort_at FROM new_task_ids
    ORDER BY sort_at ASC, id ASC
    LIMIT (SELECT LEAST(%(limit)s, active_slots.available_slots) FROM active_slots)
),
claimed_task_ids AS (
    INSERT INTO dagster_brreg.raw_record_task_states (
        raw_record_id,
        task_type,
        status,
        last_started_at,
        last_finished_at,
        next_retry_at,
        lease_until,
        last_error,
        result_summary
    )
    SELECT
        candidate_ids.id,
        %(task_type)s,
        'running',
        now(),
        NULL,
        NULL,
        now() + (%(lease_seconds)s::text || ' seconds')::interval,
        NULL,
        '{}'::jsonb
    FROM candidate_ids
    ON CONFLICT (raw_record_id, task_type) DO UPDATE
    SET
        status = 'running',
        last_started_at = now(),
        last_finished_at = NULL,
        next_retry_at = NULL,
        lease_until = now() + (%(lease_seconds)s::text || ' seconds')::interval,
        last_error = NULL,
        updated_at = now()
    WHERE dagster_brreg.raw_record_task_states.task_type = %(task_type)s
      AND (
          dagster_brreg.raw_record_task_states.status = 'pending'
          OR (
              dagster_brreg.raw_record_task_states.status = 'failed_retryable'
              AND dagster_brreg.raw_record_task_states.next_retry_at <= now()
          )
          OR (
              dagster_brreg.raw_record_task_states.status = 'running'
              AND coalesce(
                  dagster_brreg.raw_record_task_states.lease_until,
                  dagster_brreg.raw_record_task_states.last_started_at + interval '30 minutes'
              ) <= now()
          )
      )
    RETURNING raw_record_id AS id
)
SELECT
    rr.id,
    rr.organization_number,
    rr.organization_name,
    rr.website,
    rr.raw_payload
FROM claimed_task_ids
JOIN dagster_brreg.raw_records rr ON rr.id = claimed_task_ids.id
WHERE rr.is_current = true
ORDER BY rr.last_seen_at ASC, claimed_task_ids.id ASC
LIMIT %(limit)s
"""

RESET_UNSTARTED_RUNNING_TASK_RECORDS_SQL = """
WITH reset_rows AS (
    UPDATE dagster_brreg.raw_record_task_states
    SET
        status = 'pending',
        last_started_at = NULL,
        last_finished_at = NULL,
        next_retry_at = now(),
        lease_until = NULL,
        last_error = NULL,
        updated_at = now()
    WHERE task_type = %(task_type)s
      AND raw_record_id = ANY(%(raw_record_ids)s::uuid[])
      AND status = 'running'
      AND last_attempt_id IS NULL
    RETURNING raw_record_id
)
SELECT count(*)::int FROM reset_rows
"""

CREATE_TASK_ATTEMPT_SQL = """
INSERT INTO dagster_brreg.task_attempts (
    enrichment_run_id,
    raw_record_id,
    task_type,
    attempt,
    status,
    started_at,
    metadata
)
SELECT
    %(enrichment_run_id)s,
    %(raw_record_id)s,
    %(task_type)s,
    coalesce(max(attempt), 0) + 1,
    'running',
    now(),
    %(metadata)s::jsonb
FROM dagster_brreg.task_attempts
WHERE raw_record_id = %(raw_record_id)s
  AND task_type = %(task_type)s
RETURNING id, raw_record_id, attempt
"""

UPSERT_TASK_STATE_RUNNING_SQL = """
INSERT INTO dagster_brreg.raw_record_task_states (
    raw_record_id,
    task_type,
    status,
    attempt_count,
    last_attempt_id,
    last_started_at,
    last_finished_at,
    next_retry_at,
    lease_until,
    last_error,
    error_category,
    error_code,
    retry_strategy,
    result_summary
) VALUES (
    %(raw_record_id)s,
    %(task_type)s,
    %(status)s,
    %(attempt_count)s,
    %(last_attempt_id)s,
    now(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    '{}'::jsonb
)
ON CONFLICT (raw_record_id, task_type) DO UPDATE
SET
    status = EXCLUDED.status,
    attempt_count = GREATEST(dagster_brreg.raw_record_task_states.attempt_count, EXCLUDED.attempt_count),
    last_attempt_id = EXCLUDED.last_attempt_id,
    last_started_at = EXCLUDED.last_started_at,
    last_finished_at = NULL,
    next_retry_at = NULL,
    lease_until = dagster_brreg.raw_record_task_states.lease_until,
    last_error = NULL,
    error_category = NULL,
    error_code = NULL,
    retry_strategy = NULL,
    updated_at = now()
"""

FINISH_TASK_ATTEMPT_SQL = """
UPDATE dagster_brreg.task_attempts
SET
    status = %(status)s,
    finished_at = now(),
    error = %(error)s,
    error_category = %(error_category)s,
    error_code = %(error_code)s,
    retry_strategy = %(retry_strategy)s
WHERE id = %(task_attempt_id)s
"""

UPDATE_TASK_STATE_FINISHED_SQL = """
UPDATE dagster_brreg.raw_record_task_states rts
SET
    status = CASE
        WHEN %(status)s = 'failed'
          AND (
            ta.attempt >= 5
            OR %(retry_strategy)s IN (
              'change_model_or_prompt',
              'manual_config',
              'manual_input',
              'not_retryable'
            )
          ) THEN 'failed_terminal'
        WHEN %(status)s = 'failed' THEN 'failed_retryable'
        WHEN %(status)s = 'succeeded' THEN 'succeeded'
        WHEN %(status)s = 'skipped' THEN 'skipped'
        WHEN %(status)s = 'cancelled' THEN 'cancelled'
        ELSE rts.status
    END,
    attempt_count = GREATEST(rts.attempt_count, ta.attempt),
    last_attempt_id = ta.id,
    last_finished_at = now(),
    lease_until = NULL,
    next_retry_at = CASE
        WHEN %(status)s <> 'failed'
          OR ta.attempt >= 5
          OR %(retry_strategy)s IN (
            'change_model_or_prompt',
            'manual_config',
            'manual_input',
            'not_retryable'
          ) THEN NULL
        WHEN ta.attempt = 1 THEN now() + interval '5 minutes'
        WHEN ta.attempt = 2 THEN now() + interval '30 minutes'
        WHEN ta.attempt = 3 THEN now() + interval '6 hours'
        ELSE now() + interval '1 day'
    END,
    last_error = %(error)s,
    error_category = %(error_category)s,
    error_code = %(error_code)s,
    retry_strategy = %(retry_strategy)s,
    updated_at = now()
FROM dagster_brreg.task_attempts ta
WHERE ta.id = %(task_attempt_id)s
  AND rts.raw_record_id = ta.raw_record_id
  AND rts.task_type = ta.task_type
"""

FETCH_CACHED_TRANSLATIONS_SQL = """
SELECT
    category,
    source_lang,
    target_lang,
    original_hash,
    original_text,
    translated_text,
    model,
    prompt_version
FROM dagster_brreg.translation_cache
WHERE model = %(model)s
  AND prompt_version = %(prompt_version)s
  AND ({conditions})
"""

UPSERT_CACHED_TRANSLATION_SQL = """
INSERT INTO dagster_brreg.translation_cache (
    category,
    source_lang,
    target_lang,
    original_hash,
    original_text,
    translated_text,
    model,
    prompt_version,
    metadata
) VALUES (
    %(category)s,
    %(source_lang)s,
    %(target_lang)s,
    %(original_hash)s,
    %(original_text)s,
    %(translated_text)s,
    %(model)s,
    %(prompt_version)s,
    %(metadata)s::jsonb
)
ON CONFLICT (category, source_lang, target_lang, original_hash, model, prompt_version)
DO UPDATE SET
    original_text = EXCLUDED.original_text,
    translated_text = EXCLUDED.translated_text,
    updated_at = now(),
    metadata = dagster_brreg.translation_cache.metadata || EXCLUDED.metadata
"""

INSERT_TRANSLATION_RESULT_SQL = """
INSERT INTO dagster_brreg.translation_results (
    raw_record_id,
    task_attempt_id,
    status,
    translated_payload,
    model,
    prompt_version,
    error,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(task_attempt_id)s,
    %(status)s,
    %(translated_payload)s::jsonb,
    %(model)s,
    %(prompt_version)s,
    %(error)s,
    %(metadata)s::jsonb
)
"""

INSERT_DOMAIN_RESULT_SQL = """
INSERT INTO dagster_brreg.domain_results (
    raw_record_id,
    task_attempt_id,
    status,
    best_domain,
    domain_payload,
    error,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(task_attempt_id)s,
    %(status)s,
    %(best_domain)s,
    %(domain_payload)s::jsonb,
    %(error)s,
    %(metadata)s::jsonb
)
"""

INSERT_CURRENCY_RESULT_SQL = """
INSERT INTO dagster_brreg.currency_results (
    raw_record_id,
    task_attempt_id,
    status,
    original_currency,
    original_payload,
    usd_payload,
    fx_metadata,
    source_uri,
    error,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(task_attempt_id)s,
    %(status)s,
    %(original_currency)s,
    %(original_payload)s::jsonb,
    %(usd_payload)s::jsonb,
    %(fx_metadata)s::jsonb,
    %(source_uri)s,
    %(error)s,
    %(metadata)s::jsonb
)
"""

REFRESH_ENHANCED_READY_RECORDS_SQL = "REFRESH MATERIALIZED VIEW dagster_brreg.mv_brreg_enhanced_ready_records"

FETCH_PENDING_ENHANCED_BUILD_RECORDS_SQL = """
SELECT
    id,
    organization_number,
    organization_name,
    registration_status,
    website,
    country_iso2,
    raw_payload,
    payload_hash,
    translation_status,
    translation_payload,
    domain_status,
    domain_candidates,
    currency_status,
    original_payload,
    usd_payload,
    fx_metadata,
    task_statuses
FROM dagster_brreg.mv_brreg_enhanced_ready_records
ORDER BY raw_last_seen_at ASC, id ASC
LIMIT %(limit)s
"""

UPSERT_ENHANCED_RECORD_SQL = """
INSERT INTO dagster_brreg.enhanced_records (
    raw_record_id,
    task_attempt_id,
    schema_version,
    status,
    enhanced_payload,
    enhanced_payload_hash,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(task_attempt_id)s,
    %(schema_version)s,
    'built',
    %(enhanced_payload)s::jsonb,
    %(enhanced_payload_hash)s,
    %(metadata)s::jsonb
)
ON CONFLICT (raw_record_id, schema_version, enhanced_payload_hash) DO UPDATE
SET
    task_attempt_id = EXCLUDED.task_attempt_id,
    status = CASE
        WHEN dagster_brreg.enhanced_records.status = 'published' THEN 'published'
        ELSE 'built'
    END,
    enhanced_payload = EXCLUDED.enhanced_payload,
    metadata = dagster_brreg.enhanced_records.metadata || EXCLUDED.metadata,
    built_at = now(),
    error = NULL
RETURNING id
"""
