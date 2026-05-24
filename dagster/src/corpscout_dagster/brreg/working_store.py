from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from corpscout_dagster.brreg.models import BrregWorkingRawRecordRow
from corpscout_dagster.brreg.translation import CachedTermTranslation, TranslationCacheKey


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
class InsertDomainCandidate:
    raw_record_id: str
    task_attempt_id: str
    domain: str
    normalized_domain: str
    signal: str
    confidence: int
    evidence: dict[str, Any]
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
        if params_seq:
            self._cursor.executemany(SUPERSEDE_CURRENT_RAW_RECORD_SQL, params_seq)
            self._cursor.executemany(UPSERT_RAW_RECORD_SQL, params_seq)
        return UpsertResult(rows_seen=len(rows), rows_written=len(rows))

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

    def fetch_pending_raw_task_records(self, *, task_type: str, limit: int) -> list[RawTaskRecord]:
        self._cursor.execute(
            FETCH_PENDING_RAW_TASK_RECORDS_SQL,
            {
                "task_type": task_type,
                "limit": limit,
            },
        )
        return [_raw_task_record_from_row(row) for row in self._cursor.fetchall()]

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
        return TaskAttempt(id=str(row[0]), raw_record_id=str(row[1]), attempt=int(row[2]))

    def finish_task_attempt(self, *, task_attempt_id: str, status: str, error: str | None) -> None:
        self._cursor.execute(
            FINISH_TASK_ATTEMPT_SQL,
            {
                "task_attempt_id": task_attempt_id,
                "status": status,
                "error": error,
            },
        )

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

    def insert_domain_candidates(self, rows: list[InsertDomainCandidate]) -> None:
        params_seq = [
            {
                "raw_record_id": row.raw_record_id,
                "task_attempt_id": row.task_attempt_id,
                "domain": row.domain,
                "normalized_domain": row.normalized_domain,
                "signal": row.signal,
                "confidence": row.confidence,
                "evidence": _json(row.evidence),
                "metadata": _json(row.metadata),
            }
            for row in rows
        ]
        if params_seq:
            self._cursor.executemany(INSERT_DOMAIN_CANDIDATE_SQL, params_seq)


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

SUPERSEDE_CURRENT_RAW_RECORD_SQL = """
UPDATE dagster_brreg.raw_records
SET
    is_current = false,
    last_seen_at = now()
WHERE organization_number = %(organization_number)s
  AND payload_hash <> %(payload_hash)s
  AND is_current = true
"""

UPSERT_RAW_RECORD_SQL = """
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
) VALUES (
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
)
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

FETCH_PENDING_RAW_TASK_RECORDS_SQL = """
SELECT
    rr.id,
    rr.organization_number,
    rr.organization_name,
    rr.website,
    rr.raw_payload
FROM dagster_brreg.raw_records rr
WHERE rr.is_current = true
  AND NOT EXISTS (
      SELECT 1
      FROM dagster_brreg.task_attempts ta
      WHERE ta.raw_record_id = rr.id
        AND ta.task_type = %(task_type)s
  )
ORDER BY rr.last_seen_at ASC, rr.id ASC
LIMIT %(limit)s
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

FINISH_TASK_ATTEMPT_SQL = """
UPDATE dagster_brreg.task_attempts
SET
    status = %(status)s,
    finished_at = now(),
    error = %(error)s
WHERE id = %(task_attempt_id)s
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

INSERT_DOMAIN_CANDIDATE_SQL = """
INSERT INTO dagster_brreg.domain_candidates (
    raw_record_id,
    task_attempt_id,
    domain,
    normalized_domain,
    signal,
    confidence,
    evidence,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(task_attempt_id)s,
    %(domain)s,
    %(normalized_domain)s,
    %(signal)s,
    %(confidence)s,
    %(evidence)s::jsonb,
    %(metadata)s::jsonb
)
ON CONFLICT (raw_record_id, normalized_domain, signal) DO UPDATE
SET
    task_attempt_id = EXCLUDED.task_attempt_id,
    domain = EXCLUDED.domain,
    confidence = GREATEST(dagster_brreg.domain_candidates.confidence, EXCLUDED.confidence),
    evidence = dagster_brreg.domain_candidates.evidence || EXCLUDED.evidence,
    metadata = dagster_brreg.domain_candidates.metadata || EXCLUDED.metadata
"""
