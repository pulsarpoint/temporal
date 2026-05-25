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


@dataclass(frozen=True)
class DomainCandidateRow:
    normalized_domain: str
    domain: str
    signal: str
    confidence: int
    evidence: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class DomainProposalRow:
    domain: str
    normalized_domain: str
    score: int
    signals: list[str]
    status: str
    evidence: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class InsertDomainProposal:
    raw_record_id: str
    task_attempt_id: str
    domain: str
    normalized_domain: str
    score: int
    signals: list[str]
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
    domain_proposals: list[DomainProposalRow]
    task_statuses: dict[str, str]


@dataclass(frozen=True)
class InsertEnhancedRecord:
    raw_record_id: str
    task_attempt_id: str
    schema_version: str
    enhanced_payload: dict[str, Any]
    enhanced_payload_hash: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class EnhancedPublishRecord:
    enhanced_record_id: str
    raw_record_id: str
    organization_number: str
    organization_name: str | None
    registration_status: str | None
    website: str | None
    country_iso2: str
    raw_payload: dict[str, Any]
    payload_hash: str
    schema_version: str
    enhanced_payload: dict[str, Any]
    enhanced_payload_hash: str


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

    def seed_pending_raw_task_states(self, *, task_type: str, limit: int) -> int:
        self._cursor.execute(
            SEED_PENDING_RAW_TASK_STATES_SQL,
            {
                "task_type": task_type,
                "limit": limit,
            },
        )
        row = self._cursor.fetchone()
        return int(row[0] or 0) if row else 0

    def try_acquire_raw_task_run_lease(
        self,
        *,
        task_type: str,
        enrichment_run_id: str,
        dagster_run_id: str,
        max_concurrent_runs: int,
        lease_seconds: int,
    ) -> str | None:
        if max_concurrent_runs <= 0:
            raise ValueError("max_concurrent_runs must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

        self._cursor.execute(
            EXPIRE_RAW_TASK_RUN_LEASES_SQL,
            {
                "task_type": task_type,
            },
        )
        self._cursor.execute(
            TRY_ACQUIRE_RAW_TASK_RUN_LEASE_SQL,
            {
                "task_type": task_type,
                "enrichment_run_id": enrichment_run_id,
                "dagster_run_id": dagster_run_id,
                "max_concurrent_runs": max_concurrent_runs,
                "lease_seconds": lease_seconds,
            },
        )
        row = self._cursor.fetchone()
        if row is None or not bool(row[1]):
            return None
        return str(row[0])

    def renew_raw_task_run_lease(self, *, lease_id: str, lease_seconds: int) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._cursor.execute(
            RENEW_RAW_TASK_RUN_LEASE_SQL,
            {
                "lease_id": lease_id,
                "lease_seconds": lease_seconds,
            },
        )

    def release_raw_task_run_lease(self, *, lease_id: str) -> None:
        self._cursor.execute(
            RELEASE_RAW_TASK_RUN_LEASE_SQL,
            {
                "lease_id": lease_id,
            },
        )

    def fetch_pending_raw_task_records(
        self,
        *,
        task_type: str,
        limit: int,
        include_new_records: bool = True,
    ) -> list[RawTaskRecord]:
        self._cursor.execute(
            FETCH_PENDING_RAW_TASK_RECORDS_SQL,
            {
                "task_type": task_type,
                "limit": limit,
                "include_new_records": include_new_records,
            },
        )
        return [_raw_task_record_from_row(row) for row in self._cursor.fetchall()]

    def fetch_pending_domain_proposal_records(self, *, task_type: str, limit: int) -> list[RawTaskRecord]:
        self._cursor.execute(
            FETCH_PENDING_DOMAIN_PROPOSAL_RECORDS_SQL,
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

    def finish_task_attempt(self, *, task_attempt_id: str, status: str, error: str | None) -> None:
        self._cursor.execute(
            FINISH_TASK_ATTEMPT_SQL,
            {
                "task_attempt_id": task_attempt_id,
                "status": status,
                "error": error,
            },
        )
        self._cursor.execute(
            UPDATE_TASK_STATE_FINISHED_SQL,
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

    def fetch_domain_candidates_for_raw_record(self, *, raw_record_id: str) -> list[DomainCandidateRow]:
        self._cursor.execute(
            FETCH_DOMAIN_CANDIDATES_FOR_RAW_RECORD_SQL,
            {"raw_record_id": raw_record_id},
        )
        return [_domain_candidate_row_from_row(row) for row in self._cursor.fetchall()]

    def upsert_domain_proposals(self, rows: list[InsertDomainProposal]) -> None:
        params_seq = [
            {
                "raw_record_id": row.raw_record_id,
                "task_attempt_id": row.task_attempt_id,
                "domain": row.domain,
                "normalized_domain": row.normalized_domain,
                "score": row.score,
                "signals": row.signals,
                "evidence": _json(row.evidence),
                "metadata": _json(row.metadata),
            }
            for row in rows
        ]
        if params_seq:
            self._cursor.executemany(UPSERT_DOMAIN_PROPOSAL_SQL, params_seq)

    def fetch_pending_enhanced_build_records(self, *, limit: int) -> list[EnhancedBuildRecord]:
        self._cursor.execute(FETCH_PENDING_ENHANCED_BUILD_RECORDS_SQL, {"limit": limit})
        return [_enhanced_build_record_from_row(row) for row in self._cursor.fetchall()]

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

    def fetch_pending_enhanced_publish_records(self, *, limit: int) -> list[EnhancedPublishRecord]:
        self._cursor.execute(FETCH_PENDING_ENHANCED_PUBLISH_RECORDS_SQL, {"limit": limit})
        return [_enhanced_publish_record_from_row(row) for row in self._cursor.fetchall()]

    def upsert_corpscout_raw_input(self, *, record: EnhancedPublishRecord, run_id: str) -> str:
        self._cursor.execute(
            UPSERT_CORPSCOUT_RAW_INPUT_SQL,
            {
                "source_native_id": record.organization_number,
                "organization_number": record.organization_number,
                "organization_name": record.organization_name,
                "registration_status": record.registration_status,
                "website": record.website,
                "country_iso2": record.country_iso2,
                "raw_payload": _json(record.raw_payload),
                "payload_hash": record.payload_hash,
                "run_id": run_id,
            },
        )
        return _single_value(self._cursor.fetchone())

    def upsert_corpscout_enhanced_raw_input(
        self,
        *,
        record: EnhancedPublishRecord,
        raw_input_id: str,
        dagster_run_id: str,
        dagster_asset_key: str,
    ) -> str:
        enhancement = _dict(record.enhanced_payload.get("enhancement"))
        self._cursor.execute(
            UPSERT_CORPSCOUT_ENHANCED_RAW_INPUT_SQL,
            {
                "raw_input_id": raw_input_id,
                "organization_number": record.organization_number,
                "payload_hash": record.payload_hash,
                "enhancement_version": record.schema_version,
                "attempt": 1,
                "dagster_run_id": dagster_run_id,
                "dagster_asset_key": dagster_asset_key,
                "status": str(enhancement.get("status") or "partial"),
                "section_statuses": _json(_dict(enhancement.get("section_statuses"))),
                "enhanced_payload": _json(record.enhanced_payload),
                "error": None,
                "metadata": _json({"enhanced_payload_hash": record.enhanced_payload_hash}),
                "started_at": enhancement.get("started_at"),
                "enhanced_at": enhancement.get("finished_at"),
            },
        )
        return _single_value(self._cursor.fetchone())

    def mark_enhanced_record_published(
        self,
        *,
        enhanced_record_id: str,
        corpscout_raw_input_id: str,
        corpscout_enhanced_raw_input_id: str,
    ) -> None:
        self._cursor.execute(
            MARK_ENHANCED_RECORD_PUBLISHED_SQL,
            {
                "enhanced_record_id": enhanced_record_id,
                "corpscout_raw_input_id": corpscout_raw_input_id,
                "corpscout_enhanced_raw_input_id": corpscout_enhanced_raw_input_id,
            },
        )

    def mark_enhanced_record_publish_failed(self, *, enhanced_record_id: str, error: str) -> None:
        self._cursor.execute(
            MARK_ENHANCED_RECORD_PUBLISH_FAILED_SQL,
            {
                "enhanced_record_id": enhanced_record_id,
                "error": error,
            },
        )


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


def _domain_candidate_row_from_row(row) -> DomainCandidateRow:
    evidence = row[4]
    metadata = row[5]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    return DomainCandidateRow(
        normalized_domain=str(row[0]),
        domain=str(row[1]),
        signal=str(row[2]),
        confidence=int(row[3]),
        evidence=evidence,
        metadata=metadata,
    )


def _enhanced_build_record_from_row(row) -> EnhancedBuildRecord:
    raw_payload = _json_value(row[6], {})
    translation_payload = _json_value(row[9], {})
    domain_proposals = _json_value(row[11], [])
    task_statuses = _json_value(row[12], {})
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
        domain_proposals=[_domain_proposal_row_from_mapping(item) for item in domain_proposals],
        task_statuses={str(key): str(value) for key, value in task_statuses.items()},
    )


def _enhanced_publish_record_from_row(row) -> EnhancedPublishRecord:
    return EnhancedPublishRecord(
        enhanced_record_id=str(row[0]),
        raw_record_id=str(row[1]),
        organization_number=str(row[2]),
        organization_name=str(row[3]) if row[3] is not None else None,
        registration_status=str(row[4]) if row[4] is not None else None,
        website=str(row[5]) if row[5] is not None else None,
        country_iso2=str(row[6]),
        raw_payload=_json_value(row[7], {}),
        payload_hash=str(row[8]),
        schema_version=str(row[9]),
        enhanced_payload=_json_value(row[10], {}),
        enhanced_payload_hash=str(row[11]),
    )


def _domain_proposal_row_from_mapping(value: dict[str, Any]) -> DomainProposalRow:
    return DomainProposalRow(
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

SEED_PENDING_RAW_TASK_STATES_SQL = """
WITH ensured_cursor AS (
    INSERT INTO dagster_brreg.raw_record_task_cursors (task_type)
    VALUES (%(task_type)s)
    ON CONFLICT (task_type) DO NOTHING
),
current_cursor AS (
    SELECT last_seen_at, last_raw_record_id
    FROM dagster_brreg.raw_record_task_cursors
    WHERE task_type = %(task_type)s
),
next_records AS (
    SELECT
        rr.id,
        rr.last_seen_at
    FROM dagster_brreg.raw_records rr
    CROSS JOIN current_cursor cursor_state
    WHERE rr.is_current = true
      AND (
          cursor_state.last_seen_at IS NULL
          OR (rr.last_seen_at, rr.id) > (cursor_state.last_seen_at, cursor_state.last_raw_record_id)
      )
    ORDER BY rr.last_seen_at ASC, rr.id ASC
    LIMIT %(limit)s
),
inserted_states AS (
    INSERT INTO dagster_brreg.raw_record_task_states (
        raw_record_id,
        task_type,
        status,
        next_retry_at
    )
    SELECT
        next_records.id,
        %(task_type)s,
        'pending',
        next_records.last_seen_at
    FROM next_records
    ON CONFLICT (raw_record_id, task_type) DO NOTHING
),
advanced_cursor AS (
    UPDATE dagster_brreg.raw_record_task_cursors cursor_state
    SET
        last_seen_at = last_record.last_seen_at,
        last_raw_record_id = last_record.id,
        updated_at = now()
    FROM (
        SELECT id, last_seen_at
        FROM next_records
        ORDER BY last_seen_at DESC, id DESC
        LIMIT 1
    ) last_record
    WHERE cursor_state.task_type = %(task_type)s
)
SELECT count(*)::int AS seeded_raw_records
FROM next_records
"""

TRY_ACQUIRE_RAW_TASK_RUN_LEASE_SQL = """
WITH lock_task AS (
    SELECT pg_advisory_xact_lock(hashtext('dagster_brreg.raw_record_task_run_leases:' || %(task_type)s))
),
renewed AS (
    UPDATE dagster_brreg.raw_record_task_run_leases
    SET
        enrichment_run_id = %(enrichment_run_id)s,
        lease_until = now() + (%(lease_seconds)s::text || ' seconds')::interval,
        max_concurrent_runs = %(max_concurrent_runs)s,
        updated_at = now()
    WHERE task_type = %(task_type)s
      AND dagster_run_id = %(dagster_run_id)s
      AND status = 'active'
      AND lease_until > now()
    RETURNING id
),
active_leases AS (
    SELECT count(*)::int AS active_count
    FROM dagster_brreg.raw_record_task_run_leases
    CROSS JOIN lock_task
    WHERE task_type = %(task_type)s
      AND status = 'active'
      AND lease_until > now()
),
inserted AS (
    INSERT INTO dagster_brreg.raw_record_task_run_leases (
        task_type,
        enrichment_run_id,
        dagster_run_id,
        status,
        lease_until,
        max_concurrent_runs
    )
    SELECT
        %(task_type)s,
        %(enrichment_run_id)s,
        %(dagster_run_id)s,
        'active',
        now() + (%(lease_seconds)s::text || ' seconds')::interval,
        %(max_concurrent_runs)s
    FROM active_leases
    WHERE NOT EXISTS (SELECT 1 FROM renewed)
      AND active_count < %(max_concurrent_runs)s
    RETURNING id
)
SELECT id, true AS acquired FROM renewed
UNION ALL
SELECT id, true AS acquired FROM inserted
UNION ALL
SELECT NULL::uuid AS id, false AS acquired
WHERE NOT EXISTS (SELECT 1 FROM renewed)
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1
"""

EXPIRE_RAW_TASK_RUN_LEASES_SQL = """
UPDATE dagster_brreg.raw_record_task_run_leases
SET
    status = 'expired',
    released_at = now(),
    updated_at = now()
WHERE task_type = %(task_type)s
  AND status = 'active'
  AND lease_until <= now()
"""

RENEW_RAW_TASK_RUN_LEASE_SQL = """
UPDATE dagster_brreg.raw_record_task_run_leases
SET
    lease_until = now() + (%(lease_seconds)s::text || ' seconds')::interval,
    updated_at = now()
WHERE id = %(lease_id)s
  AND status = 'active'
"""

RELEASE_RAW_TASK_RUN_LEASE_SQL = """
UPDATE dagster_brreg.raw_record_task_run_leases
SET
    status = 'released',
    released_at = now(),
    updated_at = now()
WHERE id = %(lease_id)s
  AND status = 'active'
"""

FETCH_PENDING_RAW_TASK_RECORDS_SQL = """
WITH pending_task_ids AS (
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
      AND ts.last_started_at < now() - interval '30 minutes'
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
inserted_new_task_states AS (
    INSERT INTO dagster_brreg.raw_record_task_states (
        raw_record_id,
        task_type,
        status,
        next_retry_at
    )
    SELECT
        new_task_ids.id,
        %(task_type)s,
        'pending',
        new_task_ids.sort_at
    FROM new_task_ids
    ON CONFLICT (raw_record_id, task_type) DO NOTHING
    RETURNING raw_record_id AS id, next_retry_at AS sort_at
),
candidate_ids AS (
    SELECT id, sort_at FROM retryable_task_ids
    UNION ALL
    SELECT id, sort_at FROM inserted_new_task_states
),
claimable_task_ids AS (
    SELECT
        ts.raw_record_id AS id,
        candidate_ids.sort_at
    FROM candidate_ids
    JOIN dagster_brreg.raw_record_task_states ts
      ON ts.raw_record_id = candidate_ids.id
     AND ts.task_type = %(task_type)s
    JOIN dagster_brreg.raw_records rr ON rr.id = candidate_ids.id
    WHERE rr.is_current = true
      AND (
          ts.status = 'pending'
          OR (ts.status = 'failed_retryable' AND ts.next_retry_at <= now())
          OR (ts.status = 'running' AND ts.last_started_at < now() - interval '30 minutes')
      )
    ORDER BY candidate_ids.sort_at ASC, candidate_ids.id ASC
    LIMIT %(limit)s
    FOR UPDATE OF ts SKIP LOCKED
),
claimed_task_ids AS (
    UPDATE dagster_brreg.raw_record_task_states ts
    SET
        status = 'running',
        last_started_at = now(),
        last_finished_at = NULL,
        next_retry_at = NULL,
        last_error = NULL,
        updated_at = now()
    FROM claimable_task_ids
    WHERE ts.raw_record_id = claimable_task_ids.id
      AND ts.task_type = %(task_type)s
    RETURNING ts.raw_record_id AS id, claimable_task_ids.sort_at
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
ORDER BY claimed_task_ids.sort_at ASC, claimed_task_ids.id ASC
LIMIT %(limit)s
"""

FETCH_PENDING_DOMAIN_PROPOSAL_RECORDS_SQL = """
WITH eligible_records AS (
    SELECT
        rr.id,
        coalesce(ts.next_retry_at, rr.last_seen_at) AS sort_at
    FROM dagster_brreg.raw_records rr
    LEFT JOIN dagster_brreg.raw_record_task_states ts
      ON ts.raw_record_id = rr.id
     AND ts.task_type = %(task_type)s
    WHERE rr.is_current = true
      AND EXISTS (
          SELECT 1
          FROM dagster_brreg.domain_candidates dc
          WHERE dc.raw_record_id = rr.id
            AND dc.status IN ('candidate', 'accepted')
      )
      AND (
          ts.raw_record_id IS NULL
          OR ts.status = 'pending'
          OR (ts.status = 'failed_retryable' AND ts.next_retry_at <= now())
          OR (ts.status = 'running' AND ts.last_started_at < now() - interval '30 minutes')
          OR (
              ts.status IN ('succeeded', 'skipped')
              AND EXISTS (
                  SELECT 1
                  FROM dagster_brreg.domain_candidates dc
                  WHERE dc.raw_record_id = rr.id
                    AND dc.status IN ('candidate', 'accepted')
                    AND dc.updated_at > coalesce(ts.last_finished_at, '-infinity'::timestamptz)
              )
          )
      )
    ORDER BY coalesce(ts.next_retry_at, rr.last_seen_at) ASC, rr.id ASC
    LIMIT %(limit)s
),
inserted_missing_task_ids AS (
    INSERT INTO dagster_brreg.raw_record_task_states (
        raw_record_id,
        task_type,
        status,
        last_started_at,
        next_retry_at
    )
    SELECT
        eligible_records.id,
        %(task_type)s,
        'running',
        now(),
        NULL
    FROM eligible_records
    WHERE NOT EXISTS (
        SELECT 1
        FROM dagster_brreg.raw_record_task_states mts
        WHERE mts.raw_record_id = eligible_records.id
          AND mts.task_type = %(task_type)s
    )
    ON CONFLICT (raw_record_id, task_type) DO NOTHING
    RETURNING raw_record_id AS id
),
existing_claimable_records AS (
    SELECT
        mts.raw_record_id AS id
    FROM eligible_records
    JOIN dagster_brreg.raw_record_task_states mts
      ON mts.raw_record_id = eligible_records.id
     AND mts.task_type = %(task_type)s
    WHERE (
        mts.status = 'pending'
        OR (mts.status = 'failed_retryable' AND mts.next_retry_at <= now())
        OR (mts.status = 'running' AND mts.last_started_at < now() - interval '30 minutes')
        OR (
            mts.status IN ('succeeded', 'skipped')
            AND EXISTS (
                SELECT 1
                FROM dagster_brreg.domain_candidates dc
                WHERE dc.raw_record_id = eligible_records.id
                  AND dc.status IN ('candidate', 'accepted')
                  AND dc.updated_at > coalesce(mts.last_finished_at, '-infinity'::timestamptz)
            )
        )
    )
    ORDER BY eligible_records.sort_at ASC, eligible_records.id ASC
    LIMIT %(limit)s
    FOR UPDATE OF mts SKIP LOCKED
),
claimed_existing_task_ids AS (
    UPDATE dagster_brreg.raw_record_task_states mts
    SET
        status = 'running',
        last_started_at = now(),
        last_finished_at = NULL,
        next_retry_at = NULL,
        last_error = NULL,
        updated_at = now()
    FROM existing_claimable_records
    WHERE mts.raw_record_id = existing_claimable_records.id
      AND mts.task_type = %(task_type)s
    RETURNING mts.raw_record_id AS id
),
claimed_task_ids AS (
    SELECT
        claimed_existing_task_ids.id,
        eligible_records.sort_at
    FROM claimed_existing_task_ids
    JOIN eligible_records ON eligible_records.id = claimed_existing_task_ids.id
    UNION ALL
    SELECT
        inserted_missing_task_ids.id,
        eligible_records.sort_at
    FROM inserted_missing_task_ids
    JOIN eligible_records ON eligible_records.id = inserted_missing_task_ids.id
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
ORDER BY claimed_task_ids.sort_at ASC, claimed_task_ids.id ASC
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
    last_error,
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
    last_error = NULL,
    updated_at = now()
"""

FINISH_TASK_ATTEMPT_SQL = """
UPDATE dagster_brreg.task_attempts
SET
    status = %(status)s,
    finished_at = now(),
    error = %(error)s
WHERE id = %(task_attempt_id)s
"""

UPDATE_TASK_STATE_FINISHED_SQL = """
UPDATE dagster_brreg.raw_record_task_states rts
SET
    status = CASE
        WHEN %(status)s = 'failed' AND ta.attempt >= 5 THEN 'failed_terminal'
        WHEN %(status)s = 'failed' THEN 'failed_retryable'
        WHEN %(status)s = 'succeeded' THEN 'succeeded'
        WHEN %(status)s = 'skipped' THEN 'skipped'
        WHEN %(status)s = 'cancelled' THEN 'cancelled'
        ELSE rts.status
    END,
    attempt_count = GREATEST(rts.attempt_count, ta.attempt),
    last_attempt_id = ta.id,
    last_finished_at = now(),
    next_retry_at = CASE
        WHEN %(status)s <> 'failed' OR ta.attempt >= 5 THEN NULL
        WHEN ta.attempt = 1 THEN now() + interval '5 minutes'
        WHEN ta.attempt = 2 THEN now() + interval '30 minutes'
        WHEN ta.attempt = 3 THEN now() + interval '6 hours'
        ELSE now() + interval '1 day'
    END,
    last_error = %(error)s,
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
    metadata = dagster_brreg.domain_candidates.metadata || EXCLUDED.metadata,
    updated_at = now()
"""

FETCH_DOMAIN_CANDIDATES_FOR_RAW_RECORD_SQL = """
SELECT
    normalized_domain,
    domain,
    signal,
    confidence,
    evidence,
    metadata
FROM dagster_brreg.domain_candidates
WHERE raw_record_id = %(raw_record_id)s
  AND status IN ('candidate', 'accepted')
ORDER BY normalized_domain ASC, confidence DESC, signal ASC
"""

UPSERT_DOMAIN_PROPOSAL_SQL = """
INSERT INTO dagster_brreg.domain_proposals (
    raw_record_id,
    task_attempt_id,
    domain,
    normalized_domain,
    score,
    signals,
    evidence,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(task_attempt_id)s,
    %(domain)s,
    %(normalized_domain)s,
    %(score)s,
    %(signals)s,
    %(evidence)s::jsonb,
    %(metadata)s::jsonb
)
ON CONFLICT (raw_record_id, normalized_domain) DO UPDATE
SET
    task_attempt_id = EXCLUDED.task_attempt_id,
    domain = EXCLUDED.domain,
    score = EXCLUDED.score,
    signals = EXCLUDED.signals,
    evidence = EXCLUDED.evidence,
    metadata = dagster_brreg.domain_proposals.metadata || EXCLUDED.metadata,
    updated_at = now()
"""

FETCH_PENDING_ENHANCED_BUILD_RECORDS_SQL = """
WITH latest_translation AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status,
        translated_payload,
        created_at
    FROM dagster_brreg.translation_results
    WHERE status IN ('succeeded', 'skipped')
    ORDER BY raw_record_id, created_at DESC
),
proposal_rows AS (
    SELECT
        raw_record_id,
        jsonb_agg(
            jsonb_build_object(
                'domain', domain,
                'normalized_domain', normalized_domain,
                'score', score,
                'signals', signals,
                'status', status,
                'evidence', evidence,
                'metadata', metadata
            )
            ORDER BY score DESC, normalized_domain ASC
        ) AS proposals
    FROM dagster_brreg.domain_proposals
    WHERE status IN ('proposed', 'accepted')
    GROUP BY raw_record_id
),
task_status_rows AS (
    SELECT
        raw_record_id,
        jsonb_object_agg(task_type, status ORDER BY task_type) AS task_statuses
    FROM dagster_brreg.raw_record_task_states
    GROUP BY raw_record_id
)
SELECT
    rr.id,
    rr.organization_number,
    rr.organization_name,
    rr.registration_status,
    rr.website,
    rr.country_iso2,
    rr.raw_payload,
    rr.payload_hash,
    lt.status AS translation_status,
    coalesce(lt.translated_payload, '{}'::jsonb) AS translation_payload,
    coalesce(mts.status, 'skipped') AS domain_status,
    coalesce(pr.proposals, '[]'::jsonb) AS domain_proposals,
    coalesce(tsr.task_statuses, '{}'::jsonb) AS task_statuses
FROM dagster_brreg.raw_records rr
JOIN latest_translation lt ON lt.raw_record_id = rr.id
JOIN dagster_brreg.raw_record_task_states tts
  ON tts.raw_record_id = rr.id
 AND tts.task_type = 'translate'
 AND tts.status IN ('succeeded', 'skipped')
LEFT JOIN dagster_brreg.raw_record_task_states mts
  ON mts.raw_record_id = rr.id
 AND mts.task_type = 'merge_domain_proposals'
LEFT JOIN proposal_rows pr ON pr.raw_record_id = rr.id
LEFT JOIN task_status_rows tsr ON tsr.raw_record_id = rr.id
WHERE rr.is_current = true
  AND (
      mts.status IN ('succeeded', 'skipped')
      OR NOT EXISTS (
          SELECT 1
          FROM dagster_brreg.domain_candidates dc
          WHERE dc.raw_record_id = rr.id
            AND dc.status IN ('candidate', 'accepted')
      )
  )
  AND NOT EXISTS (
      SELECT 1
      FROM dagster_brreg.enhanced_records er
      WHERE er.raw_record_id = rr.id
        AND er.schema_version = 'brreg.enhanced.v1'
        AND er.status IN ('built', 'published')
        AND er.built_at >= greatest(
            coalesce(tts.last_finished_at, '-infinity'::timestamptz),
            coalesce(mts.last_finished_at, '-infinity'::timestamptz),
            lt.created_at
        )
  )
ORDER BY rr.last_seen_at ASC, rr.id ASC
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

FETCH_PENDING_ENHANCED_PUBLISH_RECORDS_SQL = """
SELECT
    er.id,
    rr.id AS raw_record_id,
    rr.organization_number,
    rr.organization_name,
    rr.registration_status,
    rr.website,
    rr.country_iso2,
    rr.raw_payload,
    rr.payload_hash,
    er.schema_version,
    er.enhanced_payload,
    er.enhanced_payload_hash
FROM dagster_brreg.enhanced_records er
JOIN dagster_brreg.raw_records rr ON rr.id = er.raw_record_id
WHERE er.status IN ('built', 'publish_failed')
  AND rr.is_current = true
ORDER BY er.built_at ASC, er.id ASC
LIMIT %(limit)s
"""

UPSERT_CORPSCOUT_RAW_INPUT_SQL = """
INSERT INTO brreg_company_raw_inputs (
    source_native_id,
    organization_number,
    organization_name,
    registration_status,
    website,
    country_iso2,
    raw_payload,
    payload_hash,
    run_id
) VALUES (
    %(source_native_id)s,
    %(organization_number)s,
    %(organization_name)s,
    %(registration_status)s,
    %(website)s,
    %(country_iso2)s,
    %(raw_payload)s::jsonb,
    %(payload_hash)s,
    %(run_id)s
)
ON CONFLICT (organization_number, payload_hash) DO UPDATE
SET
    last_seen_at = now(),
    organization_name = EXCLUDED.organization_name,
    registration_status = EXCLUDED.registration_status,
    website = EXCLUDED.website,
    country_iso2 = EXCLUDED.country_iso2,
    raw_payload = EXCLUDED.raw_payload,
    run_id = EXCLUDED.run_id,
    updated_at = now()
RETURNING id
"""

UPSERT_CORPSCOUT_ENHANCED_RAW_INPUT_SQL = """
INSERT INTO brreg_enhanced_raw_inputs (
    raw_input_id,
    organization_number,
    payload_hash,
    enhancement_version,
    attempt,
    dagster_run_id,
    dagster_asset_key,
    status,
    section_statuses,
    enhanced_payload,
    error,
    metadata,
    started_at,
    enhanced_at
) VALUES (
    %(raw_input_id)s,
    %(organization_number)s,
    %(payload_hash)s,
    %(enhancement_version)s,
    %(attempt)s,
    %(dagster_run_id)s,
    %(dagster_asset_key)s,
    %(status)s,
    %(section_statuses)s::jsonb,
    %(enhanced_payload)s::jsonb,
    %(error)s,
    %(metadata)s::jsonb,
    %(started_at)s::timestamptz,
    %(enhanced_at)s::timestamptz
)
ON CONFLICT (raw_input_id, payload_hash, enhancement_version, attempt) DO UPDATE
SET
    organization_number = EXCLUDED.organization_number,
    dagster_run_id = EXCLUDED.dagster_run_id,
    dagster_asset_key = EXCLUDED.dagster_asset_key,
    status = EXCLUDED.status,
    section_statuses = EXCLUDED.section_statuses,
    enhanced_payload = EXCLUDED.enhanced_payload,
    error = EXCLUDED.error,
    metadata = brreg_enhanced_raw_inputs.metadata || EXCLUDED.metadata,
    started_at = EXCLUDED.started_at,
    enhanced_at = EXCLUDED.enhanced_at,
    updated_at = now()
RETURNING id
"""

MARK_ENHANCED_RECORD_PUBLISHED_SQL = """
UPDATE dagster_brreg.enhanced_records
SET
    status = 'published',
    corpscout_raw_input_id = %(corpscout_raw_input_id)s,
    corpscout_enhanced_raw_input_id = %(corpscout_enhanced_raw_input_id)s,
    published_at = now(),
    error = NULL
WHERE id = %(enhanced_record_id)s
"""

MARK_ENHANCED_RECORD_PUBLISH_FAILED_SQL = """
UPDATE dagster_brreg.enhanced_records
SET
    status = 'publish_failed',
    error = %(error)s
WHERE id = %(enhanced_record_id)s
"""
