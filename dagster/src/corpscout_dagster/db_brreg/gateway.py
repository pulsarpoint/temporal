from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow
from corpscout_dagster.brreg.translation_terms import CachedTermTranslation, TranslationCacheKey
from corpscout_dagster.db_brreg.store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    CreateTaskAttempt,
    EnhancedBuildRecord,
    FinishEnrichmentRun,
    IncrementEnrichmentRunProgress,
    InsertCurrencyResult,
    InsertDomainResult,
    InsertEnhancedRecord,
    InsertTranslationResult,
    RawTaskRecord,
    TaskAttempt,
    UpsertCachedTranslation,
    UpsertResult,
)


class BrregAssetName(StrEnum):
    RAW_RECORDS = "raw_records"
    TRANSLATION_RESULTS = "translation_results"
    DOMAIN_RESULTS = "domain_results"
    CURRENCY_RESULTS = "currency_results"
    ENHANCED_RECORDS = "enhanced_records"


class BrregTaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"
    CANCELLED = "cancelled"


class BrregResultStatus(StrEnum):
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    PARTIAL = "partial"
    NOT_FOUND = "not_found"
    NOT_AVAILABLE = "not_available"
    FAILED = "failed"
    BUILT = "built"
    PUBLISHED = "published"
    PUBLISH_FAILED = "publish_failed"
    SUPERSEDED = "superseded"


@dataclass(frozen=True)
class IngestRawRecordsCommand:
    bulk_snapshot_id: str
    rows: list[BrregWorkingRawRecordRow]
    enrichment_run_id: str | None = None


@dataclass(frozen=True)
class IngestRawRecordsResult:
    rows_seen: int
    rows_written: int
    rows_inserted_new: int
    rows_existing_unchanged: int
    rows_new_versions: int

    @classmethod
    def from_upsert_result(cls, result: UpsertResult) -> IngestRawRecordsResult:
        return cls(
            rows_seen=result.rows_seen,
            rows_written=result.rows_written,
            rows_inserted_new=result.rows_inserted_new,
            rows_existing_unchanged=result.rows_existing_unchanged,
            rows_new_versions=result.rows_new_versions,
        )


@dataclass(frozen=True)
class SmokeIngestRawRecordCommand:
    dagster_run_id: str
    source_url: str
    row: BrregWorkingRawRecordRow
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BeginRawIngestCommand:
    dagster_run_id: str
    source_url: str
    run_metadata: dict[str, Any] = field(default_factory=dict)
    snapshot_metadata: dict[str, Any] = field(default_factory=dict)
    content_length_bytes: int | None = None
    compressed_payload_hash: str | None = None
    storage_uri: str | None = None


@dataclass(frozen=True)
class RawIngestContext:
    enrichment_run_id: str
    bulk_snapshot_id: str


@dataclass(frozen=True)
class BeginActionRunCommand:
    dagster_run_id: str
    run_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ActionRunContext:
    enrichment_run_id: str


@dataclass(frozen=True)
class FinishActionRunCommand:
    enrichment_run_id: str
    status: str
    error: str | None = None


@dataclass(frozen=True)
class ReconcileTranslationTasksCommand:
    model: str
    prompt_version: str


@dataclass(frozen=True)
class ReconcileTranslationTasksResult:
    reconciled_rows: int


@dataclass(frozen=True)
class ResetUnstartedRunningTasksCommand:
    task_type: str
    raw_record_ids: list[str]


@dataclass(frozen=True)
class ResetUnstartedRunningTasksResult:
    reset_rows: int


@dataclass(frozen=True)
class ClaimTaskBatchCommand:
    run_id: str
    batch_size: int
    max_parallel_tasks: int
    lease_seconds: int
    metadata: dict[str, Any] = field(default_factory=dict)
    enrichment_run_id: str | None = None


@dataclass(frozen=True)
class ClaimEnhancedBatchCommand:
    run_id: str
    batch_size: int
    metadata: dict[str, Any] = field(default_factory=dict)
    enrichment_run_id: str | None = None


@dataclass(frozen=True)
class ClaimedRawRecord:
    raw_record_id: str
    organization_number: str
    organization_name: str | None
    website: str | None
    raw_payload: dict[str, Any]
    task_attempt_id: str
    attempt: int

    @property
    def record(self) -> RawTaskRecord:
        return RawTaskRecord(
            id=self.raw_record_id,
            organization_number=self.organization_number,
            organization_name=self.organization_name,
            website=self.website,
            raw_payload=self.raw_payload,
        )


@dataclass(frozen=True)
class ClaimedRawRecordBatch:
    asset: BrregAssetName
    records: list[ClaimedRawRecord]


@dataclass(frozen=True)
class ClaimedEnhancedBuildRecord:
    build_record: EnhancedBuildRecord
    task_attempt_id: str
    attempt: int


@dataclass(frozen=True)
class ClaimedEnhancedBuildBatch:
    records: list[ClaimedEnhancedBuildRecord]


@dataclass(frozen=True)
class SubmitTaskResult:
    asset: BrregAssetName
    status: BrregTaskStatus
    raw_record_id: str
    task_attempt_id: str


@dataclass(frozen=True)
class SubmitTranslationResultCommand:
    raw_record_id: str
    task_attempt_id: str
    status: str
    translated_payload: dict[str, Any]
    model: str
    prompt_version: str
    metadata: dict[str, Any] = field(default_factory=dict)
    enrichment_run_id: str | None = None


@dataclass(frozen=True)
class SubmitDomainResultCommand:
    raw_record_id: str
    task_attempt_id: str
    status: str
    best_domain: str | None
    domain_payload: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    enrichment_run_id: str | None = None


@dataclass(frozen=True)
class SubmitCurrencyResultCommand:
    raw_record_id: str
    task_attempt_id: str
    status: str
    original_currency: str | None
    original_payload: dict[str, Any]
    usd_payload: dict[str, Any]
    fx_metadata: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    source_uri: str | None = None
    error: str | None = None
    enrichment_run_id: str | None = None


@dataclass(frozen=True)
class SubmitEnhancedRecordCommand:
    raw_record_id: str
    task_attempt_id: str
    schema_version: str
    enhanced_payload: dict[str, Any]
    enhanced_payload_hash: str
    metadata: dict[str, Any] = field(default_factory=dict)
    enrichment_run_id: str | None = None


@dataclass(frozen=True)
class SubmitTaskFailureCommand:
    asset: BrregAssetName
    raw_record_id: str
    task_attempt_id: str
    error: str
    error_category: str
    error_code: str
    retry_strategy: str
    metadata: dict[str, Any] = field(default_factory=dict)
    enrichment_run_id: str | None = None
    model: str | None = None
    prompt_version: str | None = None
    artifact_payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class RetryTaskFailuresCommand:
    task_type: str | None
    error_category: str
    limit: int


@dataclass(frozen=True)
class RetryTaskFailuresResult:
    retried_rows: int


@dataclass(frozen=True)
class FetchCachedTranslationsCommand:
    keys: list[TranslationCacheKey]
    model: str
    prompt_version: str


@dataclass(frozen=True)
class UpsertCachedTranslationsCommand:
    rows: list[UpsertCachedTranslation]


class BrregAssetGateway:
    def __init__(
        self,
        connection,
        *,
        translation_model: str | None = None,
        translation_prompt_version: str | None = None,
    ) -> None:
        self._connection = connection
        self._translation_model = translation_model
        self._translation_prompt_version = translation_prompt_version

    def ingest_raw_records(self, command: IngestRawRecordsCommand) -> IngestRawRecordsResult:
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            result = store.upsert_raw_records(command.rows, bulk_snapshot_id=command.bulk_snapshot_id)
            if command.enrichment_run_id is not None:
                store.increment_enrichment_run_progress(
                    IncrementEnrichmentRunProgress(
                        enrichment_run_id=command.enrichment_run_id,
                        records_seen=result.rows_seen,
                        records_completed=result.rows_written,
                    )
                )
        self._connection.commit()
        return IngestRawRecordsResult.from_upsert_result(result)

    def begin_raw_ingest(self, command: BeginRawIngestCommand) -> RawIngestContext:
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            enrichment_run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=command.dagster_run_id,
                    run_type="bulk_ingest",
                    metadata=command.run_metadata,
                )
            )
            bulk_snapshot_id = store.create_bulk_snapshot(
                CreateBulkSnapshot(
                    enrichment_run_id=enrichment_run_id,
                    source_url=command.source_url,
                    content_length_bytes=command.content_length_bytes,
                    compressed_payload_hash=command.compressed_payload_hash,
                    storage_uri=command.storage_uri,
                    metadata=command.snapshot_metadata,
                )
            )
        self._connection.commit()
        return RawIngestContext(enrichment_run_id=enrichment_run_id, bulk_snapshot_id=bulk_snapshot_id)

    def begin_action_run(self, command: BeginActionRunCommand) -> ActionRunContext:
        with self._connection.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=command.dagster_run_id,
                    run_type=command.run_type,
                    metadata=command.metadata,
                )
            )
        self._connection.commit()
        return ActionRunContext(enrichment_run_id=enrichment_run_id)

    def finish_action_run(self, command: FinishActionRunCommand) -> None:
        with self._connection.cursor() as cursor:
            BrregWorkingStore(cursor).finish_enrichment_run(
                FinishEnrichmentRun(
                    enrichment_run_id=command.enrichment_run_id,
                    status=command.status,
                    error=command.error,
                )
            )
        self._connection.commit()

    def reconcile_translation_tasks(
        self,
        command: ReconcileTranslationTasksCommand,
    ) -> ReconcileTranslationTasksResult:
        with self._connection.cursor() as cursor:
            reconciled_rows = BrregWorkingStore(cursor).reconcile_translation_task_states(
                model=command.model,
                prompt_version=command.prompt_version,
            )
        self._connection.commit()
        return ReconcileTranslationTasksResult(reconciled_rows=reconciled_rows)

    def smoke_ingest_raw_record(self, command: SmokeIngestRawRecordCommand) -> IngestRawRecordsResult:
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            enrichment_run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=command.dagster_run_id,
                    run_type="bulk_ingest",
                    metadata=command.metadata,
                )
            )
            bulk_snapshot_id = store.create_bulk_snapshot(
                CreateBulkSnapshot(
                    enrichment_run_id=enrichment_run_id,
                    source_url=command.source_url,
                    content_length_bytes=None,
                    compressed_payload_hash=None,
                    storage_uri=None,
                    metadata=command.metadata,
                )
            )
            result = store.upsert_raw_records([command.row], bulk_snapshot_id=bulk_snapshot_id)
        return IngestRawRecordsResult.from_upsert_result(result)

    def claim_translation_batch(self, command: ClaimTaskBatchCommand) -> ClaimedRawRecordBatch:
        return self._claim_raw_task_batch(BrregAssetName.TRANSLATION_RESULTS, command)

    def claim_domain_batch(self, command: ClaimTaskBatchCommand) -> ClaimedRawRecordBatch:
        return self._claim_raw_task_batch(BrregAssetName.DOMAIN_RESULTS, command)

    def claim_currency_batch(self, command: ClaimTaskBatchCommand) -> ClaimedRawRecordBatch:
        return self._claim_raw_task_batch(BrregAssetName.CURRENCY_RESULTS, command)

    def claim_enhanced_batch(self, command: ClaimEnhancedBatchCommand) -> ClaimedEnhancedBuildBatch:
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.refresh_enhanced_ready_records()
            build_records = store.fetch_pending_enhanced_build_records(limit=command.batch_size)
            claimed = [
                ClaimedEnhancedBuildRecord(
                    build_record=record,
                    task_attempt_id=attempt.id,
                    attempt=attempt.attempt,
                )
                for record in build_records
                for attempt in [
                    store.create_task_attempt(
                        CreateTaskAttempt(
                            enrichment_run_id=_required_enrichment_run_id(command.enrichment_run_id, command.run_id),
                            raw_record_id=record.record.id,
                            task_type=_task_type_for_asset(BrregAssetName.ENHANCED_RECORDS),
                            metadata={
                                "organization_number": record.record.organization_number,
                                "dagster_run_id": command.run_id,
                                **command.metadata,
                            },
                        )
                    )
                ]
            ]
        self._connection.commit()
        return ClaimedEnhancedBuildBatch(records=claimed)

    def submit_translation_result(self, command: SubmitTranslationResultCommand) -> SubmitTaskResult:
        status = BrregResultStatus(command.status)
        if status not in {BrregResultStatus.SUCCEEDED, BrregResultStatus.SKIPPED}:
            raise ValueError(f"invalid translation result status: {command.status}")
        task_status = BrregTaskStatus.SKIPPED if status is BrregResultStatus.SKIPPED else BrregTaskStatus.SUCCEEDED
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.insert_translation_result(
                InsertTranslationResult(
                    raw_record_id=command.raw_record_id,
                    task_attempt_id=command.task_attempt_id,
                    status=status.value,
                    translated_payload=command.translated_payload,
                    model=command.model,
                    prompt_version=command.prompt_version,
                    error=None,
                    metadata=command.metadata,
                )
            )
            self._finish_success(store, command, task_status)
        self._connection.commit()
        return SubmitTaskResult(
            asset=BrregAssetName.TRANSLATION_RESULTS,
            status=task_status,
            raw_record_id=command.raw_record_id,
            task_attempt_id=command.task_attempt_id,
        )

    def submit_translation_failure(self, command: SubmitTaskFailureCommand) -> SubmitTaskResult:
        if command.asset is not BrregAssetName.TRANSLATION_RESULTS:
            raise ValueError("translation failure command must use TRANSLATION_RESULTS asset")
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.insert_translation_result(
                InsertTranslationResult(
                    raw_record_id=command.raw_record_id,
                    task_attempt_id=command.task_attempt_id,
                    status=BrregResultStatus.FAILED.value,
                    translated_payload=None,
                    model=command.model,
                    prompt_version=command.prompt_version,
                    error=command.error,
                    metadata=_failure_metadata(command),
                )
            )
            self._finish_failure(store, command)
        self._connection.commit()
        return _failure_result(command)

    def submit_domain_result(self, command: SubmitDomainResultCommand) -> SubmitTaskResult:
        status = BrregResultStatus(command.status)
        if status not in {BrregResultStatus.SUCCEEDED, BrregResultStatus.PARTIAL, BrregResultStatus.NOT_FOUND}:
            raise ValueError(f"invalid domain result status: {command.status}")
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.insert_domain_result(
                InsertDomainResult(
                    raw_record_id=command.raw_record_id,
                    task_attempt_id=command.task_attempt_id,
                    status=status.value,
                    best_domain=command.best_domain,
                    domain_payload=command.domain_payload,
                    error=command.error,
                    metadata=command.metadata,
                )
            )
            self._finish_success(store, command, BrregTaskStatus.SUCCEEDED)
        self._connection.commit()
        return SubmitTaskResult(
            asset=BrregAssetName.DOMAIN_RESULTS,
            status=BrregTaskStatus.SUCCEEDED,
            raw_record_id=command.raw_record_id,
            task_attempt_id=command.task_attempt_id,
        )

    def submit_domain_failure(self, command: SubmitTaskFailureCommand) -> SubmitTaskResult:
        if command.asset is not BrregAssetName.DOMAIN_RESULTS:
            raise ValueError("domain failure command must use DOMAIN_RESULTS asset")
        payload = command.artifact_payload or {
            "schema_version": "crawl-service.brreg.v1",
            "status": BrregResultStatus.FAILED.value,
            "record_id": command.raw_record_id,
            "best_domain": None,
            "candidates": [],
            "errors": [{"message": command.error}],
            "warnings": [],
        }
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.insert_domain_result(
                InsertDomainResult(
                    raw_record_id=command.raw_record_id,
                    task_attempt_id=command.task_attempt_id,
                    status=BrregResultStatus.FAILED.value,
                    best_domain=None,
                    domain_payload=payload,
                    error=command.error,
                    metadata=_failure_metadata(command),
                )
            )
            self._finish_failure(store, command)
        self._connection.commit()
        return _failure_result(command)

    def submit_currency_result(self, command: SubmitCurrencyResultCommand) -> SubmitTaskResult:
        status = BrregResultStatus(command.status)
        if status not in {BrregResultStatus.SUCCEEDED, BrregResultStatus.SKIPPED, BrregResultStatus.NOT_AVAILABLE}:
            raise ValueError(f"invalid currency result status: {command.status}")
        task_status = BrregTaskStatus.SKIPPED if status in {BrregResultStatus.SKIPPED, BrregResultStatus.NOT_AVAILABLE} else BrregTaskStatus.SUCCEEDED
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.insert_currency_result(
                InsertCurrencyResult(
                    raw_record_id=command.raw_record_id,
                    task_attempt_id=command.task_attempt_id,
                    status=status.value,
                    original_currency=command.original_currency,
                    original_payload=command.original_payload,
                    usd_payload=command.usd_payload,
                    fx_metadata=command.fx_metadata,
                    source_uri=command.source_uri,
                    error=command.error,
                    metadata=command.metadata,
                )
            )
            self._finish_success(store, command, task_status)
        self._connection.commit()
        return SubmitTaskResult(
            asset=BrregAssetName.CURRENCY_RESULTS,
            status=task_status,
            raw_record_id=command.raw_record_id,
            task_attempt_id=command.task_attempt_id,
        )

    def submit_currency_failure(self, command: SubmitTaskFailureCommand) -> SubmitTaskResult:
        if command.asset is not BrregAssetName.CURRENCY_RESULTS:
            raise ValueError("currency failure command must use CURRENCY_RESULTS asset")
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.insert_currency_result(
                InsertCurrencyResult(
                    raw_record_id=command.raw_record_id,
                    task_attempt_id=command.task_attempt_id,
                    status=BrregResultStatus.FAILED.value,
                    original_currency=_failure_original_currency(command),
                    original_payload={},
                    usd_payload={},
                    fx_metadata={},
                    source_uri=None,
                    error=command.error,
                    metadata=_failure_metadata(command),
                )
            )
            self._finish_failure(store, command)
        self._connection.commit()
        return _failure_result(command)

    def submit_enhanced_record(self, command: SubmitEnhancedRecordCommand) -> SubmitTaskResult:
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.upsert_enhanced_record(
                InsertEnhancedRecord(
                    raw_record_id=command.raw_record_id,
                    task_attempt_id=command.task_attempt_id,
                    schema_version=command.schema_version,
                    enhanced_payload=command.enhanced_payload,
                    enhanced_payload_hash=command.enhanced_payload_hash,
                    metadata=command.metadata,
                )
            )
            self._finish_success(store, command, BrregTaskStatus.SUCCEEDED)
        self._connection.commit()
        return SubmitTaskResult(
            asset=BrregAssetName.ENHANCED_RECORDS,
            status=BrregTaskStatus.SUCCEEDED,
            raw_record_id=command.raw_record_id,
            task_attempt_id=command.task_attempt_id,
        )

    def submit_enhanced_failure(self, command: SubmitTaskFailureCommand) -> SubmitTaskResult:
        if command.asset is not BrregAssetName.ENHANCED_RECORDS:
            raise ValueError("enhanced failure command must use ENHANCED_RECORDS asset")
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            self._finish_failure(store, command)
        self._connection.commit()
        return _failure_result(command)

    def retry_task_failures(self, command: RetryTaskFailuresCommand) -> RetryTaskFailuresResult:
        with self._connection.cursor() as cursor:
            retried_rows = BrregWorkingStore(cursor).retry_task_failures(
                task_type=command.task_type,
                error_category=command.error_category,
                limit=command.limit,
            )
        self._connection.commit()
        return RetryTaskFailuresResult(retried_rows=retried_rows)

    def fetch_cached_translations(
        self,
        command: FetchCachedTranslationsCommand,
    ) -> dict[TranslationCacheKey, CachedTermTranslation]:
        with self._connection.cursor() as cursor:
            return BrregWorkingStore(cursor).fetch_cached_translations(
                command.keys,
                model=command.model,
                prompt_version=command.prompt_version,
            )

    def upsert_cached_translations(self, command: UpsertCachedTranslationsCommand) -> None:
        with self._connection.cursor() as cursor:
            BrregWorkingStore(cursor).upsert_cached_translations(command.rows)
        self._connection.commit()

    def reset_unstarted_running_tasks(
        self,
        command: ResetUnstartedRunningTasksCommand,
    ) -> ResetUnstartedRunningTasksResult:
        with self._connection.cursor() as cursor:
            reset_rows = BrregWorkingStore(cursor).reset_unstarted_running_task_records(
                task_type=command.task_type,
                raw_record_ids=command.raw_record_ids,
            )
        self._connection.commit()
        return ResetUnstartedRunningTasksResult(reset_rows=reset_rows)

    def fetch_task_failure_summary(self, *, task_type: str) -> dict[str, int]:
        with self._connection.cursor() as cursor:
            return BrregWorkingStore(cursor).fetch_task_failure_summary(task_type=task_type)

    def fetch_raw_task_state_summary(self, *, task_type: str) -> dict[str, int]:
        with self._connection.cursor() as cursor:
            return BrregWorkingStore(cursor).fetch_raw_task_state_summary(task_type=task_type)

    def fetch_translation_artifact_summary(self, *, model: str, prompt_version: str) -> dict[str, int]:
        with self._connection.cursor() as cursor:
            return BrregWorkingStore(cursor).fetch_translation_artifact_summary(
                model=model,
                prompt_version=prompt_version,
            )

    def fetch_domain_result_summary(self) -> dict[str, int]:
        with self._connection.cursor() as cursor:
            return BrregWorkingStore(cursor).fetch_domain_result_summary()

    def fetch_currency_result_summary(self) -> dict[str, int]:
        with self._connection.cursor() as cursor:
            return BrregWorkingStore(cursor).fetch_currency_result_summary()

    def fetch_enhanced_record_summary(self) -> dict[str, int]:
        with self._connection.cursor() as cursor:
            return BrregWorkingStore(cursor).fetch_enhanced_record_summary()

    def _claim_raw_task_batch(self, asset: BrregAssetName, command: ClaimTaskBatchCommand) -> ClaimedRawRecordBatch:
        task_type = _task_type_for_asset(asset)
        with self._connection.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            records = store.fetch_pending_raw_task_records(
                task_type=task_type,
                limit=command.batch_size,
                include_new_records=True,
                max_parallel_tasks=command.max_parallel_tasks,
                lease_seconds=command.lease_seconds,
            )
            claimed = [
                _claimed_raw_record(record, attempt)
                for record in records
                for attempt in [
                    store.create_task_attempt(
                        CreateTaskAttempt(
                            enrichment_run_id=_required_enrichment_run_id(command.enrichment_run_id, command.run_id),
                            raw_record_id=record.id,
                            task_type=task_type,
                            metadata={
                                "organization_number": record.organization_number,
                                "dagster_run_id": command.run_id,
                                **command.metadata,
                            },
                        )
                    )
                ]
            ]
        self._connection.commit()
        return ClaimedRawRecordBatch(asset=asset, records=claimed)

    def _finish_success(self, store: BrregWorkingStore, command, task_status: BrregTaskStatus) -> None:
        store.finish_task_attempt(task_attempt_id=command.task_attempt_id, status=task_status.value, error=None)
        enrichment_run_id = getattr(command, "enrichment_run_id", None)
        if enrichment_run_id is not None:
            store.increment_enrichment_run_progress(
                IncrementEnrichmentRunProgress(
                    enrichment_run_id=enrichment_run_id,
                    records_seen=1,
                    records_completed=1,
                )
            )

    def _finish_failure(self, store: BrregWorkingStore, command: SubmitTaskFailureCommand) -> None:
        store.finish_task_attempt(
            task_attempt_id=command.task_attempt_id,
            status=BrregResultStatus.FAILED.value,
            error=command.error,
            error_category=command.error_category,
            error_code=command.error_code,
            retry_strategy=command.retry_strategy,
        )
        if command.enrichment_run_id is not None:
            store.increment_enrichment_run_progress(
                IncrementEnrichmentRunProgress(
                    enrichment_run_id=command.enrichment_run_id,
                    records_seen=1,
                    records_completed=0,
                    records_failed=1,
                )
            )


def _claimed_raw_record(record: RawTaskRecord, attempt: TaskAttempt) -> ClaimedRawRecord:
    return ClaimedRawRecord(
        raw_record_id=record.id,
        organization_number=record.organization_number,
        organization_name=record.organization_name,
        website=record.website,
        raw_payload=record.raw_payload,
        task_attempt_id=attempt.id,
        attempt=attempt.attempt,
    )


def _failure_result(command: SubmitTaskFailureCommand) -> SubmitTaskResult:
    return SubmitTaskResult(
        asset=command.asset,
        status=_failure_task_status(command),
        raw_record_id=command.raw_record_id,
        task_attempt_id=command.task_attempt_id,
    )


def _failure_task_status(command: SubmitTaskFailureCommand) -> BrregTaskStatus:
    if command.retry_strategy in {"change_model_or_prompt", "manual_config", "manual_input", "not_retryable"}:
        return BrregTaskStatus.FAILED_TERMINAL
    return BrregTaskStatus.FAILED_RETRYABLE


def _failure_metadata(command: SubmitTaskFailureCommand) -> dict[str, Any]:
    return {
        **command.metadata,
        "error_category": command.error_category,
        "error_code": command.error_code,
        "retry_strategy": command.retry_strategy,
    }


def _failure_original_currency(command: SubmitTaskFailureCommand) -> str | None:
    value = command.metadata.get("original_currency")
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _required_enrichment_run_id(enrichment_run_id: str | None, run_id: str) -> str:
    return enrichment_run_id or run_id


def _task_type_for_asset(asset: BrregAssetName) -> str:
    return {
        BrregAssetName.RAW_RECORDS: "bulk_ingest",
        BrregAssetName.TRANSLATION_RESULTS: "translate",
        BrregAssetName.DOMAIN_RESULTS: "domain_results",
        BrregAssetName.CURRENCY_RESULTS: "currency_conversion",
        BrregAssetName.ENHANCED_RECORDS: "build_enhanced",
    }[asset]
