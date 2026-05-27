# BRREG Gateway-Only Database Boundary Design

Date: 2026-05-27

## Summary

Finish the `db_brreg` boundary by separating BRREG database actions from BRREG asset state views. Dagster should call `BrregAssetGateway` only for write actions such as claiming work and storing transformation outputs. Dagster should see asset state through database views, not through Python completeness helpers.

## Goals

- Remove direct `BrregWorkingStore` imports from `corpscout_dagster.brreg`.
- Keep `BrregWorkingStore` available for `db_brreg` tests and internals only.
- Add gateway methods for write actions: claim work, store transformation results/failures, retry failed work, and build enhanced records.
- Add database views that expose translation, domain, financial, and enhanced asset state for Dagster checks and UI.
- Keep external service orchestration in BRREG materialization code.
- Keep behavior, SQL, and table definitions unchanged in this step.

## Non-Goals

- Do not redesign task state tables.
- Do not change the Dagster asset graph.
- Do not change BRREG translation, domain, currency, or enhanced record behavior.
- Do not remove `enrichment_runs`; keep it as audit/debug metadata.

## Package Boundary

`corpscout_dagster.db_brreg` owns:

- SQL stores and SQL constants.
- BRREG database row DTOs.
- Gateway methods that bundle write transactions and state transitions.
- SQL views that expose asset state and row-level read models.
- Retry and failure summary queries.
- Audit run creation/progress/finalization as internal debug metadata.

`corpscout_dagster.brreg` owns:

- Dagster asset functions.
- Materialization orchestration loops.
- Calls to translation, crawl, FX, and BRREG source clients.
- Translation term extraction and enhanced payload construction.

## Gateway Write API Shape

`BrregAssetGateway` should expose action methods. It should not expose a Dagster-facing `get_asset_state()` or `assert_asset_complete()` API. Asset state belongs in SQL views.

```python
def claim_translation_batch(command: ClaimTaskBatchCommand) -> ClaimedRawRecordBatch
def submit_translation_result(command: SubmitTranslationResultCommand) -> SubmitTaskResult
def submit_translation_failure(command: SubmitTaskFailureCommand) -> SubmitTaskResult

def claim_domain_batch(command: ClaimTaskBatchCommand) -> ClaimedRawRecordBatch
def submit_domain_result(command: SubmitDomainResultCommand) -> SubmitTaskResult
def submit_domain_failure(command: SubmitTaskFailureCommand) -> SubmitTaskResult

def claim_financial_batch(command: ClaimTaskBatchCommand) -> ClaimedRawRecordBatch
def submit_financial_result(command: SubmitFinancialResultCommand) -> SubmitTaskResult
def submit_financial_failure(command: SubmitTaskFailureCommand) -> SubmitTaskResult

def build_enhanced_records() -> BuildEnhancedRecordsResult

def retry_task_failures(command: RetryTaskFailuresCommand) -> RetryTaskFailuresResult
def fetch_cached_translations(command: FetchCachedTranslationsCommand) -> dict[TranslationCacheKey, CachedTermTranslation]
def upsert_cached_translations(command: UpsertCachedTranslationsCommand) -> None
def reset_unstarted_running_tasks(command: ResetUnstartedRunningTasksCommand) -> int
```

Audit run writes can remain internal to these gateway methods. Dagster should not call explicit audit lifecycle methods unless there is a separate operational need.

The current code uses `currency` naming. The public direction should be `financial`, because currency conversion is one financial enrichment step. The first implementation may keep `currency` table names and SQL where those already exist, but the API should move toward financial naming where new methods are introduced.

## Asset State Views

Dagster asset state should be read from SQL views in `dagster_brreg`, for example:

```text
dagster_brreg.v_translation_asset_state
dagster_brreg.v_domain_asset_state
dagster_brreg.v_financial_asset_state
dagster_brreg.v_enhanced_asset_state
dagster_brreg.v_translation_asset_rows
dagster_brreg.v_domain_asset_rows
dagster_brreg.v_financial_asset_rows
dagster_brreg.v_enhanced_asset_rows
```

State views should expose columns such as:

```text
total_rows
pending_rows
running_rows
failed_retryable_rows
failed_terminal_rows
succeeded_rows
skipped_rows
missing_artifact_rows
eligible_rows
is_complete
is_blocked
```

The enhanced state view should expose how many records are currently eligible to build:

```text
current_raw_rows
translation_ready_rows
domain_ready_rows
financial_ready_rows
eligible_for_enhanced_rows
enhanced_built_rows
enhanced_missing_rows
enhanced_failed_rows
is_complete
```

## Materialization Flow

Materialization code remains in `brreg/materializations.py`, but database operations go through `BrregAssetGateway`:

```text
materialization
  -> gateway.claim_*_batch(...)
  -> external service call
  -> gateway.submit_*_result(...) or gateway.submit_*_failure(...)
```

Dagster checks and UI read the asset state views. They do not call gateway completeness methods.

Enhanced materialization is different from translation/domain/financial jobs. It should call:

```python
gateway.build_enhanced_records()
```

That method means "build enhanced rows for every currently eligible record." It should not expose a public `limit` parameter. If internal batching is needed for operational safety, that batching remains inside `db_brreg`.

## Asset Checks

Asset checks should not instantiate `BrregWorkingStore` and should not call gateway state helpers. They should query the asset state views:

- translation check reads `dagster_brreg.v_translation_asset_state`
- domain check reads `dagster_brreg.v_domain_asset_state`
- financial check reads `dagster_brreg.v_financial_asset_state`
- enhanced check reads `dagster_brreg.v_enhanced_asset_state`

Checks can attach the view columns as metadata. The completeness logic should be visible in SQL, not hidden in Python.

## Retry Jobs And Smoke

`retry_jobs.py` should call `BrregAssetGateway.retry_task_failures(...)`.

`smoke.py` should use gateway action methods directly for writes.

## Testing

Add or update tests so the boundary is enforced:

- Gateway tests for each new write method.
- View SQL tests proving asset state views expose expected columns and completeness flags.
- BRREG asset/materialization tests that assert gateway-facing behavior remains unchanged.
- Asset check tests proving checks query asset state views.
- Import-boundary test that `corpscout_dagster.brreg` production files do not import `corpscout_dagster.db_brreg.store`.

Required verification:

```bash
uv run pytest -q
uv run dagster definitions validate -w workspace.yaml
git diff --check
```

## First Implementation Scope

The first implementation should be behavior-preserving:

- add gateway command/result DTOs for the remaining write operations
- add SQL views for asset state and update asset checks to read those views
- route `brreg/materializations.py`, `retry_jobs.py`, and `smoke.py` through gateway write actions
- keep `db_brreg.store` imports only in `db_brreg` package and `tests/db_brreg`
- keep direct store tests under `tests/db_brreg` because they validate SQL implementation details
- do not change existing table semantics or Dagster asset names
