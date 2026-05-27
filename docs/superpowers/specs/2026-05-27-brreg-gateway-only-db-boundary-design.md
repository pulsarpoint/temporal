# BRREG Gateway-Only Database Boundary Design

Date: 2026-05-27

## Summary

Finish the `db_brreg` boundary by making BRREG Dagster code use `BrregAssetGateway` and small database package APIs instead of importing `BrregWorkingStore` directly. `BrregWorkingStore` remains inside `db_brreg` as the SQL implementation detail. Dagster assets and checks interact with typed gateway methods and live asset state objects.

## Goals

- Remove direct `BrregWorkingStore` imports from `corpscout_dagster.brreg`.
- Keep `BrregWorkingStore` available for `db_brreg` tests and internals only.
- Add gateway methods for audit run lifecycle, translation cache access, task failure summaries, retry operations, and live asset state reads needed by Dagster.
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
- Gateway methods that bundle transactions and state transitions.
- Live table state and completeness checks.
- Retry and failure summary queries.
- Audit run creation/progress/finalization.

`corpscout_dagster.brreg` owns:

- Dagster asset functions.
- Materialization orchestration loops.
- Calls to translation, crawl, FX, and BRREG source clients.
- Translation term extraction and enhanced payload construction.

## Public API Shape

Extend `BrregAssetGateway` with methods that cover the remaining direct store usage:

```python
def start_audit_run(command: StartAuditRunCommand) -> StartAuditRunResult
def finish_audit_run(command: FinishAuditRunCommand) -> None
def get_task_summary(task_type: str) -> dict[str, int]
def get_task_failure_summary(task_type: str) -> dict[str, int]
def retry_task_failures(command: RetryTaskFailuresCommand) -> RetryTaskFailuresResult
def reconcile_translation_tasks(command: ReconcileTranslationTasksCommand) -> ReconcileTranslationTasksResult
def get_translation_artifact_summary(command: TranslationArtifactSummaryCommand) -> dict[str, int]
def get_domain_artifact_summary() -> dict[str, int]
def get_currency_artifact_summary() -> dict[str, int]
def get_enhanced_artifact_summary() -> dict[str, int]
def fetch_cached_translations(command: FetchCachedTranslationsCommand) -> dict[TranslationCacheKey, CachedTermTranslation]
def upsert_cached_translations(command: UpsertCachedTranslationsCommand) -> None
def reset_unstarted_running_tasks(command: ResetUnstartedRunningTasksCommand) -> int
```

Use typed dataclasses where a method has more than two parameters or represents a domain action. Use direct parameters for small read-only summary helpers only when the signature stays obvious.

## Materialization Flow

Materialization code remains in `brreg/materializations.py`, but database operations go through `BrregAssetGateway`:

```text
materialization
  -> gateway.start_audit_run(...)
  -> gateway.claim_*_batch(...)
  -> external service call
  -> gateway.submit_*_result(...) or gateway.submit_*_failure(...)
  -> gateway.get_*_summary(...)
  -> gateway.finish_audit_run(...)
  -> gateway.assert_asset_complete(...)
```

This keeps orchestration readable while hiding SQL and transaction details.

## Asset Checks

Asset checks should not instantiate `BrregWorkingStore`. They should read live state through gateway methods:

- raw/translation check uses `get_asset_state(TRANSLATION_RESULTS)` and translation artifact summary.
- domain check uses `get_asset_state(DOMAIN_RESULTS)` and domain artifact summary.
- currency check uses `get_asset_state(CURRENCY_RESULTS)` and currency artifact summary.
- enhanced check uses `get_asset_state(ENHANCED_RECORDS)` and enhanced artifact summary.

Checks can still attach detailed metadata, but the gateway is the source of all database-derived values.

## Retry Jobs And Smoke

`retry_jobs.py` should call `BrregAssetGateway.retry_task_failures(...)`.

`smoke.py` can either:

- use `BrregAssetGateway.ingest_raw_records(...)` plus audit helpers, or
- use a small `db_brreg` smoke helper if direct smoke setup needs a narrower API.

The preferred first implementation is to use gateway methods directly.

## Testing

Add or update tests so the boundary is enforced:

- Gateway tests for each new method.
- BRREG asset/materialization tests that assert gateway-facing behavior remains unchanged.
- Asset check tests proving checks can run without importing `BrregWorkingStore` from BRREG modules.
- Import-boundary test that `corpscout_dagster.brreg` production files do not import `corpscout_dagster.db_brreg.store`.

Required verification:

```bash
uv run pytest -q
uv run dagster definitions validate -w workspace.yaml
git diff --check
```

## First Implementation Scope

The first implementation should be behavior-preserving:

- add gateway command/result DTOs for the remaining database operations
- route `brreg/materializations.py`, `asset_checks.py`, `retry_jobs.py`, and `smoke.py` through the gateway
- keep `db_brreg.store` imports only in `db_brreg` package and `tests/db_brreg`
- keep direct store tests under `tests/db_brreg` because they validate SQL implementation details
- do not change schemas, SQL semantics, or asset names
