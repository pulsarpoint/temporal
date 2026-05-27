# BRREG Gateway Write API And Asset Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make BRREG Dagster code write through `db_brreg` gateway actions and read asset state from SQL views instead of `BrregWorkingStore` or Python completeness helpers.

**Architecture:** `db_brreg` owns write actions and SQL view read models. Dagster materializations claim work and submit outputs through `BrregAssetGateway`; Dagster asset checks query `dagster_brreg.v_*_asset_state` views. `BrregWorkingStore` remains an internal SQL implementation detail used only by `db_brreg` and `tests/db_brreg`.

**Tech Stack:** Python 3.12, Dagster, psycopg, PostgreSQL views, pytest, existing `dagster_brreg` schema.

---

## File Structure

Create:

- `dagster/db/migrations/000019_brreg_asset_state_views.up.sql` - asset state and row read-model views.
- `dagster/db/migrations/000019_brreg_asset_state_views.down.sql` - drops the new views.
- `dagster/src/corpscout_dagster/db_brreg/views.py` - small typed reader for asset state views.
- `dagster/tests/db_brreg/test_asset_state_views_schema.py` - migration/view SQL tests.
- `dagster/tests/db_brreg/test_views.py` - view reader tests.
- `dagster/tests/brreg/test_db_boundary.py` - import-boundary test preventing BRREG production modules from importing `db_brreg.store`.

Modify:

- `dagster/src/corpscout_dagster/db_brreg/gateway.py` - add remaining write/action APIs; remove public state/completeness API from facade exports.
- `dagster/src/corpscout_dagster/db_brreg/store.py` - add low-level SQL methods only where the gateway needs them.
- `dagster/src/corpscout_dagster/db_brreg/__init__.py` - export gateway write DTOs and view reader; stop exporting `BrregWorkingStore` for production use.
- `dagster/src/corpscout_dagster/brreg/asset_checks.py` - query asset state views through `db_brreg.views`.
- `dagster/src/corpscout_dagster/brreg/materializations.py` - stop importing `BrregWorkingStore`; use gateway write actions and view checks.
- `dagster/src/corpscout_dagster/brreg/retry_jobs.py` - use gateway retry action.
- `dagster/src/corpscout_dagster/brreg/smoke.py` - use gateway write action for smoke ingest.
- `dagster/tests/db_brreg/test_gateway.py` - update gateway tests for new write APIs and removal of public completeness helpers.
- `dagster/tests/brreg/test_asset_checks.py` - update fake SQL markers to view names.
- `dagster/tests/brreg/test_assets.py` - update materialization expectations where direct store calls are removed.
- `dagster/tests/brreg/test_retry_jobs.py` and `dagster/tests/brreg/test_smoke.py` - update for gateway-backed writes.

Do not change existing table names, Dagster asset names, or external service behavior in this plan.

### Task 1: Add Boundary Test For BRREG Production Imports

**Files:**
- Create: `dagster/tests/brreg/test_db_boundary.py`

- [ ] **Step 1: Write the failing boundary test**

Create `dagster/tests/brreg/test_db_boundary.py`:

```python
from __future__ import annotations

from pathlib import Path


BRREG_SRC = Path(__file__).parents[2] / "src" / "corpscout_dagster" / "brreg"


def test_brreg_production_modules_do_not_import_db_brreg_store() -> None:
    offenders = []
    for path in sorted(BRREG_SRC.glob("*.py")):
        text = path.read_text()
        if "corpscout_dagster.db_brreg.store" in text:
            offenders.append(path.name)

    assert offenders == []
```

- [ ] **Step 2: Run the boundary test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_db_boundary.py
```

Expected: FAIL listing current offenders such as `asset_checks.py`, `materializations.py`, `retry_jobs.py`, and `smoke.py`.

### Task 2: Add SQL Asset State Views

**Files:**
- Create: `dagster/db/migrations/000019_brreg_asset_state_views.up.sql`
- Create: `dagster/db/migrations/000019_brreg_asset_state_views.down.sql`
- Create: `dagster/tests/db_brreg/test_asset_state_views_schema.py`

- [ ] **Step 1: Write migration tests**

Create `dagster/tests/db_brreg/test_asset_state_views_schema.py`:

```python
from __future__ import annotations

from pathlib import Path


MIGRATIONS_DIR = Path(__file__).parents[2] / "db" / "migrations"
UP_SQL = MIGRATIONS_DIR / "000019_brreg_asset_state_views.up.sql"
DOWN_SQL = MIGRATIONS_DIR / "000019_brreg_asset_state_views.down.sql"


def test_asset_state_migration_creates_required_views() -> None:
    sql = UP_SQL.read_text()

    for view_name in [
        "v_raw_records_asset_state",
        "v_translation_asset_state",
        "v_domain_asset_state",
        "v_financial_asset_state",
        "v_enhanced_asset_state",
        "v_translation_asset_rows",
        "v_domain_asset_rows",
        "v_financial_asset_rows",
        "v_enhanced_asset_rows",
    ]:
        assert f"CREATE OR REPLACE VIEW dagster_brreg.{view_name}" in sql


def test_asset_state_views_expose_common_state_columns() -> None:
    sql = UP_SQL.read_text()

    for column in [
        "total_rows",
        "pending_rows",
        "running_rows",
        "failed_retryable_rows",
        "failed_terminal_rows",
        "succeeded_rows",
        "skipped_rows",
        "missing_artifact_rows",
        "eligible_rows",
        "is_complete",
        "is_blocked",
    ]:
        assert column in sql


def test_enhanced_asset_state_exposes_eligible_build_count() -> None:
    sql = UP_SQL.read_text()

    for column in [
        "translation_ready_rows",
        "domain_ready_rows",
        "financial_ready_rows",
        "eligible_for_enhanced_rows",
        "enhanced_built_rows",
        "enhanced_missing_rows",
        "enhanced_failed_rows",
    ]:
        assert column in sql


def test_asset_state_migration_down_drops_views() -> None:
    sql = DOWN_SQL.read_text()

    for view_name in [
        "v_enhanced_asset_rows",
        "v_financial_asset_rows",
        "v_domain_asset_rows",
        "v_translation_asset_rows",
        "v_enhanced_asset_state",
        "v_financial_asset_state",
        "v_domain_asset_state",
        "v_translation_asset_state",
        "v_raw_records_asset_state",
    ]:
        assert f"DROP VIEW IF EXISTS dagster_brreg.{view_name}" in sql
```

- [ ] **Step 2: Run migration tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_asset_state_views_schema.py
```

Expected: FAIL because migration files do not exist.

- [ ] **Step 3: Create the up migration**

Create `dagster/db/migrations/000019_brreg_asset_state_views.up.sql` with views based on current tables. The key state views must include the common state columns and use latest artifact rows per raw record.

Use this structure for the raw state view:

```sql
CREATE OR REPLACE VIEW dagster_brreg.v_raw_records_asset_state AS
SELECT
  count(*)::int AS total_rows,
  count(*) FILTER (WHERE is_current)::int AS current_rows,
  count(*) FILTER (WHERE NOT is_current)::int AS not_current_rows,
  (count(*) FILTER (WHERE is_current) > 0) AS is_complete
FROM dagster_brreg.raw_records;
```

Use this pattern for translation, grouping by model and prompt version:

```sql
CREATE OR REPLACE VIEW dagster_brreg.v_translation_asset_rows AS
WITH current_raw AS (
  SELECT id, organization_number, organization_name
  FROM dagster_brreg.raw_records
  WHERE is_current = true
),
latest_result AS (
  SELECT DISTINCT ON (raw_record_id, model, prompt_version)
    raw_record_id,
    model,
    prompt_version,
    status,
    error,
    created_at
  FROM dagster_brreg.translation_results
  ORDER BY raw_record_id, model, prompt_version, created_at DESC
)
SELECT
  cr.id AS raw_record_id,
  cr.organization_number,
  cr.organization_name,
  lr.model,
  lr.prompt_version,
  rts.status AS task_status,
  rts.next_retry_at,
  rts.lease_until,
  rts.error_category,
  rts.error_code,
  rts.retry_strategy,
  lr.status AS artifact_status,
  lr.error AS artifact_error,
  lr.created_at AS artifact_created_at
FROM current_raw cr
LEFT JOIN dagster_brreg.raw_record_task_states rts
  ON rts.raw_record_id = cr.id
 AND rts.task_type = 'translate'
LEFT JOIN latest_result lr
  ON lr.raw_record_id = cr.id;

CREATE OR REPLACE VIEW dagster_brreg.v_translation_asset_state AS
SELECT
  coalesce(model, '') AS model,
  coalesce(prompt_version, '') AS prompt_version,
  count(*)::int AS total_rows,
  count(*) FILTER (WHERE task_status IS NULL)::int AS no_state_rows,
  count(*) FILTER (WHERE task_status = 'pending')::int AS pending_rows,
  count(*) FILTER (WHERE task_status = 'running')::int AS running_rows,
  count(*) FILTER (WHERE task_status = 'failed_retryable')::int AS failed_retryable_rows,
  count(*) FILTER (WHERE task_status = 'failed_terminal')::int AS failed_terminal_rows,
  count(*) FILTER (WHERE task_status = 'succeeded')::int AS succeeded_rows,
  count(*) FILTER (WHERE task_status = 'skipped')::int AS skipped_rows,
  count(*) FILTER (WHERE artifact_status IS NULL)::int AS missing_artifact_rows,
  count(*) FILTER (
    WHERE task_status IS NULL
       OR task_status = 'pending'
       OR (task_status = 'failed_retryable' AND coalesce(next_retry_at <= now(), true))
       OR (task_status = 'running' AND coalesce(lease_until, now() - interval '1 second') <= now())
  )::int AS eligible_rows,
  bool_and(coalesce(artifact_status IN ('succeeded', 'skipped'), false)) AS is_complete,
  bool_or(task_status = 'failed_terminal') AS is_blocked
FROM dagster_brreg.v_translation_asset_rows
GROUP BY coalesce(model, ''), coalesce(prompt_version, '');
```

Use the same shape for domain and financial/currency state. `v_financial_asset_rows` should read `currency_results` and `task_type = 'currency_conversion'` while exposing financial names in the view.

Use `mv_brreg_enhanced_ready_records` for `eligible_for_enhanced_rows` in `v_enhanced_asset_state`.

- [ ] **Step 4: Create the down migration**

Create `dagster/db/migrations/000019_brreg_asset_state_views.down.sql`:

```sql
DROP VIEW IF EXISTS dagster_brreg.v_enhanced_asset_rows;
DROP VIEW IF EXISTS dagster_brreg.v_financial_asset_rows;
DROP VIEW IF EXISTS dagster_brreg.v_domain_asset_rows;
DROP VIEW IF EXISTS dagster_brreg.v_translation_asset_rows;
DROP VIEW IF EXISTS dagster_brreg.v_enhanced_asset_state;
DROP VIEW IF EXISTS dagster_brreg.v_financial_asset_state;
DROP VIEW IF EXISTS dagster_brreg.v_domain_asset_state;
DROP VIEW IF EXISTS dagster_brreg.v_translation_asset_state;
DROP VIEW IF EXISTS dagster_brreg.v_raw_records_asset_state;
```

- [ ] **Step 5: Run migration tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_asset_state_views_schema.py
```

Expected: PASS.

### Task 3: Add Typed View Reader For Asset Checks

**Files:**
- Create: `dagster/src/corpscout_dagster/db_brreg/views.py`
- Create: `dagster/tests/db_brreg/test_views.py`
- Modify: `dagster/src/corpscout_dagster/db_brreg/__init__.py`

- [ ] **Step 1: Write view reader tests**

Create `dagster/tests/db_brreg/test_views.py`:

```python
from __future__ import annotations

from corpscout_dagster.db_brreg.views import BrregAssetStateViewReader


class FakeCursor:
    def __init__(self, row: tuple) -> None:
        self.row = row
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))

    def fetchone(self):
        return self.row


def test_view_reader_fetches_translation_state_for_model_and_prompt() -> None:
    cursor = FakeCursor((1000, 0, 0, 0, 0, 950, 50, 0, 0, True, False))
    reader = BrregAssetStateViewReader(cursor)

    state = reader.fetch_translation_state(model="qwen3:6b", prompt_version="v1")

    assert state.total_rows == 1000
    assert state.succeeded_rows == 950
    assert state.skipped_rows == 50
    assert state.is_complete is True
    sql, params = cursor.calls[0]
    assert "dagster_brreg.v_translation_asset_state" in sql
    assert params == {"model": "qwen3:6b", "prompt_version": "v1"}


def test_view_reader_fetches_domain_state() -> None:
    cursor = FakeCursor((1000, 0, 0, 0, 0, 900, 90, 10, 0, False, False))
    reader = BrregAssetStateViewReader(cursor)

    state = reader.fetch_domain_state()

    assert state.total_rows == 1000
    assert state.missing_artifact_rows == 10
    assert state.is_complete is False
    assert "dagster_brreg.v_domain_asset_state" in cursor.calls[0][0]
```

- [ ] **Step 2: Run view reader tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_views.py
```

Expected: FAIL because `corpscout_dagster.db_brreg.views` does not exist.

- [ ] **Step 3: Implement the view reader**

Create `dagster/src/corpscout_dagster/db_brreg/views.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class Cursor(Protocol):
    def execute(self, sql: str, params: dict) -> object:
        ...

    def fetchone(self):
        ...


@dataclass(frozen=True)
class BrregAssetStateView:
    total_rows: int
    pending_rows: int
    running_rows: int
    failed_retryable_rows: int
    failed_terminal_rows: int
    succeeded_rows: int
    skipped_rows: int
    missing_artifact_rows: int
    eligible_rows: int
    is_complete: bool
    is_blocked: bool


class BrregAssetStateViewReader:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def fetch_translation_state(self, *, model: str, prompt_version: str) -> BrregAssetStateView:
        self._cursor.execute(
            FETCH_TRANSLATION_ASSET_STATE_SQL,
            {"model": model, "prompt_version": prompt_version},
        )
        return _asset_state_from_row(self._cursor.fetchone())

    def fetch_domain_state(self) -> BrregAssetStateView:
        self._cursor.execute(FETCH_DOMAIN_ASSET_STATE_SQL, {})
        return _asset_state_from_row(self._cursor.fetchone())

    def fetch_financial_state(self) -> BrregAssetStateView:
        self._cursor.execute(FETCH_FINANCIAL_ASSET_STATE_SQL, {})
        return _asset_state_from_row(self._cursor.fetchone())

    def fetch_enhanced_state(self) -> BrregAssetStateView:
        self._cursor.execute(FETCH_ENHANCED_ASSET_STATE_SQL, {})
        return _asset_state_from_row(self._cursor.fetchone())


def _asset_state_from_row(row) -> BrregAssetStateView:
    if row is None:
        return BrregAssetStateView(0, 0, 0, 0, 0, 0, 0, 0, 0, False, False)
    return BrregAssetStateView(
        total_rows=int(row[0] or 0),
        pending_rows=int(row[1] or 0),
        running_rows=int(row[2] or 0),
        failed_retryable_rows=int(row[3] or 0),
        failed_terminal_rows=int(row[4] or 0),
        succeeded_rows=int(row[5] or 0),
        skipped_rows=int(row[6] or 0),
        missing_artifact_rows=int(row[7] or 0),
        eligible_rows=int(row[8] or 0),
        is_complete=bool(row[9]),
        is_blocked=bool(row[10]),
    )


FETCH_TRANSLATION_ASSET_STATE_SQL = """
SELECT
  total_rows,
  pending_rows,
  running_rows,
  failed_retryable_rows,
  failed_terminal_rows,
  succeeded_rows,
  skipped_rows,
  missing_artifact_rows,
  eligible_rows,
  is_complete,
  is_blocked
FROM dagster_brreg.v_translation_asset_state
WHERE model = %(model)s
  AND prompt_version = %(prompt_version)s
"""

FETCH_DOMAIN_ASSET_STATE_SQL = """
SELECT
  total_rows,
  pending_rows,
  running_rows,
  failed_retryable_rows,
  failed_terminal_rows,
  succeeded_rows,
  skipped_rows,
  missing_artifact_rows,
  eligible_rows,
  is_complete,
  is_blocked
FROM dagster_brreg.v_domain_asset_state
"""

FETCH_FINANCIAL_ASSET_STATE_SQL = FETCH_DOMAIN_ASSET_STATE_SQL.replace(
    "v_domain_asset_state",
    "v_financial_asset_state",
)
FETCH_ENHANCED_ASSET_STATE_SQL = FETCH_DOMAIN_ASSET_STATE_SQL.replace(
    "v_domain_asset_state",
    "v_enhanced_asset_state",
)
```

- [ ] **Step 4: Export view reader types**

Modify `dagster/src/corpscout_dagster/db_brreg/__init__.py` to export:

```python
from corpscout_dagster.db_brreg.views import BrregAssetStateView, BrregAssetStateViewReader
```

and add `"BrregAssetStateView"` and `"BrregAssetStateViewReader"` to `__all__`.

- [ ] **Step 5: Run view tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_views.py
```

Expected: PASS.

### Task 4: Update Asset Checks To Read Views

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/asset_checks.py`
- Modify: `dagster/tests/brreg/test_asset_checks.py`

- [ ] **Step 1: Update tests to expect view SQL**

In `dagster/tests/brreg/test_asset_checks.py`, update fake markers from store helper names to view names:

```python
context = _context({"v_translation_asset_state": (1000, 0, 0, 0, 0, 950, 50, 0, 0, True, False)})
```

For domain, financial/currency, and enhanced checks, use:

```python
"v_domain_asset_state"
"v_financial_asset_state"
"v_enhanced_asset_state"
```

For raw records, add a direct raw view marker:

```python
"v_raw_records_asset_state": (1000, 1000, 0, True)
```

- [ ] **Step 2: Run asset check tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_asset_checks.py
```

Expected: FAIL because `asset_checks.py` still queries store summary SQL.

- [ ] **Step 3: Refactor `asset_checks.py`**

Remove:

```python
from corpscout_dagster.db_brreg.store import BrregWorkingStore
```

Add:

```python
from corpscout_dagster.db_brreg.views import BrregAssetStateView, BrregAssetStateViewReader
```

Each check should use `BrregAssetStateViewReader(cursor)` to fetch state from views. For example:

```python
def evaluate_brreg_translation_results_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    translation_service = context.resources.translation_service
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            state = BrregAssetStateViewReader(cursor).fetch_translation_state(
                model=translation_service.model,
                prompt_version=translation_service.prompt_version,
            )
    return _asset_state_check_result(
        state,
        metadata_prefix="live_translation_results",
        table_name="dagster_brreg.v_translation_asset_state",
        extra_metadata={
            "live_translation_model": translation_service.model,
            "live_translation_prompt_version": translation_service.prompt_version,
        },
    )
```

Add a helper:

```python
def _asset_state_check_result(
    state: BrregAssetStateView,
    *,
    metadata_prefix: str,
    table_name: str,
    extra_metadata: dict | None = None,
) -> AssetCheckResult:
    metadata = {
        f"{metadata_prefix}_total": state.total_rows,
        f"{metadata_prefix}_succeeded": state.succeeded_rows,
        f"{metadata_prefix}_skipped": state.skipped_rows,
        f"{metadata_prefix}_failed_retryable": state.failed_retryable_rows,
        f"{metadata_prefix}_failed_terminal": state.failed_terminal_rows,
        f"{metadata_prefix}_missing": state.missing_artifact_rows,
        f"{metadata_prefix}_eligible": state.eligible_rows,
    }
    metadata.update(extra_metadata or {})
    return AssetCheckResult(
        passed=state.total_rows > 0 and state.is_complete and not state.is_blocked,
        metadata=metadata,
        description=(
            f"{table_name} is complete."
            if state.total_rows > 0 and state.is_complete and not state.is_blocked
            else f"{table_name} is incomplete: missing={state.missing_artifact_rows}, blocked={state.is_blocked}."
        ),
    )
```

- [ ] **Step 4: Run asset check tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_asset_checks.py
```

Expected: PASS.

### Task 5: Move Remaining Write Operations Behind Gateway

**Files:**
- Modify: `dagster/src/corpscout_dagster/db_brreg/gateway.py`
- Modify: `dagster/src/corpscout_dagster/db_brreg/store.py`
- Modify: `dagster/tests/db_brreg/test_gateway.py`

- [ ] **Step 1: Add failing gateway tests for retry, cache, and raw ingest setup**

In `dagster/tests/db_brreg/test_gateway.py`, add tests for:

```python
def test_gateway_retries_task_failures() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    result = gateway.retry_task_failures(
        RetryTaskFailuresCommand(task_type="translate", error_category="invalid_llm_output", limit=5000)
    )

    assert result.retried_rows == 1
    assert any("retry_rows AS" in sql for sql, _ in connection.cursor_instance.calls)


def test_gateway_fetches_and_upserts_translation_cache() -> None:
    connection = FakeConnection()
    gateway = BrregAssetGateway(connection)

    gateway.upsert_cached_translations(UpsertCachedTranslationsCommand(rows=[]))
    result = gateway.fetch_cached_translations(
        FetchCachedTranslationsCommand(keys=[], model="qwen3:6b", prompt_version="v1")
    )

    assert result == {}
    assert any("translation_cache" in sql for sql, _ in connection.cursor_instance.calls)
```

Add the command/result DTO imports expected by those tests.

- [ ] **Step 2: Run gateway tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_gateway.py
```

Expected: FAIL because the command DTOs and methods do not exist.

- [ ] **Step 3: Implement gateway write methods**

In `dagster/src/corpscout_dagster/db_brreg/gateway.py`, add dataclasses:

```python
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
```

Add gateway methods:

```python
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
```

- [ ] **Step 4: Export gateway DTOs**

Update `dagster/src/corpscout_dagster/db_brreg/__init__.py` to export the new command/result classes.

- [ ] **Step 5: Run gateway tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_gateway.py
```

Expected: PASS.

### Task 6: Route Retry Jobs And Smoke Through Gateway

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/retry_jobs.py`
- Modify: `dagster/src/corpscout_dagster/brreg/smoke.py`
- Modify: `dagster/tests/brreg/test_retry_jobs.py`
- Modify: `dagster/tests/brreg/test_smoke.py`

- [ ] **Step 1: Update retry jobs**

Replace `BrregWorkingStore` use in `retry_jobs.py` with:

```python
from corpscout_dagster.db_brreg import BrregAssetGateway, RetryTaskFailuresCommand
```

and:

```python
result = BrregAssetGateway(conn).retry_task_failures(
    RetryTaskFailuresCommand(
        task_type=task_type,
        error_category=error_category,
        limit=limit,
    )
)
retried_rows = result.retried_rows
```

- [ ] **Step 2: Update smoke writes**

Route smoke raw ingest through gateway methods. If creating a full raw ingest context is larger than this task, add a narrow gateway method:

```python
def smoke_ingest_raw_record(command: SmokeIngestRawRecordCommand) -> None
```

that creates the audit run, bulk snapshot, and raw row inside `db_brreg`.

- [ ] **Step 3: Run retry and smoke tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_retry_jobs.py tests/brreg/test_smoke.py
```

Expected: PASS.

### Task 7: Remove Store Access From Materializations

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/materializations.py`
- Modify: `dagster/src/corpscout_dagster/db_brreg/gateway.py`
- Modify: `dagster/tests/brreg/test_assets.py`
- Modify: `dagster/tests/db_brreg/test_gateway.py`

- [ ] **Step 1: Add gateway methods for remaining materialization DB actions**

Add gateway methods for the remaining store use in `materializations.py`:

```python
def begin_raw_ingest(command: BeginRawIngestCommand) -> RawIngestContext
def finish_action_run(command: FinishActionRunCommand) -> None
def reconcile_translation_tasks(command: ReconcileTranslationTasksCommand) -> ReconcileTranslationTasksResult
def get_task_failure_summary(task_type: str) -> dict[str, int]
def reset_unstarted_running_tasks(command: ResetUnstartedRunningTasksCommand) -> int
```

These are write or audit/debug actions, not asset-state APIs. They should not be used by asset checks.

- [ ] **Step 2: Replace materialization store imports**

Remove `BrregWorkingStore` and store command imports from `materializations.py`. Replace with gateway DTOs and methods.

- [ ] **Step 3: Remove Python completeness assertions from materializations**

Remove calls to:

```python
gateway.assert_asset_complete(...)
```

Materialization should fail for failures in the current action run, while live completeness is shown by asset state views and asset checks.

- [ ] **Step 4: Keep run metadata sourced from view/check-compatible summaries**

If materialization metadata still needs live counts, fetch them through `BrregAssetStateViewReader` or directly from gateway action summaries, not through `BrregWorkingStore`.

- [ ] **Step 5: Run materialization tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_assets.py tests/db_brreg/test_gateway.py
```

Expected: PASS after updating tests for removed `assert_asset_complete` behavior.

### Task 8: Enhanced Build Action Without Public Limit

**Files:**
- Modify: `dagster/src/corpscout_dagster/db_brreg/gateway.py`
- Modify: `dagster/src/corpscout_dagster/brreg/materializations.py`
- Modify: `dagster/tests/db_brreg/test_gateway.py`
- Modify: `dagster/tests/brreg/test_assets.py`

- [ ] **Step 1: Add gateway result type**

Add:

```python
@dataclass(frozen=True)
class BuildEnhancedRecordsResult:
    eligible_rows: int
    built_rows: int
    failed_rows: int
```

- [ ] **Step 2: Implement `build_enhanced_records` as all eligible rows**

Implement:

```python
def build_enhanced_records(self, command: BuildEnhancedRecordsCommand) -> BuildEnhancedRecordsResult:
    ...
```

The command may include `run_id`, metadata, and a payload builder callback, but it must not expose a public `limit`. Internal batching can use `DEFAULT_ENHANCED_RECORD_BATCH_SIZE` or a private constructor parameter.

- [ ] **Step 3: Update enhanced materialization**

Change `materialize_brreg_enhanced_records` to call `gateway.build_enhanced_records(...)` instead of claiming one externally configured batch.

- [ ] **Step 4: Run enhanced tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_assets.py::test_materialize_brreg_enhanced_records_writes_ready_records tests/db_brreg/test_gateway.py
```

Expected: PASS. Use the actual enhanced test name if it differs; locate it with `rg "enhanced_records" tests/brreg/test_assets.py`.

### Task 9: Enforce Boundary And Verify Everything

**Files:**
- Modify only files required by failing checks.

- [ ] **Step 1: Run boundary test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_db_boundary.py
```

Expected: PASS.

- [ ] **Step 2: Run all database package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg
```

Expected: PASS.

- [ ] **Step 3: Run full tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q
```

Expected: all tests pass.

- [ ] **Step 4: Validate Dagster definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run dagster definitions validate -w workspace.yaml
```

Expected output includes:

```text
Validation successful for code location corpscout_dagster.
All code locations passed validation.
```

- [ ] **Step 5: Check import boundary with ripgrep**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
rg "corpscout_dagster\\.db_brreg\\.store" dagster/src/corpscout_dagster/brreg
```

Expected: no output.

- [ ] **Step 6: Check whitespace**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git diff --check
```

Expected: no output.

### Task 10: Commit Gateway/View Boundary Implementation

**Files:**
- All files changed by Tasks 1-9.

- [ ] **Step 1: Review changed files**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git status --short
git diff --stat
```

Expected: changes are limited to `db_brreg`, BRREG materialization/check/retry/smoke callers, migration files, and tests.

- [ ] **Step 2: Commit implementation**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/db/migrations dagster/src/corpscout_dagster/db_brreg dagster/src/corpscout_dagster/brreg dagster/tests/db_brreg dagster/tests/brreg
git commit -m "Use BRREG gateway writes and asset state views"
```

Expected: commit succeeds on `main`, because the user explicitly approved working directly on `main`.

## Self-Review

Spec coverage:

- Gateway write API is covered by Tasks 5-8.
- SQL asset state views are covered by Tasks 2-4.
- Removal of `BrregWorkingStore` from BRREG production modules is covered by Tasks 1, 6, 7, and 9.
- Enhanced build without public limit is covered by Task 8.
- Verification is covered by Task 9.

Filler-token scan:

- This plan contains concrete file paths, commands, expected outcomes, and code snippets for each implementation task.

Type consistency:

- Public write API remains under `BrregAssetGateway`.
- Asset-state read API is `BrregAssetStateViewReader`, which reads SQL views only.
- `BrregWorkingStore` remains in `db_brreg.store` and direct store tests remain under `tests/db_brreg`.
