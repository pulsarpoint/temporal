# BRREG DB Package Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move BRREG database access and database-facing DTOs behind a Dagster-independent `corpscout_dagster.db_brreg` package.

**Architecture:** `corpscout_dagster.db_brreg` owns SQL stores, database row DTOs, the asset gateway, task state, leases, transactions, artifact writes, and live completeness checks. `corpscout_dagster.brreg` keeps orchestration, BRREG source parsing, external service calls, FX, translation term extraction, and enhanced payload construction. Dagster imports the database facade from `db_brreg`, not SQL implementation details from `brreg`.

**Tech Stack:** Python 3.12, Dagster, psycopg, pytest, existing BRREG working-store SQL.

---

## File Structure

Create:

- `dagster/src/corpscout_dagster/db_brreg/__init__.py` - public facade exports for BRREG database APIs.
- `dagster/src/corpscout_dagster/db_brreg/gateway.py` - moved `BrregAssetGateway`, typed asset enums, command/result DTOs, and typed errors.
- `dagster/src/corpscout_dagster/db_brreg/store.py` - moved `BrregWorkingStore`, SQL constants, store command/result DTOs.
- `dagster/src/corpscout_dagster/db_brreg/models.py` - database write row DTOs previously mixed into `brreg.models`.
- `dagster/src/corpscout_dagster/db_brreg/writer.py` - moved Corpscout BRREG raw input table writer.
- `dagster/tests/db_brreg/test_public_imports.py` - import boundary tests for the new facade.
- `dagster/tests/db_brreg/test_gateway.py` - moved gateway tests.
- `dagster/tests/db_brreg/test_store.py` - moved working-store tests.
- `dagster/tests/db_brreg/test_store_schema.py` - moved working-store schema tests.
- `dagster/tests/db_brreg/test_writer.py` - moved BRREG raw input writer tests.

Modify:

- `dagster/src/corpscout_dagster/brreg/__init__.py` - stop exporting database facade classes directly from `brreg`.
- `dagster/src/corpscout_dagster/brreg/models.py` - keep BRREG source parsing model, import DB row DTOs from `db_brreg.models` for `to_corpscout_row()` and `to_working_row()`.
- `dagster/src/corpscout_dagster/brreg/materializations.py` - import gateway/store DTOs from `db_brreg`.
- `dagster/src/corpscout_dagster/brreg/asset_checks.py` - import store from `db_brreg`.
- `dagster/src/corpscout_dagster/brreg/retry_jobs.py` - import store from `db_brreg`.
- `dagster/src/corpscout_dagster/brreg/smoke.py` - import store and DB rows from `db_brreg`.
- `dagster/src/corpscout_dagster/brreg/crawl_service.py` - import `RawTaskRecord` from `db_brreg.store`.
- `dagster/src/corpscout_dagster/brreg/enhanced_payload.py` - import `DomainResultCandidateRow` and `RawTaskRecord` from `db_brreg.store`.
- `dagster/src/corpscout_dagster/brreg/source.py` - keep importing `BrregRawRecord` from `brreg.models`.
- BRREG tests under `dagster/tests/brreg/` - update remaining imports from old DB locations.

Do not change SQL behavior, table names, asset names, or materialization behavior in this plan.

### Task 1: Add Import Boundary Tests

**Files:**
- Create: `dagster/tests/db_brreg/test_public_imports.py`
- Modify: none
- Test: `dagster/tests/db_brreg/test_public_imports.py`

- [ ] **Step 1: Write the failing public facade test**

Create `dagster/tests/db_brreg/test_public_imports.py`:

```python
from __future__ import annotations


def test_db_brreg_public_facade_exports_database_gateway_types() -> None:
    from corpscout_dagster.db_brreg import (
        AssetBlockedByActiveTasksError,
        AssetIncompleteError,
        BrregAssetGateway,
        BrregAssetName,
        BrregAssetState,
        BrregTaskStatus,
    )

    assert BrregAssetName.TRANSLATION_RESULTS.value == "translation_results"
    assert BrregTaskStatus.FAILED_TERMINAL.value == "failed_terminal"
    assert BrregAssetGateway.__name__ == "BrregAssetGateway"
    assert BrregAssetState.__name__ == "BrregAssetState"
    assert issubclass(AssetIncompleteError, RuntimeError)
    assert issubclass(AssetBlockedByActiveTasksError, RuntimeError)
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_public_imports.py
```

Expected: FAIL with `ModuleNotFoundError: No module named 'corpscout_dagster.db_brreg'`.

- [ ] **Step 3: Commit the red test**

Only commit this test if the current worktree is intentionally being committed task-by-task. If the existing uncommitted gateway refactor must remain grouped, keep the test uncommitted until the final task.

```bash
git add dagster/tests/db_brreg/test_public_imports.py
git commit -m "test: define BRREG db package facade"
```

### Task 2: Create `db_brreg` Package And Move Database DTOs

**Files:**
- Create: `dagster/src/corpscout_dagster/db_brreg/__init__.py`
- Create: `dagster/src/corpscout_dagster/db_brreg/models.py`
- Modify: `dagster/src/corpscout_dagster/brreg/models.py`
- Test: `dagster/tests/db_brreg/test_public_imports.py`

- [ ] **Step 1: Create the package directory**

Run:

```bash
mkdir -p /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster/src/corpscout_dagster/db_brreg
```

- [ ] **Step 2: Move database row DTOs into `db_brreg.models`**

Create `dagster/src/corpscout_dagster/db_brreg/models.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CorpscoutBrregRawInputRow:
    source_native_id: str
    organization_number: str
    organization_name: str
    registration_status: str
    website: str | None
    country_iso2: str
    raw_payload: dict[str, Any]
    payload_hash: str
    run_id: str


@dataclass(frozen=True)
class BrregWorkingRawRecordRow:
    source_native_id: str
    organization_number: str
    organization_name: str
    registration_status: str
    website: str | None
    country_iso2: str
    raw_payload: dict[str, Any]
    payload_hash: str
    metadata: dict[str, Any]
```

- [ ] **Step 3: Update `brreg.models` to use the moved DTOs**

In `dagster/src/corpscout_dagster/brreg/models.py`, remove the local `CorpscoutBrregRawInputRow` and `BrregWorkingRawRecordRow` dataclasses, and add:

```python
from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow, CorpscoutBrregRawInputRow
```

Keep `BrregRawRecord`, `_blank_to_none()`, and `_payload_hash()` in `brreg.models`.

- [ ] **Step 4: Add temporary facade exports for DB row DTOs**

Create `dagster/src/corpscout_dagster/db_brreg/__init__.py` with row DTO exports first:

```python
from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow, CorpscoutBrregRawInputRow

__all__ = [
    "BrregWorkingRawRecordRow",
    "CorpscoutBrregRawInputRow",
]
```

- [ ] **Step 5: Run model-level tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_source.py tests/brreg/test_writer.py
```

Expected: source tests pass; writer tests may still pass through old imports until writer is moved.

### Task 3: Move Store, Gateway, And Writer Into `db_brreg`

**Files:**
- Move: `dagster/src/corpscout_dagster/brreg/asset_gateway.py` to `dagster/src/corpscout_dagster/db_brreg/gateway.py`
- Move: `dagster/src/corpscout_dagster/brreg/working_store.py` to `dagster/src/corpscout_dagster/db_brreg/store.py`
- Move: `dagster/src/corpscout_dagster/brreg/writer.py` to `dagster/src/corpscout_dagster/db_brreg/writer.py`
- Modify: `dagster/src/corpscout_dagster/db_brreg/gateway.py`
- Modify: `dagster/src/corpscout_dagster/db_brreg/store.py`
- Modify: `dagster/src/corpscout_dagster/db_brreg/writer.py`
- Modify: `dagster/src/corpscout_dagster/db_brreg/__init__.py`
- Test: `dagster/tests/db_brreg/test_public_imports.py`

- [ ] **Step 1: Move files with git**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git mv dagster/src/corpscout_dagster/brreg/asset_gateway.py dagster/src/corpscout_dagster/db_brreg/gateway.py
git mv dagster/src/corpscout_dagster/brreg/working_store.py dagster/src/corpscout_dagster/db_brreg/store.py
git mv dagster/src/corpscout_dagster/brreg/writer.py dagster/src/corpscout_dagster/db_brreg/writer.py
```

- [ ] **Step 2: Update imports inside moved files**

In `dagster/src/corpscout_dagster/db_brreg/gateway.py`, replace:

```python
from corpscout_dagster.brreg.models import BrregWorkingRawRecordRow
from corpscout_dagster.brreg.working_store import (
```

with:

```python
from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow
from corpscout_dagster.db_brreg.store import (
```

In `dagster/src/corpscout_dagster/db_brreg/store.py`, replace:

```python
from corpscout_dagster.brreg.models import BrregWorkingRawRecordRow
```

with:

```python
from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow
```

Keep:

```python
from corpscout_dagster.brreg.translation_terms import CachedTermTranslation, TranslationCacheKey
```

because translation term extraction remains in the BRREG pipeline package.

In `dagster/src/corpscout_dagster/db_brreg/writer.py`, replace:

```python
from corpscout_dagster.brreg.models import CorpscoutBrregRawInputRow
```

with:

```python
from corpscout_dagster.db_brreg.models import CorpscoutBrregRawInputRow
```

- [ ] **Step 3: Publish facade exports**

Replace `dagster/src/corpscout_dagster/db_brreg/__init__.py` with:

```python
from corpscout_dagster.db_brreg.gateway import (
    AssetBlockedByActiveTasksError,
    AssetIncompleteError,
    BrregAssetGateway,
    BrregAssetName,
    BrregAssetState,
    BrregTaskStatus,
)
from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow, CorpscoutBrregRawInputRow
from corpscout_dagster.db_brreg.store import (
    BrregWorkingStore,
    DomainResultCandidateRow,
    EnhancedBuildRecord,
    RawTaskRecord,
    TaskAttempt,
)
from corpscout_dagster.db_brreg.writer import BrregRawInputWriter

__all__ = [
    "AssetBlockedByActiveTasksError",
    "AssetIncompleteError",
    "BrregAssetGateway",
    "BrregAssetName",
    "BrregAssetState",
    "BrregRawInputWriter",
    "BrregTaskStatus",
    "BrregWorkingRawRecordRow",
    "BrregWorkingStore",
    "CorpscoutBrregRawInputRow",
    "DomainResultCandidateRow",
    "EnhancedBuildRecord",
    "RawTaskRecord",
    "TaskAttempt",
]
```

- [ ] **Step 4: Run the import boundary test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg/test_public_imports.py
```

Expected: PASS.

### Task 4: Update Production Imports

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/__init__.py`
- Modify: `dagster/src/corpscout_dagster/brreg/materializations.py`
- Modify: `dagster/src/corpscout_dagster/brreg/asset_checks.py`
- Modify: `dagster/src/corpscout_dagster/brreg/retry_jobs.py`
- Modify: `dagster/src/corpscout_dagster/brreg/smoke.py`
- Modify: `dagster/src/corpscout_dagster/brreg/crawl_service.py`
- Modify: `dagster/src/corpscout_dagster/brreg/enhanced_payload.py`

- [ ] **Step 1: Update `brreg.__init__`**

Replace database facade exports in `dagster/src/corpscout_dagster/brreg/__init__.py` with source model exports only:

```python
from corpscout_dagster.brreg.models import BrregRawRecord

__all__ = ["BrregRawRecord"]
```

- [ ] **Step 2: Update materialization imports**

In `dagster/src/corpscout_dagster/brreg/materializations.py`, replace:

```python
from corpscout_dagster.brreg.asset_gateway import (
```

with:

```python
from corpscout_dagster.db_brreg.gateway import (
```

Replace:

```python
from corpscout_dagster.brreg.working_store import (
```

with:

```python
from corpscout_dagster.db_brreg.store import (
```

- [ ] **Step 3: Update direct store imports**

Apply these import replacements:

```text
corpscout_dagster.brreg.working_store -> corpscout_dagster.db_brreg.store
corpscout_dagster.brreg.asset_gateway -> corpscout_dagster.db_brreg.gateway
corpscout_dagster.brreg.writer -> corpscout_dagster.db_brreg.writer
```

The affected production files must include:

```text
dagster/src/corpscout_dagster/brreg/asset_checks.py
dagster/src/corpscout_dagster/brreg/retry_jobs.py
dagster/src/corpscout_dagster/brreg/smoke.py
dagster/src/corpscout_dagster/brreg/crawl_service.py
dagster/src/corpscout_dagster/brreg/enhanced_payload.py
```

- [ ] **Step 4: Update DB model imports**

Apply this import replacement where the type is a database write row:

```text
from corpscout_dagster.brreg.models import CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.models import BrregWorkingRawRecordRow
```

to:

```text
from corpscout_dagster.db_brreg.models import CorpscoutBrregRawInputRow
from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow
```

Do not move imports of:

```python
from corpscout_dagster.brreg.models import BrregRawRecord
```

because `BrregRawRecord` is source parsing logic.

- [ ] **Step 5: Verify no production code imports moved DB modules from old paths**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
rg "corpscout_dagster\\.brreg\\.(asset_gateway|working_store|writer)" dagster/src
```

Expected: no output.

### Task 5: Move Database Tests And Update Test Imports

**Files:**
- Move: `dagster/tests/brreg/test_asset_gateway.py` to `dagster/tests/db_brreg/test_gateway.py`
- Move: `dagster/tests/brreg/test_working_store.py` to `dagster/tests/db_brreg/test_store.py`
- Move: `dagster/tests/brreg/test_working_store_schema.py` to `dagster/tests/db_brreg/test_store_schema.py`
- Move: `dagster/tests/brreg/test_writer.py` to `dagster/tests/db_brreg/test_writer.py`
- Modify: moved test imports
- Modify: remaining BRREG tests that import DB types

- [ ] **Step 1: Move test files with git**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
mkdir -p dagster/tests/db_brreg
git mv dagster/tests/brreg/test_asset_gateway.py dagster/tests/db_brreg/test_gateway.py
git mv dagster/tests/brreg/test_working_store.py dagster/tests/db_brreg/test_store.py
git mv dagster/tests/brreg/test_working_store_schema.py dagster/tests/db_brreg/test_store_schema.py
git mv dagster/tests/brreg/test_writer.py dagster/tests/db_brreg/test_writer.py
```

- [ ] **Step 2: Update moved gateway test imports**

In `dagster/tests/db_brreg/test_gateway.py`, replace:

```python
from corpscout_dagster.brreg.asset_gateway import (
```

with:

```python
from corpscout_dagster.db_brreg.gateway import (
```

Replace:

```python
from corpscout_dagster.brreg.working_store import EnhancedBuildRecord, RawTaskRecord
```

with:

```python
from corpscout_dagster.db_brreg.store import EnhancedBuildRecord, RawTaskRecord
```

- [ ] **Step 3: Update moved store and schema test imports**

In `dagster/tests/db_brreg/test_store.py`, replace:

```python
from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.working_store import (
```

with:

```python
from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.db_brreg.store import (
```

In `dagster/tests/db_brreg/test_store_schema.py`, replace:

```python
from corpscout_dagster.brreg.working_store import UPDATE_TASK_STATE_FINISHED_SQL
from corpscout_dagster.brreg.working_store import RETRY_TASK_FAILURES_SQL
```

with:

```python
from corpscout_dagster.db_brreg.store import UPDATE_TASK_STATE_FINISHED_SQL
from corpscout_dagster.db_brreg.store import RETRY_TASK_FAILURES_SQL
```

- [ ] **Step 4: Update moved writer test imports**

In `dagster/tests/db_brreg/test_writer.py`, replace:

```python
from corpscout_dagster.brreg.models import CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.writer import BrregRawInputWriter, UpsertResult
```

with:

```python
from corpscout_dagster.db_brreg.models import CorpscoutBrregRawInputRow
from corpscout_dagster.db_brreg.writer import BrregRawInputWriter, UpsertResult
```

- [ ] **Step 5: Update remaining BRREG tests that import DB types**

Apply these replacements in `dagster/tests/brreg`:

```text
corpscout_dagster.brreg.working_store -> corpscout_dagster.db_brreg.store
corpscout_dagster.brreg.asset_gateway -> corpscout_dagster.db_brreg.gateway
corpscout_dagster.brreg.writer -> corpscout_dagster.db_brreg.writer
CorpscoutBrregRawInputRow from corpscout_dagster.brreg.models -> CorpscoutBrregRawInputRow from corpscout_dagster.db_brreg.models
```

Keep `BrregRawRecord` imports from `corpscout_dagster.brreg.models`.

- [ ] **Step 6: Verify no tests import moved DB modules from old paths**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
rg "corpscout_dagster\\.brreg\\.(asset_gateway|working_store|writer)" dagster/tests
```

Expected: no output.

### Task 6: Run Focused Tests And Fix Import Breaks

**Files:**
- Modify any file reported by focused test import failures.
- Test: `dagster/tests/db_brreg`, selected BRREG tests.

- [ ] **Step 1: Run database package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/db_brreg
```

Expected: PASS. If a failure reports an old import path, update that import to `corpscout_dagster.db_brreg`.

- [ ] **Step 2: Run BRREG asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q tests/brreg/test_assets.py tests/brreg/test_asset_checks.py tests/brreg/test_retry_jobs.py tests/brreg/test_smoke.py
```

Expected: PASS. If a failure reports `ModuleNotFoundError` for old DB paths, update the import in the failing module or test.

- [ ] **Step 3: Verify old DB file paths no longer exist**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
test ! -f dagster/src/corpscout_dagster/brreg/asset_gateway.py
test ! -f dagster/src/corpscout_dagster/brreg/working_store.py
test ! -f dagster/src/corpscout_dagster/brreg/writer.py
```

Expected: all commands exit 0.

### Task 7: Validate Dagster Definitions And Full Test Suite

**Files:**
- Modify only files required by validation failures.
- Test: full Dagster suite and definitions.

- [ ] **Step 1: Run full tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run pytest -q
```

Expected: all tests pass.

- [ ] **Step 2: Validate Dagster definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/dagster
uv run dagster definitions validate -w workspace.yaml
```

Expected:

```text
Validation successful for code location corpscout_dagster.
All code locations passed validation.
```

The command may print Dagster's supersession warning for `definitions validate`; that warning is acceptable.

- [ ] **Step 3: Check moved import boundary**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
rg "corpscout_dagster\\.brreg\\.(asset_gateway|working_store|writer)" dagster/src dagster/tests
```

Expected: no output.

- [ ] **Step 4: Check diff whitespace**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git diff --check
```

Expected: no output.

### Task 8: Commit The Package Move

**Files:**
- All files changed by Tasks 1-7.

- [ ] **Step 1: Review changed files**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git status --short
git diff --stat
```

Expected: changes are limited to `db_brreg` package creation, BRREG import updates, moved DB tests, and the approved plan/spec files already in history.

- [ ] **Step 2: Commit implementation**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add dagster/src/corpscout_dagster/db_brreg dagster/src/corpscout_dagster/brreg dagster/tests/db_brreg dagster/tests/brreg
git commit -m "Move BRREG database APIs into db_brreg package"
```

Expected: commit succeeds. If the repository already contains intentionally uncommitted gateway refactor changes, this commit includes them as part of the database package boundary move.

## Self-Review

Spec coverage:

- `db_brreg` package creation is covered by Tasks 2-3.
- Database facade and typed API relocation is covered by Tasks 1, 3, and 4.
- Store, leases, task state, artifact writes, and completeness checks remain in gateway/store and are covered by Task 3.
- Dagster as caller is covered by Task 4.
- Tests moved to package boundary are covered by Task 5.
- Validation commands are covered by Task 7.

Placeholder scan:

- This plan contains no filler tokens, undefined task names, or open-ended implementation steps.

Type consistency:

- Public imports use `corpscout_dagster.db_brreg`.
- Store implementation imports DB row DTOs from `corpscout_dagster.db_brreg.models`.
- `BrregRawRecord` remains in `corpscout_dagster.brreg.models`.
