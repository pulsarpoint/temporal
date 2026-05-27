# BRREG Database Package Boundary Design

Date: 2026-05-27

## Summary

Create a Dagster-independent `corpscout_dagster.db_brreg` package for BRREG persistence. The package owns BRREG database access, typed database-facing APIs, task state management, leases, artifact writes, transactions, and live asset state checks. Dagster remains the orchestrator and calls this package through a typed facade.

## Goals

- Move BRREG database code out of `corpscout_dagster.brreg`.
- Hide SQL, queue leases, task state transitions, and artifact writes behind package methods.
- Let Dagster see BRREG database state as typed asset views, not as raw SQL tables.
- Keep external service orchestration in the BRREG pipeline layer.
- Keep the move behavior-preserving in the first step.

## Package Boundary

Create:

```text
dagster/src/corpscout_dagster/db_brreg/
  __init__.py
  gateway.py
  store.py
  models.py
```

Move:

- `brreg/asset_gateway.py` to `db_brreg/gateway.py`.
- `brreg/working_store.py` to `db_brreg/store.py`.
- Database-specific DTOs from `working_store.py` remain in `store.py` for the first move.
- Database row models from `brreg/models.py` move to `db_brreg/models.py` only when they represent persisted rows or DB write inputs.

Keep in `corpscout_dagster.brreg`:

- Dagster asset definitions.
- Materialization orchestration.
- BRREG source clients and parsing.
- Translation term extraction.
- Enhanced payload construction.
- Crawl, FX, and translation service clients.

## Public API

Dagster and other callers should import only from the `db_brreg` package facade:

```python
from corpscout_dagster.db_brreg import BrregAssetGateway, BrregAssetName
```

The gateway continues to expose typed methods for:

- raw ingest
- translation/domain/currency/enhanced claims
- translation/domain/currency/enhanced submit success
- translation/domain/currency/enhanced submit failure
- live asset state reads
- completeness assertions

The package must not import Dagster or require Dagster context.

## Resource Relationship

Postgres connection configuration can be a Dagster resource. The `db_brreg` package should receive a connection or connection factory from callers. It should not know how Dagster provides that connection.

Expected flow:

```text
Dagster asset
  -> gets Postgres connection/resource
  -> calls db_brreg gateway to claim rows
  -> calls external service/resource
  -> calls db_brreg gateway to submit result
  -> reads db_brreg live asset state
  -> succeeds/fails materialization based on live state
```

## Asset View Model

Dagster assets represent live database views of BRREG state:

- `brreg_raw_records`
- `brreg_translation_results`
- `brreg_domain_results`
- `brreg_currency_results`
- `brreg_enhanced_records`

Materialization runs are execution attempts. The source of truth for asset state is the live database state exposed by `db_brreg`, not `enrichment_runs`.

`enrichment_runs` remains audit/debug metadata only.

## Testing

Move or update tests so persistence tests live next to the new package boundary:

```text
dagster/tests/db_brreg/
  test_gateway.py
  test_store.py
  test_schema.py
```

Dagster asset tests should verify that asset functions call the gateway and surface gateway errors as failed materializations.

Required verification after the move:

```bash
uv run pytest -q
uv run dagster definitions validate -w workspace.yaml
git diff --check
```

## First Implementation Scope

The first implementation should be a behavior-preserving package move:

- create `db_brreg`
- move gateway/store/database models
- update imports
- move tests
- keep SQL and table definitions unchanged
- keep materialization behavior unchanged

Do not refactor SQL, resource creation, or materialization structure in the same step unless required by the move.
