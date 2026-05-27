# Corpscout Dagster

Dagster pipelines for source ingestion and enrichment.

## Local Setup

```bash
uv sync
cp .env.example .env
make validate
make webserver
```

## Docker Compose

Dagster runs as its own Docker Compose stack. It is not attached to the
Corpscout or Temporal Docker networks. Containers resolve `companycollect` to
the Tailscale address `100.85.212.113`, so Dagster connects to Postgres,
Temporal, and other services through published host ports. The web UI is
published on `DAGSTER_PORT`, default `3000`.

```bash
cp .env.example .env
make up
make logs
```

`make up` pulls `DAGSTER_IMAGE` and starts the stack. It does not build the
image on the remote machine; the image is produced by GitHub Actions and pushed
to GHCR. Use `make build` only for local developer checks.

The compose stack runs:

- `dagster-webserver` on `DAGSTER_PORT`, default `3000`.
- `dagster-daemon` for schedules, sensors, and queued runs.
- `translation-service` on `TRANSLATION_SERVICE_PORT`, default `8095`.
- `crawl-service` on `CRAWL_SERVICE_PORT`, default `8096`.

Dagster runs `dagster-migrate` first and waits for the translation and crawl
services to pass `/healthz` before starting the webserver or daemon. In Docker
Compose, Dagster should use the service DNS names:

```bash
TRANSLATION_SERVICE_URL=http://translation-service:8095
CRAWL_SERVICE_URL=http://crawl-service:8096
```

The services still use prebuilt GHCR images by default:

```bash
TRANSLATION_SERVICE_IMAGE=ghcr.io/pulsarpoint/corpscout-translation-service:latest
CRAWL_SERVICE_IMAGE=ghcr.io/pulsarpoint/corpscout-crawl-service:latest
```

Dagster runtime state is bind-mounted from `DAGSTER_HOME_DIR`, default
`./.dagster_home`, so logs, run metadata, and local instance files are visible
on the host. The tracked `dagster.yaml` enables Dagster run monitoring in both
local and Docker Compose runs.

## Database Migrations

Dagster-owned migrations live in `db/migrations` and run through Docker Compose
using the official `migrate/migrate` image. The remote host only needs Docker
Compose, this folder, and `DAGSTER_MIGRATIONS_DATABASE_URL` in `.env`.

Use a migration-only DSN with `x-migrations-table=dagster_schema_migrations` so
Dagster migration versions do not collide with Corpscout application migrations.

```bash
make migrate-up
make migrate-version
make migrate-down
```

## BRREG Maintenance

Dagster can leave a run in `STARTED` when Docker Compose is recreated while the
in-process worker is running. Recover BRREG rows with:

```bash
make cleanup-stale-brreg-runs
```

This cancels stale `dagster_brreg.enrichment_runs`, marks still-running
`task_attempts` cancelled, resets matching `raw_record_task_states` to
`failed_retryable` with `next_retry_at = now()`, and marks matching Dagster run
records canceled when the local Dagster instance still has them.

## BRREG Raw Records

The `brreg_raw_records` asset downloads the BRREG bulk gzip payload from
`https://data.brreg.no/enhetsregisteret/api/enheter/lastned` and upserts rows into
Dagster-owned table `dagster_brreg.raw_records`.

The operator-facing Dagster catalog exposes the BRREG source and independent
enrichment artifacts:

- `brreg_ingest_raw_job` materializes `brreg_raw_records`.
- `brreg_translate_job` materializes `brreg_translation_results`.
- `brreg_domain_job` calls the crawl service and stores one
  `dagster_brreg.domain_results` artifact per company.
- `brreg_currency_job` converts BRREG capital values to USD artifacts in
  `dagster_brreg.currency_results`.
- `brreg_build_enhanced_job` combines the artifacts into
  `dagster_brreg.enhanced_records`.
- `brreg_full_enrichment_job` runs the source and all artifact assets.

Translation, domain enrichment, and currency conversion all depend on
`brreg_raw_records`, but they can run independently after raw ingestion. The
translation job calls the standalone translation service at
`TRANSLATION_SERVICE_URL`, writes reusable term translations to
`dagster_brreg.translation_cache`, and stores per-row artifacts in
`dagster_brreg.translation_results`. Current `raw_records` are the input set;
`raw_record_task_states` is execution bookkeeping only. Before each translation
run, Dagster reconciles task state from actual translation artifacts for the
current model and prompt version so stale `succeeded` or `failed_terminal` task
states cannot hide rows that still lack usable translated output. Translation
keeps claiming `BRREG_TRANSLATION_BATCH_SIZE` chunks until there are no
claimable rows left; `BRREG_TRANSLATION_MAX_BATCHES_PER_RUN=0` means drain the
claimable work in one materialization.

Domain discovery is one Dagster business task inside `brreg_domain_job`, backed
by the standalone crawl service.
The service owns DuckDuckGo/Yandex search, crawl4ai/Chromium crawling, LLM
verification, scoring, and structured errors. Dagster claims rows, calls the
service, stores one response artifact per company in `dagster_brreg.domain_results`,
and leaves search/crawl/verification internals out of the Dagster asset graph.

The enhanced build requires successful or skipped translation and domain result
tasks. It reads the latest `dagster_brreg.domain_results.domain_payload` and
converts accepted domain candidates into the enhanced payload. BRREG capital
amounts are preserved in the original currency and converted to USD cents using
ECB rates when `kapital.belop` and `kapital.valuta` are present. Set
`BRREG_FX_RATE_DATE=YYYY-MM-DD` to use the latest ECB rate on or before a fixed
date; leave it empty to use the latest daily ECB feed. Financials are currently
emitted as `not_available` until the BRREG financial extraction job is
implemented. Publishing to Corpscout handoff tables is kept as internal code and
is not exposed as a Dagster catalog option:

```text
dagster_brreg.enhanced_records
  -> brreg_company_raw_inputs
  -> brreg_enhanced_raw_inputs
```

Corpscout remains responsible for unpacking `brreg_enhanced_raw_inputs` into
normalized `brreg_source_*` tables and creating suggestions.

## BRREG Observability Views

Migration `000005` adds read-only views for checking what has run, what failed,
what is ready to retry, and which domains were proposed:

- `dagster_brreg.v_enrichment_run_summary`
- `dagster_brreg.v_task_state_summary`
- `dagster_brreg.v_failed_task_states`
- `dagster_brreg.v_raw_record_task_overview`
- `dagster_brreg.v_domain_enrichment_summary`

Useful queries:

```sql
SELECT task_type, status, row_count, retry_ready_count, stale_running_count, next_retry_at
FROM dagster_brreg.v_task_state_summary
ORDER BY task_type, status;
```

```sql
SELECT organization_number, organization_name, task_type, status, attempt_count, next_retry_at, last_error
FROM dagster_brreg.v_failed_task_states
ORDER BY next_retry_at NULLS LAST, updated_at DESC
LIMIT 100;
```

```sql
SELECT organization_number, organization_name, best_domain, best_domain_score, domain_candidates_by_signal
FROM dagster_brreg.v_domain_enrichment_summary
WHERE domain_proposal_count > 0
ORDER BY best_domain_score DESC NULLS LAST
LIMIT 100;
```

Optional environment:

```bash
BRREG_RAW_RECORD_BATCH_SIZE=5000
BRREG_RAW_RECORD_LIMIT=1000
BRREG_TRANSLATION_BATCH_SIZE=50
BRREG_TRANSLATION_MAX_BATCHES_PER_RUN=0
BRREG_TRANSLATION_MAX_PARALLEL_TASKS=50
BRREG_TRANSLATION_PROVIDER=local
BRREG_TRANSLATION_MODEL=qwen3:6b
BRREG_TRANSLATION_PROMPT_VERSION=v1
TRANSLATION_SERVICE_TIMEOUT_SECONDS=300
BRREG_DOMAIN_RESULT_BATCH_SIZE=10
BRREG_DOMAIN_RESULT_MAX_BATCHES_PER_RUN=0
BRREG_DOMAIN_RESULT_MAX_PARALLEL_TASKS=1
CRAWL_SERVICE_TIMEOUT_SECONDS=300
BRREG_ENHANCED_RECORD_BATCH_SIZE=500
BRREG_PUBLISH_ENHANCED_RECORD_BATCH_SIZE=250
BRREG_FX_RATE_DATE=2026-05-21
BRREG_STALE_RUN_CLEANUP_MINUTES=30
```

Required environment:

```bash
CORPSCOUT_DATABASE_URL=postgresql://user:password@companycollect:5432/corpscout?sslmode=disable
DAGSTER_MIGRATIONS_DATABASE_URL=postgresql://user:password@companycollect:5432/corpscout?sslmode=disable&x-migrations-table=dagster_schema_migrations
```

Verify the database write path without leaving a row behind:

```bash
make smoke-brreg-db
```

The container image is built by `.github/workflows/dagster-image.yml` and pushed
to `ghcr.io/pulsarpoint/corpscout-dagster`.
