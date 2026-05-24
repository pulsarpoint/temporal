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

Dagster runtime state is bind-mounted from `DAGSTER_HOME_DIR`, default
`./.dagster_home`, so logs, run metadata, and local instance files are visible
on the host.

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

## BRREG Working Raw Records

The `brreg_working_raw_records` asset downloads the BRREG bulk gzip payload from
`https://data.brreg.no/enhetsregisteret/api/enheter/lastned` and upserts rows into
Dagster-owned table `dagster_brreg.raw_records`.

BRREG now has separate Dagster jobs for each independent stage:

- `brreg_ingest_job` materializes `brreg_working_raw_records`.
- `brreg_translate_job` materializes `brreg_translation_results`.
- `brreg_domain_website_field_job` materializes the fast BRREG website signal.
- `brreg_domain_duckduckgo_job` materializes DuckDuckGo/crawler signal rows.
- `brreg_domain_crtsh_job` materializes crt.sh certificate signal rows.
- `brreg_domain_wikidata_job` materializes Wikidata signal rows.
- `brreg_domain_dns_heuristic_job` materializes DNS heuristic signal rows.
- `brreg_domain_proposals_job` scores signal rows into proposed domains.
- `brreg_domain_enrichment_job` materializes all domain signal jobs and proposals.

Translation and domain enrichment both read current rows from
`dagster_brreg.raw_records`; they do not depend on each other. The translation
job uses the same OpenAI-compatible local LLM request shape as the old Temporal
worker, writes reusable term translations to `dagster_brreg.translation_cache`,
and records per-row task attempts in `dagster_brreg.task_attempts`. Domain
signals are stored independently in `dagster_brreg.domain_candidates`; the
proposal job merges those observations into `dagster_brreg.domain_proposals`
with a score, source signals, and evidence. Each signal keeps its own batch size
because DuckDuckGo/crawler, crt.sh, Wikidata, website-field parsing, and DNS
heuristics have different speed and rate-limit profiles. Each run continues
claiming pending batches until `BRREG_DOMAIN_MAX_BATCHES_PER_RUN` is reached or
there are no pending records left for that signal.
Default translation/domain jobs claim records that have not attempted that task
yet; retrying failed attempts should be exposed as an explicit retry job/action.

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
BRREG_TRANSLATION_BATCH_SIZE=50
BRREG_DOMAIN_WEBSITE_FIELD_BATCH_SIZE=5000
BRREG_DOMAIN_DUCKDUCKGO_BATCH_SIZE=10
BRREG_DOMAIN_CRTSH_BATCH_SIZE=10
BRREG_DOMAIN_WIKIDATA_BATCH_SIZE=25
BRREG_DOMAIN_DNS_HEURISTIC_BATCH_SIZE=100
BRREG_DOMAIN_PROPOSAL_BATCH_SIZE=500
BRREG_DOMAIN_MAX_BATCHES_PER_RUN=20
BRREG_TRANSLATION_MODEL=qwen3:6b
BRREG_TRANSLATION_PROMPT_VERSION=v1
LLM_BASE_URL=http://100.77.62.33:8888
LLM_API_KEY=local
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
