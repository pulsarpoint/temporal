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
- `brreg_domain_enrichment_job` materializes `brreg_domain_candidates`.

Translation and domain enrichment both read current rows from
`dagster_brreg.raw_records`; they do not depend on each other. The translation
job uses the same OpenAI-compatible local LLM request shape as the old Temporal
worker, writes reusable term translations to `dagster_brreg.translation_cache`,
and records per-row task attempts in `dagster_brreg.task_attempts`. The domain
job ports the old Temporal Python activity signals into Dagster: BRREG website
field, DuckDuckGo when `crawl4ai` is installed, Wikidata, crt.sh, and heuristic
DNS.
Default translation/domain jobs claim records that have not attempted that task
yet; retrying failed attempts should be exposed as an explicit retry job/action.

Optional environment:

```bash
BRREG_TRANSLATION_BATCH_SIZE=50
BRREG_DOMAIN_BATCH_SIZE=25
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
