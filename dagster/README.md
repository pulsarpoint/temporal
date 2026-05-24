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

Dagster is configured to run on the same host network as Temporal workers and
Corpscout Postgres.

```bash
cp .env.example .env
make up
make logs
```

The compose stack runs:

- `dagster-webserver` on `DAGSTER_PORT`, default `3000`.
- `dagster-daemon` for schedules, sensors, and queued runs.

Dagster runtime state is bind-mounted from `DAGSTER_HOME_DIR`, default
`./.dagster_home`, so logs, run metadata, and local instance files are visible
on the host.

The container image is built by `.github/workflows/dagster-image.yml` and pushed
to `ghcr.io/pulsarpoint/corpscout-dagster`.
