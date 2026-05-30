# Corpscout Services

This compose file runs only the standalone Python services used by Corpscout
and Temporal workflows:

- `translation-service` on port `8095`
- `crawl-service` on port `8096`

It does not start Temporal, workers, Postgres, or Dagster.

## Configure

Create local env files from the examples:

```bash
cp translation-service/.env.example translation-service/.env
cp crawl-service/.env.example crawl-service/.env
```

Set real provider URLs, models, and API keys in those ignored `.env` files.

Optional host port overrides:

```bash
TRANSLATION_SERVICE_PORT=18095
CRAWL_SERVICE_PORT=18096
```

## Run

```bash
docker compose up --build
```

Health checks:

```bash
curl http://localhost:8095/healthz
curl http://localhost:8096/healthz
```
