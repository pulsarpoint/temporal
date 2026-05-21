# CLAUDE.md — data-pipelines

Temporal-based data pipeline that pulls company records from registries and writes them to the corpscout database.

## Repository layout

```
data-pipelines/
├── temporal/                   # Local dev Temporal cluster (docker compose)
├── services/
│   ├── go-worker/              # Go Temporal worker: WriteRawInputs, MarkExecutionComplete, domain enrichment
│   └── python-worker/          # Python Temporal worker: FetchPage activities (Companies House, Brreg, …)
└── Makefile                    # top-level shortcuts
```

## Common commands

```bash
# Local Temporal cluster (for dev/testing without the server)
make temporal-up     # starts Temporal + UI on localhost:7233 / localhost:8089
make temporal-down

# Go worker
make build           # GOWORK=off go build → services/go-worker/bin/worker
make test            # runs go + python tests

# From services/go-worker/
make build
make test
make run             # docker compose up -d --build
make logs

# From services/python-worker/
make run
make logs
```

### GOWORK=off is required for the Go worker

The `github.com/pulsarpoint/data-pipelines` module lives inside the `ppoint/` monorepo, which has a parent `go.work`. Always pass `GOWORK=off` or use the Makefile.

## Architecture

```
corpscout scheduler (River job: DataTaskWorker)
    ↓  ExecuteWorkflow
Temporal server (namespace: corpscout)
    ├── Go worker  (task queue: corpscout-pipelines)
    │   ├── PullCompaniesHouse workflow
    │   ├── PullBrreg workflow
    │   ├── EnrichCompanyDomains workflow
    │   └── Activities: WriteRawInputs, MarkExecutionComplete,
    │                   FilterForDomainDiscovery, WriteDiscoveredDomains
    └── Python worker  (task queue: corpscout-pipelines-python)
        └── Activities: fetch_companies_house_list, fetch_brreg_list,
                        discover_company_domains
    ↓
corpscout PostgreSQL DB (on companycollect)
    ├── companies_house_company_raw_inputs
    ├── brreg_company_raw_inputs
    ├── temporal_executions   (workflow tracking for the UI)
    └── company_domains
```

## Workflow design: ContinueAsNew

Bulk pulls iterate through millions of records. To avoid Temporal's workflow history size limit, workflows call `workflow.NewContinueAsNewError` after every **50 pages** (5,000 records). The cursor, RunID, and accumulated totals are carried forward in the new run's input. `MarkExecutionComplete` is only called on the final run when `has_more = false`.

Key fields in `PullCompaniesHouseInput` / `PullBrregInput`:
- `Cursor` — pagination cursor carried across ContinueAsNew
- `RunID` — UUID generated once on the first run, reused so all batches share the same batch ID in the DB
- `Accumulated` — running `RecordsWritten` / `PagesFetched` totals

## Pagination: Companies House

The CH Advanced Search API:
- Returns up to 100 records per page (`size=100`)
- `start_index + size` must not exceed 10,000 → max 100 pages (0–99) per date bucket
- Does **not** return a reliable `total_results` — use `len(items) == PAGE_SIZE` as `has_more`
- Cursor format: `"YYYY-MM-DD,N"` — `incorporated_from` date + 0-indexed page offset
- When page offset hits `_CH_MAX_PAGE` (99), the cursor rolls to the last item's `date_of_creation` to start the next date bucket

## Remote server: companycollect

Both workers run on `companycollect` (Tailscale IP `100.85.212.113`). The Temporal server also runs there.

**SSH:** `ssh graovic@100.85.212.113`

**Worker paths on server:**
```
/home/graovic/temporal/services/go-worker/
/home/graovic/temporal/services/python-worker/
```

**Deploy from Mac (always exclude .env):**
```bash
rsync -av --exclude='.env' \
  services/go-worker/ \
  graovic@100.85.212.113:/home/graovic/temporal/services/go-worker/

rsync -av --exclude='.env' \
  services/python-worker/ \
  graovic@100.85.212.113:/home/graovic/temporal/services/python-worker/

ssh graovic@100.85.212.113 \
  "cd /home/graovic/temporal/services/go-worker && docker compose up -d --build && \
   cd /home/graovic/temporal/services/python-worker && docker compose up -d --build"
```

**CRITICAL — .env is environment-specific and must never be overwritten by rsync:**

| Variable | Mac .env | Server .env |
|---|---|---|
| `TEMPORAL_HOST` | `companycollect:7233` | `localhost:7233` |
| `CORPSCOUT_DB_URL` | `...@companycollect:5432/...` | `...@localhost:5432/...` |

The server containers use `network_mode: host` so they reach Temporal and Postgres via `localhost`. The Mac reaches them via Tailscale.

## Temporal server

| | |
|---|---|
| gRPC | `100.85.212.113:7233` (or `localhost:7233` on server) |
| Web UI | `http://100.85.212.113:8089` |
| Namespace | `corpscout` |
| Go task queue | `corpscout-pipelines` |
| Python task queue | `corpscout-pipelines-python` |

## Environment variables

### go-worker
- `TEMPORAL_HOST` — Temporal gRPC address. **Mac:** `companycollect:7233`. **Server:** `localhost:7233`.
- `CORPSCOUT_DB_URL` — corpscout Postgres DSN. **Mac:** `postgres://corpscout:password123@companycollect:5432/corpscout?sslmode=disable`. **Server:** `postgres://corpscout:password123@localhost:5432/corpscout?sslmode=disable`.
- `OUTPUT_DIR` — local path for result files (default `/var/lib/data-pipelines/results`).

### python-worker
- `TEMPORAL_HOST` — same as above.
- `COMPANIES_HOUSE_API_KEY` — CH API key (HTTP Basic auth, password is empty string).

## corpscout DB

```
Host (from Mac): 100.85.212.113
Port:            5432
DB:              corpscout
User:            corpscout
Password:        password123
```

**Quick query from Mac:**
```bash
docker run --rm postgres:16-alpine psql \
  "postgres://corpscout:password123@100.85.212.113:5432/corpscout?sslmode=disable" \
  -c "SELECT COUNT(*) FROM companies;"
```

**Trigger source_process River job manually** (when MarkExecutionComplete didn't enqueue it):
```bash
docker run --rm postgres:16-alpine psql \
  "postgres://corpscout:password123@100.85.212.113:5432/corpscout?sslmode=disable" \
  -c "INSERT INTO river_job(kind,args,state,queue,priority,scheduled_at,max_attempts,tags,metadata)
      VALUES ('source_process','{\"source_name\":\"companies_house\"}','available','source_process',1,now(),25,'{}','{}');"
```

## Adding a new source

1. Add `FetchPage` activity in `services/python-worker/activities/fetch_<source>_list.py`
2. Register it in `services/python-worker/main.py`
3. Add `Write<Source>Record` method to `services/go-worker/activities/activities.go`
4. Add workflow `Pull<Source>` in `services/go-worker/workflows/` (copy `pull_companies_house.go`, adjust source name)
5. Register workflow and activities in `services/go-worker/cmd/worker/main.go`
6. Add source to `sourceWorkflowType` and `sourceDefaultCountry` maps in `corpscout/scheduler/internal/workers/data_task.go`
7. Add `Write<Source>RawInput` DB activity + migration in corpscout if a new raw_inputs table is needed
8. Add processor in `corpscout/scheduler/internal/workers/<source>_processor.go`
9. Wire processor into `SourceProcessWorker` switch in `source_process.go`
