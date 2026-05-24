# BRREG Dagster Working Store Design

## Decision

Dagster owns a BRREG working store in Postgres under schema `dagster_brreg`.

This store is separate from Dagster's internal run database and separate from Corpscout's source/review tables. It holds business pipeline artifacts while BRREG data moves through raw extraction, translation, domain discovery, financial extraction, enhanced payload construction, and final publish.

Corpscout Postgres remains the published source of record for review and suggestions. Dagster working tables are the source of truth for in-progress enrichment state.

## Storage Boundary

Dagster working store:

- BRREG bulk snapshot metadata.
- Per-company raw payloads and payload hashes.
- Task attempts and retry status.
- Translation results.
- Domain candidates and evidence.
- Financial extraction results and USD conversion metadata.
- Final enhanced JSON before publish.
- Publish status and Corpscout ids after publish.

Corpscout storage:

- `brreg_company_raw_inputs` as published original source evidence.
- `brreg_enhanced_raw_inputs` as published final enhanced payload history.
- `brreg_source_*` normalized source read models.
- Suggestions, review, and approval state.

Dagster internal storage:

- Dagster run metadata, asset materialization events, daemon state, and schedules only.
- No BRREG business datasets.

## Data Flow

1. Dagster downloads the BRREG bulk file.
2. Dagster records one `dagster_brreg.bulk_snapshots` row.
3. Dagster parses companies into `dagster_brreg.raw_records`.
4. Translation writes `dagster_brreg.translation_results`.
5. Domain discovery writes `dagster_brreg.domain_candidates`.
6. Financial extraction writes `dagster_brreg.financial_results`.
7. Enhanced payload build writes `dagster_brreg.enhanced_records`.
8. Publish reads a complete `dagster_brreg.enhanced_records` row and writes original raw plus enhanced JSON to Corpscout Postgres in one transaction.
9. Corpscout unpacks enhanced JSON into normalized `brreg_source_*` tables and creates suggestions from those normalized tables.

Corpscout should not receive partial translation/domain/financial statuses for the Dagster-owned path. Those statuses live in `dagster_brreg.task_attempts` and task-specific result tables.

## Schema

The first implementation should create these tables.

```sql
CREATE SCHEMA IF NOT EXISTS dagster_brreg;

CREATE TABLE dagster_brreg.enrichment_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dagster_run_id TEXT NOT NULL UNIQUE,
  run_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_completed INTEGER NOT NULL DEFAULT 0,
  records_failed INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_runs_type CHECK (
    run_type IN ('bulk_ingest', 'full_enrichment', 'retry_failed', 'publish')
  ),
  CONSTRAINT chk_brreg_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_brreg_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE dagster_brreg.bulk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  enrichment_run_id UUID NOT NULL REFERENCES dagster_brreg.enrichment_runs(id) ON DELETE CASCADE,
  source_url TEXT NOT NULL,
  downloaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  content_length_bytes BIGINT,
  compressed_payload_hash TEXT,
  storage_uri TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_bulk_snapshot_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_brreg_bulk_snapshot_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE dagster_brreg.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES dagster_brreg.bulk_snapshots(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  organization_number TEXT NOT NULL,
  organization_name TEXT,
  registration_status TEXT,
  website TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'NO',
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_working_source_native CHECK (source_native_id = organization_number),
  CONSTRAINT chk_brreg_working_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_brreg_working_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (organization_number, payload_hash)
);

CREATE TABLE dagster_brreg.task_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  enrichment_run_id UUID NOT NULL REFERENCES dagster_brreg.enrichment_runs(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_type TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  worker_id TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_task_attempt_type CHECK (
    task_type IN ('parse_raw', 'translate', 'discover_domains', 'extract_financials', 'build_enhanced', 'publish')
  ),
  CONSTRAINT chk_brreg_task_attempt_status CHECK (
    status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_brreg_task_attempt_attempt CHECK (attempt > 0),
  CONSTRAINT chk_brreg_task_attempt_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, task_type, attempt)
);

CREATE TABLE dagster_brreg.translation_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  translated_payload JSONB,
  model TEXT,
  prompt_version TEXT,
  fx_source TEXT,
  fx_rate_date DATE,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_translation_status CHECK (
    status IN ('succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_brreg_translation_payload_object CHECK (
    translated_payload IS NULL OR jsonb_typeof(translated_payload) = 'object'
  ),
  CONSTRAINT chk_brreg_translation_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE dagster_brreg.domain_candidates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  signal TEXT NOT NULL,
  confidence SMALLINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate',
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_domain_candidate_confidence CHECK (confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_brreg_domain_candidate_status CHECK (
    status IN ('candidate', 'accepted', 'rejected', 'failed')
  ),
  CONSTRAINT chk_brreg_domain_candidate_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_domain_candidate_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, normalized_domain, signal)
);

CREATE TABLE dagster_brreg.financial_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  fiscal_year INTEGER,
  status TEXT NOT NULL,
  original_currency TEXT,
  financial_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  usd_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_uri TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_financial_status CHECK (
    status IN ('succeeded', 'failed', 'not_available', 'skipped')
  ),
  CONSTRAINT chk_brreg_financial_payload_object CHECK (jsonb_typeof(financial_payload) = 'object'),
  CONSTRAINT chk_brreg_financial_usd_payload_object CHECK (jsonb_typeof(usd_payload) = 'object'),
  CONSTRAINT chk_brreg_financial_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object')
);

CREATE TABLE dagster_brreg.enhanced_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  schema_version TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'built',
  enhanced_payload JSONB NOT NULL,
  enhanced_payload_hash TEXT NOT NULL,
  corpscout_raw_input_id UUID,
  corpscout_enhanced_raw_input_id UUID,
  corpscout_source_company_id UUID,
  built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_enhanced_status CHECK (
    status IN ('built', 'published', 'publish_failed', 'superseded')
  ),
  CONSTRAINT chk_brreg_enhanced_payload_object CHECK (jsonb_typeof(enhanced_payload) = 'object'),
  CONSTRAINT chk_brreg_enhanced_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, schema_version, enhanced_payload_hash)
);
```

## Indexes

```sql
CREATE INDEX idx_dagster_brreg_raw_records_org
  ON dagster_brreg.raw_records (organization_number);

CREATE INDEX idx_dagster_brreg_raw_records_hash
  ON dagster_brreg.raw_records (payload_hash);

CREATE INDEX idx_dagster_brreg_task_attempts_queue
  ON dagster_brreg.task_attempts (task_type, status, started_at)
  WHERE status IN ('queued', 'running', 'failed');

CREATE INDEX idx_dagster_brreg_task_attempts_raw
  ON dagster_brreg.task_attempts (raw_record_id, task_type, attempt DESC);

CREATE INDEX idx_dagster_brreg_translation_latest
  ON dagster_brreg.translation_results (raw_record_id, created_at DESC);

CREATE INDEX idx_dagster_brreg_domain_candidates_raw
  ON dagster_brreg.domain_candidates (raw_record_id, confidence DESC);

CREATE INDEX idx_dagster_brreg_financial_results_raw
  ON dagster_brreg.financial_results (raw_record_id, fiscal_year DESC);

CREATE INDEX idx_dagster_brreg_enhanced_publish_queue
  ON dagster_brreg.enhanced_records (status, built_at)
  WHERE status IN ('built', 'publish_failed');
```

## Completion Rules

A raw record is ready to build enhanced JSON when:

- raw payload exists in `dagster_brreg.raw_records`,
- latest translation result is `succeeded` or explicitly `skipped`,
- domain discovery has either at least one candidate result or a successful no-results task attempt,
- financial extraction is `succeeded`, `not_available`, or explicitly `skipped`.

The enhanced payload builder records section-level statuses in `enhanced_payload.enhancement.section_statuses`. It does not rely on Corpscout raw input state.

## Publish Contract

Publish is the only step that writes to Corpscout tables.

In one transaction per company, publish should:

1. Upsert original raw data into `brreg_company_raw_inputs`.
2. Insert final enhanced JSON into `brreg_enhanced_raw_inputs`.
3. Invoke the Corpscout database unpack function/procedure.
4. Update `dagster_brreg.enhanced_records` with Corpscout ids and `status='published'`.

If any Corpscout write fails, the transaction rolls back and `dagster_brreg.enhanced_records.status` becomes `publish_failed`.

## Impact On Current Dagster Code

The current `brreg_raw_inputs` asset writes raw rows directly to Corpscout. Under this design, it should be refactored into two separate responsibilities:

- `brreg_working_raw_records`: download and upsert BRREG bulk rows into `dagster_brreg.raw_records`.
- `brreg_publish_enhanced_records`: publish completed raw plus enhanced records to Corpscout.

The existing Corpscout raw writer remains useful, but it belongs in the publish step, not the first raw extraction asset.

## First Implementation Slice

The next implementation slice should:

1. Add SQL migrations or setup scripts for `dagster_brreg`.
2. Add Python repository methods for `enrichment_runs`, `bulk_snapshots`, and `raw_records`.
3. Refactor the BRREG bulk asset to write to `dagster_brreg.raw_records`.
4. Keep the rollback smoke test, but point it at the working store first.
5. Add a second publish smoke test only after enhanced records exist.
