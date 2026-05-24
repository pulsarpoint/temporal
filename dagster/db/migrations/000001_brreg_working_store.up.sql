CREATE EXTENSION IF NOT EXISTS pgcrypto;

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
  CONSTRAINT chk_dagster_brreg_runs_type CHECK (
    run_type IN ('bulk_ingest', 'full_enrichment', 'retry_failed', 'publish')
  ),
  CONSTRAINT chk_dagster_brreg_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_dagster_brreg_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
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
  CONSTRAINT chk_dagster_brreg_bulk_snapshot_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_dagster_brreg_bulk_snapshot_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
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
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_dagster_brreg_raw_source_native CHECK (source_native_id = organization_number),
  CONSTRAINT chk_dagster_brreg_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_dagster_brreg_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
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
  CONSTRAINT chk_dagster_brreg_task_attempt_type CHECK (
    task_type IN ('parse_raw', 'translate', 'discover_domains', 'extract_financials', 'build_enhanced', 'publish')
  ),
  CONSTRAINT chk_dagster_brreg_task_attempt_status CHECK (
    status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_dagster_brreg_task_attempt_attempt CHECK (attempt > 0),
  CONSTRAINT chk_dagster_brreg_task_attempt_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
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
  CONSTRAINT chk_dagster_brreg_translation_status CHECK (
    status IN ('succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_dagster_brreg_translation_payload_object CHECK (
    translated_payload IS NULL OR jsonb_typeof(translated_payload) = 'object'
  ),
  CONSTRAINT chk_dagster_brreg_translation_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
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
  CONSTRAINT chk_dagster_brreg_domain_confidence CHECK (confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_dagster_brreg_domain_status CHECK (
    status IN ('candidate', 'accepted', 'rejected', 'failed')
  ),
  CONSTRAINT chk_dagster_brreg_domain_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_dagster_brreg_domain_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
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
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_dagster_brreg_financial_status CHECK (
    status IN ('succeeded', 'failed', 'not_available', 'skipped')
  ),
  CONSTRAINT chk_dagster_brreg_financial_payload_object CHECK (jsonb_typeof(financial_payload) = 'object'),
  CONSTRAINT chk_dagster_brreg_financial_usd_payload_object CHECK (jsonb_typeof(usd_payload) = 'object'),
  CONSTRAINT chk_dagster_brreg_financial_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object'),
  CONSTRAINT chk_dagster_brreg_financial_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
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
  CONSTRAINT chk_dagster_brreg_enhanced_status CHECK (
    status IN ('built', 'published', 'publish_failed', 'superseded')
  ),
  CONSTRAINT chk_dagster_brreg_enhanced_payload_object CHECK (jsonb_typeof(enhanced_payload) = 'object'),
  CONSTRAINT chk_dagster_brreg_enhanced_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, schema_version, enhanced_payload_hash)
);

CREATE INDEX idx_dagster_brreg_raw_records_org
  ON dagster_brreg.raw_records (organization_number);

CREATE INDEX idx_dagster_brreg_raw_records_hash
  ON dagster_brreg.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_dagster_brreg_raw_records_current_org
  ON dagster_brreg.raw_records (organization_number)
  WHERE is_current;

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
