CREATE TABLE IF NOT EXISTS dagster_brreg.currency_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  original_currency TEXT,
  original_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  usd_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_uri TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_dagster_brreg_currency_results_status CHECK (
    status IN ('succeeded', 'failed', 'not_available', 'skipped')
  ),
  CONSTRAINT chk_dagster_brreg_currency_results_original_payload_object CHECK (
    jsonb_typeof(original_payload) = 'object'
  ),
  CONSTRAINT chk_dagster_brreg_currency_results_usd_payload_object CHECK (jsonb_typeof(usd_payload) = 'object'),
  CONSTRAINT chk_dagster_brreg_currency_results_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object'),
  CONSTRAINT chk_dagster_brreg_currency_results_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_currency_results_raw_created
  ON dagster_brreg.currency_results (raw_record_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_currency_results_status
  ON dagster_brreg.currency_results (status, created_at DESC);

ALTER TABLE dagster_brreg.task_attempts
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempt_type;

ALTER TABLE dagster_brreg.task_attempts
  ADD CONSTRAINT chk_dagster_brreg_task_attempt_type CHECK (
    task_type IN (
      'parse_raw',
      'translate',
      'discover_domains',
      'domain_results',
      'currency_conversion',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_duckduckgo_search',
      'domain_crtsh',
      'domain_wikidata',
      'domain_dns_heuristic',
      'domain_web_search_llm',
      'merge_domain_proposals',
      'extract_financials',
      'build_enhanced',
      'publish'
    )
  );

ALTER TABLE dagster_brreg.enrichment_runs
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_runs_type;

ALTER TABLE dagster_brreg.enrichment_runs
  ADD CONSTRAINT chk_dagster_brreg_runs_type CHECK (
    run_type IN (
      'bulk_ingest',
      'translate',
      'discover_domains',
      'domain_results',
      'currency_conversion',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_duckduckgo_search',
      'domain_crtsh',
      'domain_wikidata',
      'domain_dns_heuristic',
      'domain_web_search_llm',
      'merge_domain_proposals',
      'build_enhanced',
      'full_enrichment',
      'retry_failed',
      'publish'
    )
  );

ALTER TABLE dagster_brreg.raw_record_task_states
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_raw_record_task_states_type;

ALTER TABLE dagster_brreg.raw_record_task_states
  ADD CONSTRAINT chk_dagster_brreg_raw_record_task_states_type CHECK (
    task_type IN (
      'translate',
      'domain_results',
      'currency_conversion',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_duckduckgo_search',
      'domain_crtsh',
      'domain_wikidata',
      'domain_dns_heuristic',
      'domain_web_search_llm',
      'merge_domain_proposals',
      'build_enhanced',
      'publish',
      'discover_domains',
      'extract_financials'
    )
  );
