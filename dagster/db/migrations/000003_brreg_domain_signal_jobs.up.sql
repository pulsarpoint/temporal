CREATE TABLE IF NOT EXISTS dagster_brreg.domain_proposals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  score SMALLINT NOT NULL,
  signals TEXT[] NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'proposed',
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_dagster_brreg_domain_proposals_score CHECK (score BETWEEN 1 AND 100),
  CONSTRAINT chk_dagster_brreg_domain_proposals_status CHECK (
    status IN ('proposed', 'accepted', 'rejected', 'superseded')
  ),
  CONSTRAINT chk_dagster_brreg_domain_proposals_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_dagster_brreg_domain_proposals_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, normalized_domain)
);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_candidates_signal
  ON dagster_brreg.domain_candidates (signal, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_proposals_raw_score
  ON dagster_brreg.domain_proposals (raw_record_id, score DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_proposals_status
  ON dagster_brreg.domain_proposals (status, score DESC)
  WHERE status IN ('proposed', 'accepted');

ALTER TABLE dagster_brreg.task_attempts
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempt_type;

ALTER TABLE dagster_brreg.task_attempts
  ADD CONSTRAINT chk_dagster_brreg_task_attempt_type CHECK (
    task_type IN (
      'parse_raw',
      'translate',
      'discover_domains',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_crtsh',
      'domain_wikidata',
      'domain_dns_heuristic',
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
      'domain_website_field',
      'domain_duckduckgo',
      'domain_crtsh',
      'domain_wikidata',
      'domain_dns_heuristic',
      'merge_domain_proposals',
      'build_enhanced',
      'full_enrichment',
      'retry_failed',
      'publish'
    )
  );
