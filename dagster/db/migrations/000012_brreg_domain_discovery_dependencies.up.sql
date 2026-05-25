CREATE TABLE IF NOT EXISTS dagster_brreg.domain_search_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  provider TEXT NOT NULL,
  query TEXT NOT NULL,
  rank INTEGER NOT NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  title TEXT,
  description TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_dagster_brreg_domain_search_provider CHECK (provider IN ('duckduckgo')),
  CONSTRAINT chk_dagster_brreg_domain_search_rank CHECK (rank > 0),
  CONSTRAINT chk_dagster_brreg_domain_search_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, provider, query, rank, url)
);

CREATE TABLE IF NOT EXISTS dagster_brreg.domain_crawl_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  search_result_id UUID REFERENCES dagster_brreg.domain_search_results(id) ON DELETE SET NULL,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  status TEXT NOT NULL,
  markdown TEXT,
  markdown_hash TEXT,
  llm_confidence SMALLINT,
  llm_decision TEXT,
  llm_reason TEXT,
  llm_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_dagster_brreg_domain_crawl_status CHECK (
    status IN ('succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_dagster_brreg_domain_crawl_confidence CHECK (
    llm_confidence IS NULL OR llm_confidence BETWEEN 1 AND 100
  ),
  CONSTRAINT chk_dagster_brreg_domain_crawl_evidence_object CHECK (jsonb_typeof(llm_evidence) = 'object'),
  CONSTRAINT chk_dagster_brreg_domain_crawl_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, url)
);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_search_results_raw
  ON dagster_brreg.domain_search_results (raw_record_id, provider, rank);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_crawl_results_raw
  ON dagster_brreg.domain_crawl_results (raw_record_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_crawl_results_decision
  ON dagster_brreg.domain_crawl_results (normalized_domain, llm_confidence DESC)
  WHERE llm_confidence IS NOT NULL;

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

CREATE OR REPLACE VIEW dagster_brreg.v_domain_enrichment_summary AS
WITH candidate_counts AS (
  SELECT
    raw_record_id,
    count(*) AS domain_candidate_count,
    count(*) FILTER (WHERE status = 'accepted') AS accepted_candidate_count,
    count(*) FILTER (WHERE signal = 'website_field') AS website_field_candidate_count,
    count(*) FILTER (WHERE signal = 'web_search_llm') AS web_search_llm_candidate_count,
    max(updated_at) AS last_candidate_updated_at
  FROM dagster_brreg.domain_candidates
  GROUP BY raw_record_id
),
candidate_signal_counts AS (
  SELECT
    raw_record_id,
    jsonb_object_agg(signal, signal_count ORDER BY signal) AS domain_candidates_by_signal
  FROM (
    SELECT
      raw_record_id,
      signal,
      count(*) AS signal_count
    FROM dagster_brreg.domain_candidates
    GROUP BY raw_record_id, signal
  ) counts
  GROUP BY raw_record_id
),
search_counts AS (
  SELECT
    raw_record_id,
    count(*) AS duckduckgo_search_result_count,
    max(updated_at) AS last_search_result_updated_at
  FROM dagster_brreg.domain_search_results
  GROUP BY raw_record_id
),
crawl_counts AS (
  SELECT
    raw_record_id,
    count(*) AS crawl_result_count,
    count(*) FILTER (WHERE llm_decision = 'accepted') AS accepted_crawl_result_count,
    max(updated_at) AS last_crawl_result_updated_at
  FROM dagster_brreg.domain_crawl_results
  GROUP BY raw_record_id
),
proposal_counts AS (
  SELECT
    raw_record_id,
    count(*) AS domain_proposal_count,
    count(*) FILTER (WHERE status = 'accepted') AS accepted_proposal_count,
    max(updated_at) AS last_proposal_updated_at
  FROM dagster_brreg.domain_proposals
  GROUP BY raw_record_id
),
best_proposals AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    normalized_domain AS best_domain,
    domain AS best_domain_observed_value,
    score AS best_domain_score,
    signals AS best_domain_signals,
    status AS best_domain_status,
    evidence AS best_domain_evidence
  FROM dagster_brreg.domain_proposals
  WHERE status IN ('proposed', 'accepted')
  ORDER BY raw_record_id, score DESC, updated_at DESC, normalized_domain ASC
)
SELECT
  rr.id AS raw_record_id,
  rr.organization_number,
  rr.organization_name,
  rr.website,
  coalesce(cc.domain_candidate_count, 0) AS domain_candidate_count,
  coalesce(cc.accepted_candidate_count, 0) AS accepted_candidate_count,
  coalesce(cc.website_field_candidate_count, 0) AS website_field_candidate_count,
  coalesce(sc.duckduckgo_search_result_count, 0) AS duckduckgo_search_result_count,
  coalesce(crc.crawl_result_count, 0) AS crawl_result_count,
  coalesce(crc.accepted_crawl_result_count, 0) AS accepted_crawl_result_count,
  coalesce(cc.web_search_llm_candidate_count, 0) AS web_search_llm_candidate_count,
  coalesce(csc.domain_candidates_by_signal, '{}'::jsonb) AS domain_candidates_by_signal,
  coalesce(pc.domain_proposal_count, 0) AS domain_proposal_count,
  coalesce(pc.accepted_proposal_count, 0) AS accepted_proposal_count,
  bp.best_domain,
  bp.best_domain_observed_value,
  bp.best_domain_score,
  bp.best_domain_signals,
  bp.best_domain_status,
  bp.best_domain_evidence,
  cc.last_candidate_updated_at,
  sc.last_search_result_updated_at,
  crc.last_crawl_result_updated_at,
  pc.last_proposal_updated_at
FROM dagster_brreg.raw_records rr
LEFT JOIN candidate_counts cc
  ON cc.raw_record_id = rr.id
LEFT JOIN candidate_signal_counts csc
  ON csc.raw_record_id = rr.id
LEFT JOIN search_counts sc
  ON sc.raw_record_id = rr.id
LEFT JOIN crawl_counts crc
  ON crc.raw_record_id = rr.id
LEFT JOIN proposal_counts pc
  ON pc.raw_record_id = rr.id
LEFT JOIN best_proposals bp
  ON bp.raw_record_id = rr.id
WHERE rr.is_current = true;
