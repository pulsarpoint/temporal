CREATE OR REPLACE VIEW dagster_brreg.v_domain_enrichment_summary AS
WITH candidate_counts AS (
  SELECT
    raw_record_id,
    count(*) AS domain_candidate_count,
    count(*) FILTER (WHERE status = 'accepted') AS accepted_candidate_count,
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
  pc.last_proposal_updated_at
FROM dagster_brreg.raw_records rr
LEFT JOIN candidate_counts cc
  ON cc.raw_record_id = rr.id
LEFT JOIN candidate_signal_counts csc
  ON csc.raw_record_id = rr.id
LEFT JOIN proposal_counts pc
  ON pc.raw_record_id = rr.id
LEFT JOIN best_proposals bp
  ON bp.raw_record_id = rr.id
WHERE rr.is_current = true;

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

DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_crawl_results_decision;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_crawl_results_raw;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_search_results_raw;
DROP TABLE IF EXISTS dagster_brreg.domain_crawl_results;
DROP TABLE IF EXISTS dagster_brreg.domain_search_results;
