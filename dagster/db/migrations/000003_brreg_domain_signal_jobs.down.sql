ALTER TABLE dagster_brreg.enrichment_runs
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_runs_type;

ALTER TABLE dagster_brreg.enrichment_runs
  ADD CONSTRAINT chk_dagster_brreg_runs_type CHECK (
    run_type IN (
      'bulk_ingest',
      'translate',
      'discover_domains',
      'build_enhanced',
      'full_enrichment',
      'retry_failed',
      'publish'
    )
  );

ALTER TABLE dagster_brreg.task_attempts
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempt_type;

ALTER TABLE dagster_brreg.task_attempts
  ADD CONSTRAINT chk_dagster_brreg_task_attempt_type CHECK (
    task_type IN ('parse_raw', 'translate', 'discover_domains', 'extract_financials', 'build_enhanced', 'publish')
  );

DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_proposals_status;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_proposals_raw_score;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_candidates_signal;
DROP TABLE IF EXISTS dagster_brreg.domain_proposals;
