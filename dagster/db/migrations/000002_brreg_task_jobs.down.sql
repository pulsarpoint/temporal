ALTER TABLE dagster_brreg.enrichment_runs
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_runs_type;

ALTER TABLE dagster_brreg.enrichment_runs
  ADD CONSTRAINT chk_dagster_brreg_runs_type CHECK (
    run_type IN ('bulk_ingest', 'full_enrichment', 'retry_failed', 'publish')
  );

DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_task_success;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_translation_success;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_translation_cache_lookup;
DROP TABLE IF EXISTS dagster_brreg.translation_cache;
