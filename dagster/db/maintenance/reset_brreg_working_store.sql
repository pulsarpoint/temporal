-- Destructive BRREG Dagster working-store reset for clean-state MVP testing.
-- This clears Dagster BRREG orchestration/artifact tables only. It does not
-- touch Corpscout source/suggestion tables outside the dagster_brreg schema.

TRUNCATE TABLE
  dagster_brreg.enhanced_records,
  dagster_brreg.currency_results,
  dagster_brreg.domain_results,
  dagster_brreg.domain_crawl_results,
  dagster_brreg.domain_search_results,
  dagster_brreg.domain_proposals,
  dagster_brreg.domain_candidates,
  dagster_brreg.translation_results,
  dagster_brreg.translation_cache,
  dagster_brreg.task_attempts,
  dagster_brreg.raw_record_task_states,
  dagster_brreg.raw_record_task_cursors,
  dagster_brreg.raw_records,
  dagster_brreg.bulk_snapshots,
  dagster_brreg.enrichment_runs
RESTART IDENTITY CASCADE;
