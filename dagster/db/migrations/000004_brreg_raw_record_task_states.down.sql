DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_task_states_updated;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_task_states_queue;
DROP TABLE IF EXISTS dagster_brreg.raw_record_task_states;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_candidates_raw_updated;
ALTER TABLE dagster_brreg.domain_candidates
  DROP COLUMN IF EXISTS updated_at;
