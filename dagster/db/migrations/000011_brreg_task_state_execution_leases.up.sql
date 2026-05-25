ALTER TABLE dagster_brreg.raw_record_task_states
  ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_states_running_lease
  ON dagster_brreg.raw_record_task_states (task_type, lease_until, raw_record_id)
  WHERE status = 'running';

DELETE FROM dagster_brreg.raw_record_task_states
WHERE status = 'pending'
  AND attempt_count = 0
  AND last_attempt_id IS NULL
  AND last_started_at IS NULL
  AND last_finished_at IS NULL
  AND last_error IS NULL;

DROP TABLE IF EXISTS dagster_brreg.raw_record_task_run_leases;

DROP TABLE IF EXISTS dagster_brreg.raw_record_task_cursors;
