CREATE INDEX IF NOT EXISTS idx_dagster_brreg_raw_records_current_last_seen_id
  ON dagster_brreg.raw_records (last_seen_at, id)
  WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_states_pending_retry_queue
  ON dagster_brreg.raw_record_task_states (task_type, status, next_retry_at, raw_record_id)
  WHERE status IN ('pending', 'failed_retryable');

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_states_running_stale_queue
  ON dagster_brreg.raw_record_task_states (task_type, status, last_started_at, raw_record_id)
  WHERE status = 'running';
