DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_task_states_failure_retry;

DROP VIEW IF EXISTS dagster_brreg.v_task_failure_summary;

CREATE OR REPLACE VIEW dagster_brreg.v_failed_task_states AS
SELECT
  rr.id AS raw_record_id,
  rr.organization_number,
  rr.organization_name,
  rr.website,
  rts.task_type,
  rts.status,
  rts.attempt_count,
  rts.last_attempt_id,
  rts.last_started_at,
  rts.last_finished_at,
  rts.next_retry_at,
  coalesce(rts.next_retry_at <= now(), false) AS retry_ready,
  rts.last_error,
  rts.result_summary,
  rts.updated_at
FROM dagster_brreg.raw_record_task_states rts
JOIN dagster_brreg.raw_records rr
  ON rr.id = rts.raw_record_id
WHERE rts.status IN ('failed_retryable', 'failed_terminal')
ORDER BY rts.next_retry_at NULLS LAST, rts.updated_at DESC;

CREATE OR REPLACE VIEW dagster_brreg.v_raw_record_task_overview AS
SELECT
  rr.id AS raw_record_id,
  rr.organization_number,
  rr.organization_name,
  rr.registration_status,
  rr.website,
  rr.is_current,
  count(rts.task_type) AS tracked_task_count,
  count(*) FILTER (WHERE rts.status = 'succeeded') AS succeeded_task_count,
  count(*) FILTER (WHERE rts.status = 'skipped') AS skipped_task_count,
  count(*) FILTER (WHERE rts.status = 'running') AS running_task_count,
  count(*) FILTER (WHERE rts.status = 'failed_retryable') AS failed_retryable_task_count,
  count(*) FILTER (WHERE rts.status = 'failed_terminal') AS failed_terminal_task_count,
  min(rts.next_retry_at) FILTER (WHERE rts.status = 'failed_retryable') AS next_retry_at,
  coalesce(
    jsonb_object_agg(
      rts.task_type,
      jsonb_build_object(
        'status', rts.status,
        'attempt_count', rts.attempt_count,
        'last_started_at', rts.last_started_at,
        'last_finished_at', rts.last_finished_at,
        'next_retry_at', rts.next_retry_at,
        'last_error', rts.last_error
      )
      ORDER BY rts.task_type
    ) FILTER (WHERE rts.task_type IS NOT NULL),
    '{}'::jsonb
  ) AS task_states
FROM dagster_brreg.raw_records rr
LEFT JOIN dagster_brreg.raw_record_task_states rts
  ON rts.raw_record_id = rr.id
GROUP BY rr.id;

ALTER TABLE dagster_brreg.raw_record_task_states
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_states_retry_strategy,
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_states_error_category,
  DROP COLUMN IF EXISTS retry_strategy,
  DROP COLUMN IF EXISTS error_code,
  DROP COLUMN IF EXISTS error_category;

ALTER TABLE dagster_brreg.task_attempts
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempts_retry_strategy,
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempts_error_category,
  DROP COLUMN IF EXISTS retry_strategy,
  DROP COLUMN IF EXISTS error_code,
  DROP COLUMN IF EXISTS error_category;
