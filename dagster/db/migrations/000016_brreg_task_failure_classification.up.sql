ALTER TABLE dagster_brreg.task_attempts
  ADD COLUMN IF NOT EXISTS error_category TEXT,
  ADD COLUMN IF NOT EXISTS error_code TEXT,
  ADD COLUMN IF NOT EXISTS retry_strategy TEXT;

ALTER TABLE dagster_brreg.raw_record_task_states
  ADD COLUMN IF NOT EXISTS error_category TEXT,
  ADD COLUMN IF NOT EXISTS error_code TEXT,
  ADD COLUMN IF NOT EXISTS retry_strategy TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_dagster_brreg_task_attempts_error_category'
  ) THEN
    ALTER TABLE dagster_brreg.task_attempts
      ADD CONSTRAINT chk_dagster_brreg_task_attempts_error_category CHECK (
        error_category IS NULL OR error_category IN (
          'transient_external',
          'rate_limited',
          'invalid_llm_output',
          'invalid_input',
          'blocked_by_config',
          'not_found',
          'internal_error',
          'interrupted',
          'unknown'
        )
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_dagster_brreg_task_attempts_retry_strategy'
  ) THEN
    ALTER TABLE dagster_brreg.task_attempts
      ADD CONSTRAINT chk_dagster_brreg_task_attempts_retry_strategy CHECK (
        retry_strategy IS NULL OR retry_strategy IN (
          'automatic',
          'change_model_or_prompt',
          'manual_config',
          'manual_input',
          'not_retryable'
        )
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_dagster_brreg_task_states_error_category'
  ) THEN
    ALTER TABLE dagster_brreg.raw_record_task_states
      ADD CONSTRAINT chk_dagster_brreg_task_states_error_category CHECK (
        error_category IS NULL OR error_category IN (
          'transient_external',
          'rate_limited',
          'invalid_llm_output',
          'invalid_input',
          'blocked_by_config',
          'not_found',
          'internal_error',
          'interrupted',
          'unknown'
        )
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_dagster_brreg_task_states_retry_strategy'
  ) THEN
    ALTER TABLE dagster_brreg.raw_record_task_states
      ADD CONSTRAINT chk_dagster_brreg_task_states_retry_strategy CHECK (
        retry_strategy IS NULL OR retry_strategy IN (
          'automatic',
          'change_model_or_prompt',
          'manual_config',
          'manual_input',
          'not_retryable'
        )
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_states_failure_retry
  ON dagster_brreg.raw_record_task_states (task_type, status, error_category, next_retry_at)
  WHERE status IN ('failed_retryable', 'failed_terminal');

CREATE OR REPLACE VIEW dagster_brreg.v_task_failure_summary AS
SELECT
  rts.task_type,
  rts.status,
  coalesce(rts.error_category, 'unknown') AS error_category,
  coalesce(rts.error_code, 'unknown_error') AS error_code,
  coalesce(rts.retry_strategy, 'automatic') AS retry_strategy,
  count(*) AS row_count,
  count(*) FILTER (
    WHERE rts.status = 'failed_retryable'
      AND rts.next_retry_at <= now()
  ) AS retry_ready_count,
  min(rts.next_retry_at) FILTER (WHERE rts.next_retry_at IS NOT NULL) AS next_retry_at,
  max(rts.updated_at) AS last_updated_at
FROM dagster_brreg.raw_record_task_states rts
JOIN dagster_brreg.raw_records rr
  ON rr.id = rts.raw_record_id
WHERE rr.is_current = true
  AND rts.status IN ('failed_retryable', 'failed_terminal')
GROUP BY
  rts.task_type,
  rts.status,
  coalesce(rts.error_category, 'unknown'),
  coalesce(rts.error_code, 'unknown_error'),
  coalesce(rts.retry_strategy, 'automatic');

CREATE OR REPLACE VIEW dagster_brreg.v_failed_task_states AS
SELECT
  rr.id AS raw_record_id,
  rr.organization_number,
  rr.organization_name,
  rr.website,
  rts.task_type,
  rts.status,
  rts.error_category,
  rts.error_code,
  rts.retry_strategy,
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
        'error_category', rts.error_category,
        'error_code', rts.error_code,
        'retry_strategy', rts.retry_strategy,
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
