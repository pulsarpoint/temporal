ALTER TABLE dagster_brreg.domain_candidates
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_candidates_raw_updated
  ON dagster_brreg.domain_candidates (raw_record_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS dagster_brreg.raw_record_task_states (
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  last_started_at TIMESTAMPTZ,
  last_finished_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  last_error TEXT,
  result_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (raw_record_id, task_type),
  CONSTRAINT chk_dagster_brreg_raw_record_task_states_status CHECK (
    status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_dagster_brreg_raw_record_task_states_attempt CHECK (attempt_count >= 0),
  CONSTRAINT chk_dagster_brreg_raw_record_task_states_summary_object CHECK (jsonb_typeof(result_summary) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_states_queue
  ON dagster_brreg.raw_record_task_states (task_type, status, next_retry_at, last_started_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_states_updated
  ON dagster_brreg.raw_record_task_states (task_type, updated_at DESC);

INSERT INTO dagster_brreg.raw_record_task_states (
  raw_record_id,
  task_type,
  status,
  attempt_count,
  last_attempt_id,
  last_started_at,
  last_finished_at,
  next_retry_at,
  last_error,
  result_summary
)
SELECT
  latest.raw_record_id,
  latest.task_type,
  CASE
    WHEN latest.status = 'running' THEN 'running'
    WHEN latest.status = 'succeeded' THEN 'succeeded'
    WHEN latest.status = 'skipped' THEN 'skipped'
    WHEN latest.status = 'cancelled' THEN 'cancelled'
    WHEN latest.status = 'failed' THEN 'failed_retryable'
    ELSE 'pending'
  END,
  latest.attempt,
  latest.id,
  latest.started_at,
  latest.finished_at,
  CASE
    WHEN latest.status = 'failed' THEN coalesce(latest.finished_at, now()) + interval '1 day'
    ELSE NULL
  END,
  latest.error,
  '{}'::jsonb
FROM (
  SELECT DISTINCT ON (raw_record_id, task_type)
    id,
    raw_record_id,
    task_type,
    attempt,
    status,
    started_at,
    finished_at,
    error
  FROM dagster_brreg.task_attempts
  WHERE raw_record_id IS NOT NULL
  ORDER BY raw_record_id, task_type, attempt DESC, started_at DESC NULLS LAST
) latest
ON CONFLICT (raw_record_id, task_type) DO NOTHING;
