CREATE TABLE IF NOT EXISTS dagster_brreg.raw_record_task_cursors (
  task_type TEXT PRIMARY KEY,
  last_seen_at TIMESTAMPTZ,
  last_raw_record_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
