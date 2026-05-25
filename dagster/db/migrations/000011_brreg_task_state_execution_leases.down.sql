CREATE TABLE IF NOT EXISTS dagster_brreg.raw_record_task_run_leases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_type TEXT NOT NULL,
  enrichment_run_id UUID REFERENCES dagster_brreg.enrichment_runs(id) ON DELETE CASCADE,
  dagster_run_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  lease_until TIMESTAMPTZ NOT NULL,
  max_concurrent_runs INTEGER NOT NULL DEFAULT 1,
  released_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_dagster_brreg_task_run_leases_status CHECK (
    status IN ('active', 'released', 'expired')
  ),
  CONSTRAINT chk_dagster_brreg_task_run_leases_max_concurrent CHECK (max_concurrent_runs > 0),
  CONSTRAINT chk_dagster_brreg_task_run_leases_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_task_run_leases_active
  ON dagster_brreg.raw_record_task_run_leases (task_type, lease_until)
  WHERE status = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS idx_dagster_brreg_task_run_leases_active_run
  ON dagster_brreg.raw_record_task_run_leases (task_type, dagster_run_id)
  WHERE status = 'active';

CREATE TABLE IF NOT EXISTS dagster_brreg.raw_record_task_cursors (
  task_type TEXT PRIMARY KEY,
  last_seen_at TIMESTAMPTZ,
  last_raw_record_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_task_states_running_lease;

ALTER TABLE dagster_brreg.raw_record_task_states
  DROP COLUMN IF EXISTS lease_until;
