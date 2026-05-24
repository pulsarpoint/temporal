CREATE TABLE IF NOT EXISTS dagster_brreg.translation_cache (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  category TEXT NOT NULL,
  source_lang TEXT NOT NULL DEFAULT 'no',
  target_lang TEXT NOT NULL DEFAULT 'en',
  original_hash TEXT NOT NULL,
  original_text TEXT NOT NULL,
  translated_text TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_dagster_brreg_translation_cache_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (category, source_lang, target_lang, original_hash, model, prompt_version)
);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_translation_cache_lookup
  ON dagster_brreg.translation_cache (
    category,
    source_lang,
    target_lang,
    original_hash,
    model,
    prompt_version
  );

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_translation_success
  ON dagster_brreg.translation_results (raw_record_id, created_at DESC)
  WHERE status = 'succeeded';

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_task_success
  ON dagster_brreg.domain_candidates (raw_record_id, created_at DESC)
  WHERE status IN ('candidate', 'accepted');

ALTER TABLE dagster_brreg.enrichment_runs
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_runs_type;

ALTER TABLE dagster_brreg.enrichment_runs
  ADD CONSTRAINT chk_dagster_brreg_runs_type CHECK (
    run_type IN (
      'bulk_ingest',
      'translate',
      'discover_domains',
      'build_enhanced',
      'full_enrichment',
      'retry_failed',
      'publish'
    )
  );
