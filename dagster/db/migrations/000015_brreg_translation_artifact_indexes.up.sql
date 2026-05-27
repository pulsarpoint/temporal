CREATE INDEX IF NOT EXISTS idx_dagster_brreg_translation_results_model_prompt_raw_latest
  ON dagster_brreg.translation_results (model, prompt_version, raw_record_id, created_at DESC);
