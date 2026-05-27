UPDATE dagster_brreg.translation_results tr
SET
  model = NULL,
  prompt_version = NULL
FROM dagster_brreg.task_attempts ta
JOIN dagster_brreg.enrichment_runs er
  ON er.id = ta.enrichment_run_id
WHERE tr.task_attempt_id = ta.id
  AND tr.status = 'failed'
  AND ta.task_type = 'translate'
  AND tr.translated_payload IS NULL
  AND tr.model = er.metadata->>'model'
  AND tr.prompt_version = er.metadata->>'prompt_version';
