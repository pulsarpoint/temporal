UPDATE dagster_brreg.translation_results tr
SET
  model = coalesce(tr.model, er.metadata->>'model'),
  prompt_version = coalesce(tr.prompt_version, er.metadata->>'prompt_version')
FROM dagster_brreg.task_attempts ta
JOIN dagster_brreg.enrichment_runs er
  ON er.id = ta.enrichment_run_id
WHERE tr.task_attempt_id = ta.id
  AND tr.status = 'failed'
  AND ta.task_type = 'translate'
  AND (tr.model IS NULL OR tr.prompt_version IS NULL)
  AND er.metadata ? 'model'
  AND er.metadata ? 'prompt_version';
