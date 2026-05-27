CREATE OR REPLACE VIEW dagster_brreg.v_raw_records_asset_state AS
SELECT
  count(*)::int AS total_rows,
  count(*) FILTER (WHERE is_current)::int AS current_rows,
  count(*) FILTER (WHERE NOT is_current)::int AS not_current_rows,
  0::int AS pending_rows,
  0::int AS running_rows,
  0::int AS failed_retryable_rows,
  0::int AS failed_terminal_rows,
  count(*) FILTER (WHERE is_current)::int AS succeeded_rows,
  0::int AS skipped_rows,
  0::int AS missing_artifact_rows,
  0::int AS eligible_rows,
  (count(*) FILTER (WHERE is_current) > 0) AS is_complete,
  false AS is_blocked
FROM dagster_brreg.raw_records;

CREATE OR REPLACE VIEW dagster_brreg.v_translation_asset_rows AS
WITH current_raw AS (
  SELECT id, organization_number, organization_name
  FROM dagster_brreg.raw_records
  WHERE is_current = true
),
latest_result AS (
  SELECT DISTINCT ON (raw_record_id, coalesce(model, ''), coalesce(prompt_version, ''))
    raw_record_id,
    coalesce(model, '') AS model,
    coalesce(prompt_version, '') AS prompt_version,
    status,
    error,
    created_at
  FROM dagster_brreg.translation_results
  ORDER BY raw_record_id, coalesce(model, ''), coalesce(prompt_version, ''), created_at DESC
)
SELECT
  cr.id AS raw_record_id,
  cr.organization_number,
  cr.organization_name,
  lr.model,
  lr.prompt_version,
  rts.status AS task_status,
  rts.next_retry_at,
  rts.last_started_at AS lease_started_at,
  rts.error_category,
  rts.error_code,
  rts.retry_strategy,
  lr.status AS artifact_status,
  lr.error AS artifact_error,
  lr.created_at AS artifact_created_at
FROM current_raw cr
LEFT JOIN dagster_brreg.raw_record_task_states rts
  ON rts.raw_record_id = cr.id
 AND rts.task_type = 'translate'
LEFT JOIN latest_result lr
  ON lr.raw_record_id = cr.id;

CREATE OR REPLACE VIEW dagster_brreg.v_translation_asset_state AS
WITH current_raw AS (
  SELECT id
  FROM dagster_brreg.raw_records
  WHERE is_current = true
),
raw_counts AS (
  SELECT count(*)::int AS total_rows
  FROM current_raw
),
task_counts AS (
  SELECT
    count(*) FILTER (WHERE rts.status IS NULL)::int AS no_state_rows,
    count(*) FILTER (WHERE rts.status = 'pending')::int AS pending_rows,
    count(*) FILTER (WHERE rts.status = 'running')::int AS running_rows,
    count(*) FILTER (WHERE rts.status = 'failed_retryable')::int AS failed_retryable_rows,
    count(*) FILTER (WHERE rts.status = 'failed_terminal')::int AS failed_terminal_rows,
    count(*) FILTER (WHERE rts.status = 'succeeded')::int AS succeeded_rows,
    count(*) FILTER (WHERE rts.status = 'skipped')::int AS skipped_rows,
    count(*) FILTER (
      WHERE rts.status IS NULL
         OR rts.status = 'pending'
         OR (rts.status = 'failed_retryable' AND coalesce(rts.next_retry_at <= now(), true))
         OR rts.status = 'running'
    )::int AS eligible_rows
  FROM current_raw cr
  LEFT JOIN dagster_brreg.raw_record_task_states rts
    ON rts.raw_record_id = cr.id
   AND rts.task_type = 'translate'
),
latest_result AS (
  SELECT DISTINCT ON (raw_record_id, coalesce(model, ''), coalesce(prompt_version, ''))
    raw_record_id,
    coalesce(model, '') AS model,
    coalesce(prompt_version, '') AS prompt_version,
    status
  FROM dagster_brreg.translation_results
  ORDER BY raw_record_id, coalesce(model, ''), coalesce(prompt_version, ''), created_at DESC
),
artifact_counts AS (
  SELECT
    lr.model,
    lr.prompt_version,
    count(*) FILTER (WHERE lr.status = 'succeeded')::int AS artifact_succeeded_rows,
    count(*) FILTER (WHERE lr.status = 'skipped')::int AS artifact_skipped_rows,
    count(*) FILTER (WHERE lr.status = 'failed')::int AS artifact_failed_rows
  FROM latest_result lr
  JOIN current_raw cr ON cr.id = lr.raw_record_id
  GROUP BY lr.model, lr.prompt_version
)
SELECT
  ac.model,
  ac.prompt_version,
  rc.total_rows,
  tc.no_state_rows,
  tc.pending_rows,
  tc.running_rows,
  tc.failed_retryable_rows,
  tc.failed_terminal_rows,
  tc.succeeded_rows,
  tc.skipped_rows,
  greatest(
    rc.total_rows
      - ac.artifact_succeeded_rows
      - ac.artifact_skipped_rows
      - ac.artifact_failed_rows,
    0
  )::int AS missing_artifact_rows,
  tc.eligible_rows,
  (
    rc.total_rows > 0
    AND ac.artifact_failed_rows = 0
    AND (
      rc.total_rows
        - ac.artifact_succeeded_rows
        - ac.artifact_skipped_rows
        - ac.artifact_failed_rows
    ) = 0
  ) AS is_complete,
  (tc.failed_terminal_rows > 0 OR ac.artifact_failed_rows > 0) AS is_blocked
FROM artifact_counts ac
CROSS JOIN raw_counts rc
CROSS JOIN task_counts tc;

CREATE OR REPLACE VIEW dagster_brreg.v_domain_asset_rows AS
WITH current_raw AS (
  SELECT id, organization_number, organization_name
  FROM dagster_brreg.raw_records
  WHERE is_current = true
),
latest_result AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    best_domain,
    error,
    created_at
  FROM dagster_brreg.domain_results
  ORDER BY raw_record_id, created_at DESC
)
SELECT
  cr.id AS raw_record_id,
  cr.organization_number,
  cr.organization_name,
  rts.status AS task_status,
  rts.next_retry_at,
  rts.last_started_at AS lease_started_at,
  rts.error_category,
  rts.error_code,
  rts.retry_strategy,
  lr.status AS artifact_status,
  lr.best_domain,
  lr.error AS artifact_error,
  lr.created_at AS artifact_created_at
FROM current_raw cr
LEFT JOIN dagster_brreg.raw_record_task_states rts
  ON rts.raw_record_id = cr.id
 AND rts.task_type = 'domain_results'
LEFT JOIN latest_result lr
  ON lr.raw_record_id = cr.id;

CREATE OR REPLACE VIEW dagster_brreg.v_domain_asset_state AS
SELECT
  count(*)::int AS total_rows,
  count(*) FILTER (WHERE task_status IS NULL)::int AS no_state_rows,
  count(*) FILTER (WHERE task_status = 'pending')::int AS pending_rows,
  count(*) FILTER (WHERE task_status = 'running')::int AS running_rows,
  count(*) FILTER (WHERE task_status = 'failed_retryable')::int AS failed_retryable_rows,
  count(*) FILTER (WHERE task_status = 'failed_terminal')::int AS failed_terminal_rows,
  count(*) FILTER (WHERE task_status = 'succeeded')::int AS succeeded_rows,
  count(*) FILTER (WHERE task_status = 'skipped')::int AS skipped_rows,
  count(*) FILTER (WHERE artifact_status IS NULL)::int AS missing_artifact_rows,
  count(*) FILTER (
    WHERE task_status IS NULL
       OR task_status = 'pending'
       OR (task_status = 'failed_retryable' AND coalesce(next_retry_at <= now(), true))
       OR task_status = 'running'
  )::int AS eligible_rows,
  (count(*) > 0 AND bool_and(coalesce(artifact_status IN ('succeeded', 'not_found', 'partial'), false))) AS is_complete,
  bool_or(task_status = 'failed_terminal' OR artifact_status = 'failed') AS is_blocked
FROM dagster_brreg.v_domain_asset_rows;

CREATE OR REPLACE VIEW dagster_brreg.v_financial_asset_rows AS
WITH current_raw AS (
  SELECT id, organization_number, organization_name
  FROM dagster_brreg.raw_records
  WHERE is_current = true
),
latest_result AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    original_currency,
    error,
    created_at
  FROM dagster_brreg.currency_results
  ORDER BY raw_record_id, created_at DESC
)
SELECT
  cr.id AS raw_record_id,
  cr.organization_number,
  cr.organization_name,
  rts.status AS task_status,
  rts.next_retry_at,
  rts.last_started_at AS lease_started_at,
  rts.error_category,
  rts.error_code,
  rts.retry_strategy,
  lr.status AS artifact_status,
  lr.original_currency,
  lr.error AS artifact_error,
  lr.created_at AS artifact_created_at
FROM current_raw cr
LEFT JOIN dagster_brreg.raw_record_task_states rts
  ON rts.raw_record_id = cr.id
 AND rts.task_type = 'currency_conversion'
LEFT JOIN latest_result lr
  ON lr.raw_record_id = cr.id;

CREATE OR REPLACE VIEW dagster_brreg.v_financial_asset_state AS
SELECT
  count(*)::int AS total_rows,
  count(*) FILTER (WHERE task_status IS NULL)::int AS no_state_rows,
  count(*) FILTER (WHERE task_status = 'pending')::int AS pending_rows,
  count(*) FILTER (WHERE task_status = 'running')::int AS running_rows,
  count(*) FILTER (WHERE task_status = 'failed_retryable')::int AS failed_retryable_rows,
  count(*) FILTER (WHERE task_status = 'failed_terminal')::int AS failed_terminal_rows,
  count(*) FILTER (WHERE task_status = 'succeeded')::int AS succeeded_rows,
  count(*) FILTER (WHERE task_status = 'skipped')::int AS skipped_rows,
  count(*) FILTER (WHERE artifact_status IS NULL)::int AS missing_artifact_rows,
  count(*) FILTER (
    WHERE task_status IS NULL
       OR task_status = 'pending'
       OR (task_status = 'failed_retryable' AND coalesce(next_retry_at <= now(), true))
       OR task_status = 'running'
  )::int AS eligible_rows,
  (count(*) > 0 AND bool_and(coalesce(artifact_status IN ('succeeded', 'skipped', 'not_available'), false))) AS is_complete,
  bool_or(task_status = 'failed_terminal' OR artifact_status = 'failed') AS is_blocked
FROM dagster_brreg.v_financial_asset_rows;

CREATE OR REPLACE VIEW dagster_brreg.v_enhanced_asset_rows AS
WITH current_raw AS (
  SELECT id, organization_number, organization_name
  FROM dagster_brreg.raw_records
  WHERE is_current = true
),
latest_result AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    error,
    built_at
  FROM dagster_brreg.enhanced_records
  ORDER BY raw_record_id, built_at DESC
)
SELECT
  cr.id AS raw_record_id,
  cr.organization_number,
  cr.organization_name,
  rts.status AS task_status,
  rts.next_retry_at,
  rts.last_started_at AS lease_started_at,
  rts.error_category,
  rts.error_code,
  rts.retry_strategy,
  lr.status AS artifact_status,
  lr.error AS artifact_error,
  lr.built_at AS artifact_created_at
FROM current_raw cr
LEFT JOIN dagster_brreg.raw_record_task_states rts
  ON rts.raw_record_id = cr.id
 AND rts.task_type = 'build_enhanced'
LEFT JOIN latest_result lr
  ON lr.raw_record_id = cr.id;

CREATE OR REPLACE VIEW dagster_brreg.v_enhanced_asset_state AS
WITH current_raw AS (
  SELECT id
  FROM dagster_brreg.raw_records
  WHERE is_current = true
),
task_rows AS (
  SELECT *
  FROM dagster_brreg.v_enhanced_asset_rows
),
translation_ready AS (
  SELECT raw_record_id
  FROM dagster_brreg.raw_record_task_states
  WHERE task_type = 'translate'
    AND status IN ('succeeded', 'skipped')
),
domain_ready AS (
  SELECT raw_record_id
  FROM dagster_brreg.raw_record_task_states
  WHERE task_type = 'domain_results'
    AND status IN ('succeeded', 'skipped')
),
financial_ready AS (
  SELECT raw_record_id
  FROM dagster_brreg.raw_record_task_states
  WHERE task_type = 'currency_conversion'
    AND status IN ('succeeded', 'skipped')
),
eligible_ready AS (
  SELECT id AS raw_record_id
  FROM dagster_brreg.mv_brreg_enhanced_ready_records
)
SELECT
  count(tr.raw_record_id)::int AS total_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.task_status IS NULL)::int AS no_state_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.task_status = 'pending')::int AS pending_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.task_status = 'running')::int AS running_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.task_status = 'failed_retryable')::int AS failed_retryable_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.task_status = 'failed_terminal')::int AS failed_terminal_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.task_status = 'succeeded')::int AS succeeded_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.task_status = 'skipped')::int AS skipped_rows,
  count(er.raw_record_id)::int AS missing_artifact_rows,
  count(er.raw_record_id)::int AS eligible_rows,
  count(tts.raw_record_id)::int AS translation_ready_rows,
  count(dts.raw_record_id)::int AS domain_ready_rows,
  count(fts.raw_record_id)::int AS financial_ready_rows,
  count(er.raw_record_id)::int AS eligible_for_enhanced_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.artifact_status IN ('built', 'published'))::int AS enhanced_built_rows,
  count(er.raw_record_id)::int AS enhanced_missing_rows,
  count(tr.raw_record_id) FILTER (WHERE tr.artifact_status = 'publish_failed')::int AS enhanced_failed_rows,
  (
    count(tr.raw_record_id) > 0
    AND count(er.raw_record_id) = 0
    AND count(er.raw_record_id) = 0
    AND count(tr.raw_record_id) FILTER (WHERE tr.artifact_status = 'publish_failed') = 0
  ) AS is_complete,
  bool_or(tr.task_status = 'failed_terminal' OR tr.artifact_status = 'publish_failed') AS is_blocked
FROM current_raw cr
LEFT JOIN task_rows tr ON tr.raw_record_id = cr.id
LEFT JOIN translation_ready tts ON tts.raw_record_id = cr.id
LEFT JOIN domain_ready dts ON dts.raw_record_id = cr.id
LEFT JOIN financial_ready fts ON fts.raw_record_id = cr.id
LEFT JOIN eligible_ready er ON er.raw_record_id = cr.id;
