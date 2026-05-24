CREATE OR REPLACE VIEW dagster_brreg.v_enrichment_run_summary AS
SELECT
  er.id,
  er.dagster_run_id,
  er.run_type,
  er.status,
  er.started_at,
  er.finished_at,
  er.finished_at - er.started_at AS duration,
  er.records_seen,
  er.records_completed,
  er.records_failed,
  er.error,
  er.metadata,
  count(ta.id) AS task_attempts,
  count(*) FILTER (WHERE ta.status = 'running') AS task_attempts_running,
  count(*) FILTER (WHERE ta.status = 'succeeded') AS task_attempts_succeeded,
  count(*) FILTER (WHERE ta.status = 'failed') AS task_attempts_failed,
  count(*) FILTER (WHERE ta.status = 'skipped') AS task_attempts_skipped,
  count(DISTINCT tr.id) AS translation_results,
  count(DISTINCT dc.id) AS domain_candidates,
  count(DISTINCT dp.id) AS domain_proposals
FROM dagster_brreg.enrichment_runs er
LEFT JOIN dagster_brreg.task_attempts ta
  ON ta.enrichment_run_id = er.id
LEFT JOIN dagster_brreg.translation_results tr
  ON tr.task_attempt_id = ta.id
LEFT JOIN dagster_brreg.domain_candidates dc
  ON dc.task_attempt_id = ta.id
LEFT JOIN dagster_brreg.domain_proposals dp
  ON dp.task_attempt_id = ta.id
GROUP BY er.id;

CREATE OR REPLACE VIEW dagster_brreg.v_task_state_summary AS
SELECT
  rts.task_type,
  rts.status,
  count(*) AS row_count,
  count(*) FILTER (
    WHERE rts.status = 'failed_retryable'
      AND rts.next_retry_at <= now()
  ) AS retry_ready_count,
  count(*) FILTER (
    WHERE rts.status = 'running'
      AND rts.last_started_at < now() - interval '30 minutes'
  ) AS stale_running_count,
  min(rts.next_retry_at) FILTER (WHERE rts.next_retry_at IS NOT NULL) AS next_retry_at,
  max(rts.updated_at) AS last_updated_at
FROM dagster_brreg.raw_record_task_states rts
GROUP BY rts.task_type, rts.status;

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

CREATE OR REPLACE VIEW dagster_brreg.v_domain_enrichment_summary AS
WITH candidate_counts AS (
  SELECT
    raw_record_id,
    count(*) AS domain_candidate_count,
    count(*) FILTER (WHERE status = 'accepted') AS accepted_candidate_count,
    max(updated_at) AS last_candidate_updated_at
  FROM dagster_brreg.domain_candidates
  GROUP BY raw_record_id
),
candidate_signal_counts AS (
  SELECT
    raw_record_id,
    jsonb_object_agg(signal, signal_count ORDER BY signal) AS domain_candidates_by_signal
  FROM (
    SELECT
      raw_record_id,
      signal,
      count(*) AS signal_count
    FROM dagster_brreg.domain_candidates
    GROUP BY raw_record_id, signal
  ) counts
  GROUP BY raw_record_id
),
proposal_counts AS (
  SELECT
    raw_record_id,
    count(*) AS domain_proposal_count,
    count(*) FILTER (WHERE status = 'accepted') AS accepted_proposal_count,
    max(updated_at) AS last_proposal_updated_at
  FROM dagster_brreg.domain_proposals
  GROUP BY raw_record_id
),
best_proposals AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    normalized_domain AS best_domain,
    domain AS best_domain_observed_value,
    score AS best_domain_score,
    signals AS best_domain_signals,
    status AS best_domain_status,
    evidence AS best_domain_evidence
  FROM dagster_brreg.domain_proposals
  WHERE status IN ('proposed', 'accepted')
  ORDER BY raw_record_id, score DESC, updated_at DESC, normalized_domain ASC
)
SELECT
  rr.id AS raw_record_id,
  rr.organization_number,
  rr.organization_name,
  rr.website,
  coalesce(cc.domain_candidate_count, 0) AS domain_candidate_count,
  coalesce(cc.accepted_candidate_count, 0) AS accepted_candidate_count,
  coalesce(csc.domain_candidates_by_signal, '{}'::jsonb) AS domain_candidates_by_signal,
  coalesce(pc.domain_proposal_count, 0) AS domain_proposal_count,
  coalesce(pc.accepted_proposal_count, 0) AS accepted_proposal_count,
  bp.best_domain,
  bp.best_domain_observed_value,
  bp.best_domain_score,
  bp.best_domain_signals,
  bp.best_domain_status,
  bp.best_domain_evidence,
  cc.last_candidate_updated_at,
  pc.last_proposal_updated_at
FROM dagster_brreg.raw_records rr
LEFT JOIN candidate_counts cc
  ON cc.raw_record_id = rr.id
LEFT JOIN candidate_signal_counts csc
  ON csc.raw_record_id = rr.id
LEFT JOIN proposal_counts pc
  ON pc.raw_record_id = rr.id
LEFT JOIN best_proposals bp
  ON bp.raw_record_id = rr.id
WHERE rr.is_current = true;
