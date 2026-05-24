INSERT INTO dagster_brreg.domain_candidates (
  raw_record_id,
  task_attempt_id,
  domain,
  normalized_domain,
  signal,
  confidence,
  status,
  evidence,
  metadata,
  created_at,
  updated_at
)
SELECT
  raw_record_id,
  task_attempt_id,
  domain,
  regexp_replace(normalized_domain, '/+$', ''),
  signal,
  confidence,
  status,
  evidence,
  metadata,
  created_at,
  now()
FROM dagster_brreg.domain_candidates
WHERE normalized_domain ~ '/+$'
ON CONFLICT (raw_record_id, normalized_domain, signal) DO UPDATE
SET
  confidence = GREATEST(dagster_brreg.domain_candidates.confidence, EXCLUDED.confidence),
  evidence = dagster_brreg.domain_candidates.evidence || EXCLUDED.evidence,
  metadata = dagster_brreg.domain_candidates.metadata || EXCLUDED.metadata,
  updated_at = now();

DELETE FROM dagster_brreg.domain_candidates
WHERE normalized_domain ~ '/+$';

INSERT INTO dagster_brreg.domain_proposals (
  raw_record_id,
  task_attempt_id,
  domain,
  normalized_domain,
  score,
  signals,
  status,
  evidence,
  metadata,
  created_at,
  updated_at
)
SELECT
  raw_record_id,
  task_attempt_id,
  domain,
  regexp_replace(normalized_domain, '/+$', ''),
  score,
  signals,
  status,
  evidence,
  metadata,
  created_at,
  now()
FROM dagster_brreg.domain_proposals
WHERE normalized_domain ~ '/+$'
ON CONFLICT (raw_record_id, normalized_domain) DO UPDATE
SET
  score = GREATEST(dagster_brreg.domain_proposals.score, EXCLUDED.score),
  signals = (
    SELECT array_agg(DISTINCT signal ORDER BY signal)
    FROM unnest(dagster_brreg.domain_proposals.signals || EXCLUDED.signals) AS signal
  ),
  evidence = dagster_brreg.domain_proposals.evidence || EXCLUDED.evidence,
  metadata = dagster_brreg.domain_proposals.metadata || EXCLUDED.metadata,
  updated_at = now();

DELETE FROM dagster_brreg.domain_proposals
WHERE normalized_domain ~ '/+$';
