CREATE MATERIALIZED VIEW IF NOT EXISTS dagster_brreg.mv_brreg_enhanced_ready_records AS
WITH latest_translation AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status,
        translated_payload,
        created_at
    FROM dagster_brreg.translation_results
    WHERE status IN ('succeeded', 'skipped')
    ORDER BY raw_record_id, created_at DESC
),
latest_domain_result AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status,
        domain_payload,
        created_at
    FROM dagster_brreg.domain_results
    WHERE status IN ('succeeded', 'not_found', 'partial')
    ORDER BY raw_record_id, created_at DESC
),
latest_currency_result AS (
    SELECT DISTINCT ON (raw_record_id)
        raw_record_id,
        status,
        original_payload,
        usd_payload,
        fx_metadata,
        created_at
    FROM dagster_brreg.currency_results
    WHERE status IN ('succeeded', 'skipped', 'not_available')
    ORDER BY raw_record_id, created_at DESC
),
domain_rows AS (
    SELECT
        ldr.raw_record_id,
        jsonb_agg(
            jsonb_build_object(
                'domain', candidate->>'domain',
                'normalized_domain', candidate->>'normalized_domain',
                'score', CASE
                    WHEN jsonb_typeof(candidate->'confidence') = 'number' THEN (candidate->>'confidence')::int
                    ELSE 0
                END,
                'signals', jsonb_build_array(coalesce(candidate->>'source', 'crawl_service')),
                'status', CASE
                    WHEN candidate->>'decision' = 'accepted' THEN 'accepted'
                    ELSE 'proposed'
                END,
                'evidence', coalesce(candidate->'evidence', '{}'::jsonb),
                'metadata', coalesce(candidate->'metadata', '{}'::jsonb)
                    || jsonb_build_object('source', 'crawl-service')
            )
            ORDER BY
                CASE
                    WHEN jsonb_typeof(candidate->'confidence') = 'number' THEN (candidate->>'confidence')::int
                    ELSE 0
                END DESC,
                candidate->>'normalized_domain' ASC
        ) AS candidates
    FROM latest_domain_result ldr
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(ldr.domain_payload->'candidates', '[]'::jsonb)) AS item(candidate)
    WHERE coalesce(candidate->>'normalized_domain', '') <> ''
    GROUP BY ldr.raw_record_id
),
task_status_rows AS (
    SELECT
        raw_record_id,
        jsonb_object_agg(task_type, status ORDER BY task_type) AS task_statuses
    FROM dagster_brreg.raw_record_task_states
    GROUP BY raw_record_id
)
SELECT
    rr.id,
    rr.organization_number,
    rr.organization_name,
    rr.registration_status,
    rr.website,
    rr.country_iso2,
    rr.raw_payload,
    rr.payload_hash,
    lt.status AS translation_status,
    coalesce(lt.translated_payload, '{}'::jsonb) AS translation_payload,
    coalesce(ldr.status, dts.status, 'not_found') AS domain_status,
    coalesce(dr.candidates, '[]'::jsonb) AS domain_candidates,
    coalesce(lcr.status, fts.status, 'skipped') AS currency_status,
    coalesce(lcr.original_payload, '{}'::jsonb) AS original_payload,
    coalesce(lcr.usd_payload, '{}'::jsonb) AS usd_payload,
    coalesce(lcr.fx_metadata, '{}'::jsonb) AS fx_metadata,
    coalesce(tsr.task_statuses, '{}'::jsonb) AS task_statuses,
    rr.last_seen_at AS raw_last_seen_at
FROM dagster_brreg.raw_records rr
JOIN latest_translation lt ON lt.raw_record_id = rr.id
JOIN dagster_brreg.raw_record_task_states tts
  ON tts.raw_record_id = rr.id
 AND tts.task_type = 'translate'
 AND tts.status IN ('succeeded', 'skipped')
JOIN dagster_brreg.raw_record_task_states dts
  ON dts.raw_record_id = rr.id
 AND dts.task_type = 'domain_results'
 AND dts.status IN ('succeeded', 'skipped')
JOIN dagster_brreg.raw_record_task_states fts
  ON fts.raw_record_id = rr.id
 AND fts.task_type = 'currency_conversion'
 AND fts.status IN ('succeeded', 'skipped')
LEFT JOIN latest_domain_result ldr ON ldr.raw_record_id = rr.id
LEFT JOIN domain_rows dr ON dr.raw_record_id = rr.id
LEFT JOIN latest_currency_result lcr ON lcr.raw_record_id = rr.id
LEFT JOIN task_status_rows tsr ON tsr.raw_record_id = rr.id
WHERE rr.is_current = true
  AND NOT EXISTS (
      SELECT 1
      FROM dagster_brreg.enhanced_records er
      WHERE er.raw_record_id = rr.id
        AND er.schema_version = 'brreg.enhanced.v1'
        AND er.status IN ('built', 'published')
        AND er.built_at >= greatest(
            coalesce(tts.last_finished_at, '-infinity'::timestamptz),
            coalesce(dts.last_finished_at, '-infinity'::timestamptz),
            coalesce(fts.last_finished_at, '-infinity'::timestamptz),
            coalesce(ldr.created_at, '-infinity'::timestamptz),
            coalesce(lcr.created_at, '-infinity'::timestamptz),
            lt.created_at
        )
  )
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dagster_brreg_mv_enhanced_ready_records_raw
  ON dagster_brreg.mv_brreg_enhanced_ready_records (id);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_mv_enhanced_ready_records_queue
  ON dagster_brreg.mv_brreg_enhanced_ready_records (raw_last_seen_at, id);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_mv_enhanced_ready_records_org
  ON dagster_brreg.mv_brreg_enhanced_ready_records (organization_number);
