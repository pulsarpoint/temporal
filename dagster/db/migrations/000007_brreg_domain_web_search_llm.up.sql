ALTER TABLE dagster_brreg.task_attempts
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempt_type;

ALTER TABLE dagster_brreg.task_attempts
  ADD CONSTRAINT chk_dagster_brreg_task_attempt_type CHECK (
    task_type IN (
      'parse_raw',
      'translate',
      'discover_domains',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_crtsh',
      'domain_wikidata',
      'domain_dns_heuristic',
      'domain_web_search_llm',
      'merge_domain_proposals',
      'extract_financials',
      'build_enhanced',
      'publish'
    )
  );

ALTER TABLE dagster_brreg.enrichment_runs
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_runs_type;

ALTER TABLE dagster_brreg.enrichment_runs
  ADD CONSTRAINT chk_dagster_brreg_runs_type CHECK (
    run_type IN (
      'bulk_ingest',
      'translate',
      'discover_domains',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_crtsh',
      'domain_wikidata',
      'domain_dns_heuristic',
      'domain_web_search_llm',
      'merge_domain_proposals',
      'build_enhanced',
      'full_enrichment',
      'retry_failed',
      'publish'
    )
  );
