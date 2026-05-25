# BRREG Domain Discovery Dependencies Design

## Decision

BRREG domain discovery should become a dependency-driven pipeline instead of a set of mostly parallel signals.

For the MVP, the active path is:

1. Use the BRREG website field if it exists and normalizes to a valid domain.
2. If no website-field domain exists, run DuckDuckGo first-page search and store the raw search results.
3. Crawl selected DuckDuckGo results and use the LLM to verify whether a site belongs to the company.
4. Build domain proposals only from accepted website-field or web-search LLM candidates.

`crtsh` and `wikidata` should be removed from the active BRREG domain discovery graph for now. Their code can remain in the repository as inactive fallback code, but the default Dagster jobs should not run them and proposal materialization should not wait on them.

## Rationale

The current graph runs `website_field`, `duckduckgo`, `web_search_llm`, `crtsh`, and `wikidata` as peer signals. That produces unnecessary work and noisy candidates.

The practical source priority is not peer based:

- A valid website already present in BRREG is strong evidence and should stop further discovery for that company.
- DuckDuckGo plus page crawl plus LLM verification is the most useful missing-domain path because it can inspect search snippets and page content.
- `crtsh` can return domains with weak company-name correlation.
- `wikidata` coverage is inconsistent and adds another external dependency before the primary path is stable.

The new flow should optimize for explainable results and operational visibility over broad signal collection.

## Alternatives Considered

### Keep All Signals Parallel

This preserves the existing architecture and may find more domains, but it keeps wasting work for companies that already have a website and makes result quality harder to reason about.

### Sequential Main Path With Inactive Fallbacks

This is the chosen approach. Website-field and DuckDuckGo/LLM become the active path. `crtsh` and `wikidata` remain available in code but are not part of the default graph.

### Delete `crtsh` And `wikidata` Immediately

This reduces code surface, but it is premature. Keeping them inactive makes it cheaper to reintroduce one later if the 1k validation set shows a clear gap.

## Data Model

`dagster_brreg.domain_candidates` should continue to store only actual candidate domains worth proposing.

Two artifact tables should be added so we can inspect and debug the search path without polluting candidates:

```sql
CREATE TABLE dagster_brreg.domain_search_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  provider TEXT NOT NULL,
  query TEXT NOT NULL,
  rank INTEGER NOT NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  title TEXT,
  description TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_dagster_brreg_domain_search_provider CHECK (provider IN ('duckduckgo')),
  CONSTRAINT chk_dagster_brreg_domain_search_rank CHECK (rank > 0),
  CONSTRAINT chk_dagster_brreg_domain_search_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, provider, query, rank, url)
);

CREATE TABLE dagster_brreg.domain_crawl_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES dagster_brreg.raw_records(id) ON DELETE CASCADE,
  search_result_id UUID REFERENCES dagster_brreg.domain_search_results(id) ON DELETE SET NULL,
  task_attempt_id UUID REFERENCES dagster_brreg.task_attempts(id) ON DELETE SET NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  status TEXT NOT NULL,
  markdown TEXT,
  markdown_hash TEXT,
  llm_confidence SMALLINT,
  llm_decision TEXT,
  llm_reason TEXT,
  llm_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_dagster_brreg_domain_crawl_status CHECK (
    status IN ('succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_dagster_brreg_domain_crawl_confidence CHECK (
    llm_confidence IS NULL OR llm_confidence BETWEEN 1 AND 100
  ),
  CONSTRAINT chk_dagster_brreg_domain_crawl_evidence_object CHECK (jsonb_typeof(llm_evidence) = 'object'),
  CONSTRAINT chk_dagster_brreg_domain_crawl_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, url)
);
```

Recommended indexes:

```sql
CREATE INDEX idx_dagster_brreg_domain_search_results_raw
  ON dagster_brreg.domain_search_results (raw_record_id, provider, rank);

CREATE INDEX idx_dagster_brreg_domain_crawl_results_raw
  ON dagster_brreg.domain_crawl_results (raw_record_id, status, created_at DESC);

CREATE INDEX idx_dagster_brreg_domain_crawl_results_decision
  ON dagster_brreg.domain_crawl_results (normalized_domain, llm_confidence DESC)
  WHERE llm_confidence IS NOT NULL;
```

## Task Types

The active BRREG domain task types should be:

- `domain_website_field`
- `domain_duckduckgo_search`
- `domain_web_search_llm`
- `merge_domain_proposals`

Inactive task types:

- `domain_crtsh`
- `domain_wikidata`

Existing historical rows for inactive task types can remain. New default Dagster runs should not create them.

## Claim Rules

`domain_website_field` claims every current raw record that has no final task state for `domain_website_field`.

If a website-field candidate is found:

- Insert one `domain_candidates` row with `signal = 'website_field'`.
- Mark `domain_website_field` as `succeeded`.
- Downstream search tasks must skip that raw record.

If no valid website exists:

- Mark `domain_website_field` as `skipped` with a result summary such as `{"reason": "missing_website"}`.
- Make the raw record eligible for `domain_duckduckgo_search`.

`domain_duckduckgo_search` claims only records where:

- `domain_website_field` is `skipped` or `succeeded` with zero candidates.
- There is no accepted or candidate `website_field` domain for the raw record.
- The DuckDuckGo task is missing, pending, retryable, or stale.

DuckDuckGo writes `domain_search_results`. It should not write `domain_candidates` directly.

`domain_web_search_llm` claims only records where:

- There are stored DuckDuckGo search results.
- There is no accepted or candidate `website_field` domain.
- There is no accepted web-search LLM candidate already present.
- The LLM task is missing, pending, retryable, or stale.

The LLM task crawls selected search results, writes `domain_crawl_results`, and writes `domain_candidates` only for verified domains.

`merge_domain_proposals` claims records where:

- There is at least one candidate or accepted domain candidate.
- Candidate rows are newer than the last successful proposal merge, or there is no previous merge task state.

## Dagster Graph

The default domain enrichment job should materialize:

```text
brreg_domain_website_field_candidates
brreg_domain_duckduckgo_search_results
brreg_domain_web_search_llm_candidates
brreg_domain_proposals
```

`brreg_domain_proposals` should depend only on the active assets above.

The separate jobs for `brreg_domain_crtsh_candidates` and `brreg_domain_wikidata_candidates` should be removed from the default definitions. They can either be omitted entirely from `Definitions` or left behind a clearly named experimental job only if needed later.

## Search And Crawl Behavior

DuckDuckGo should crawl only the first result page.

Search queries should use company facts from `raw_records`:

- Company name.
- Country or `Norway`.
- Business address when present.

The system should store all non-excluded first-page search results in `domain_search_results`. Excluded domains such as BRREG, Proff, Gulesider, and other directories should be stored only if useful for debugging, but they should not become candidate domains.

The LLM stage should:

- Triage search results by snippet/title/domain first.
- Crawl only domains above the triage threshold.
- Verify page markdown against company name, organization number, address, and country.
- Write rejected or failed crawl attempts to `domain_crawl_results`.
- Write `domain_candidates` only when verification confidence meets the configured threshold.

## Observability

Existing task views should be updated to include the new task types.

The raw record task overview should make it easy to answer:

- Was a BRREG website present?
- Was DuckDuckGo searched?
- How many search results were stored?
- How many pages were crawled?
- What did the LLM accept or reject?
- Was a final domain proposal created?

`v_domain_enrichment_summary` should count:

- website-field candidates
- DuckDuckGo search results
- crawl attempts
- verified web-search candidates
- final proposals

## Migration And Compatibility

This change does not need to preserve compatibility with the old full-BRREG working dataset. The current remote Dagster working dataset has been reduced to 1,000 validation raw records.

The migration should:

- Add `domain_search_results`.
- Add `domain_crawl_results`.
- Update task-type check constraints to include `domain_duckduckgo_search`.
- Keep existing `domain_duckduckgo`, `domain_crtsh`, and `domain_wikidata` task types valid for historical rows if those rows exist in other environments.
- Update observability views.

No Corpscout table changes are required for this step.

## Testing

Unit tests should cover:

- Website-field candidate found skips DuckDuckGo and LLM eligibility.
- Missing website makes a record eligible for DuckDuckGo.
- DuckDuckGo writes search result artifacts and does not write candidates directly.
- LLM stage claims only rows with stored search results and no website-field candidate.
- LLM accepted result writes both crawl artifact and domain candidate.
- LLM rejected result writes crawl artifact without a domain candidate.
- Proposal merge uses only stored domain candidates.
- Default Dagster definitions no longer include active `crtsh` and `wikidata` jobs.

Validation commands:

```bash
uv run pytest -q
make validate
git diff --check
```

Remote validation on the 1,000-row dataset should check:

- `domain_website_field` processes all 1,000 rows.
- Rows with website-field candidates do not get DuckDuckGo task states.
- DuckDuckGo result rows are stored for only missing-website rows.
- LLM crawl rows are inspectable per company.
- Domain proposals are created only from accepted candidates.

## Open Follow-Ups

After the 1,000-row validation run, decide whether to permanently delete `crtsh` and `wikidata` code or keep them as manual fallback experiments.
