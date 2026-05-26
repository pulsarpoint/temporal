# BRREG Domain Discovery Service Design

## Goal

Move BRREG domain discovery internals out of Dagster into a standalone, testable service. Dagster should only orchestrate business outputs: raw company data, translated data, domain result, and final enhanced payload.

## Boundary

Dagster owns queueing, retries, DB writes, and final artifact assembly. The crawl service owns search, crawl, LLM verification, scoring, and structured errors.

Dagster should no longer expose separate business workflow state for DuckDuckGo search, crawl, and LLM verification. Those are implementation details inside the service. Dagster stores one result artifact per company.

## API

Primary endpoint:

`POST /v1/brreg/domain-discovery`

Request:

```json
{
  "record_id": "uuid-or-source-id",
  "organization_number": "810202572",
  "organization_name": "BORTIGARD AS",
  "raw_payload": {},
  "existing_website": null,
  "country": "NO",
  "llm": {
    "provider": "local",
    "model": "qwen3:6b"
  },
  "limits": {
    "max_search_results": 5,
    "max_crawl_candidates": 3,
    "timeout_seconds": 60
  }
}
```

Response:

```json
{
  "schema_version": "crawl-service.brreg.v1",
  "status": "succeeded",
  "record_id": "uuid-or-source-id",
  "organization_number": "810202572",
  "best_domain": "bortigard.no",
  "candidates": [
    {
      "domain": "bortigard.no",
      "normalized_domain": "bortigard.no",
      "confidence": 87,
      "decision": "accepted",
      "source": "duckduckgo_web_llm",
      "evidence": {},
      "metadata": {}
    }
  ],
  "search_artifacts": [],
  "crawl_artifacts": [],
  "errors": [],
  "warnings": [],
  "duration_ms": 12345,
  "service_version": "0.1.0"
}
```

Statuses:

- `succeeded`: one or more accepted candidates.
- `not_found`: service completed but found no acceptable domain.
- `partial`: some internal steps failed but usable evidence/candidates exist.
- `failed`: service could not complete the unit of work.

## Service Internals

Execution order:

1. If BRREG already has a website, normalize it and return it as a high-confidence candidate. Skip search/crawl unless explicitly requested later.
2. Build a small set of search queries from company name, organization number, country, and address context.
3. Use DuckDuckGo first page only.
4. Crawl a limited number of candidate URLs and convert pages to markdown.
5. Ask LLM to verify whether each crawled page belongs to the BRREG company.
6. Score and return candidates with evidence.

No CRTSH, Wikidata, or DNS heuristics in the first service version.

## Dagster Changes

Replace the separate domain search/crawl/verification assets with one business asset:

`brreg_domain_results`

Dagster writes to a simplified result table:

`dagster_brreg.domain_results`

Suggested columns:

- `id`
- `raw_record_id`
- `task_attempt_id`
- `status`
- `best_domain`
- `domain_payload JSONB`
- `error`
- `created_at`
- `metadata JSONB`

The full service response goes into `domain_payload`. The final enhanced payload builder reads this table.

## Tests

Service tests:

- fake crawler/search/LLM unit tests
- fixture-based BRREG tests using real 300 raw records
- opt-in real crawl/LLM stress test
- tests for existing website short-circuit
- tests for structured errors and partial results

Dagster tests:

- claim rows for `domain_results`
- call service client
- store one `domain_results` row per company
- enhanced payload includes domain candidates from `domain_results`

## Non-Goals

- No Corpscout UI changes in this step.
- No automatic normalized Corpscout source table publishing in this step.
- No detailed Dagster state machine for search/crawl/verify internals.
