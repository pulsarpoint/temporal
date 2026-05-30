# Corpscout Crawl Service

Python service for discovering official company domains from search pages and
candidate websites.

The public API is intentionally small. The generic crawl4ai capability is an
internal Python service used by domain discovery, not a public URL-plus-prompt
endpoint. Pipeline integration is intentionally out of scope for this service.

## Run

```bash
make sync
make run
```

Health check:

```bash
curl http://localhost:8096/healthz
```

Domain discovery:

```bash
curl -X POST 'http://localhost:8096/v1/domains/discover' \
  -H 'content-type: application/json' \
  -d '{
    "company_name": "BORTIGARD AS",
    "organization_number": "810202572",
    "country": "NO",
    "city": "HOLMESTRAND"
  }'
```

Use a specific search engine for one request:

```bash
curl -X POST 'http://localhost:8096/v1/domains/discover' \
  -H 'content-type: application/json' \
  -d '{
    "company_name": "BORTIGARD AS",
    "country": "NO",
    "search_engine": "yandex"
  }'
```

Supported search engines:

- `duckduckgo`
- `yandex`

Default thresholds:

- Search-page candidate URLs with score `< 50` are not crawled.
- Candidate websites with final score `>= 70` are returned as related sites.
- Only pages classified as `company_website` with `owned_domain=true` are
  accepted as official owned domains.

The response includes:

- `search`: full structured crawl4ai response for the search page, including
  markdown, hash, links, metadata, and direct LLM search analysis output.
- `links`: direct LLM-scored candidate URLs/domains from the search page.
- `site_checks`: structured crawl plus direct LLM verification for each
  crawled candidate URL.
- `related_sites`: pages related to the company, including official sites,
  social profiles, registry profiles, directory profiles, and references.
- `primary_web_presence`: best related site marked as a primary web presence.
- `domains`: accepted normalized owned domains only.

Temporary BRREG compatibility endpoint:

```bash
curl -X POST 'http://localhost:8096/v1/brreg/domain-discovery' \
  -H 'content-type: application/json' \
  -d '{
    "record_id": "record-1",
    "organization_number": "810202572",
    "organization_name": "BORTIGARD AS",
    "raw_payload": {
      "organisasjonsnummer": "810202572",
      "navn": "BORTIGARD AS"
    },
    "country": "NO"
  }'
```

This wrapper maps BRREG input into `POST /v1/domains/discover` internally.

## Internal Crawl4Ai Service

`Crawl4AiService` is initialized once when FastAPI starts. It owns the
crawl4ai browser lifecycle and LLM configuration.

Internal request fields:

- `url`
- `llm_enabled`
- optional `llm_query`
- optional `llm_schema`
- optional `timeout_seconds`
- optional `purpose`

Internal response fields:

- `url`, `final_url`, `status`
- `markdown`, `markdown_hash`
- `links`
- `llm_output` when LLM extraction is enabled
- `error`, `duration_ms`, `metadata`

Domain discovery builds on this internal service in two phases:

1. Crawl the first search page without crawl4ai LLM extraction, then run a
   direct compact LLM analysis over the search markdown and extracted links.
2. Crawl each candidate at or above the search threshold without crawl4ai LLM
   extraction, then run a direct compact LLM analysis over the site content.

Known social, registry, directory, and reference domains are never treated as
owned company domains. They can still be returned in `related_sites` when the
page is useful evidence for the company.

For BRREG requests, the wrapper also passes registered address, postal code,
business activity, statutory purpose, and industry codes into both search and
site analysis. Site verification returns identity-signal and activity-alignment
fields internally. The service downgrades an otherwise accepted owned-domain
result when the page conflicts with BRREG activity context and does not contain
legal identity evidence such as legal name, organization number, address, or
city.

## LLM Configuration

The service requires one OpenAI-compatible LLM config at startup:

```bash
CRAWL_SERVICE_LLM_MODEL=qwen3:6b
CRAWL_SERVICE_LLM_BASE_URL=http://100.77.62.33:8888
CRAWL_SERVICE_LLM_API_KEY=
```

`CRAWL_SERVICE_LLM_MODEL` and `CRAWL_SERVICE_LLM_BASE_URL` are required. The
service fails startup if either is missing. `CRAWL_SERVICE_LLM_API_KEY` may be
empty for local LLM endpoints that do not require authentication.

## Crawler Configuration

The Docker image installs crawl4ai and Playwright Chromium in cached layers.
Default crawler settings:

```bash
DOMAIN_CRAWLER_BROWSER_TYPE=chromium
DOMAIN_CRAWLER_CHROME_CHANNEL=chromium
DOMAIN_CRAWLER_HEADLESS=true
DOMAIN_CRAWLER_LIGHT_MODE=true
```

Directory and registry domains are excluded from candidate crawling by default,
but they can still appear in `links` with `metadata.excluded=true`.

```bash
# Replace the built-in default exclusion list entirely.
DOMAIN_SEARCH_EXCLUDED_DOMAINS=proff.no,gulesider.no,brreg.no

# Add domains on top of the built-in defaults.
DOMAIN_SEARCH_EXTRA_EXCLUDED_DOMAINS=example-directory.no

# Allow a domain even if it appears in the built-in/default exclusion list.
DOMAIN_SEARCH_ALLOWED_DOMAINS=proff.no
```

## Tests

```bash
make test
```

The deterministic acceptance fixture lives in
`tests/data/brreg_domain_acceptance_entries_10.json`.
