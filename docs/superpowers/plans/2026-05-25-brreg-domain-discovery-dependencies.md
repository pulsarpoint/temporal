# BRREG Domain Discovery Dependencies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make BRREG domain discovery dependency-driven: website field first, DuckDuckGo search only when website is missing, LLM crawl/verification only from stored search results, and proposals only from accepted candidates.

**Architecture:** Add explicit search and crawl artifact tables to `dagster_brreg`, then update the working-store claim queries so each task only claims records whose upstream dependency is satisfied and whose higher-priority domain source did not already produce a candidate. Keep `crtsh` and `wikidata` code present but remove those assets from the default Dagster definitions.

**Tech Stack:** Python 3.12, Dagster, psycopg, PostgreSQL migrations through golang-migrate, pytest, crawl4ai, DeepSeek-compatible OpenAI chat completions.

---

## File Structure

- Modify `dagster/db/migrations/000012_brreg_domain_discovery_dependencies.up.sql`: add artifact tables, update task-type constraints, recreate observability views.
- Modify `dagster/db/migrations/000012_brreg_domain_discovery_dependencies.down.sql`: reverse migration.
- Modify `dagster/src/corpscout_dagster/brreg/working_store.py`: add dataclasses, insert/fetch methods, and dependency-aware claim SQL.
- Modify `dagster/src/corpscout_dagster/brreg/domain_search_llm.py`: split DuckDuckGo search result collection from crawl/LLM verification.
- Modify `dagster/src/corpscout_dagster/brreg/assets.py`: add DuckDuckGo-search artifact asset, update LLM asset to read stored search results, update dependencies.
- Modify `dagster/src/corpscout_dagster/definitions.py`: remove `crtsh` and `wikidata` from default active jobs/assets.
- Modify `dagster/tests/brreg/test_working_store_schema.py`: assert migration/table/constraint/view definitions.
- Modify `dagster/tests/brreg/test_working_store.py`: assert insert/fetch methods and claim SQL dependencies.
- Modify `dagster/tests/brreg/test_domain_search_llm.py`: assert search artifact collection and crawl verification behavior.
- Modify `dagster/tests/brreg/test_assets.py`: assert asset/job definitions and per-task materialization behavior.

---

### Task 1: Migration For Search And Crawl Artifacts

**Files:**
- Create: `dagster/db/migrations/000012_brreg_domain_discovery_dependencies.up.sql`
- Create: `dagster/db/migrations/000012_brreg_domain_discovery_dependencies.down.sql`
- Test: `dagster/tests/brreg/test_working_store_schema.py`

- [ ] **Step 1: Write schema tests for new artifact tables**

Add these tests to `dagster/tests/brreg/test_working_store_schema.py`:

```python
def test_domain_discovery_dependencies_migration_adds_search_and_crawl_artifacts() -> None:
    sql = Path("db/migrations/000012_brreg_domain_discovery_dependencies.up.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_search_results" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_crawl_results" in sql
    assert "provider IN ('duckduckgo')" in sql
    assert "UNIQUE (raw_record_id, provider, query, rank, url)" in sql
    assert "UNIQUE (raw_record_id, url)" in sql
    assert "idx_dagster_brreg_domain_search_results_raw" in sql
    assert "idx_dagster_brreg_domain_crawl_results_raw" in sql
    assert "idx_dagster_brreg_domain_crawl_results_decision" in sql


def test_domain_discovery_dependencies_migration_updates_task_types_and_views() -> None:
    sql = Path("db/migrations/000012_brreg_domain_discovery_dependencies.up.sql").read_text()

    assert "'domain_duckduckgo_search'" in sql
    assert "'domain_duckduckgo'" in sql
    assert "'domain_crtsh'" in sql
    assert "'domain_wikidata'" in sql
    assert "CREATE OR REPLACE VIEW dagster_brreg.v_domain_enrichment_summary" in sql
    assert "domain_search_results" in sql
    assert "domain_crawl_results" in sql
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest dagster/tests/brreg/test_working_store_schema.py -q
```

Expected: fails because migration `000012` does not exist.

- [ ] **Step 3: Create migration**

Create `dagster/db/migrations/000012_brreg_domain_discovery_dependencies.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS dagster_brreg.domain_search_results (
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
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_dagster_brreg_domain_search_provider CHECK (provider IN ('duckduckgo')),
  CONSTRAINT chk_dagster_brreg_domain_search_rank CHECK (rank > 0),
  CONSTRAINT chk_dagster_brreg_domain_search_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, provider, query, rank, url)
);

CREATE TABLE IF NOT EXISTS dagster_brreg.domain_crawl_results (
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
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_search_results_raw
  ON dagster_brreg.domain_search_results (raw_record_id, provider, rank);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_crawl_results_raw
  ON dagster_brreg.domain_crawl_results (raw_record_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dagster_brreg_domain_crawl_results_decision
  ON dagster_brreg.domain_crawl_results (normalized_domain, llm_confidence DESC)
  WHERE llm_confidence IS NOT NULL;

ALTER TABLE dagster_brreg.task_attempts
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempt_type;

ALTER TABLE dagster_brreg.task_attempts
  ADD CONSTRAINT chk_dagster_brreg_task_attempt_type CHECK (
    task_type IN (
      'parse_raw',
      'translate',
      'discover_domains',
      'extract_financials',
      'build_enhanced',
      'publish',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_duckduckgo_search',
      'domain_crtsh',
      'domain_wikidata',
      'domain_web_search_llm',
      'merge_domain_proposals'
    )
  );

ALTER TABLE dagster_brreg.raw_record_task_states
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_raw_record_task_states_type;

ALTER TABLE dagster_brreg.raw_record_task_states
  ADD CONSTRAINT chk_dagster_brreg_raw_record_task_states_type CHECK (
    task_type IN (
      'translate',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_duckduckgo_search',
      'domain_crtsh',
      'domain_wikidata',
      'domain_web_search_llm',
      'merge_domain_proposals',
      'build_enhanced',
      'publish',
      'discover_domains',
      'extract_financials'
    )
  );

CREATE OR REPLACE VIEW dagster_brreg.v_domain_enrichment_summary AS
SELECT
  rr.id AS raw_record_id,
  rr.organization_number,
  rr.organization_name,
  count(DISTINCT dc.id) FILTER (WHERE dc.signal = 'website_field') AS website_field_candidates,
  count(DISTINCT dsr.id) AS duckduckgo_search_results,
  count(DISTINCT dcr.id) AS crawl_results,
  count(DISTINCT dc.id) FILTER (WHERE dc.signal = 'web_search_llm') AS web_search_llm_candidates,
  count(DISTINCT dp.id) AS domain_proposals
FROM dagster_brreg.raw_records rr
LEFT JOIN dagster_brreg.domain_candidates dc
  ON dc.raw_record_id = rr.id
LEFT JOIN dagster_brreg.domain_search_results dsr
  ON dsr.raw_record_id = rr.id
LEFT JOIN dagster_brreg.domain_crawl_results dcr
  ON dcr.raw_record_id = rr.id
LEFT JOIN dagster_brreg.domain_proposals dp
  ON dp.raw_record_id = rr.id
WHERE rr.is_current
GROUP BY rr.id, rr.organization_number, rr.organization_name;
```

Create `dagster/db/migrations/000012_brreg_domain_discovery_dependencies.down.sql`:

```sql
DROP VIEW IF EXISTS dagster_brreg.v_domain_enrichment_summary;

ALTER TABLE dagster_brreg.task_attempts
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_task_attempt_type;

ALTER TABLE dagster_brreg.task_attempts
  ADD CONSTRAINT chk_dagster_brreg_task_attempt_type CHECK (
    task_type IN (
      'parse_raw',
      'translate',
      'discover_domains',
      'extract_financials',
      'build_enhanced',
      'publish',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_crtsh',
      'domain_wikidata',
      'domain_web_search_llm',
      'merge_domain_proposals'
    )
  );

ALTER TABLE dagster_brreg.raw_record_task_states
  DROP CONSTRAINT IF EXISTS chk_dagster_brreg_raw_record_task_states_type;

ALTER TABLE dagster_brreg.raw_record_task_states
  ADD CONSTRAINT chk_dagster_brreg_raw_record_task_states_type CHECK (
    task_type IN (
      'translate',
      'domain_website_field',
      'domain_duckduckgo',
      'domain_crtsh',
      'domain_wikidata',
      'domain_web_search_llm',
      'merge_domain_proposals',
      'build_enhanced',
      'publish',
      'discover_domains',
      'extract_financials'
    )
  );

DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_crawl_results_decision;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_crawl_results_raw;
DROP INDEX IF EXISTS dagster_brreg.idx_dagster_brreg_domain_search_results_raw;
DROP TABLE IF EXISTS dagster_brreg.domain_crawl_results;
DROP TABLE IF EXISTS dagster_brreg.domain_search_results;
```

- [ ] **Step 4: Run schema tests**

Run:

```bash
uv run pytest dagster/tests/brreg/test_working_store_schema.py -q
```

Expected: pass.

- [ ] **Step 5: Commit migration**

Run:

```bash
git add dagster/db/migrations/000012_brreg_domain_discovery_dependencies.up.sql dagster/db/migrations/000012_brreg_domain_discovery_dependencies.down.sql dagster/tests/brreg/test_working_store_schema.py
git commit -m "feat: add brreg domain discovery artifact tables"
```

---

### Task 2: Working Store Artifact Methods And Claim Rules

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/working_store.py`
- Test: `dagster/tests/brreg/test_working_store.py`

- [ ] **Step 1: Write tests for search/crawl insert methods**

Add these tests to `dagster/tests/brreg/test_working_store.py`:

```python
def test_insert_domain_search_results_uses_artifact_table() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    store.insert_domain_search_results(
        [
            InsertDomainSearchResult(
                raw_record_id="raw-1",
                task_attempt_id="attempt-1",
                provider="duckduckgo",
                query='"BORTIGARD AS" Norway official website',
                rank=1,
                url="https://www.bortigard.no/",
                domain="www.bortigard.no",
                normalized_domain="bortigard.no",
                title="Bortigard AS",
                description="Norwegian property company.",
                metadata={"source": "first_page"},
            )
        ]
    )

    sql, params = cursor.many_calls[-1]
    assert "INSERT INTO dagster_brreg.domain_search_results" in sql
    assert params[0]["provider"] == "duckduckgo"
    assert params[0]["normalized_domain"] == "bortigard.no"


def test_insert_domain_crawl_results_uses_artifact_table() -> None:
    cursor = FakeCursor()
    store = BrregWorkingStore(cursor)

    store.insert_domain_crawl_results(
        [
            InsertDomainCrawlResult(
                raw_record_id="raw-1",
                search_result_id="search-1",
                task_attempt_id="attempt-1",
                url="https://www.bortigard.no/",
                domain="www.bortigard.no",
                normalized_domain="bortigard.no",
                status="succeeded",
                markdown="BORTIGARD AS",
                markdown_hash="hash",
                llm_confidence=84,
                llm_decision="accepted",
                llm_reason="Exact legal name matched.",
                llm_evidence={"matched": ["BORTIGARD AS"]},
                metadata={"prompt_version": "v1"},
            )
        ]
    )

    sql, params = cursor.many_calls[-1]
    assert "INSERT INTO dagster_brreg.domain_crawl_results" in sql
    assert params[0]["llm_confidence"] == 84
    assert params[0]["llm_decision"] == "accepted"
```

- [ ] **Step 2: Write tests for dependency-aware claim SQL**

Add tests:

```python
def test_fetch_pending_duckduckgo_search_requires_missing_website_candidate() -> None:
    cursor = FakeCursor(fetchall_values=[])
    store = BrregWorkingStore(cursor)

    store.fetch_pending_duckduckgo_search_records(
        limit=10,
        max_parallel_tasks=2,
        lease_seconds=1800,
    )

    sql, params = cursor.calls[-1]
    assert params["task_type"] == "domain_duckduckgo_search"
    assert "wts.task_type = 'domain_website_field'" in sql
    assert "dc.signal = 'website_field'" in sql
    assert "NOT EXISTS" in sql


def test_fetch_pending_web_search_llm_requires_search_results_and_no_website_candidate() -> None:
    cursor = FakeCursor(fetchall_values=[])
    store = BrregWorkingStore(cursor)

    store.fetch_pending_web_search_llm_records(
        limit=10,
        max_parallel_tasks=1,
        lease_seconds=1800,
    )

    sql, params = cursor.calls[-1]
    assert params["task_type"] == "domain_web_search_llm"
    assert "dagster_brreg.domain_search_results dsr" in sql
    assert "dc.signal = 'website_field'" in sql
    assert "dc.signal = 'web_search_llm'" in sql
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
uv run pytest dagster/tests/brreg/test_working_store.py -q
```

Expected: fails because new dataclasses and methods are missing.

- [ ] **Step 4: Add working-store dataclasses**

Add after `InsertDomainCandidate`:

```python
@dataclass(frozen=True)
class InsertDomainSearchResult:
    raw_record_id: str
    task_attempt_id: str
    provider: str
    query: str
    rank: int
    url: str
    domain: str
    normalized_domain: str
    title: str | None
    description: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class DomainSearchResultRow:
    id: str
    raw_record_id: str
    query: str
    rank: int
    url: str
    domain: str
    normalized_domain: str
    title: str | None
    description: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class InsertDomainCrawlResult:
    raw_record_id: str
    search_result_id: str | None
    task_attempt_id: str
    url: str
    domain: str
    normalized_domain: str
    status: str
    markdown: str | None
    markdown_hash: str | None
    llm_confidence: int | None
    llm_decision: str | None
    llm_reason: str | None
    llm_evidence: dict[str, Any]
    metadata: dict[str, Any]
```

- [ ] **Step 5: Add insert/fetch methods**

Add methods to `BrregWorkingStore` near domain candidate methods:

```python
    def insert_domain_search_results(self, rows: list[InsertDomainSearchResult]) -> None:
        params_seq = [
            {
                "raw_record_id": row.raw_record_id,
                "task_attempt_id": row.task_attempt_id,
                "provider": row.provider,
                "query": row.query,
                "rank": row.rank,
                "url": row.url,
                "domain": row.domain,
                "normalized_domain": row.normalized_domain,
                "title": row.title,
                "description": row.description,
                "metadata": _json(row.metadata),
            }
            for row in rows
        ]
        if params_seq:
            self._cursor.executemany(INSERT_DOMAIN_SEARCH_RESULT_SQL, params_seq)

    def fetch_domain_search_results_for_raw_record(self, *, raw_record_id: str) -> list[DomainSearchResultRow]:
        self._cursor.execute(
            FETCH_DOMAIN_SEARCH_RESULTS_FOR_RAW_RECORD_SQL,
            {"raw_record_id": raw_record_id},
        )
        return [_domain_search_result_row_from_row(row) for row in self._cursor.fetchall()]

    def insert_domain_crawl_results(self, rows: list[InsertDomainCrawlResult]) -> None:
        params_seq = [
            {
                "raw_record_id": row.raw_record_id,
                "search_result_id": row.search_result_id,
                "task_attempt_id": row.task_attempt_id,
                "url": row.url,
                "domain": row.domain,
                "normalized_domain": row.normalized_domain,
                "status": row.status,
                "markdown": row.markdown,
                "markdown_hash": row.markdown_hash,
                "llm_confidence": row.llm_confidence,
                "llm_decision": row.llm_decision,
                "llm_reason": row.llm_reason,
                "llm_evidence": _json(row.llm_evidence),
                "metadata": _json(row.metadata),
            }
            for row in rows
        ]
        if params_seq:
            self._cursor.executemany(INSERT_DOMAIN_CRAWL_RESULT_SQL, params_seq)
```

- [ ] **Step 6: Add SQL constants**

Add SQL constants:

```python
INSERT_DOMAIN_SEARCH_RESULT_SQL = """
INSERT INTO dagster_brreg.domain_search_results (
    raw_record_id,
    task_attempt_id,
    provider,
    query,
    rank,
    url,
    domain,
    normalized_domain,
    title,
    description,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(task_attempt_id)s,
    %(provider)s,
    %(query)s,
    %(rank)s,
    %(url)s,
    %(domain)s,
    %(normalized_domain)s,
    %(title)s,
    %(description)s,
    %(metadata)s::jsonb
)
ON CONFLICT (raw_record_id, provider, query, rank, url) DO UPDATE
SET
    domain = EXCLUDED.domain,
    normalized_domain = EXCLUDED.normalized_domain,
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    metadata = dagster_brreg.domain_search_results.metadata || EXCLUDED.metadata,
    updated_at = now()
"""

FETCH_DOMAIN_SEARCH_RESULTS_FOR_RAW_RECORD_SQL = """
SELECT
    id,
    raw_record_id,
    query,
    rank,
    url,
    domain,
    normalized_domain,
    title,
    description,
    metadata
FROM dagster_brreg.domain_search_results
WHERE raw_record_id = %(raw_record_id)s
ORDER BY rank ASC, url ASC
"""

INSERT_DOMAIN_CRAWL_RESULT_SQL = """
INSERT INTO dagster_brreg.domain_crawl_results (
    raw_record_id,
    search_result_id,
    task_attempt_id,
    url,
    domain,
    normalized_domain,
    status,
    markdown,
    markdown_hash,
    llm_confidence,
    llm_decision,
    llm_reason,
    llm_evidence,
    metadata
) VALUES (
    %(raw_record_id)s,
    %(search_result_id)s,
    %(task_attempt_id)s,
    %(url)s,
    %(domain)s,
    %(normalized_domain)s,
    %(status)s,
    %(markdown)s,
    %(markdown_hash)s,
    %(llm_confidence)s,
    %(llm_decision)s,
    %(llm_reason)s,
    %(llm_evidence)s::jsonb,
    %(metadata)s::jsonb
)
ON CONFLICT (raw_record_id, url) DO UPDATE
SET
    search_result_id = EXCLUDED.search_result_id,
    task_attempt_id = EXCLUDED.task_attempt_id,
    domain = EXCLUDED.domain,
    normalized_domain = EXCLUDED.normalized_domain,
    status = EXCLUDED.status,
    markdown = EXCLUDED.markdown,
    markdown_hash = EXCLUDED.markdown_hash,
    llm_confidence = EXCLUDED.llm_confidence,
    llm_decision = EXCLUDED.llm_decision,
    llm_reason = EXCLUDED.llm_reason,
    llm_evidence = EXCLUDED.llm_evidence,
    metadata = dagster_brreg.domain_crawl_results.metadata || EXCLUDED.metadata,
    updated_at = now()
"""
```

- [ ] **Step 7: Add dependency-aware claim methods**

Add methods:

```python
    def fetch_pending_duckduckgo_search_records(
        self,
        *,
        limit: int,
        max_parallel_tasks: int,
        lease_seconds: int,
    ) -> list[RawTaskRecord]:
        return self._fetch_pending_domain_dependency_records(
            sql=FETCH_PENDING_DUCKDUCKGO_SEARCH_RECORDS_SQL,
            task_type="domain_duckduckgo_search",
            limit=limit,
            max_parallel_tasks=max_parallel_tasks,
            lease_seconds=lease_seconds,
        )

    def fetch_pending_web_search_llm_records(
        self,
        *,
        limit: int,
        max_parallel_tasks: int,
        lease_seconds: int,
    ) -> list[RawTaskRecord]:
        return self._fetch_pending_domain_dependency_records(
            sql=FETCH_PENDING_WEB_SEARCH_LLM_RECORDS_SQL,
            task_type="domain_web_search_llm",
            limit=limit,
            max_parallel_tasks=max_parallel_tasks,
            lease_seconds=lease_seconds,
        )

    def _fetch_pending_domain_dependency_records(
        self,
        *,
        sql: str,
        task_type: str,
        limit: int,
        max_parallel_tasks: int,
        lease_seconds: int,
    ) -> list[RawTaskRecord]:
        if limit <= 0:
            return []
        if max_parallel_tasks <= 0:
            raise ValueError("max_parallel_tasks must be positive")
        self._cursor.execute(
            sql,
            {
                "task_type": task_type,
                "limit": limit,
                "max_parallel_tasks": max_parallel_tasks,
                "lease_seconds": lease_seconds,
            },
        )
        return [_raw_task_record_from_row(row) for row in self._cursor.fetchall()]
```

Implement `FETCH_PENDING_DUCKDUCKGO_SEARCH_RECORDS_SQL` by copying the lease/advisory-lock shape from `FETCH_PENDING_RAW_TASK_RECORDS_SQL` and changing eligible records to:

```sql
WHERE rr.is_current = true
  AND EXISTS (
      SELECT 1
      FROM dagster_brreg.raw_record_task_states wts
      WHERE wts.raw_record_id = rr.id
        AND wts.task_type = 'domain_website_field'
        AND wts.status IN ('succeeded', 'skipped')
  )
  AND NOT EXISTS (
      SELECT 1
      FROM dagster_brreg.domain_candidates dc
      WHERE dc.raw_record_id = rr.id
        AND dc.signal = 'website_field'
        AND dc.status IN ('candidate', 'accepted')
  )
```

Implement `FETCH_PENDING_WEB_SEARCH_LLM_RECORDS_SQL` with the same lease/advisory-lock shape and this eligibility:

```sql
WHERE rr.is_current = true
  AND EXISTS (
      SELECT 1
      FROM dagster_brreg.domain_search_results dsr
      WHERE dsr.raw_record_id = rr.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM dagster_brreg.domain_candidates dc
      WHERE dc.raw_record_id = rr.id
        AND dc.signal = 'website_field'
        AND dc.status IN ('candidate', 'accepted')
  )
  AND NOT EXISTS (
      SELECT 1
      FROM dagster_brreg.domain_candidates dc
      WHERE dc.raw_record_id = rr.id
        AND dc.signal = 'web_search_llm'
        AND dc.status IN ('candidate', 'accepted')
  )
```

- [ ] **Step 8: Run working-store tests**

Run:

```bash
uv run pytest dagster/tests/brreg/test_working_store.py -q
```

Expected: pass.

- [ ] **Step 9: Commit working-store changes**

Run:

```bash
git add dagster/src/corpscout_dagster/brreg/working_store.py dagster/tests/brreg/test_working_store.py
git commit -m "feat: add brreg domain discovery working store artifacts"
```

---

### Task 3: Split DuckDuckGo Search From LLM Crawl Verification

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/domain_search_llm.py`
- Test: `dagster/tests/brreg/test_domain_search_llm.py`

- [ ] **Step 1: Write tests for collecting search results without verification**

Add:

```python
@pytest.mark.asyncio
async def test_collect_duckduckgo_search_results_returns_first_page_artifacts() -> None:
    crawler = FakeCrawler()

    results = await collect_duckduckgo_search_results(
        raw_payload={
            "organisasjonsnummer": "810202572",
            "forretningsadresse": {"adresse": ["Løkkeveien 18"], "poststed": "HOLMESTRAND"},
        },
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        country="NO",
        crawler_factory=lambda: crawler,
    )

    assert [result.normalized_domain for result in results] == ["bortigard.no", "wrong.example"]
    assert all(result.query for result in results)
    assert "html.duckduckgo.com/html/" in crawler.urls[0]
```

- [ ] **Step 2: Write tests for verifying stored search results**

Add:

```python
@pytest.mark.asyncio
async def test_verify_stored_search_results_returns_candidates_and_crawl_artifacts() -> None:
    crawler = FakeCrawler()
    llm = FakeDomainSearchLLM()
    search_results = [
        SearchResult(
            query='"BORTIGARD AS" Norway official website',
            rank=1,
            url="https://www.bortigard.no/",
            domain="www.bortigard.no",
            normalized_domain="bortigard.no",
            title="Bortigard AS",
            description="Norwegian property company.",
        )
    ]

    verified = await verify_domain_search_results_with_llm(
        raw_payload={"organisasjonsnummer": "810202572"},
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        country="NO",
        search_results=search_results,
        classifier=llm,
        crawler_factory=lambda: crawler,
        triage_threshold=50,
        verification_threshold=60,
        max_verified_candidates=3,
        prompt_version="v1",
    )

    assert len(verified.candidates) == 1
    assert verified.candidates[0].normalized_domain == "bortigard.no"
    assert len(verified.crawl_results) == 1
    assert verified.crawl_results[0].status == "succeeded"
    assert verified.crawl_results[0].llm_confidence == 84
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
uv run pytest dagster/tests/brreg/test_domain_search_llm.py -q
```

Expected: fails because split functions and result dataclasses are missing.

- [ ] **Step 4: Add verification result dataclasses**

Add:

```python
@dataclass(frozen=True)
class DomainCrawlArtifact:
    search_result: SearchResult
    status: str
    markdown: str | None
    markdown_hash: str | None
    llm_confidence: int | None
    llm_decision: str | None
    llm_reason: str | None
    llm_evidence: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class VerifiedDomainSearchResults:
    candidates: list[DomainCandidate]
    crawl_results: list[DomainCrawlArtifact]
```

- [ ] **Step 5: Add search collection function**

Add:

```python
async def collect_duckduckgo_search_results(
    *,
    raw_payload: dict[str, Any],
    organization_number: str,
    organization_name: str | None,
    country: str = "NO",
    crawler_factory=None,
) -> list[SearchResult]:
    company = build_domain_search_company_facts(
        raw_payload=raw_payload,
        organization_number=organization_number,
        organization_name=organization_name,
        country=country,
    )
    if not company.organization_name:
        return []
    if crawler_factory is None:
        try:
            from crawl4ai import AsyncWebCrawler  # type: ignore[import]
        except ModuleNotFoundError:
            logger.warning("duckduckgo domain search skipped because crawl4ai is not installed")
            return []
        crawler_factory = lambda: AsyncWebCrawler(config=domain_crawler_browser_config_from_env())

    collected: list[SearchResult] = []
    seen: set[tuple[str, str]] = set()
    async with crawler_factory() as crawler:
        for query in build_domain_search_queries(company):
            for result in await crawl_duckduckgo_first_page(crawler=crawler, query=query):
                key = (result.normalized_domain, result.url)
                if key in seen:
                    continue
                seen.add(key)
                collected.append(result)
    return collected
```

- [ ] **Step 6: Add verification function**

Add:

```python
async def verify_domain_search_results_with_llm(
    *,
    raw_payload: dict[str, Any],
    organization_number: str,
    organization_name: str | None,
    country: str,
    search_results: list[SearchResult],
    classifier: DomainSearchLLM | None = None,
    crawler_factory=None,
    triage_threshold: int = DEFAULT_TRIAGE_THRESHOLD,
    verification_threshold: int = DEFAULT_VERIFICATION_THRESHOLD,
    max_verified_candidates: int = DEFAULT_MAX_VERIFIED_CANDIDATES,
    prompt_version: str = DEFAULT_DOMAIN_LLM_PROMPT_VERSION,
) -> VerifiedDomainSearchResults:
    prompt_version = os.environ.get("DOMAIN_LLM_PROMPT_VERSION") or prompt_version
    company = build_domain_search_company_facts(
        raw_payload=raw_payload,
        organization_number=organization_number,
        organization_name=organization_name,
        country=country,
    )
    if not company.organization_name or not search_results:
        return VerifiedDomainSearchResults(candidates=[], crawl_results=[])
    if classifier is None:
        try:
            classifier = DirectDomainSearchLLM.from_env()
        except MissingDomainLLMConfig as exc:
            logger.warning("web search LLM domain signal skipped: %s", exc)
            return VerifiedDomainSearchResults(candidates=[], crawl_results=[])
    if crawler_factory is None:
        try:
            from crawl4ai import AsyncWebCrawler  # type: ignore[import]
        except ModuleNotFoundError:
            logger.warning("web search LLM domain signal skipped because crawl4ai is not installed")
            return VerifiedDomainSearchResults(candidates=[], crawl_results=[])
        crawler_factory = lambda: AsyncWebCrawler(config=domain_crawler_browser_config_from_env())

    triage = classifier.triage_search_results(
        company=company,
        results=search_results,
        prompt_version=prompt_version,
    )
    triage_by_domain = {
        decision.normalized_domain: decision
        for decision in triage
        if decision.confidence > triage_threshold
    }
    if not triage_by_domain:
        return VerifiedDomainSearchResults(candidates=[], crawl_results=[])

    candidates: list[DomainCandidate] = []
    crawl_artifacts: list[DomainCrawlArtifact] = []
    async with crawler_factory() as crawler:
        for result in _selected_results_for_verification(search_results, triage_by_domain)[:max_verified_candidates]:
            markdown = await crawl_candidate_markdown(crawler=crawler, url=result.url)
            verification = classifier.verify_candidate(
                company=company,
                result=result,
                markdown=markdown,
                prompt_version=prompt_version,
            )
            accepted = verification is not None and verification.confidence >= verification_threshold
            crawl_artifacts.append(
                DomainCrawlArtifact(
                    search_result=result,
                    status="succeeded",
                    markdown=markdown,
                    markdown_hash=_stable_hash(markdown),
                    llm_confidence=verification.confidence if verification else None,
                    llm_decision="accepted" if accepted else "rejected",
                    llm_reason=verification.reason if verification else "LLM returned no verification decision.",
                    llm_evidence={"matched_evidence": verification.matched_evidence} if verification else {},
                    metadata={"prompt_version": prompt_version},
                )
            )
            if accepted:
                candidates.append(
                    _candidate_from_verified_search_result(
                        company=company,
                        result=result,
                        triage=triage_by_domain[result.normalized_domain],
                        verification=verification,
                        markdown=markdown,
                        prompt_version=prompt_version,
                    )
                )
    return VerifiedDomainSearchResults(candidates=candidates, crawl_results=crawl_artifacts)
```

Add helper:

```python
def _stable_hash(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()
```

- [ ] **Step 7: Preserve existing public function**

Update the body of the existing `discover_web_search_llm_domain_candidates` function to call the split functions while keeping its current parameters unchanged:

```python
    search_results = await collect_duckduckgo_search_results(
        raw_payload=raw_payload,
        organization_number=organization_number,
        organization_name=organization_name,
        country=country,
        crawler_factory=crawler_factory,
    )
    verified = await verify_domain_search_results_with_llm(
        raw_payload=raw_payload,
        organization_number=organization_number,
        organization_name=organization_name,
        country=country,
        search_results=search_results,
        classifier=classifier,
        crawler_factory=crawler_factory,
        triage_threshold=triage_threshold,
        verification_threshold=verification_threshold,
        max_verified_candidates=max_verified_candidates,
        prompt_version=prompt_version,
    )
    return verified.candidates
```

- [ ] **Step 8: Run domain-search tests**

Run:

```bash
uv run pytest dagster/tests/brreg/test_domain_search_llm.py -q
```

Expected: pass.

- [ ] **Step 9: Commit domain-search split**

Run:

```bash
git add dagster/src/corpscout_dagster/brreg/domain_search_llm.py dagster/tests/brreg/test_domain_search_llm.py
git commit -m "feat: split brreg domain search artifacts from llm verification"
```

---

### Task 4: Update Dagster Assets To Use Dependency Chain

**Files:**
- Modify: `dagster/src/corpscout_dagster/brreg/assets.py`
- Test: `dagster/tests/brreg/test_assets.py`

- [ ] **Step 1: Write tests for new DuckDuckGo-search asset and old inactive jobs**

Update imports and add assertions:

```python
def test_brreg_domain_assets_use_dependency_driven_active_graph() -> None:
    asset_keys = {asset.key.path[-1] for asset in BRREG_DOMAIN_ASSETS}

    assert "brreg_domain_website_field_candidates" in asset_keys
    assert "brreg_domain_duckduckgo_search_results" in asset_keys
    assert "brreg_domain_web_search_llm_candidates" in asset_keys
    assert "brreg_domain_proposals" in asset_keys
    assert "brreg_domain_crtsh_candidates" not in asset_keys
    assert "brreg_domain_wikidata_candidates" not in asset_keys
```

Add materialization test:

```python
def test_materialize_brreg_duckduckgo_search_results_writes_search_artifacts() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572"},
            )
        ],
    ]

    result = materialize_brreg_duckduckgo_search_results(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=10,
        max_parallel_tasks=1,
    )

    assert result["rows_seen"] == 1
    assert "search_results_written" in result
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    assert any("domain_duckduckgo_search" in sql for sql in sql_calls)
```

- [ ] **Step 2: Run asset tests to verify they fail**

Run:

```bash
uv run pytest dagster/tests/brreg/test_assets.py -q
```

Expected: fails because active graph and materializer are not changed.

- [ ] **Step 3: Update asset constants and imports**

In `assets.py`, import split functions:

```python
from corpscout_dagster.brreg.domain_search_llm import (
    SearchResult,
    collect_duckduckgo_search_results,
    verify_domain_search_results_with_llm,
)
```

Change constants:

```python
DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_BATCH_SIZE = 10
DEFAULT_DOMAIN_WEB_SEARCH_LLM_BATCH_SIZE = 10
DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_MAX_PARALLEL_TASKS = 2
DEFAULT_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS = 1

DOMAIN_SIGNAL_ASSET_KEYS = [
    AssetKey("brreg_domain_website_field_candidates"),
    AssetKey("brreg_domain_duckduckgo_search_results"),
    AssetKey("brreg_domain_web_search_llm_candidates"),
]
```

- [ ] **Step 4: Add DuckDuckGo-search asset**

Replace `brreg_domain_duckduckgo_candidates` with:

```python
@asset(
    name="brreg_domain_duckduckgo_search_results",
    deps=[AssetKey("brreg_domain_website_field_candidates")],
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_DUCKDUCKGO_SEARCH_BATCH_SIZE", DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_DUCKDUCKGO_SEARCH_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_duckduckgo_search_results(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_DUCKDUCKGO_SEARCH_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_DUCKDUCKGO_SEARCH_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_duckduckgo_search_results(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )
```

- [ ] **Step 5: Add DuckDuckGo-search materializer**

Add `materialize_brreg_duckduckgo_search_results` using the loop shape from `materialize_brreg_domain_signal_candidates`, but claim through `fetch_pending_duckduckgo_search_records` and call a new helper:

```python
def _discover_record_duckduckgo_search_results(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
) -> int:
    results = asyncio.run(
        collect_duckduckgo_search_results(
            raw_payload=record.raw_payload,
            organization_number=record.organization_number,
            organization_name=record.organization_name,
            country="NO",
        )
    )
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_domain_search_results(
            [
                InsertDomainSearchResult(
                    raw_record_id=record.id,
                    task_attempt_id=attempt.id,
                    provider="duckduckgo",
                    query=result.query,
                    rank=result.rank,
                    url=result.url,
                    domain=result.domain,
                    normalized_domain=result.normalized_domain,
                    title=result.title,
                    description=result.description,
                    metadata={},
                )
                for result in results
            ]
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()
    return len(results)
```

- [ ] **Step 6: Update web-search LLM materializer**

Keep asset name `brreg_domain_web_search_llm_candidates`, but set dependency:

```python
@asset(
    name="brreg_domain_web_search_llm_candidates",
    deps=[AssetKey("brreg_domain_duckduckgo_search_results")],
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_WEB_SEARCH_LLM_BATCH_SIZE", DEFAULT_DOMAIN_WEB_SEARCH_LLM_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS,
        ),
    ),
)
```

Change it to claim with `fetch_pending_web_search_llm_records`. In `_discover_record_domain_signal`, branch `signal == "web_search_llm"` so it reads stored search results:

```python
search_rows = store.fetch_domain_search_results_for_raw_record(raw_record_id=record.id)
search_row_by_domain_url = {
    (row.normalized_domain, row.url): row
    for row in search_rows
}
search_results = [
    SearchResult(
        query=row.query,
        rank=row.rank,
        url=row.url,
        domain=row.domain,
        normalized_domain=row.normalized_domain,
        title=row.title or "",
        description=row.description or "",
    )
    for row in search_rows
]
verified = asyncio.run(
    verify_domain_search_results_with_llm(
        raw_payload=record.raw_payload,
        organization_number=record.organization_number,
        organization_name=record.organization_name,
        country="NO",
        search_results=search_results,
    )
)
crawl_rows: list[InsertDomainCrawlResult] = []
for artifact in verified.crawl_results:
    search_row = search_row_by_domain_url.get(
        (artifact.search_result.normalized_domain, artifact.search_result.url)
    )
    if search_row is None:
        raise RuntimeError("verified crawl artifact has no matching stored search result")
    crawl_rows.append(
        InsertDomainCrawlResult(
            raw_record_id=record.id,
            search_result_id=search_row.id,
            task_attempt_id=attempt.id,
            url=artifact.search_result.url,
            domain=artifact.search_result.domain,
            normalized_domain=artifact.search_result.normalized_domain,
            status=artifact.status,
            markdown=artifact.markdown,
            markdown_hash=artifact.markdown_hash,
            llm_confidence=artifact.llm_confidence,
            llm_decision=artifact.llm_decision,
            llm_reason=artifact.llm_reason,
            llm_evidence=artifact.llm_evidence,
            metadata=artifact.metadata,
        )
    )
store.insert_domain_crawl_results(
    crawl_rows
)
store.insert_domain_candidates(
    [
        InsertDomainCandidate(
            raw_record_id=record.id,
            task_attempt_id=attempt.id,
            domain=candidate.domain,
            normalized_domain=candidate.normalized_domain,
            signal=candidate.signal,
            confidence=candidate.confidence,
            evidence=candidate.evidence,
            metadata=candidate.metadata,
        )
        for candidate in verified.candidates
    ]
)
```

The explicit `RuntimeError` keeps a bad artifact mapping from silently writing incomplete lineage.

- [ ] **Step 7: Update website-field task skip summary**

In `_discover_record_domain_signal`, when `signal == "website_field"` and no candidates are found, mark the task state as skipped instead of succeeded:

```python
task_status = "succeeded" if candidates else "skipped"
store.finish_task_attempt(task_attempt_id=attempt.id, status=task_status, error=None)
```

The existing `finish_task_attempt` method already maps `skipped` into `raw_record_task_states.status = 'skipped'`, so no additional task-state method is needed for this step.

- [ ] **Step 8: Run asset tests**

Run:

```bash
uv run pytest dagster/tests/brreg/test_assets.py -q
```

Expected: pass.

- [ ] **Step 9: Commit asset changes**

Run:

```bash
git add dagster/src/corpscout_dagster/brreg/assets.py dagster/tests/brreg/test_assets.py
git commit -m "feat: make brreg domain assets dependency driven"
```

---

### Task 5: Update Dagster Definitions

**Files:**
- Modify: `dagster/src/corpscout_dagster/definitions.py`
- Test: `dagster/tests/brreg/test_assets.py`

- [ ] **Step 1: Write definitions test**

Update the existing job-name test:

```python
def test_definitions_expose_only_active_domain_jobs() -> None:
    job_names = {job.name for job in defs.jobs}

    assert "brreg_domain_website_field_job" in job_names
    assert "brreg_domain_duckduckgo_search_job" in job_names
    assert "brreg_domain_web_search_llm_job" in job_names
    assert "brreg_domain_proposals_job" in job_names
    assert "brreg_domain_enrichment_job" in job_names
    assert "brreg_domain_crtsh_job" not in job_names
    assert "brreg_domain_wikidata_job" not in job_names
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest dagster/tests/brreg/test_assets.py::test_definitions_expose_only_active_domain_jobs -q
```

Expected: fails because old jobs are still active.

- [ ] **Step 3: Update definitions**

Change imports and `BRREG_DOMAIN_ASSETS`:

```python
from corpscout_dagster.brreg.assets import (
    brreg_domain_duckduckgo_search_results,
    brreg_domain_proposals,
    brreg_domain_web_search_llm_candidates,
    brreg_domain_website_field_candidates,
    brreg_enhanced_records,
    brreg_publish_enhanced_records,
    brreg_translation_results,
    brreg_working_raw_records,
)

BRREG_DOMAIN_ASSETS = [
    brreg_domain_website_field_candidates,
    brreg_domain_duckduckgo_search_results,
    brreg_domain_web_search_llm_candidates,
    brreg_domain_proposals,
]
```

Change jobs:

```python
define_asset_job(
    "brreg_domain_duckduckgo_search_job",
    selection=AssetSelection.assets(brreg_domain_duckduckgo_search_results),
),
define_asset_job("brreg_domain_enrichment_job", selection=AssetSelection.assets(*BRREG_DOMAIN_ASSETS)),
```

Remove the active `brreg_domain_crtsh_job` and `brreg_domain_wikidata_job` entries.

- [ ] **Step 4: Run definitions tests**

Run:

```bash
uv run pytest dagster/tests/brreg/test_assets.py -q
```

Expected: pass.

- [ ] **Step 5: Commit definitions**

Run:

```bash
git add dagster/src/corpscout_dagster/definitions.py dagster/tests/brreg/test_assets.py
git commit -m "feat: remove brreg crtsh and wikidata from active dagster graph"
```

---

### Task 6: Full Local Verification

**Files:**
- All files changed above.

- [ ] **Step 1: Run BRREG tests**

Run:

```bash
uv run pytest dagster/tests/brreg -q
```

Expected: all BRREG tests pass.

- [ ] **Step 2: Run all Dagster tests**

Run:

```bash
uv run pytest -q
```

Expected: all tests pass.

- [ ] **Step 3: Validate Dagster definitions**

Run:

```bash
make validate
```

Expected: all code locations pass validation.

- [ ] **Step 4: Check whitespace**

Run:

```bash
git diff --check
```

Expected: no output and exit code 0.

- [ ] **Step 5: Commit fixes if verification required any edits**

Run only if Step 1-4 required follow-up edits:

```bash
git add dagster/src dagster/tests dagster/db/migrations
git commit -m "test: verify brreg domain discovery dependency flow"
```

---

### Task 7: Push, Build, Deploy, And Validate Remote 1k Dataset

**Files:**
- Remote: `/home/graovic/temporal/dagster/docker-compose.yml`
- Remote database: `dagster_brreg`

- [ ] **Step 1: Push commits**

Run:

```bash
git push origin main
```

Expected: push succeeds.

- [ ] **Step 2: Wait for GitHub image build**

Run:

```bash
gh run list --repo pulsarpoint/temporal --workflow "Build Dagster Image" --limit 1
```

Expected: latest run for the pushed commit reaches `completed` with `success`.

- [ ] **Step 3: Deploy on remote**

Run:

```bash
ssh graovic@100.85.212.113 'bash -s' <<'REMOTE'
set -euo pipefail
cd /home/graovic/temporal
git pull --ff-only origin main
cd dagster
docker compose pull dagster-webserver dagster-daemon
docker compose run --rm dagster-migrate-up
docker compose up -d --force-recreate dagster-webserver dagster-daemon
REMOTE
```

Expected: migration reaches version `12`, webserver and daemon start.

- [ ] **Step 4: Verify remote schema and UI**

Run:

```bash
ssh graovic@100.85.212.113 "cd /home/graovic/temporal/dagster && docker compose run --rm dagster-migrate-version && curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3000/assets"
```

Expected:

```text
12
200
```

- [ ] **Step 5: Run website-field job on 1k dataset from Dagster UI or CLI**

Use Dagster UI job `brreg_domain_website_field_job` with:

```yaml
ops:
  brreg_domain_website_field_candidates:
    config:
      batch_size: 5000
      max_batches_per_run: 0
      max_parallel_tasks: 50
resources: {}
```

Then verify:

```bash
ssh graovic@100.85.212.113 "docker exec ppoint-postgres psql -U corpscout -d corpscout -P pager=off -c \"select task_type, status, count(*) from dagster_brreg.raw_record_task_states group by task_type, status order by task_type, status; select count(*) from dagster_brreg.domain_candidates where signal = 'website_field';\""
```

Expected: `domain_website_field` has 1,000 completed states across `succeeded` and `skipped`; website-field candidate count is near the 250 rows that have website values.

- [ ] **Step 6: Run DuckDuckGo-search job**

Use Dagster UI job `brreg_domain_duckduckgo_search_job` with:

```yaml
ops:
  brreg_domain_duckduckgo_search_results:
    config:
      batch_size: 10
      max_batches_per_run: 0
      max_parallel_tasks: 2
resources: {}
```

Verify:

```bash
ssh graovic@100.85.212.113 "docker exec ppoint-postgres psql -U corpscout -d corpscout -P pager=off -c \"select count(*) from dagster_brreg.domain_search_results; select count(*) from dagster_brreg.raw_record_task_states where task_type = 'domain_duckduckgo_search';\""
```

Expected: DuckDuckGo task states exist only for records without website-field candidates.

- [ ] **Step 7: Run LLM verification job**

Use Dagster UI job `brreg_domain_web_search_llm_job` with:

```yaml
ops:
  brreg_domain_web_search_llm_candidates:
    config:
      batch_size: 10
      max_batches_per_run: 0
      max_parallel_tasks: 1
resources: {}
```

Verify:

```bash
ssh graovic@100.85.212.113 "docker exec ppoint-postgres psql -U corpscout -d corpscout -P pager=off -c \"select count(*) from dagster_brreg.domain_crawl_results; select count(*) from dagster_brreg.domain_candidates where signal = 'web_search_llm';\""
```

Expected: crawl artifacts are present and web-search candidates exist only for verified domains.

- [ ] **Step 8: Run proposal merge**

Use Dagster UI job `brreg_domain_proposals_job` with:

```yaml
ops:
  brreg_domain_proposals:
    config:
      batch_size: 500
      max_batches_per_run: 0
      max_parallel_tasks: 50
resources: {}
```

Verify:

```bash
ssh graovic@100.85.212.113 "docker exec ppoint-postgres psql -U corpscout -d corpscout -P pager=off -c \"select count(*) from dagster_brreg.domain_proposals; select signal, count(*) from dagster_brreg.domain_candidates group by signal order by signal;\""
```

Expected: proposals are created from `website_field` and `web_search_llm` candidates only.
