from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date

import psycopg
from dagster import AssetKey, Field, Int, asset

from corpscout_dagster.brreg.domain_enrichment import build_domain_proposals, discover_domain_candidates_for_signal
from corpscout_dagster.brreg.domain_search_llm import (
    SearchResult,
    collect_duckduckgo_search_results,
    verify_domain_search_results_with_llm,
)
from corpscout_dagster.brreg.crawl_service import CrawlServiceClient, HttpCrawlServiceClient
from corpscout_dagster.brreg.enhanced_payload import (
    BRREG_ENHANCED_SCHEMA_VERSION,
    build_brreg_enhanced_payload,
    enhanced_payload_hash,
)
from corpscout_dagster.brreg.fx_rates import FxRateSet, load_ecb_rates_for_date, load_latest_ecb_rates
from corpscout_dagster.brreg.models import BrregRawRecord, BrregWorkingRawRecordRow
from corpscout_dagster.brreg.source import BRREG_API_BASE_URL, BRREG_BULK_PATH, iter_brreg_bulk_records
from corpscout_dagster.brreg.translation import (
    DEFAULT_LLM_MODEL,
    DEFAULT_PROMPT_VERSION,
    CachedTermTranslation,
    HttpTranslationServiceTermTranslator,
    TermTranslator,
    TranslationCacheKey,
    TranslationItem,
    build_translation_payload,
    extract_translation_items,
    translation_cache_key,
    translation_item_id,
)
from corpscout_dagster.brreg.working_store import (
    BrregWorkingStore,
    CreateBulkSnapshot,
    CreateEnrichmentRun,
    CreateTaskAttempt,
    FinishEnrichmentRun,
    EnhancedPublishRecord,
    IncrementEnrichmentRunProgress,
    InsertDomainCrawlResult,
    InsertDomainCandidate,
    InsertDomainResult,
    InsertDomainProposal,
    InsertDomainSearchResult,
    InsertEnhancedRecord,
    InsertTranslationResult,
    RawTaskRecord,
    TaskAttempt,
    UpsertCachedTranslation,
)


BRREG_BULK_URL = f"{BRREG_API_BASE_URL}{BRREG_BULK_PATH}"
DEFAULT_RAW_RECORD_BATCH_SIZE = 5000
DEFAULT_TRANSLATION_RECORD_BATCH_SIZE = 50
DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN = 0
DEFAULT_DOMAIN_WEBSITE_FIELD_BATCH_SIZE = 5000
DEFAULT_DOMAIN_DUCKDUCKGO_BATCH_SIZE = 10
DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_BATCH_SIZE = 10
DEFAULT_DOMAIN_CRTSH_BATCH_SIZE = 10
DEFAULT_DOMAIN_WIKIDATA_BATCH_SIZE = 25
DEFAULT_DOMAIN_WEB_SEARCH_LLM_BATCH_SIZE = 10
DEFAULT_DOMAIN_PROPOSAL_BATCH_SIZE = 500
DEFAULT_DOMAIN_RESULT_BATCH_SIZE = 10
DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN = 0
DEFAULT_ENHANCED_RECORD_BATCH_SIZE = 500
DEFAULT_PUBLISH_ENHANCED_RECORD_BATCH_SIZE = 250
DEFAULT_TASK_LEASE_SECONDS = 1800
DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS = DEFAULT_TRANSLATION_RECORD_BATCH_SIZE
DEFAULT_DOMAIN_WEBSITE_FIELD_MAX_PARALLEL_TASKS = 50
DEFAULT_DOMAIN_DUCKDUCKGO_MAX_PARALLEL_TASKS = 2
DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_MAX_PARALLEL_TASKS = 1
DEFAULT_DOMAIN_CRTSH_MAX_PARALLEL_TASKS = 5
DEFAULT_DOMAIN_WIKIDATA_MAX_PARALLEL_TASKS = 5
DEFAULT_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS = 1
DEFAULT_DOMAIN_PROPOSAL_MAX_PARALLEL_TASKS = 50
DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS = 1

DOMAIN_SIGNAL_ASSET_KEYS = [
    AssetKey("brreg_domain_website_field_candidates"),
    AssetKey("brreg_domain_duckduckgo_search_results"),
    AssetKey("brreg_domain_web_search_llm_candidates"),
]


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


@dataclass(frozen=True)
class BrregBatchRunConfig:
    batch_size: int
    max_batches_per_run: int
    max_parallel_tasks: int


def brreg_batch_run_config_schema(
    *,
    batch_size_default: int,
    max_batches_default: int,
    max_parallel_tasks_default: int,
) -> dict[str, Field]:
    return {
        "batch_size": Field(
            Int,
            default_value=batch_size_default,
            description="Number of BRREG rows claimed in each batch.",
        ),
        "max_batches_per_run": Field(
            Int,
            default_value=max_batches_default,
            description="Maximum batches for this run. Use 0 to keep running until no pending rows remain.",
        ),
        "max_parallel_tasks": Field(
            Int,
            default_value=max_parallel_tasks_default,
            description="Maximum active BRREG company tasks for this task type.",
        ),
    }


def resolve_brreg_batch_run_config(
    context,
    *,
    batch_size_env: str,
    batch_size_default: int,
    max_batches_env: str,
    max_batches_default: int,
    max_parallel_tasks_env: str = "BRREG_TASK_MAX_PARALLEL_TASKS",
    max_parallel_tasks_default: int = DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
) -> BrregBatchRunConfig:
    op_config = getattr(context, "op_config", None) or {}
    return BrregBatchRunConfig(
        batch_size=int(op_config.get("batch_size", _env_int(batch_size_env, batch_size_default))),
        max_batches_per_run=int(
            op_config.get(
                "max_batches_per_run",
                _env_int(max_batches_env, max_batches_default),
            )
        ),
        max_parallel_tasks=int(
            op_config.get(
                "max_parallel_tasks",
                _env_int(max_parallel_tasks_env, max_parallel_tasks_default),
            )
        ),
    )


def build_brreg_working_raw_record_rows(
    *,
    records: Iterable[BrregRawRecord | None],
) -> list[BrregWorkingRawRecordRow]:
    return [record.to_working_row() for record in records if record is not None]


@asset(name="brreg_working_raw_records")
def brreg_working_raw_records(context) -> dict[str, int]:
    return materialize_brreg_working_raw_records(
        context,
        records=iter_brreg_bulk_records(),
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=DEFAULT_RAW_RECORD_BATCH_SIZE,
    )


@asset(
    name="brreg_translation_results",
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_TRANSLATION_BATCH_SIZE", DEFAULT_TRANSLATION_RECORD_BATCH_SIZE),
        max_batches_default=_env_int(
            "BRREG_TRANSLATION_MAX_BATCHES_PER_RUN",
            DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
        ),
        max_parallel_tasks_default=_env_int(
            "BRREG_TRANSLATION_MAX_PARALLEL_TASKS",
            DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_translation_results(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_TRANSLATION_BATCH_SIZE",
        batch_size_default=DEFAULT_TRANSLATION_RECORD_BATCH_SIZE,
        max_batches_env="BRREG_TRANSLATION_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_TRANSLATION_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_translation_results(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        translator=HttpTranslationServiceTermTranslator.from_env(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
        model=(
            os.environ.get("BRREG_TRANSLATION_MODEL")
            or os.environ.get("TRANSLATION_DEFAULT_MODEL")
            or DEFAULT_LLM_MODEL
        ),
        prompt_version=os.environ.get("BRREG_TRANSLATION_PROMPT_VERSION") or DEFAULT_PROMPT_VERSION,
    )


@asset(
    name="brreg_domain_results",
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_RESULT_BATCH_SIZE", DEFAULT_DOMAIN_RESULT_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_RESULT_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_RESULT_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_results(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_RESULT_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_RESULT_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_RESULT_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_RESULT_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_domain_results(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        crawl_service_client=HttpCrawlServiceClient.from_env(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )


@asset(
    name="brreg_domain_website_field_candidates",
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_WEBSITE_FIELD_BATCH_SIZE", DEFAULT_DOMAIN_WEBSITE_FIELD_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_WEBSITE_FIELD_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_WEBSITE_FIELD_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_website_field_candidates(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_WEBSITE_FIELD_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_WEBSITE_FIELD_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_WEBSITE_FIELD_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_WEBSITE_FIELD_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="website_field",
        task_type="domain_website_field",
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )


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


@asset(
    name="brreg_domain_duckduckgo_candidates",
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_DUCKDUCKGO_BATCH_SIZE", DEFAULT_DOMAIN_DUCKDUCKGO_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_DUCKDUCKGO_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_DUCKDUCKGO_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_duckduckgo_candidates(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_DUCKDUCKGO_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_DUCKDUCKGO_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_DUCKDUCKGO_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_DUCKDUCKGO_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="duckduckgo",
        task_type="domain_duckduckgo",
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )


@asset(
    name="brreg_domain_crtsh_candidates",
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_CRTSH_BATCH_SIZE", DEFAULT_DOMAIN_CRTSH_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_CRTSH_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_CRTSH_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_crtsh_candidates(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_CRTSH_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_CRTSH_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_CRTSH_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_CRTSH_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="crtsh",
        task_type="domain_crtsh",
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )


@asset(
    name="brreg_domain_wikidata_candidates",
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_WIKIDATA_BATCH_SIZE", DEFAULT_DOMAIN_WIKIDATA_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_WIKIDATA_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_WIKIDATA_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_wikidata_candidates(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_WIKIDATA_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_WIKIDATA_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_WIKIDATA_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_WIKIDATA_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        signal="wikidata",
        task_type="domain_wikidata",
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )


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
def brreg_domain_web_search_llm_candidates(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_WEB_SEARCH_LLM_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_WEB_SEARCH_LLM_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_web_search_llm_candidates(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )


@asset(
    name="brreg_domain_proposals",
    deps=DOMAIN_SIGNAL_ASSET_KEYS,
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_PROPOSAL_BATCH_SIZE", DEFAULT_DOMAIN_PROPOSAL_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_PROPOSAL_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_PROPOSAL_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_proposals(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_PROPOSAL_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_PROPOSAL_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_PROPOSAL_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_PROPOSAL_MAX_PARALLEL_TASKS,
    )
    return materialize_brreg_domain_proposals(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )


@asset(name="brreg_enhanced_records", deps=[AssetKey("brreg_translation_results"), AssetKey("brreg_domain_results")])
def brreg_enhanced_records(context) -> dict[str, int]:
    return materialize_brreg_enhanced_records(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=_env_int("BRREG_ENHANCED_RECORD_BATCH_SIZE", DEFAULT_ENHANCED_RECORD_BATCH_SIZE),
    )


@asset(
    name="brreg_domain_enhanced_records",
    deps=[AssetKey("brreg_translation_results")],
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=_env_int("BRREG_DOMAIN_RESULT_BATCH_SIZE", DEFAULT_DOMAIN_RESULT_BATCH_SIZE),
        max_batches_default=_env_int("BRREG_DOMAIN_RESULT_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=_env_int(
            "BRREG_DOMAIN_RESULT_MAX_PARALLEL_TASKS",
            DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS,
        ),
    ),
)
def brreg_domain_enhanced_records(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_DOMAIN_RESULT_BATCH_SIZE",
        batch_size_default=DEFAULT_DOMAIN_RESULT_BATCH_SIZE,
        max_batches_env="BRREG_DOMAIN_RESULT_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_DOMAIN_RESULT_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS,
    )
    domain_result = materialize_brreg_domain_results(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        crawl_service_client=HttpCrawlServiceClient.from_env(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )
    enhanced_result = materialize_brreg_enhanced_records(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=_env_int("BRREG_ENHANCED_RECORD_BATCH_SIZE", DEFAULT_ENHANCED_RECORD_BATCH_SIZE),
    )
    return {
        **{f"domain_{key}": value for key, value in domain_result.items()},
        **{f"enhanced_{key}": value for key, value in enhanced_result.items()},
    }


@asset(name="brreg_publish_enhanced_records", deps=[AssetKey("brreg_enhanced_records")])
def brreg_publish_enhanced_records(context) -> dict[str, int]:
    return materialize_brreg_publish_enhanced_records(
        context,
        connection_factory=psycopg.connect,
        database_url=_corpscout_database_url(),
        batch_size=_env_int("BRREG_PUBLISH_ENHANCED_RECORD_BATCH_SIZE", DEFAULT_PUBLISH_ENHANCED_RECORD_BATCH_SIZE),
    )


def materialize_brreg_working_raw_records(
    context,
    *,
    records: Iterable[BrregRawRecord | None],
    connection_factory,
    database_url: str,
    batch_size: int,
) -> dict[str, int]:
    rows_seen = 0
    rows_written = 0
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            enrichment_run_id = store.create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, "bulk_ingest"),
                    run_type="bulk_ingest",
                    metadata={"source": "brreg", "dagster_run_id": context.run_id},
                )
            )
            bulk_snapshot_id = store.create_bulk_snapshot(
                CreateBulkSnapshot(
                    enrichment_run_id=enrichment_run_id,
                    source_url=BRREG_BULK_URL,
                    content_length_bytes=None,
                    compressed_payload_hash=None,
                    storage_uri=None,
                    metadata={"format": "gzip-json"},
                )
            )
        conn.commit()

        try:
            for batch in _iter_working_row_batches(records, batch_size=batch_size):
                with conn.cursor() as cursor:
                    store = BrregWorkingStore(cursor)
                    result = store.upsert_raw_records(batch, bulk_snapshot_id=bulk_snapshot_id)
                    store.increment_enrichment_run_progress(
                        IncrementEnrichmentRunProgress(
                            enrichment_run_id=enrichment_run_id,
                            records_seen=result.rows_seen,
                            records_completed=result.rows_written,
                        )
                    )
                conn.commit()
                rows_seen += result.rows_seen
                rows_written += result.rows_written
                context.log.info(
                    "BRREG raw ingest batch committed rows_seen=%s rows_written=%s total_rows_seen=%s",
                    result.rows_seen,
                    result.rows_written,
                    rows_seen,
                )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded",
                        error=None,
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    context.add_output_metadata(
        {
            "rows_seen": rows_seen,
            "rows_written": rows_written,
            "dagster_run_id": context.run_id,
        }
    )
    return {"rows_seen": rows_seen, "rows_written": rows_written}


def materialize_brreg_translation_results(
    context,
    *,
    connection_factory,
    database_url: str,
    translator: TermTranslator,
    batch_size: int,
    max_batches_per_run: int = DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
    max_parallel_tasks: int = DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
    model: str,
    prompt_version: str,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, "translate"),
                    run_type="translate",
                    metadata={
                        "source": "brreg",
                        "dagster_run_id": context.run_id,
                        "model": model,
                        "prompt_version": prompt_version,
                    },
                )
            )
        conn.commit()

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                        task_type="translate",
                        limit=batch_size,
                        include_new_records=True,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()

                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                completed, failed = _translate_record_batch(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    records=records,
                    translator=translator,
                    model=model,
                    prompt_version=prompt_version,
                )
                rows_seen += len(records)
                rows_completed += completed
                rows_failed += failed

            context.log.info(
                "BRREG translation batches committed rows_seen=%s rows_completed=%s rows_failed=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} translation rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
    )
    return result


def materialize_brreg_domain_signal_candidates(
    context,
    *,
    connection_factory,
    database_url: str,
    signal: str,
    task_type: str,
    batch_size: int,
    max_batches_per_run: int = 1,
    max_parallel_tasks: int = DEFAULT_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    domains_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id, "signal": signal},
                )
            )
        conn.commit()

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                        task_type=task_type,
                        limit=batch_size,
                        include_new_records=True,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()
                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                rows_seen += len(records)
                completed, failed, written = _process_domain_signal_records(
                    connection_factory=connection_factory,
                    database_url=database_url,
                    shared_conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    records=records,
                    signal=signal,
                    task_type=task_type,
                    max_parallel_tasks=max_parallel_tasks,
                )
                rows_completed += completed
                rows_failed += failed
                domains_written += written

            context.log.info(
                "BRREG domain signal batches committed signal=%s rows_seen=%s rows_completed=%s rows_failed=%s domains_written=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                signal,
                rows_seen,
                rows_completed,
                rows_failed,
                domains_written,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} domain enrichment rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "domains_written": domains_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "signal": signal,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
    )
    return result


def materialize_brreg_domain_results(
    context,
    *,
    connection_factory,
    database_url: str,
    crawl_service_client: CrawlServiceClient,
    batch_size: int,
    max_batches_per_run: int = DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
    max_parallel_tasks: int = DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    domain_results_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    task_type = "domain_results"
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id, "service": "crawl-service"},
                )
            )
        conn.commit()

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_raw_task_records(
                        task_type=task_type,
                        limit=batch_size,
                        include_new_records=True,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()
                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                rows_seen += len(records)
                for record in records:
                    attempt = _create_task_attempt(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        record=record,
                        task_type=task_type,
                    )
                    try:
                        payload = crawl_service_client.discover_brreg_domain(record)
                        task_succeeded = _write_domain_result(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            payload=payload,
                        )
                        domain_results_written += 1
                        if task_succeeded:
                            rows_completed += 1
                        else:
                            rows_failed += 1
                    except Exception as exc:
                        conn.rollback()
                        _mark_domain_result_failed(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            error=str(exc),
                        )
                        rows_failed += 1
                        domain_results_written += 1

            context.log.info(
                "BRREG domain result batches committed rows_seen=%s rows_completed=%s rows_failed=%s domain_results_written=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                domain_results_written,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} domain result rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "domain_results_written": domain_results_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
    )
    return result


def materialize_brreg_duckduckgo_search_results(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
    max_batches_per_run: int = 1,
    max_parallel_tasks: int = DEFAULT_DOMAIN_DUCKDUCKGO_SEARCH_MAX_PARALLEL_TASKS,
    search_collector=collect_duckduckgo_search_results,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    search_results_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    task_type = "domain_duckduckgo_search"
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id, "provider": "duckduckgo"},
                )
            )
        conn.commit()

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_duckduckgo_search_records(
                        limit=batch_size,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()
                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                rows_seen += len(records)
                completed, failed, written = _process_duckduckgo_search_records(
                    connection_factory=connection_factory,
                    database_url=database_url,
                    shared_conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    records=records,
                    max_parallel_tasks=max_parallel_tasks,
                    search_collector=search_collector,
                )
                rows_completed += completed
                rows_failed += failed
                search_results_written += written

            context.log.info(
                "BRREG DuckDuckGo search batches committed rows_seen=%s rows_completed=%s rows_failed=%s search_results_written=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                search_results_written,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} DuckDuckGo search rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "search_results_written": search_results_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
    )
    return result


def materialize_brreg_web_search_llm_candidates(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
    max_batches_per_run: int = 1,
    max_parallel_tasks: int = DEFAULT_DOMAIN_WEB_SEARCH_LLM_MAX_PARALLEL_TASKS,
    verifier=verify_domain_search_results_with_llm,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    domains_written = 0
    crawl_results_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    task_type = "domain_web_search_llm"
    enrichment_run_id: str | None = None
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id, "signal": "web_search_llm"},
                )
            )
        conn.commit()

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_web_search_llm_records(
                        limit=batch_size,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()
                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                rows_seen += len(records)
                completed, failed, written, crawl_written = _process_web_search_llm_records(
                    connection_factory=connection_factory,
                    database_url=database_url,
                    shared_conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    records=records,
                    max_parallel_tasks=max_parallel_tasks,
                    verifier=verifier,
                )
                rows_completed += completed
                rows_failed += failed
                domains_written += written
                crawl_results_written += crawl_written

            context.log.info(
                "BRREG web-search LLM batches committed rows_seen=%s rows_completed=%s rows_failed=%s domains_written=%s crawl_results_written=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                domains_written,
                crawl_results_written,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} web-search LLM rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "domains_written": domains_written,
        "crawl_results_written": crawl_results_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "signal": "web_search_llm",
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
    )
    return result


def materialize_brreg_domain_proposals(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
    max_batches_per_run: int = 1,
    max_parallel_tasks: int = DEFAULT_DOMAIN_PROPOSAL_MAX_PARALLEL_TASKS,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batches_per_run < 0:
        raise ValueError("max_batches_per_run must be zero or positive")
    if max_parallel_tasks <= 0:
        raise ValueError("max_parallel_tasks must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    proposals_written = 0
    batches_processed = 0
    stopped_reason = "max_batches_reached"
    enrichment_run_id: str | None = None
    task_type = "merge_domain_proposals"
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id},
                )
            )
        conn.commit()

        try:
            while max_batches_per_run == 0 or batches_processed < max_batches_per_run:
                with conn.cursor() as cursor:
                    records = BrregWorkingStore(cursor).fetch_pending_domain_proposal_records(
                        task_type=task_type,
                        limit=batch_size,
                        max_parallel_tasks=max_parallel_tasks,
                        lease_seconds=DEFAULT_TASK_LEASE_SECONDS,
                    )
                conn.commit()
                if not records:
                    stopped_reason = "no_pending_records"
                    break

                batches_processed += 1
                for record in records:
                    rows_seen += 1
                    attempt = _create_task_attempt(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        record=record,
                        task_type=task_type,
                    )
                    try:
                        proposals_written += _merge_record_domain_proposals(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                        )
                        rows_completed += 1
                    except Exception as exc:
                        conn.rollback()
                        _mark_record_task_failed(
                            conn=conn,
                            enrichment_run_id=enrichment_run_id,
                            attempt=attempt,
                            record=record,
                            task_type=task_type,
                            error=str(exc),
                        )
                        rows_failed += 1

            context.log.info(
                "BRREG domain proposal batches committed rows_seen=%s rows_completed=%s rows_failed=%s proposals_written=%s batches_processed=%s max_batches_per_run=%s max_parallel_tasks=%s stopped_reason=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                proposals_written,
                batches_processed,
                max_batches_per_run,
                max_parallel_tasks,
                stopped_reason,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} domain proposal rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "proposals_written": proposals_written,
        "batches_processed": batches_processed,
    }
    context.add_output_metadata(
        {
            **result,
            "dagster_run_id": context.run_id,
            "task_type": task_type,
            "max_batches_per_run": max_batches_per_run,
            "max_parallel_tasks": max_parallel_tasks,
            "stopped_reason": stopped_reason,
        }
    )
    return result


def materialize_brreg_enhanced_records(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
    fx_rate_loader: Callable[[str | None], FxRateSet] | None = None,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    enhanced_records_built = 0
    enrichment_run_id: str | None = None
    task_type = "build_enhanced"
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id},
                )
            )
        conn.commit()

        try:
            with conn.cursor() as cursor:
                records = BrregWorkingStore(cursor).fetch_pending_enhanced_build_records(limit=batch_size)
            fx_rates = None
            if _enhanced_build_records_need_fx(records):
                loader = fx_rate_loader or _load_brreg_fx_rates
                fx_rates = loader(os.environ.get("BRREG_FX_RATE_DATE"))

            for build_record in records:
                rows_seen += 1
                attempt = _create_task_attempt(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    record=build_record.record,
                    task_type=task_type,
                )
                try:
                    _build_record_enhanced_payload(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        build_record=build_record,
                        fx_rates=fx_rates,
                        dagster_run_id=context.run_id,
                    )
                    rows_completed += 1
                    enhanced_records_built += 1
                except Exception as exc:
                    conn.rollback()
                    _mark_record_task_failed(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        record=build_record.record,
                        task_type=task_type,
                        error=str(exc),
                    )
                    rows_failed += 1

            context.log.info(
                "BRREG enhanced record batch committed rows_seen=%s rows_completed=%s rows_failed=%s enhanced_records_built=%s",
                rows_seen,
                rows_completed,
                rows_failed,
                enhanced_records_built,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} enhanced record rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
        "enhanced_records_built": enhanced_records_built,
    }
    context.add_output_metadata({**result, "dagster_run_id": context.run_id, "task_type": task_type})
    return result


def materialize_brreg_publish_enhanced_records(
    context,
    *,
    connection_factory,
    database_url: str,
    batch_size: int,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    rows_seen = 0
    rows_completed = 0
    rows_failed = 0
    enrichment_run_id: str | None = None
    task_type = "publish"
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            enrichment_run_id = BrregWorkingStore(cursor).create_enrichment_run(
                CreateEnrichmentRun(
                    dagster_run_id=_enrichment_run_key(context, task_type),
                    run_type=task_type,
                    metadata={"source": "brreg", "dagster_run_id": context.run_id},
                )
            )
        conn.commit()

        try:
            with conn.cursor() as cursor:
                records = BrregWorkingStore(cursor).fetch_pending_enhanced_publish_records(limit=batch_size)

            for publish_record in records:
                rows_seen += 1
                raw_record = _raw_task_record_from_publish_record(publish_record)
                attempt = _create_task_attempt(
                    conn=conn,
                    enrichment_run_id=enrichment_run_id,
                    record=raw_record,
                    task_type=task_type,
                )
                try:
                    _publish_record_to_corpscout(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        record=publish_record,
                        dagster_run_id=context.run_id,
                    )
                    rows_completed += 1
                except Exception as exc:
                    conn.rollback()
                    _mark_record_task_failed(
                        conn=conn,
                        enrichment_run_id=enrichment_run_id,
                        attempt=attempt,
                        record=raw_record,
                        task_type=task_type,
                        error=str(exc),
                    )
                    with conn.cursor() as cursor:
                        BrregWorkingStore(cursor).mark_enhanced_record_publish_failed(
                            enhanced_record_id=publish_record.enhanced_record_id,
                            error=str(exc),
                        )
                    conn.commit()
                    rows_failed += 1

            context.log.info(
                "BRREG enhanced publish batch committed rows_seen=%s rows_completed=%s rows_failed=%s",
                rows_seen,
                rows_completed,
                rows_failed,
            )

            with conn.cursor() as cursor:
                BrregWorkingStore(cursor).finish_enrichment_run(
                    FinishEnrichmentRun(
                        enrichment_run_id=enrichment_run_id,
                        status="succeeded" if rows_failed == 0 else "failed",
                        error=None if rows_failed == 0 else f"{rows_failed} enhanced publish rows failed",
                    )
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if enrichment_run_id is not None:
                with conn.cursor() as cursor:
                    BrregWorkingStore(cursor).finish_enrichment_run(
                        FinishEnrichmentRun(
                            enrichment_run_id=enrichment_run_id,
                            status="failed",
                            error=str(exc),
                        )
                    )
                conn.commit()
            raise

    result = {
        "rows_seen": rows_seen,
        "rows_completed": rows_completed,
        "rows_failed": rows_failed,
    }
    context.add_output_metadata({**result, "dagster_run_id": context.run_id, "task_type": task_type})
    return result


def _iter_working_row_batches(
    records: Iterable[BrregRawRecord | None],
    *,
    batch_size: int,
) -> Iterable[list[BrregWorkingRawRecordRow]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    batch: list[BrregWorkingRawRecordRow] = []
    for record in records:
        if record is None:
            continue
        batch.append(record.to_working_row())
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _create_task_attempt(
    *,
    conn,
    enrichment_run_id: str,
    record: RawTaskRecord,
    task_type: str,
) -> TaskAttempt:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        attempt = store.create_task_attempt(
            CreateTaskAttempt(
                enrichment_run_id=enrichment_run_id,
                raw_record_id=record.id,
                task_type=task_type,
                metadata={"organization_number": record.organization_number},
            )
        )
    conn.commit()
    return attempt


def _translate_record_batch(
    *,
    conn,
    enrichment_run_id: str,
    records: list[RawTaskRecord],
    translator: TermTranslator,
    model: str,
    prompt_version: str,
) -> tuple[int, int]:
    attempts_by_record_id: dict[str, TaskAttempt] = {}
    items_by_record_id: dict[str, list[TranslationItem]] = {}
    for record in records:
        attempts_by_record_id[record.id] = _create_task_attempt(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            record=record,
            task_type="translate",
        )
        items_by_record_id[record.id] = extract_translation_items(record.raw_payload)

    unique_items = _unique_translation_items(
        item
        for items in items_by_record_id.values()
        for item in items
    )
    try:
        keys = [translation_cache_key(item) for item in unique_items]
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            cached = store.fetch_cached_translations(keys, model=model, prompt_version=prompt_version)

        missing_items = [
            item
            for item in unique_items
            if translation_cache_key(item) not in cached
        ]
        new_cache_rows = _translate_missing_terms(
            translator=translator,
            missing_items=missing_items,
            model=model,
            prompt_version=prompt_version,
        )
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            store.upsert_cached_translations(new_cache_rows)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        for record in records:
            _mark_record_task_failed(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                task_type="translate",
                error=str(exc),
            )
        return 0, len(records)

    for row in new_cache_rows:
        key = TranslationCacheKey(
            category=row.category,
            source_lang=row.source_lang,
            target_lang=row.target_lang,
            original_hash=row.original_hash,
        )
        cached[key] = CachedTermTranslation(
            category=row.category,
            original_text=row.original_text,
            translated_text=row.translated_text,
            model=row.model,
            prompt_version=row.prompt_version,
        )

    rows_completed = 0
    rows_failed = 0
    for record in records:
        try:
            _write_translation_record_result(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                items=items_by_record_id[record.id],
                cached_translations=cached,
                model=model,
                prompt_version=prompt_version,
            )
            rows_completed += 1
        except Exception as exc:
            conn.rollback()
            _mark_record_task_failed(
                conn=conn,
                enrichment_run_id=enrichment_run_id,
                attempt=attempts_by_record_id[record.id],
                record=record,
                task_type="translate",
                error=str(exc),
            )
            rows_failed += 1
    return rows_completed, rows_failed


def _write_translation_record_result(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    items: list[TranslationItem],
    cached_translations: dict[TranslationCacheKey, CachedTermTranslation],
    model: str,
    prompt_version: str,
) -> None:
    if not items:
        payload = build_translation_payload(
            raw_payload=record.raw_payload,
            items=[],
            cached_translations={},
            model=model,
            prompt_version=prompt_version,
        )
        status = "skipped"
        metadata = {"reason": "no_translatable_terms"}
    else:
        payload = build_translation_payload(
            raw_payload=record.raw_payload,
            items=items,
            cached_translations=cached_translations,
            model=model,
            prompt_version=prompt_version,
        )
        status = "succeeded"
        metadata = {}

    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_translation_result(
            InsertTranslationResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status=status,
                translated_payload=payload,
                model=model,
                prompt_version=prompt_version,
                error=None,
                metadata=metadata,
            )
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status=status, error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()


def _write_domain_result(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    payload: dict,
) -> bool:
    status = str(payload.get("status") or "failed")
    task_status = "failed" if status == "failed" else "succeeded"
    error = _domain_result_error(payload)
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_domain_result(
            InsertDomainResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status=status,
                best_domain=payload.get("best_domain"),
                domain_payload=payload,
                error=error,
                metadata={
                    "source": "crawl-service",
                    "service_version": payload.get("service_version"),
                    "model": payload.get("model"),
                    "provider": payload.get("provider"),
                },
            )
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status=task_status, error=error)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=1,
                records_completed=1 if task_status == "succeeded" else 0,
                records_failed=0 if task_status == "succeeded" else 1,
            )
        )
    conn.commit()
    return task_status == "succeeded"


def _mark_domain_result_failed(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    error: str,
) -> None:
    payload = {
        "schema_version": "crawl-service.brreg.v1",
        "status": "failed",
        "record_id": record.id,
        "organization_number": record.organization_number,
        "best_domain": None,
        "candidates": [],
        "search_artifacts": [],
        "crawl_artifacts": [],
        "errors": [{"code": "dagster_crawl_service_call_failed", "message": "Crawl service call failed.", "detail": {"error": error}}],
        "warnings": [],
    }
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_domain_result(
            InsertDomainResult(
                raw_record_id=record.id,
                task_attempt_id=attempt.id,
                status="failed",
                best_domain=None,
                domain_payload=payload,
                error=error,
                metadata={"source": "dagster"},
            )
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="failed", error=error)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=1,
                records_completed=0,
                records_failed=1,
            )
        )
    conn.commit()


def _domain_result_error(payload: dict) -> str | None:
    errors = payload.get("errors")
    if not isinstance(errors, list) or not errors:
        return None
    first = errors[0]
    if not isinstance(first, dict):
        return str(first)
    message = first.get("message") or first.get("code")
    return str(message) if message else None


def _translate_missing_terms(
    *,
    translator: TermTranslator,
    missing_items: list[TranslationItem],
    model: str,
    prompt_version: str,
) -> list[UpsertCachedTranslation]:
    rows: list[UpsertCachedTranslation] = []
    unique_items = _unique_translation_items(missing_items)
    if not unique_items:
        return rows
    translated_by_id = translator.translate_terms(
        category="mixed",
        items=unique_items,
        source_lang="no",
        target_lang="en",
        model=model,
        prompt_version=prompt_version,
    )
    for item in unique_items:
        translated_text = translated_by_id.get(translation_item_id(item), "").strip()
        if not translated_text:
            continue
        key = translation_cache_key(item)
        rows.append(
            UpsertCachedTranslation(
                category=item.category,
                source_lang=key.source_lang,
                target_lang=key.target_lang,
                original_hash=key.original_hash,
                original_text=item.text,
                translated_text=translated_text,
                model=model,
                prompt_version=prompt_version,
                metadata={"source": "dagster"},
            )
        )
    return rows


def _unique_translation_items(items: Iterable[TranslationItem]) -> list[TranslationItem]:
    seen: set[TranslationCacheKey] = set()
    unique: list[TranslationItem] = []
    for item in sorted(items, key=lambda value: (value.category, value.text.strip().lower())):
        key = translation_cache_key(item)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _resolve_maybe_awaitable(value):
    if inspect.isawaitable(value):
        return asyncio.run(value)
    return value


def _process_domain_signal_records(
    *,
    connection_factory,
    database_url: str,
    shared_conn,
    enrichment_run_id: str,
    records: list[RawTaskRecord],
    signal: str,
    task_type: str,
    max_parallel_tasks: int,
) -> tuple[int, int, int]:
    if max_parallel_tasks <= 1 or len(records) <= 1:
        completed = 0
        failed = 0
        domains_written = 0
        for record in records:
            record_completed, record_failed, record_domains = _process_domain_signal_record(
                conn=shared_conn,
                enrichment_run_id=enrichment_run_id,
                record=record,
                signal=signal,
                task_type=task_type,
            )
            completed += record_completed
            failed += record_failed
            domains_written += record_domains
        return completed, failed, domains_written

    completed = 0
    failed = 0
    domains_written = 0
    worker_count = min(max_parallel_tasks, len(records))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                _process_domain_signal_record_with_new_connection,
                connection_factory=connection_factory,
                database_url=database_url,
                enrichment_run_id=enrichment_run_id,
                record=record,
                signal=signal,
                task_type=task_type,
            )
            for record in records
        ]
        for future in as_completed(futures):
            record_completed, record_failed, record_domains = future.result()
            completed += record_completed
            failed += record_failed
            domains_written += record_domains
    return completed, failed, domains_written


def _process_domain_signal_record_with_new_connection(
    *,
    connection_factory,
    database_url: str,
    enrichment_run_id: str,
    record: RawTaskRecord,
    signal: str,
    task_type: str,
) -> tuple[int, int, int]:
    with connection_factory(database_url) as conn:
        return _process_domain_signal_record(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            record=record,
            signal=signal,
            task_type=task_type,
        )


def _process_domain_signal_record(
    *,
    conn,
    enrichment_run_id: str,
    record: RawTaskRecord,
    signal: str,
    task_type: str,
) -> tuple[int, int, int]:
    attempt = _create_task_attempt(
        conn=conn,
        enrichment_run_id=enrichment_run_id,
        record=record,
        task_type=task_type,
    )
    try:
        domains_written = _discover_record_domain_signal(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            attempt=attempt,
            record=record,
            signal=signal,
        )
        return 1, 0, domains_written
    except Exception as exc:
        conn.rollback()
        _mark_record_task_failed(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            attempt=attempt,
            record=record,
            task_type=task_type,
            error=str(exc),
        )
        return 0, 1, 0


def _process_duckduckgo_search_records(
    *,
    connection_factory,
    database_url: str,
    shared_conn,
    enrichment_run_id: str,
    records: list[RawTaskRecord],
    max_parallel_tasks: int,
    search_collector,
) -> tuple[int, int, int]:
    if max_parallel_tasks <= 1 or len(records) <= 1:
        completed = 0
        failed = 0
        results_written = 0
        for record in records:
            record_completed, record_failed, record_results = _process_duckduckgo_search_record(
                conn=shared_conn,
                enrichment_run_id=enrichment_run_id,
                record=record,
                search_collector=search_collector,
            )
            completed += record_completed
            failed += record_failed
            results_written += record_results
        return completed, failed, results_written

    completed = 0
    failed = 0
    results_written = 0
    worker_count = min(max_parallel_tasks, len(records))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                _process_duckduckgo_search_record_with_new_connection,
                connection_factory=connection_factory,
                database_url=database_url,
                enrichment_run_id=enrichment_run_id,
                record=record,
                search_collector=search_collector,
            )
            for record in records
        ]
        for future in as_completed(futures):
            record_completed, record_failed, record_results = future.result()
            completed += record_completed
            failed += record_failed
            results_written += record_results
    return completed, failed, results_written


def _process_duckduckgo_search_record_with_new_connection(
    *,
    connection_factory,
    database_url: str,
    enrichment_run_id: str,
    record: RawTaskRecord,
    search_collector,
) -> tuple[int, int, int]:
    with connection_factory(database_url) as conn:
        return _process_duckduckgo_search_record(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            record=record,
            search_collector=search_collector,
        )


def _process_duckduckgo_search_record(
    *,
    conn,
    enrichment_run_id: str,
    record: RawTaskRecord,
    search_collector,
) -> tuple[int, int, int]:
    task_type = "domain_duckduckgo_search"
    attempt = _create_task_attempt(
        conn=conn,
        enrichment_run_id=enrichment_run_id,
        record=record,
        task_type=task_type,
    )
    try:
        results_written = _discover_record_duckduckgo_search_results(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            attempt=attempt,
            record=record,
            search_collector=search_collector,
        )
        return 1, 0, results_written
    except Exception as exc:
        conn.rollback()
        _mark_record_task_failed(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            attempt=attempt,
            record=record,
            task_type=task_type,
            error=str(exc),
        )
        return 0, 1, 0


def _process_web_search_llm_records(
    *,
    connection_factory,
    database_url: str,
    shared_conn,
    enrichment_run_id: str,
    records: list[RawTaskRecord],
    max_parallel_tasks: int,
    verifier,
) -> tuple[int, int, int, int]:
    if max_parallel_tasks <= 1 or len(records) <= 1:
        completed = 0
        failed = 0
        domains_written = 0
        crawl_results_written = 0
        for record in records:
            record_completed, record_failed, record_domains, record_crawls = _process_web_search_llm_record(
                conn=shared_conn,
                enrichment_run_id=enrichment_run_id,
                record=record,
                verifier=verifier,
            )
            completed += record_completed
            failed += record_failed
            domains_written += record_domains
            crawl_results_written += record_crawls
        return completed, failed, domains_written, crawl_results_written

    completed = 0
    failed = 0
    domains_written = 0
    crawl_results_written = 0
    worker_count = min(max_parallel_tasks, len(records))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                _process_web_search_llm_record_with_new_connection,
                connection_factory=connection_factory,
                database_url=database_url,
                enrichment_run_id=enrichment_run_id,
                record=record,
                verifier=verifier,
            )
            for record in records
        ]
        for future in as_completed(futures):
            record_completed, record_failed, record_domains, record_crawls = future.result()
            completed += record_completed
            failed += record_failed
            domains_written += record_domains
            crawl_results_written += record_crawls
    return completed, failed, domains_written, crawl_results_written


def _process_web_search_llm_record_with_new_connection(
    *,
    connection_factory,
    database_url: str,
    enrichment_run_id: str,
    record: RawTaskRecord,
    verifier,
) -> tuple[int, int, int, int]:
    with connection_factory(database_url) as conn:
        return _process_web_search_llm_record(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            record=record,
            verifier=verifier,
        )


def _process_web_search_llm_record(
    *,
    conn,
    enrichment_run_id: str,
    record: RawTaskRecord,
    verifier,
) -> tuple[int, int, int, int]:
    task_type = "domain_web_search_llm"
    attempt = _create_task_attempt(
        conn=conn,
        enrichment_run_id=enrichment_run_id,
        record=record,
        task_type=task_type,
    )
    try:
        domains_written, crawl_results_written = _verify_record_web_search_llm_candidates(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            attempt=attempt,
            record=record,
            verifier=verifier,
        )
        return 1, 0, domains_written, crawl_results_written
    except Exception as exc:
        conn.rollback()
        _mark_record_task_failed(
            conn=conn,
            enrichment_run_id=enrichment_run_id,
            attempt=attempt,
            record=record,
            task_type=task_type,
            error=str(exc),
        )
        return 0, 1, 0, 0


def _discover_record_domain_signal(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    signal: str,
) -> int:
    candidates = asyncio.run(
        discover_domain_candidates_for_signal(
            signal=signal,
            raw_payload=record.raw_payload,
            organization_number=record.organization_number,
            organization_name=record.organization_name,
            website=record.website,
            country="NO",
        )
    )
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
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
                for candidate in candidates
            ]
        )
        task_status = "skipped" if signal == "website_field" and not candidates else "succeeded"
        store.finish_task_attempt(task_attempt_id=attempt.id, status=task_status, error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()
    return len(candidates)


def _discover_record_duckduckgo_search_results(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    search_collector,
) -> int:
    search_results = _resolve_maybe_awaitable(
        search_collector(
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
                for result in search_results
            ]
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()
    return len(search_results)


def _verify_record_web_search_llm_candidates(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    verifier,
) -> tuple[int, int]:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
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
    verified = _resolve_maybe_awaitable(
        verifier(
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
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.insert_domain_crawl_results(crawl_rows)
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
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()
    return len(verified.candidates), len(crawl_rows)


def _merge_record_domain_proposals(*, conn, enrichment_run_id: str, attempt: TaskAttempt, record: RawTaskRecord) -> int:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        candidates = store.fetch_domain_candidates_for_raw_record(raw_record_id=record.id)
        proposals = build_domain_proposals(candidates)
        store.upsert_domain_proposals(
            [
                InsertDomainProposal(
                    raw_record_id=record.id,
                    task_attempt_id=attempt.id,
                    domain=proposal.domain,
                    normalized_domain=proposal.normalized_domain,
                    score=proposal.score,
                    signals=proposal.signals,
                    evidence=proposal.evidence,
                    metadata=proposal.metadata,
                )
                for proposal in proposals
            ]
        )
        store.finish_task_attempt(
            task_attempt_id=attempt.id,
            status="succeeded" if proposals else "skipped",
            error=None,
        )
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()
    return len(proposals)


def _build_record_enhanced_payload(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    build_record,
    fx_rates: FxRateSet | None = None,
    dagster_run_id: str,
) -> None:
    payload = build_brreg_enhanced_payload(
        record=build_record.record,
        payload_hash=build_record.payload_hash,
        translation_status=build_record.translation_status,
        translation_payload=build_record.translation_payload,
        domain_status=build_record.domain_status,
        domain_proposals=build_record.domain_proposals,
        task_statuses=build_record.task_statuses,
        fx_rates=fx_rates,
        dagster_run_id=dagster_run_id,
    )
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.upsert_enhanced_record(
            InsertEnhancedRecord(
                raw_record_id=build_record.record.id,
                task_attempt_id=attempt.id,
                schema_version=BRREG_ENHANCED_SCHEMA_VERSION,
                enhanced_payload=payload,
                enhanced_payload_hash=enhanced_payload_hash(payload),
                metadata={
                    "source": "dagster",
                    "task_statuses": build_record.task_statuses,
                    "raw_payload_hash": build_record.payload_hash,
                },
            )
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()


def _enhanced_build_records_need_fx(records) -> bool:
    for build_record in records:
        capital = build_record.record.raw_payload.get("kapital")
        if isinstance(capital, dict) and ("belop" in capital or "valuta" in capital):
            return True
    return False


def _load_brreg_fx_rates(rate_date: str | None) -> FxRateSet:
    if not rate_date:
        return load_latest_ecb_rates()
    return load_ecb_rates_for_date(date.fromisoformat(rate_date))


def _publish_record_to_corpscout(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: EnhancedPublishRecord,
    dagster_run_id: str,
) -> None:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        raw_input_id = store.upsert_corpscout_raw_input(record=record, run_id=dagster_run_id)
        enhanced_input_id = store.upsert_corpscout_enhanced_raw_input(
            record=record,
            raw_input_id=raw_input_id,
            dagster_run_id=dagster_run_id,
            dagster_asset_key="brreg_publish_enhanced_records",
        )
        store.mark_enhanced_record_published(
            enhanced_record_id=record.enhanced_record_id,
            corpscout_raw_input_id=raw_input_id,
            corpscout_enhanced_raw_input_id=enhanced_input_id,
        )
        store.finish_task_attempt(task_attempt_id=attempt.id, status="succeeded", error=None)
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(enrichment_run_id=enrichment_run_id, records_seen=1, records_completed=1)
        )
    conn.commit()


def _raw_task_record_from_publish_record(record: EnhancedPublishRecord) -> RawTaskRecord:
    return RawTaskRecord(
        id=record.raw_record_id,
        organization_number=record.organization_number,
        organization_name=record.organization_name,
        website=record.website,
        raw_payload=record.raw_payload,
    )


def _mark_record_task_failed(
    *,
    conn,
    enrichment_run_id: str,
    attempt: TaskAttempt,
    record: RawTaskRecord,
    task_type: str,
    error: str,
) -> None:
    with conn.cursor() as cursor:
        store = BrregWorkingStore(cursor)
        store.finish_task_attempt(task_attempt_id=attempt.id, status="failed", error=error)
        if task_type == "translate":
            store.insert_translation_result(
                InsertTranslationResult(
                    raw_record_id=record.id,
                    task_attempt_id=attempt.id,
                    status="failed",
                    translated_payload=None,
                    model=None,
                    prompt_version=None,
                    error=error,
                    metadata={},
                )
            )
        store.increment_enrichment_run_progress(
            IncrementEnrichmentRunProgress(
                enrichment_run_id=enrichment_run_id,
                records_seen=1,
                records_completed=0,
                records_failed=1,
            )
        )
    conn.commit()


def _corpscout_database_url() -> str:
    value = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("CORPSCOUT_DB_URL")
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL or CORPSCOUT_DB_URL is required")
    return value


def _enrichment_run_key(context, run_type: str) -> str:
    return f"{context.run_id}:{run_type}"
