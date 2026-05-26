from __future__ import annotations

import psycopg
from dagster import asset

from corpscout_dagster.brreg.asset_config import (
    brreg_batch_run_config_schema,
    corpscout_database_url,
    env_int,
    resolve_brreg_batch_run_config,
)
from corpscout_dagster.brreg.crawl_service import HttpCrawlServiceClient
from corpscout_dagster.brreg.materializations import (
    DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN,
    DEFAULT_DOMAIN_RESULT_BATCH_SIZE,
    DEFAULT_DOMAIN_RESULT_MAX_PARALLEL_TASKS,
    materialize_brreg_domain_results,
)


@asset(
    name="brreg_domain_results",
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=env_int("BRREG_DOMAIN_RESULT_BATCH_SIZE", DEFAULT_DOMAIN_RESULT_BATCH_SIZE),
        max_batches_default=env_int("BRREG_DOMAIN_RESULT_MAX_BATCHES_PER_RUN", DEFAULT_DOMAIN_MAX_BATCHES_PER_RUN),
        max_parallel_tasks_default=env_int(
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
        database_url=corpscout_database_url(),
        crawl_service_client=HttpCrawlServiceClient.from_env(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
    )
