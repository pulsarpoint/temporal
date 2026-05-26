from __future__ import annotations

import os
from dataclasses import dataclass

from dagster import Field, Int

from corpscout_dagster.brreg.materializations import DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


def corpscout_database_url() -> str:
    value = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("CORPSCOUT_DB_URL")
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL or CORPSCOUT_DB_URL is required")
    return value


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
        batch_size=int(op_config.get("batch_size", env_int(batch_size_env, batch_size_default))),
        max_batches_per_run=int(
            op_config.get(
                "max_batches_per_run",
                env_int(max_batches_env, max_batches_default),
            )
        ),
        max_parallel_tasks=int(
            op_config.get(
                "max_parallel_tasks",
                env_int(max_parallel_tasks_env, max_parallel_tasks_default),
            )
        ),
    )
