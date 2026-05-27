from __future__ import annotations

from dataclasses import dataclass

from dagster import Field, Int, job, op

from corpscout_dagster.db_brreg.store import BrregWorkingStore


DEFAULT_RETRY_LIMIT = 5000


@dataclass(frozen=True)
class RetryFailureSelection:
    task_type: str | None
    error_category: str
    job_name: str
    op_name: str


def retry_brreg_task_failures(
    context,
    *,
    connection_factory,
    database_url: str,
    task_type: str | None,
    error_category: str,
    limit: int,
) -> dict:
    if limit <= 0:
        raise ValueError("limit must be positive")
    context.log.info(
        "BRREG retry task failures started task_type=%s error_category=%s limit=%s",
        task_type or "any",
        error_category,
        limit,
    )
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            retried_rows = BrregWorkingStore(cursor).retry_task_failures(
                task_type=task_type,
                error_category=error_category,
                limit=limit,
            )
        conn.commit()
    result = {
        "retried_rows": retried_rows,
        "limit": limit,
        "error_category": error_category,
        "task_type": task_type or "any",
    }
    context.add_output_metadata(result)
    context.log.info(
        "BRREG retry task failures completed task_type=%s error_category=%s limit=%s retried_rows=%s",
        task_type or "any",
        error_category,
        limit,
        retried_rows,
    )
    return result


def _retry_op(selection: RetryFailureSelection):
    @op(
        name=selection.op_name,
        required_resource_keys={"postgres"},
        config_schema={
            "limit": Field(
                Int,
                default_value=DEFAULT_RETRY_LIMIT,
                description="Maximum failed BRREG task rows to reset to pending.",
            )
        },
    )
    def _op(context) -> dict:
        postgres = context.resources.postgres
        op_config = getattr(context, "op_config", None) or {}
        return retry_brreg_task_failures(
            context,
            connection_factory=postgres.connection_factory,
            database_url=postgres.database_url,
            task_type=selection.task_type,
            error_category=selection.error_category,
            limit=int(op_config.get("limit", DEFAULT_RETRY_LIMIT)),
        )

    return _op


_retry_translation_invalid_llm_output = _retry_op(
    RetryFailureSelection(
        task_type="translate",
        error_category="invalid_llm_output",
        job_name="brreg_retry_translation_invalid_llm_output_job",
        op_name="retry_translation_invalid_llm_output",
    )
)
_retry_translation_transient_external = _retry_op(
    RetryFailureSelection(
        task_type="translate",
        error_category="transient_external",
        job_name="brreg_retry_translation_transient_external_job",
        op_name="retry_translation_transient_external",
    )
)
_retry_translation_rate_limited = _retry_op(
    RetryFailureSelection(
        task_type="translate",
        error_category="rate_limited",
        job_name="brreg_retry_translation_rate_limited_job",
        op_name="retry_translation_rate_limited",
    )
)
_retry_domain_rate_limited = _retry_op(
    RetryFailureSelection(
        task_type="domain_results",
        error_category="rate_limited",
        job_name="brreg_retry_domain_rate_limited_job",
        op_name="retry_domain_rate_limited",
    )
)
_retry_domain_transient_external = _retry_op(
    RetryFailureSelection(
        task_type="domain_results",
        error_category="transient_external",
        job_name="brreg_retry_domain_transient_external_job",
        op_name="retry_domain_transient_external",
    )
)
_retry_currency_transient_external = _retry_op(
    RetryFailureSelection(
        task_type="currency_conversion",
        error_category="transient_external",
        job_name="brreg_retry_currency_transient_external_job",
        op_name="retry_currency_transient_external",
    )
)
_retry_interrupted_failures = _retry_op(
    RetryFailureSelection(
        task_type=None,
        error_category="interrupted",
        job_name="brreg_retry_interrupted_failures_job",
        op_name="retry_interrupted_failures",
    )
)


@job(name="brreg_retry_translation_invalid_llm_output_job")
def brreg_retry_translation_invalid_llm_output_job() -> None:
    _retry_translation_invalid_llm_output()


@job(name="brreg_retry_translation_transient_external_job")
def brreg_retry_translation_transient_external_job() -> None:
    _retry_translation_transient_external()


@job(name="brreg_retry_translation_rate_limited_job")
def brreg_retry_translation_rate_limited_job() -> None:
    _retry_translation_rate_limited()


@job(name="brreg_retry_domain_rate_limited_job")
def brreg_retry_domain_rate_limited_job() -> None:
    _retry_domain_rate_limited()


@job(name="brreg_retry_domain_transient_external_job")
def brreg_retry_domain_transient_external_job() -> None:
    _retry_domain_transient_external()


@job(name="brreg_retry_currency_transient_external_job")
def brreg_retry_currency_transient_external_job() -> None:
    _retry_currency_transient_external()


@job(name="brreg_retry_interrupted_failures_job")
def brreg_retry_interrupted_failures_job() -> None:
    _retry_interrupted_failures()
