from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx
import psycopg

from corpscout_dagster.brreg.translation_terms import extract_translation_items


ROOT = Path(__file__).parents[1]
COMPOSE_FILE = ROOT / "docker-compose.mock.yml"
ENV_FILE = ROOT / ".env.mock"
DEFAULT_DATABASE_URL = "postgresql://corpscout:corpscout@localhost:15432/corpscout?sslmode=disable"
DEFAULT_MOCK_SEED = "brreg-e2e-v1"
JOBS = {
    "raw": "brreg_ingest_raw_job",
    "translation": "brreg_translate_job",
    "domain": "brreg_domain_job",
    "currency": "brreg_currency_job",
    "enhanced": "brreg_build_enhanced_job",
    "retry_translation": "brreg_retry_translation_transient_external_job",
    "retry_domain": "brreg_retry_domain_transient_external_job",
}


def main() -> None:
    _ensure_env_file()
    _compose("up", "-d", "--build")
    _compose("run", "--rm", "dagster-migrate-up")
    _wait_for_http("http://localhost:18095/healthz")
    _wait_for_http("http://localhost:18096/healthz")
    _reset_database()
    _post("http://localhost:18095/__mock/reset")
    _post("http://localhost:18096/__mock/reset")

    _run_job(JOBS["raw"])
    _assert_scalar("raw_current", _scalar("SELECT count(*) FROM dagster_brreg.raw_records WHERE is_current"), 1000)
    expected = _expected_mock_counts()

    _run_job(JOBS["translation"])
    first_translation = _task_state_counts("translate")
    _assert_counts("first_translation", first_translation, expected["first_translation"])
    _run_job(JOBS["retry_translation"])
    _run_job(JOBS["translation"])

    _run_job(JOBS["domain"])
    first_domain = _task_state_counts("domain_results")
    _assert_counts("first_domain", first_domain, expected["first_domain"])
    _run_job(JOBS["retry_domain"])
    _run_job(JOBS["domain"])

    _run_job(JOBS["currency"])
    _run_job(JOBS["enhanced"])

    summary = _summary()
    _assert_scalar("active_running_tasks", summary["active_running_tasks"], 0)
    _assert_scalar("raw_current", summary["raw_current"], 1000)
    _assert_positive("translation_results", summary["translation_results"])
    _assert_positive("domain_results", summary["domain_results"])
    _assert_positive("currency_results", summary["currency_results"])
    _assert_scalar("enhanced_records", summary["enhanced_records"], summary["eligible_for_enhanced"])
    _assert_counts("final_translation", summary["task_states"]["translate"], expected["final_translation"])
    _assert_counts("final_domain", summary["task_states"]["domain_results"], expected["final_domain"])
    _assert_counts("final_currency", summary["task_states"]["currency_conversion"], expected["currency"])
    _assert_artifact_counts(summary, expected)
    if first_translation.get("failed_retryable", 0) <= 0:
        raise AssertionError("expected first translation run to create retryable failures")
    if first_domain.get("failed_retryable", 0) <= 0:
        raise AssertionError("expected first domain run to create retryable failures")

    print(json.dumps({"first_translation": first_translation, "first_domain": first_domain, **summary}, indent=2, sort_keys=True))


def _run_job(job_name: str) -> None:
    _compose(
        "run",
        "--rm",
        "dagster-webserver",
        "dagster",
        "job",
        "execute",
        "-m",
        "corpscout_dagster.definitions",
        "-a",
        "defs",
        "-j",
        job_name,
    )


def _summary() -> dict[str, Any]:
    return {
        "raw_current": _scalar("SELECT count(*) FROM dagster_brreg.raw_records WHERE is_current"),
        "translation_results": _scalar("SELECT count(*) FROM dagster_brreg.translation_results"),
        "domain_results": _scalar("SELECT count(*) FROM dagster_brreg.domain_results"),
        "currency_results": _scalar("SELECT count(*) FROM dagster_brreg.currency_results"),
        "enhanced_records": _scalar("SELECT count(*) FROM dagster_brreg.enhanced_records WHERE status = 'built'"),
        "eligible_for_enhanced": _scalar(
            """
            SELECT count(*)
            FROM dagster_brreg.raw_records rr
            JOIN dagster_brreg.raw_record_task_states tts
              ON tts.raw_record_id = rr.id AND tts.task_type = 'translate' AND tts.status IN ('succeeded', 'skipped')
            JOIN dagster_brreg.raw_record_task_states dts
              ON dts.raw_record_id = rr.id AND dts.task_type = 'domain_results' AND dts.status IN ('succeeded', 'skipped')
            JOIN dagster_brreg.raw_record_task_states cts
              ON cts.raw_record_id = rr.id AND cts.task_type = 'currency_conversion' AND cts.status IN ('succeeded', 'skipped')
            WHERE rr.is_current = true
            """
        ),
        "active_running_tasks": _scalar(
            """
            SELECT count(*)
            FROM dagster_brreg.raw_record_task_states
            WHERE status = 'running'
              AND coalesce(lease_until, last_started_at + interval '30 minutes') > now()
            """
        ),
        "task_states": {
            task_type: _task_state_counts(task_type)
            for task_type in ["translate", "domain_results", "currency_conversion", "build_enhanced"]
        },
        "artifact_statuses": {
            "translation": _status_counts("translation_results"),
            "domain": _status_counts("domain_results"),
            "currency": _status_counts("currency_results"),
            "enhanced": _status_counts("enhanced_records"),
        },
    }


def _reset_database() -> None:
    sql = """
    TRUNCATE TABLE
      dagster_brreg.enhanced_records,
      dagster_brreg.currency_results,
      dagster_brreg.domain_results,
      dagster_brreg.translation_results,
      dagster_brreg.translation_cache,
      dagster_brreg.raw_record_task_states,
      dagster_brreg.task_attempts,
      dagster_brreg.bulk_snapshots,
      dagster_brreg.raw_records,
      dagster_brreg.enrichment_runs
    RESTART IDENTITY CASCADE
    """
    with psycopg.connect(_database_url()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def _expected_mock_counts() -> dict[str, dict[str, int]]:
    first_translation: Counter[str] = Counter()
    final_translation: Counter[str] = Counter()
    first_domain: Counter[str] = Counter()
    final_domain: Counter[str] = Counter()
    currency: Counter[str] = Counter()
    domain_artifacts: Counter[str] = Counter()
    seed = _env_values().get("MOCK_SEED") or DEFAULT_MOCK_SEED

    for organization_number, raw_payload in _raw_records():
        if extract_translation_items(raw_payload):
            translation_outcome = _mock_outcome(seed, "translation", organization_number)
            if translation_outcome == "fail_once":
                first_translation["failed_retryable"] += 1
                final_translation["succeeded"] += 1
            elif translation_outcome == "terminal":
                first_translation["failed_terminal"] += 1
                final_translation["failed_terminal"] += 1
            else:
                first_translation["succeeded"] += 1
                final_translation["succeeded"] += 1
        else:
            first_translation["skipped"] += 1
            final_translation["skipped"] += 1

        domain_outcome = _mock_outcome(seed, "domain", organization_number)
        if domain_outcome == "fail_once":
            first_domain["failed_retryable"] += 1
            final_domain["succeeded"] += 1
            domain_artifacts["failed"] += 1
            domain_artifacts["succeeded"] += 1
        elif domain_outcome == "terminal":
            first_domain["failed_terminal"] += 1
            final_domain["failed_terminal"] += 1
            domain_artifacts["failed"] += 1
        elif domain_outcome == "not_found":
            first_domain["succeeded"] += 1
            final_domain["succeeded"] += 1
            domain_artifacts["not_found"] += 1
        else:
            first_domain["succeeded"] += 1
            final_domain["succeeded"] += 1
            domain_artifacts["succeeded"] += 1

        if isinstance(raw_payload.get("kapital"), dict):
            currency["succeeded"] += 1
        else:
            currency["skipped"] += 1

    return {
        "first_translation": dict(first_translation),
        "final_translation": dict(final_translation),
        "first_domain": dict(first_domain),
        "final_domain": dict(final_domain),
        "currency": dict(currency),
        "domain_artifacts": dict(domain_artifacts),
    }


def _raw_records() -> list[tuple[str, dict[str, Any]]]:
    with psycopg.connect(_database_url()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT organization_number, raw_payload
                FROM dagster_brreg.raw_records
                WHERE is_current = true
                ORDER BY organization_number
                """
            )
            return [(str(organization_number), dict(raw_payload)) for organization_number, raw_payload in cursor.fetchall()]


def _task_state_counts(task_type: str) -> dict[str, int]:
    with psycopg.connect(_database_url()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, count(*)::int
                FROM dagster_brreg.raw_record_task_states
                WHERE task_type = %s
                GROUP BY status
                ORDER BY status
                """,
                (task_type,),
            )
            return {str(status): int(count) for status, count in cursor.fetchall()}


def _status_counts(table_name: str) -> dict[str, int]:
    with psycopg.connect(_database_url()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT status, count(*)::int FROM dagster_brreg.{table_name} GROUP BY status ORDER BY status")
            return {str(status): int(count) for status, count in cursor.fetchall()}


def _scalar(sql: str) -> int:
    with psycopg.connect(_database_url()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            return int(row[0] if row else 0)


def _post(url: str) -> None:
    response = httpx.post(url, timeout=30)
    response.raise_for_status()


def _wait_for_http(url: str) -> None:
    deadline = time.monotonic() + 120
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=5)
            if response.status_code == 200:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError(f"timed out waiting for {url}: {last_error}")


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "--env-file", str(ENV_FILE), "-f", str(COMPOSE_FILE), *args],
        cwd=ROOT,
        check=True,
    )


def _database_url() -> str:
    return _env_values().get("MOCK_HOST_DATABASE_URL") or DEFAULT_DATABASE_URL


def _ensure_env_file() -> None:
    if not ENV_FILE.exists():
        ENV_FILE.write_text((ROOT / ".env.mock.example").read_text())


def _env_values() -> dict[str, str]:
    if not ENV_FILE.exists():
        return {}
    values: dict[str, str] = {}
    for line in ENV_FILE.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _assert_scalar(label: str, actual: int, expected: int) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected}, got {actual}")


def _assert_positive(label: str, actual: int) -> None:
    if actual <= 0:
        raise AssertionError(f"{label}: expected positive count, got {actual}")


def _assert_counts(label: str, actual: dict[str, int], expected: dict[str, int]) -> None:
    normalized_actual = {key: value for key, value in actual.items() if value}
    normalized_expected = {key: value for key, value in expected.items() if value}
    if normalized_actual != normalized_expected:
        raise AssertionError(f"{label}: expected {normalized_expected}, got {normalized_actual}")


def _assert_artifact_counts(summary: dict[str, Any], expected: dict[str, dict[str, int]]) -> None:
    translation_artifacts = summary["artifact_statuses"]["translation"]
    _assert_scalar(
        "translation succeeded artifacts",
        translation_artifacts.get("succeeded", 0),
        expected["final_translation"].get("succeeded", 0),
    )
    _assert_scalar(
        "translation skipped artifacts",
        translation_artifacts.get("skipped", 0),
        expected["final_translation"].get("skipped", 0),
    )
    minimum_failed_translations = (
        expected["first_translation"].get("failed_retryable", 0)
        + expected["first_translation"].get("failed_terminal", 0)
    )
    if translation_artifacts.get("failed", 0) < minimum_failed_translations:
        raise AssertionError(
            "translation failed artifacts: expected at least "
            f"{minimum_failed_translations}, got {translation_artifacts.get('failed', 0)}"
        )
    _assert_counts("domain artifacts", summary["artifact_statuses"]["domain"], expected["domain_artifacts"])
    _assert_counts("currency artifacts", summary["artifact_statuses"]["currency"], expected["currency"])


def _mock_outcome(seed: str, task: str, organization_number: str) -> str:
    bucket = _mock_bucket(seed, task, organization_number)
    if 80 <= bucket < 90:
        return "fail_once"
    if 90 <= bucket < 95:
        return "terminal"
    if task == "domain" and 95 <= bucket < 100:
        return "not_found"
    return "success"


def _mock_bucket(seed: str, task: str, organization_number: str) -> int:
    digest = hashlib.sha256(f"{seed}:{task}:{organization_number}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


if __name__ == "__main__":
    main()
