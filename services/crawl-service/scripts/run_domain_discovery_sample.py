from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

from corpscout_crawl_service.models import BrregDomainDiscoveryRequest, DomainDiscoverLimits
from corpscout_crawl_service.service import CrawlService


def main() -> None:
    args = _parse_args()
    _load_env_file(args.env_file)

    records = _load_records(args.input, args.limit)
    engines = [engine.strip().lower() for engine in args.engines.split(",") if engine.strip()]
    if not engines:
        raise SystemExit("at least one search engine is required")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    asyncio.run(_run(records=records, engines=engines, args=args))


async def _run(*, records: list[dict[str, Any]], engines: list[str], args: argparse.Namespace) -> None:
    run_summary: dict[str, Any] = {
        "input": str(args.input),
        "record_count": len(records),
        "engines": engines,
        "mode": "http" if args.base_url else "in_process",
        "base_url": args.base_url,
        "started_at_unix": int(time.time()),
        "settings": {
            "search_candidate_threshold": args.search_candidate_threshold,
            "domain_threshold": args.domain_threshold,
            "max_search_candidates": args.max_search_candidates,
            "max_site_checks": args.max_site_checks,
            "timeout_seconds": args.timeout_seconds,
        },
        "engine_summaries": {},
    }
    if args.base_url:
        async with httpx.AsyncClient(base_url=args.base_url.rstrip("/"), timeout=args.http_timeout_seconds) as client:
            for engine in engines:
                engine_summary = await _run_engine(
                    service=None,
                    http_client=client,
                    records=records,
                    engine=engine,
                    args=args,
                )
                run_summary["engine_summaries"][engine] = engine_summary
    else:
        service = CrawlService()
        await service.start()
        try:
            for engine in engines:
                engine_summary = await _run_engine(
                    service=service,
                    http_client=None,
                    records=records,
                    engine=engine,
                    args=args,
                )
                run_summary["engine_summaries"][engine] = engine_summary
        finally:
            await service.close()
    run_summary["finished_at_unix"] = int(time.time())
    _write_json(args.output_dir / "summary.json", run_summary)


async def _run_engine(
    *,
    service: CrawlService | None,
    http_client: httpx.AsyncClient | None,
    records: list[dict[str, Any]],
    engine: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    output_path = args.output_dir / f"{engine}.jsonl"
    summary_path = args.output_dir / f"{engine}_summary.json"
    status_counts: Counter[str] = Counter()
    error_counts: Counter[str] = Counter()
    discovered_domains: list[dict[str, Any]] = []
    related_sites: list[dict[str, Any]] = []
    primary_web_presences: list[dict[str, Any]] = []
    rows_seen = 0
    rows_failed = 0

    with output_path.open("w", encoding="utf-8") as output:
        semaphore = asyncio.Semaphore(args.concurrency)
        tasks = [
            asyncio.create_task(
                _run_record(
                    index=index,
                    total=len(records),
                    record=record,
                    engine=engine,
                    service=service,
                    http_client=http_client,
                    args=args,
                    semaphore=semaphore,
                )
            )
            for index, record in enumerate(records, start=1)
        ]
        for task in asyncio.as_completed(tasks):
            result = await task
            rows_seen += 1
            status_counts[result["status"]] += 1
            for error_code in result["error_codes"]:
                error_counts[error_code] += 1
            if result["rows_failed"]:
                rows_failed += 1
            if result["discovered_domain"] is not None:
                discovered_domains.append(result["discovered_domain"])
            related_sites.extend(result["related_sites"])
            if result["primary_web_presence"] is not None:
                primary_web_presences.append(result["primary_web_presence"])
            output.write(result["line"])
            output.flush()
            print(
                f"{engine} {rows_seen}/{len(records)} "
                f"status_counts={dict(status_counts)} errors={dict(error_counts)}",
                flush=True,
            )

    summary = {
        "engine": engine,
        "rows_seen": rows_seen,
        "rows_failed": rows_failed,
        "status_counts": dict(status_counts),
        "error_counts": dict(error_counts),
        "discovered_domain_count": len(discovered_domains),
        "discovered_domains": discovered_domains,
        "related_site_count": len(related_sites),
        "related_sites": related_sites,
        "primary_web_presence_count": len(primary_web_presences),
        "primary_web_presences": primary_web_presences,
        "output_path": str(output_path),
    }
    _write_json(summary_path, summary)
    return summary


async def _run_record(
    *,
    index: int,
    total: int,
    record: dict[str, Any],
    engine: str,
    service: CrawlService | None,
    http_client: httpx.AsyncClient | None,
    args: argparse.Namespace,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    async with semaphore:
        if args.sleep_seconds > 0:
            await asyncio.sleep(args.sleep_seconds * ((index - 1) % max(args.concurrency, 1)))
        started = time.monotonic()
        row = {
            "index": index,
            "total": total,
            "engine": engine,
            "record_id": str(record.get("record_id") or ""),
            "organization_number": str(record.get("organization_number") or ""),
            "organization_name": _organization_name(record),
        }
        try:
            request = _request_from_record(record, engine=engine, args=args)
            payload = await _discover(request=request, service=service, http_client=http_client)
            discovered_domain = None
            if payload.get("best_domain"):
                discovered_domain = {
                    **row,
                    "best_domain": payload["best_domain"],
                    "status": payload["status"],
                    "duration_ms": payload["duration_ms"],
                }
            related_sites = [
                {
                    **row,
                    "url": str(site.get("url") or ""),
                    "normalized_domain": str(site.get("normalized_domain") or ""),
                    "score": int(site.get("score") or 0),
                    "site_type": str(site.get("site_type") or ""),
                    "relationship": str(site.get("relationship") or ""),
                    "owned_domain": bool(site.get("owned_domain")),
                }
                for site in payload.get("related_sites") or []
                if isinstance(site, dict)
            ]
            primary_web_presence = payload.get("primary_web_presence")
            if isinstance(primary_web_presence, dict):
                primary_web_presence = {
                    **row,
                    "url": str(primary_web_presence.get("url") or ""),
                    "normalized_domain": str(primary_web_presence.get("normalized_domain") or ""),
                    "score": int(primary_web_presence.get("score") or 0),
                    "site_type": str(primary_web_presence.get("site_type") or ""),
                    "relationship": str(primary_web_presence.get("relationship") or ""),
                    "owned_domain": bool(primary_web_presence.get("owned_domain")),
                }
            else:
                primary_web_presence = None
            return {
                "line": json.dumps({**row, "response": payload}, ensure_ascii=False, sort_keys=True) + "\n",
                "status": str(payload["status"]),
                "error_codes": [str(error.get("code") or "unknown") for error in payload.get("errors") or []],
                "discovered_domain": discovered_domain,
                "related_sites": related_sites,
                "primary_web_presence": primary_web_presence,
                "rows_failed": 0,
            }
        except Exception as exc:
            payload = {
                "status": "failed",
                "errors": [
                    {
                        "code": "runner_exception",
                        "message": "Domain discovery runner failed for this record.",
                        "detail": {"type": type(exc).__name__, "message": str(exc)},
                    }
                ],
            }
            return {
                "line": json.dumps(
                    {**row, "response": payload, "duration_ms": int((time.monotonic() - started) * 1000)},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n",
                "status": "exception",
                "error_codes": [type(exc).__name__],
                "discovered_domain": None,
                "related_sites": [],
                "primary_web_presence": None,
                "rows_failed": 1,
            }


async def _discover(
    *,
    request: BrregDomainDiscoveryRequest,
    service: CrawlService | None,
    http_client: httpx.AsyncClient | None,
) -> dict[str, Any]:
    if http_client is not None:
        response = await http_client.post("/v1/brreg/domain-discovery", json=request.model_dump(mode="json"))
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("HTTP response must be a JSON object")
        return payload
    if service is None:
        raise ValueError("service is required when base_url is not provided")
    return (await service.discover_brreg_domain(request)).model_dump(mode="json")


def _request_from_record(record: dict[str, Any], *, engine: str, args: argparse.Namespace) -> BrregDomainDiscoveryRequest:
    raw_payload = record.get("raw_payload")
    if not isinstance(raw_payload, dict):
        raise ValueError("record raw_payload must be an object")
    organization_number = str(record.get("organization_number") or raw_payload.get("organisasjonsnummer") or "")
    record_id = str(record.get("record_id") or organization_number)
    return BrregDomainDiscoveryRequest(
        record_id=record_id,
        organization_number=organization_number,
        organization_name=_organization_name(record),
        raw_payload=raw_payload,
        country="NO",
        search_provider=engine,
        limits=DomainDiscoverLimits(
            max_search_candidates=args.max_search_candidates,
            max_site_checks=args.max_site_checks,
            search_candidate_threshold=args.search_candidate_threshold,
            domain_threshold=args.domain_threshold,
            timeout_seconds=args.timeout_seconds,
        ),
    )


def _organization_name(record: dict[str, Any]) -> str:
    raw_payload = record.get("raw_payload")
    if isinstance(raw_payload, dict) and raw_payload.get("navn"):
        return str(raw_payload["navn"])
    return str(record.get("organization_name") or "")


def _load_records(path: Path, limit: int) -> list[dict[str, Any]]:
    values = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(values, list):
        raise ValueError("input file must contain a JSON array")
    records = [value for value in values if isinstance(value, dict)]
    if limit > 0:
        records = records[:limit]
    if not records:
        raise ValueError("input file did not contain any records")
    return records


def _load_env_file(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run crawl-service domain discovery for a BRREG sample.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--base-url", default="")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--engines", default="duckduckgo,yandex")
    parser.add_argument("--max-search-candidates", type=int, default=5)
    parser.add_argument("--max-site-checks", type=int, default=3)
    parser.add_argument("--search-candidate-threshold", type=int, default=50)
    parser.add_argument("--domain-threshold", type=int, default=70)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--http-timeout-seconds", type=int, default=360)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    return parser.parse_args(sys.argv[1:])


if __name__ == "__main__":
    main()
