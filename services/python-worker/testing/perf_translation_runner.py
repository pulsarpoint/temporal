from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

WORKER_DIR = Path(__file__).resolve().parents[1]
if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

from contracts import TranslateTermsInput, TranslationItem  # noqa: E402
from activities.llm_translation import (  # noqa: E402
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    DSPyTranslationService,
    build_translation_messages,
    normalize_base_url,
    openai_api_base,
    parse_translation_payload,
    run_dspy_translation,
    translation_max_tokens,
)

DEFAULT_CORPUS_PATH = WORKER_DIR / "testing" / "fixtures" / "translation_perf_corpus.json"
DEFAULT_OUTPUT_DIR = WORKER_DIR / "testing" / ".perf-results"
RESULTS_FILENAME = "translation_perf_results.jsonl"


@dataclass(frozen=True)
class CorpusItem:
    id: str
    text: str
    expected_any: list[str] = field(default_factory=list)
    forbidden_any: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TranslationCase:
    name: str
    category: str
    source_lang: str
    target_lang: str
    items: list[CorpusItem]


Translator = Callable[[TranslateTermsInput], Awaitable[dict[str, str]]]


def load_corpus(path: Path = DEFAULT_CORPUS_PATH) -> list[TranslationCase]:
    data = json.loads(path.read_text(encoding="utf-8"))
    raw_cases = data.get("cases")
    if not isinstance(raw_cases, list) or not raw_cases:
        raise ValueError(f"{path} must contain a non-empty cases list")

    cases: list[TranslationCase] = []
    for raw_case in raw_cases:
        items = [
            CorpusItem(
                id=str(raw_item["id"]),
                text=str(raw_item["text"]),
                expected_any=list(raw_item.get("expected_any", [])),
                forbidden_any=list(raw_item.get("forbidden_any", [])),
            )
            for raw_item in raw_case.get("items", [])
        ]
        if not items:
            raise ValueError(f"case {raw_case.get('name', '<unnamed>')} has no items")
        cases.append(
            TranslationCase(
                name=str(raw_case["name"]),
                category=str(raw_case["category"]),
                source_lang=str(raw_case.get("source_lang") or "no"),
                target_lang=str(raw_case.get("target_lang") or "en"),
                items=items,
            )
        )
    return cases


def validate_translations(case: TranslationCase, translations: dict[str, str]) -> list[str]:
    errors: list[str] = []
    for item in case.items:
        translation = translations.get(item.id, "").strip()
        if not translation:
            errors.append(f"{item.id} missing translation")
            continue

        translated_text = translation.casefold()
        source_text = item.text.strip().casefold()
        if translated_text == source_text:
            errors.append(f'{item.id} copied source text: "{item.text}"')

        if item.expected_any and not any(expected.casefold() in translated_text for expected in item.expected_any):
            errors.append(f"{item.id} missing expected text: one of {item.expected_any}")

        for forbidden in item.forbidden_any:
            if forbidden.casefold() in translated_text:
                errors.append(f"{item.id} contains forbidden text: {forbidden}")
    return errors


async def run_benchmark(
    cases: list[TranslationCase],
    *,
    runner: str,
    model: str,
    base_url: str,
    limit: int = 0,
) -> dict[str, Any]:
    selected_cases = cases[:limit] if limit > 0 else cases
    translator = make_translator(runner=runner, model=model, base_url=base_url)
    started = time.perf_counter()
    case_results = []

    for case in selected_cases:
        payload = TranslateTermsInput(
            category=case.category,
            source_lang=case.source_lang,
            target_lang=case.target_lang,
            items=[TranslationItem(id=item.id, text=item.text) for item in case.items],
            model=model,
        )
        case_started = time.perf_counter()
        error = ""
        translations: dict[str, str] = {}
        try:
            translations = await translator(payload)
        except Exception as exc:  # noqa: BLE001 - perf harness records failures and continues.
            error = f"{type(exc).__name__}: {exc}"
        elapsed = time.perf_counter() - case_started
        validation_errors = validate_translations(case, translations)
        if error:
            validation_errors.insert(0, error)
        case_results.append(
            {
                "name": case.name,
                "category": case.category,
                "item_count": len(case.items),
                "seconds": round(elapsed, 3),
                "ok": not validation_errors,
                "errors": validation_errors,
                "translations": translations,
            }
        )

    total_seconds = time.perf_counter() - started
    error_count = sum(len(case["errors"]) for case in case_results)
    return {
        "run_id": datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ"),
        "created_at": datetime.now(UTC).isoformat(),
        "runner": runner,
        "model": model,
        "base_url": normalize_base_url(base_url),
        "case_count": len(selected_cases),
        "item_count": sum(len(case.items) for case in selected_cases),
        "total_seconds": round(total_seconds, 3),
        "ok": error_count == 0,
        "error_count": error_count,
        "cases": case_results,
    }


def make_translator(*, runner: str, model: str, base_url: str) -> Translator:
    if runner == "service":
        service = DSPyTranslationService(default_model=model, default_base_url=base_url)

        async def translate_with_service(payload: TranslateTermsInput) -> dict[str, str]:
            result = await service.translate_terms(payload)
            return {term.id: term.translation for term in result.translations}

        return translate_with_service

    if runner == "direct-http":
        return make_direct_http_translator(model=model, base_url=base_url)

    if runner == "dspy":
        async def translate_with_dspy(payload: TranslateTermsInput) -> dict[str, str]:
            return await asyncio.to_thread(run_dspy_translation, payload, model, base_url)

        return translate_with_dspy

    raise ValueError(f"unsupported runner {runner!r}")


def make_direct_http_translator(*, model: str, base_url: str) -> Translator:
    import httpx

    api_url = openai_api_base(base_url) + "/chat/completions"
    api_key = os.environ.get("LLM_API_KEY", "local")

    async def translate_directly(payload: TranslateTermsInput) -> dict[str, str]:
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(
                api_url,
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": model,
                    "messages": build_translation_messages(payload),
                    "temperature": 0,
                    "max_tokens": translation_max_tokens(payload),
                    "chat_template_kwargs": {"enable_thinking": False},
                },
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
        return parse_translation_payload(str(content), {item.id for item in payload.items})

    return translate_directly


def append_result(result: dict[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / RESULTS_FILENAME
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(result, ensure_ascii=False, sort_keys=True))
        handle.write("\n")
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run translation correctness and latency checks against fixture text.")
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--runner", choices=["service", "direct-http", "dspy"], default="service")
    parser.add_argument("--model", default=os.environ.get("LLM_INTEGRATION_MODEL") or os.environ.get("LLM_MODEL") or DEFAULT_LLM_MODEL)
    parser.add_argument("--base-url", default=os.environ.get("LLM_INTEGRATION_BASE_URL") or os.environ.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL)
    parser.add_argument("--limit", type=int, default=0, help="Run only the first N cases.")
    parser.add_argument("--allow-errors", action="store_true", help="Write results and exit 0 even when validation fails.")
    return parser


async def async_main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cases = load_corpus(args.corpus)
    result = await run_benchmark(
        cases,
        runner=args.runner,
        model=args.model,
        base_url=args.base_url,
        limit=args.limit,
    )
    output_path = append_result(result, output_dir=args.output_dir)
    print(json.dumps({k: result[k] for k in ("run_id", "runner", "model", "case_count", "item_count", "total_seconds", "ok", "error_count")}, ensure_ascii=False))
    print(f"wrote {output_path}")
    for case in result["cases"]:
        status = "ok" if case["ok"] else "failed"
        print(f"{case['name']}: {status} {case['seconds']}s")
        for error in case["errors"]:
            print(f"  - {error}")
    return 0 if result["ok"] or args.allow_errors else 1


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
