from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
from dataclasses import asdict
from typing import Any, Awaitable, Callable

from temporalio import activity

from contracts import (
    TranslatedTerm,
    TranslateTermsInput,
    TranslateTermsResult,
    TranslationFailure,
)

log = logging.getLogger(__name__)

DEFAULT_LLM_BASE_URL = "http://100.77.62.33:8888"
DEFAULT_LLM_MODEL = "qwen3:6b"

TranslationRunner = Callable[[TranslateTermsInput, str, str], dict[str, str] | Awaitable[dict[str, str]]]


class DSPyTranslationService:
    def __init__(
        self,
        *,
        default_model: str | None = None,
        default_base_url: str | None = None,
        runner: TranslationRunner | None = None,
    ) -> None:
        self.model = default_model or os.environ.get("LLM_MODEL") or DEFAULT_LLM_MODEL
        self.base_url = normalize_base_url(default_base_url or os.environ.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL)
        self._runner = runner or self._run_with_llm_api

    async def translate_terms(self, payload: TranslateTermsInput) -> TranslateTermsResult:
        model = payload.model or self.model
        translated_by_id = await self.run_translation(payload, model)

        requested_ids = {item.id for item in payload.items}
        missing_items = [item for item in payload.items if not translated_by_id.get(item.id, "").strip()]
        if missing_items and len(missing_items) < len(payload.items):
            retry_payload = TranslateTermsInput(
                category=payload.category,
                items=missing_items,
                source_lang=payload.source_lang,
                target_lang=payload.target_lang,
                model=payload.model,
                prompt_version=payload.prompt_version,
            )
            translated_by_id.update(await self.run_translation(retry_payload, model))

        translations: list[TranslatedTerm] = []
        failures: list[TranslationFailure] = []

        for item in payload.items:
            translated = translated_by_id.get(item.id, "").strip()
            if translated:
                translations.append(TranslatedTerm(id=item.id, translation=translated))
            else:
                failures.append(TranslationFailure(id=item.id, error="missing translation"))

        for item_id in translated_by_id:
            if item_id not in requested_ids:
                activity.logger.warning("LLM translation returned unexpected id", extra={"id": item_id})

        return TranslateTermsResult(translations=translations, failures=failures, model=model)

    async def run_translation(self, payload: TranslateTermsInput, model: str) -> dict[str, str]:
        raw_result = self._runner(payload, model, self.base_url)
        return await raw_result if inspect.isawaitable(raw_result) else raw_result

    async def _run_with_llm_api(self, payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
        return await run_direct_translation(payload, model, base_url)

    async def _run_with_dspy(self, payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
        return await asyncio.to_thread(run_dspy_translation, payload, model, base_url)


async def run_direct_translation(payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
    import httpx

    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            openai_api_base(base_url) + "/chat/completions",
            headers={"Authorization": f"Bearer {os.environ.get('LLM_API_KEY', 'local')}"},
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


def translation_max_tokens(payload: TranslateTermsInput) -> int:
    return min(4096, max(512, len(payload.items) * 96))


def build_translation_messages(payload: TranslateTermsInput) -> list[dict[str, str]]:
    items_json = json.dumps(
        [translation_item_payload(item) for item in payload.items],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    if any(item.category for item in payload.items):
        instruction = (
            f"Translate {payload.source_lang or 'no'} business registry text to {payload.target_lang or 'en'}.\n"
            "Use each item's category as context."
        )
    else:
        instruction = (
            f"Translate {payload.source_lang or 'no'} business registry {payload.category} text "
            f"to {payload.target_lang or 'en'}."
        )
    return [
        {
            "role": "user",
            "content": (
                "/no_think\n"
                f"{instruction}\n"
                'Return only JSON: {"translations":[{"id":"...","translation":"..."}]}\n'
                "Preserve every input id exactly. Include one translation per input item.\n"
                f"Items: {items_json}"
            ),
        }
    ]


def translation_item_payload(item: Any) -> dict[str, str]:
    values = {"id": item.id, "text": item.text}
    if getattr(item, "category", ""):
        values["category"] = item.category
    return values


def run_dspy_translation(payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
    import dspy

    class TranslateBusinessTerms(dspy.Signature):
        """/no_think

        Translate business registry text to English.
        Preserve ids exactly. Return only the translations, no extra text.
        """

        category: str = dspy.InputField(desc="Translation category such as legal_form, status, role, or activity.")
        source_lang: str = dspy.InputField(desc="BCP-47 source language code, for example no, da, or et.")
        target_lang: str = dspy.InputField(desc="BCP-47 target language code, usually en.")
        items_json: str = dspy.InputField(desc="JSON array with items: [{id,text,category?}].")
        translations: list[dict] = dspy.OutputField(desc="One object per input item with 'id' and 'translation' fields.")

    lm = dspy.LM(
        f"openai/{model}",
        api_key=os.environ.get("LLM_API_KEY", "local"),
        api_base=openai_api_base(base_url),
        cache=False,
        temperature=0,
        max_tokens=2048,
    )
    items_json = json.dumps(
        [translation_item_payload(item) for item in payload.items],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    predictor = dspy.Predict(TranslateBusinessTerms)
    allowed_ids = {item.id for item in payload.items}

    try:
        with dspy.context(lm=lm):
            prediction = predictor(
                category=payload.category,
                source_lang=payload.source_lang or "no",
                target_lang=payload.target_lang or "en",
                items_json=items_json,
            )
        return {
            t["id"]: t["translation"]
            for t in prediction.translations
            if isinstance(t, dict) and t.get("id") in allowed_ids and str(t.get("translation", "")).strip()
        }
    except Exception:
        log.warning("dspy predict failed, falling back to raw lm output", exc_info=True)

    content = ""
    try:
        outputs = lm.history[-1].get("outputs", []) if lm.history else []
        content = outputs[0] if outputs else ""
    except Exception:
        log.warning("could not extract raw lm output from history", exc_info=True)

    try:
        return parse_translation_payload(str(content), allowed_ids)
    except Exception:
        log.warning("parse failed content=%r", str(content)[:200], exc_info=True)
        return {}


def parse_translation_payload(content: str, allowed_ids: set[str]) -> dict[str, str]:
    cleaned = clean_json_content(content)
    parsed = load_json_with_repairs(cleaned)
    for key in ("translations", "items"):
        if isinstance(parsed, dict) and key in parsed:
            values = parsed[key]
            if isinstance(values, list):
                return translations_from_list(values, allowed_ids)
    if isinstance(parsed, dict) and "translations_json" in parsed:
        inner = parsed["translations_json"]
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except json.JSONDecodeError:
                pass
        if isinstance(inner, list):
            return translations_from_list(inner, allowed_ids)
    if isinstance(parsed, list):
        return translations_from_list(parsed, allowed_ids)
    if isinstance(parsed, dict):
        return {key: str(value) for key, value in parsed.items() if key in allowed_ids and str(value).strip()}
    return {}


def load_json_with_repairs(content: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        repaired = repair_missing_translation_keys(content)
        if repaired != content:
            return json.loads(repaired)
        try:
            from json_repair import repair_json
        except ImportError:
            raise
        return json.loads(repair_json(content))


def repair_missing_translation_keys(content: str) -> str:
    return re.sub(
        r'\{"id"\s*:\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\}',
        r'{"id":"\1","translation":"\2"}',
        content,
    )


def translations_from_list(values: list[Any], allowed_ids: set[str]) -> dict[str, str]:
    translated: dict[str, str] = {}
    for value in values:
        if not isinstance(value, dict):
            continue
        item_id = str(value.get("id", ""))
        if item_id not in allowed_ids:
            continue
        translation = str(value.get("translation") or value.get("text") or "").strip()
        if translation:
            translated[item_id] = translation
    return translated


def clean_json_content(content: str) -> str:
    cleaned = content.strip()
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL).strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json|JSON)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    if cleaned.startswith("{{") and cleaned.endswith("}}"):
        candidate = cleaned[1:-1].strip()
        if is_json(candidate):
            return candidate
    if cleaned.startswith('{"{') and cleaned.endswith('"}'):
        candidate = cleaned[2:-2].replace('\\"', '"').strip()
        if is_json(candidate):
            return candidate
    return cleaned


def is_json(value: str) -> bool:
    try:
        json.loads(value)
        return True
    except json.JSONDecodeError:
        return False


def normalize_base_url(base_url: str) -> str:
    trimmed = base_url.strip().rstrip("/")
    if trimmed.endswith("/v1/chat/completions"):
        trimmed = trimmed[: -len("/v1/chat/completions")]
    if trimmed.endswith("/v1"):
        trimmed = trimmed[: -len("/v1")]
    return trimmed.rstrip("/")


def openai_api_base(base_url: str) -> str:
    return normalize_base_url(base_url) + "/v1"


def default_translation_service() -> DSPyTranslationService:
    return DSPyTranslationService()


@activity.defn(name="TranslateTermsWithDSPy")
async def translate_terms_with_dspy(payload: TranslateTermsInput) -> TranslateTermsResult:
    return await default_translation_service().translate_terms(payload)
