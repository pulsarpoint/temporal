from __future__ import annotations

import asyncio
import inspect
import json
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
        self._runner = runner or self._run_with_dspy

    async def translate_terms(self, payload: TranslateTermsInput) -> TranslateTermsResult:
        model = payload.model or self.model
        translated_by_id = await self.run_translation(payload, model)

        requested_ids = {item.id for item in payload.items}
        missing_items = [item for item in payload.items if not translated_by_id.get(item.id, "").strip()]
        if missing_items and len(missing_items) < len(payload.items):
            retry_payload = TranslateTermsInput(
                category=payload.category,
                items=missing_items,
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
                activity.logger.warning("DSPy translation returned unexpected id", extra={"id": item_id})

        return TranslateTermsResult(translations=translations, failures=failures, model=model)

    async def run_translation(self, payload: TranslateTermsInput, model: str) -> dict[str, str]:
        raw_result = self._runner(payload, model, self.base_url)
        return await raw_result if inspect.isawaitable(raw_result) else raw_result

    async def _run_with_dspy(self, payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
        return await asyncio.to_thread(run_dspy_translation, payload, model, base_url)


class TranslateBusinessTerms:
    """DSPy signature is built dynamically to keep importing this module cheap in tests."""


def run_dspy_translation(payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
    import dspy

    class TranslateBusinessTermsSignature(dspy.Signature):
        """Translate Norwegian business registry text to English.

        Return a JSON object using this exact shape:
        {"translations":[{"id":"same id from input","translation":"English translation"}]}
        Preserve ids exactly. Never use source text as JSON keys. Return JSON only.
        """

        category: str = dspy.InputField(desc="Translation category such as org_form, capital_type, or activity.")
        items_json: str = dspy.InputField(desc="JSON object with items: [{id,text}].")
        translations_json: str = dspy.OutputField(desc="JSON object with translations: [{id,translation}].")

    lm = dspy.LM(
        f"openai/{model}",
        api_key=os.environ.get("LLM_API_KEY", "local"),
        api_base=openai_api_base(base_url),
        cache=False,
        temperature=0,
        max_tokens=2048,
    )
    predictor = dspy.Predict(TranslateBusinessTermsSignature)
    items_json = json.dumps(
        {"items": [asdict(item) for item in payload.items]},
        ensure_ascii=False,
        separators=(",", ":"),
    )

    with dspy.context(lm=lm):
        prediction = predictor(category=payload.category, items_json=items_json)

    content = getattr(prediction, "translations_json", "")
    return parse_translation_payload(str(content), {item.id for item in payload.items})


def parse_translation_payload(content: str, allowed_ids: set[str]) -> dict[str, str]:
    cleaned = clean_json_content(content)
    parsed = load_json_with_repairs(cleaned)
    if isinstance(parsed, dict) and "translations" in parsed:
        values = parsed["translations"]
        if not isinstance(values, list):
            return {}
        return translations_from_list(values, allowed_ids)
    if isinstance(parsed, dict) and "items" in parsed:
        values = parsed["items"]
        if not isinstance(values, list):
            return {}
        return translations_from_list(values, allowed_ids)
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
