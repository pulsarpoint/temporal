from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

import httpx


DEFAULT_LLM_BASE_URL = "http://100.77.62.33:8888"
DEFAULT_LLM_MODEL = "qwen3:6b"
DEFAULT_PROMPT_VERSION = "v1"
DEFAULT_TRANSLATION_SERVICE_URL = "http://translation-service:8095"
TRANSLATION_PAYLOAD_SCHEMA_VERSION = "brreg.translation_terms.v1"


@dataclass(frozen=True)
class TranslationItem:
    category: str
    text: str


@dataclass(frozen=True)
class TranslationCacheKey:
    category: str
    source_lang: str
    target_lang: str
    original_hash: str


@dataclass(frozen=True)
class CachedTermTranslation:
    category: str
    original_text: str
    translated_text: str
    model: str
    prompt_version: str


class TermTranslator(Protocol):
    def translate_terms(
        self,
        *,
        category: str,
        items: list[TranslationItem],
        source_lang: str,
        target_lang: str,
        model: str,
        prompt_version: str,
    ) -> dict[str, str]:
        ...


class HttpTranslationServiceTermTranslator:
    def __init__(
        self,
        *,
        base_url: str,
        provider: str = "default",
        timeout_seconds: float = 300,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._provider = provider
        self._timeout_seconds = timeout_seconds
        self._http_client = http_client

    @classmethod
    def from_env(cls) -> "HttpTranslationServiceTermTranslator":
        return cls(
            base_url=os.environ.get("TRANSLATION_SERVICE_URL", DEFAULT_TRANSLATION_SERVICE_URL),
            provider=(
                os.environ.get("BRREG_TRANSLATION_PROVIDER")
                or os.environ.get("TRANSLATION_DEFAULT_PROVIDER")
                or "default"
            ),
            timeout_seconds=float(os.environ.get("TRANSLATION_SERVICE_TIMEOUT_SECONDS", "300")),
        )

    def translate_terms(
        self,
        *,
        category: str,
        items: list[TranslationItem],
        source_lang: str,
        target_lang: str,
        model: str,
        prompt_version: str,
    ) -> dict[str, str]:
        if not items:
            return {}
        response = self._post(
            {
                "provider": self._provider,
                "model": model,
                "prompt_version": prompt_version,
                "source_lang": source_lang,
                "target_lang": target_lang,
                "items": [
                    {
                        "id": translation_item_id(item),
                        "category": item.category,
                        "text": item.text,
                    }
                    for item in items
                ],
            }
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("translation service returned a non-object response")
        if payload.get("status") == "failed":
            raise RuntimeError(_translation_service_error_message(payload))
        translations = payload.get("translations")
        if not isinstance(translations, list):
            raise RuntimeError("translation service response is missing translations")
        return {
            str(item.get("id")): str(item.get("translation")).strip()
            for item in translations
            if isinstance(item, dict)
            and item.get("id") is not None
            and str(item.get("translation") or "").strip()
        }

    def _post(self, payload: dict[str, Any]) -> httpx.Response:
        if self._http_client is not None:
            response = self._http_client.post(
                f"{self._base_url}/v1/translate/terms",
                json=payload,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            return response
        with httpx.Client(timeout=self._timeout_seconds) as client:
            response = client.post(f"{self._base_url}/v1/translate/terms", json=payload)
            response.raise_for_status()
            return response


class DirectLLMTermTranslator:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout_seconds: float = 120,
    ) -> None:
        self._base_url = _openai_api_base(base_url or os.environ.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL)
        self._api_key = api_key or os.environ.get("LLM_API_KEY") or "local"
        self._timeout_seconds = timeout_seconds

    def translate_terms(
        self,
        *,
        category: str,
        items: list[TranslationItem],
        source_lang: str,
        target_lang: str,
        model: str,
        prompt_version: str,
    ) -> dict[str, str]:
        if not items:
            return {}
        translated_by_id = self._translate_once(
            category=category,
            items=items,
            source_lang=source_lang,
            target_lang=target_lang,
            model=model,
            prompt_version=prompt_version,
        )

        missing_items = [item for item in items if not translated_by_id.get(translation_item_id(item), "").strip()]
        if missing_items and len(missing_items) < len(items):
            translated_by_id.update(
                self._translate_once(
                    category=category,
                    items=missing_items,
                    source_lang=source_lang,
                    target_lang=target_lang,
                    model=model,
                    prompt_version=prompt_version,
                )
            )
        return translated_by_id

    def _translate_once(
        self,
        *,
        category: str,
        items: list[TranslationItem],
        source_lang: str,
        target_lang: str,
        model: str,
        prompt_version: str,
    ) -> dict[str, str]:
        response = httpx.post(
            f"{self._base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self._api_key}"},
            json={
                "model": model,
                "messages": build_translation_messages(
                    category=category,
                    items=items,
                    source_lang=source_lang,
                    target_lang=target_lang,
                    prompt_version=prompt_version,
                ),
                "temperature": 0,
                "max_tokens": translation_max_tokens(items),
                "chat_template_kwargs": {"enable_thinking": False},
            },
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return parse_translation_response(str(content), {translation_item_id(item) for item in items})


def extract_translation_items(raw_payload: dict[str, Any]) -> list[TranslationItem]:
    items: list[TranslationItem] = []
    for key, category in [
        ("organisasjonsform", "org_form"),
        ("institusjonellSektorkode", "sector_code"),
        ("naeringskode1", "industry_code"),
        ("naeringskode2", "industry_code"),
        ("naeringskode3", "industry_code"),
    ]:
        value = raw_payload.get(key)
        if isinstance(value, dict):
            _append_text(items, category, value.get("beskrivelse"))

    capital = raw_payload.get("kapital")
    if isinstance(capital, dict):
        _append_text(items, "capital_type", capital.get("type"))

    for key, category in [
        ("aktivitet", "activity"),
        ("vedtektsfestetFormaal", "statutory_purpose"),
        ("frivilligMvaRegistrertBeskrivelser", "vat_description"),
    ]:
        value = raw_payload.get(key)
        if isinstance(value, list):
            for item in value:
                _append_text(items, category, item)

    return _deduplicate_items(items)


def translation_cache_key(
    item: TranslationItem,
    *,
    source_lang: str = "no",
    target_lang: str = "en",
) -> TranslationCacheKey:
    normalized = item.text.strip().lower()
    return TranslationCacheKey(
        category=item.category,
        source_lang=source_lang,
        target_lang=target_lang,
        original_hash=hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
    )


def build_translation_payload(
    *,
    raw_payload: dict[str, Any],
    items: Iterable[TranslationItem],
    cached_translations: dict[TranslationCacheKey, CachedTermTranslation],
    model: str,
    prompt_version: str,
    source_lang: str = "no",
    target_lang: str = "en",
) -> dict[str, Any]:
    terms: list[dict[str, str]] = []
    for item in items:
        cached = cached_translations[translation_cache_key(item, source_lang=source_lang, target_lang=target_lang)]
        terms.append(
            {
                "category": item.category,
                "original_text": item.text,
                "translated_text": cached.translated_text,
            }
        )
    return {
        "schema_version": TRANSLATION_PAYLOAD_SCHEMA_VERSION,
        "source_language": source_lang,
        "target_language": target_lang,
        "model": model,
        "prompt_version": prompt_version,
        "organization_number": str(raw_payload.get("organisasjonsnummer") or ""),
        "terms": terms,
    }


def build_translation_messages(
    *,
    category: str,
    items: list[TranslationItem],
    source_lang: str,
    target_lang: str,
    prompt_version: str,
) -> list[dict[str, str]]:
    item_payload = [
        {"id": translation_item_id(item), "text": item.text, "category": item.category}
        for item in items
    ]
    items_json = json.dumps(item_payload, ensure_ascii=False, separators=(",", ":"))
    if any(item.category for item in items):
        instruction = (
            f"Translate {source_lang} business registry text to {target_lang}.\n"
            "Use each item's category as context."
        )
    else:
        instruction = f"Translate {source_lang} business registry {category} text to {target_lang}."
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


def translation_item_id(item: TranslationItem) -> str:
    key = translation_cache_key(item)
    return f"{key.category}:{key.original_hash}"


def translation_max_tokens(items: list[TranslationItem]) -> int:
    return min(4096, max(512, len(items) * 96))


def parse_translation_response(content: str, allowed_ids: set[str]) -> dict[str, str]:
    parsed = _load_json_with_repairs(_clean_json_content(content))
    if isinstance(parsed, dict):
        values = parsed.get("translations") or parsed.get("items")
        if isinstance(values, list):
            return _translations_from_list(values, allowed_ids)
        inner = parsed.get("translations_json")
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except json.JSONDecodeError:
                pass
        if isinstance(inner, list):
            return _translations_from_list(inner, allowed_ids)
        return {
            key: str(value).strip()
            for key, value in parsed.items()
            if key in allowed_ids and str(value).strip()
        }
    if isinstance(parsed, list):
        return _translations_from_list(parsed, allowed_ids)
    return {}


def _load_json_with_repairs(content: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        repaired = _repair_missing_translation_keys(content)
        if repaired != content:
            return json.loads(repaired)
        try:
            from json_repair import repair_json
        except ImportError:
            raise
        return json.loads(repair_json(content))


def _repair_missing_translation_keys(content: str) -> str:
    return re.sub(
        r'\{"id"\s*:\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\}',
        r'{"id":"\1","translation":"\2"}',
        content,
    )


def _append_text(items: list[TranslationItem], category: str, value: Any) -> None:
    if isinstance(value, str) and value.strip():
        items.append(TranslationItem(category=category, text=value.strip()))


def _deduplicate_items(items: list[TranslationItem]) -> list[TranslationItem]:
    seen: set[tuple[str, str]] = set()
    unique: list[TranslationItem] = []
    for item in items:
        key = (item.category, item.text.strip().lower())
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _translations_from_list(values: list[Any], allowed_ids: set[str]) -> dict[str, str]:
    translated: dict[str, str] = {}
    for value in values:
        if not isinstance(value, dict):
            continue
        item_id = str(value.get("id") or "")
        text = str(value.get("translation") or value.get("text") or "").strip()
        if item_id in allowed_ids and text:
            translated[item_id] = text
    return translated


def _clean_json_content(content: str) -> str:
    cleaned = content.strip()
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL).strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json|JSON)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    return cleaned


def _openai_api_base(base_url: str) -> str:
    trimmed = base_url.strip().rstrip("/")
    if trimmed.endswith("/v1/chat/completions"):
        trimmed = trimmed[: -len("/v1/chat/completions")]
    if trimmed.endswith("/v1"):
        trimmed = trimmed[: -len("/v1")]
    return trimmed.rstrip("/") + "/v1"


def _translation_service_error_message(payload: dict[str, Any]) -> str:
    error = payload.get("error")
    if not isinstance(error, dict):
        return "translation service failed"
    message = error.get("message") or error.get("code")
    return str(message) if message else "translation service failed"
