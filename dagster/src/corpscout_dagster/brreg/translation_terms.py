from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

import httpx


DEFAULT_LLM_MODEL = "qwen3:6b"
DEFAULT_PROMPT_VERSION = "v1"
DEFAULT_TRANSLATION_SERVICE_URL = "http://translation-service:8095"
DEFAULT_TRANSLATION_SERVICE_MAX_RETRIES = 3
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


class TranslationServiceError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_category: str | None = None,
        error_code: str | None = None,
        retry_strategy: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_category = error_category
        self.error_code = error_code
        self.retry_strategy = retry_strategy

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TranslationServiceError":
        error = payload.get("error")
        if not isinstance(error, dict):
            return cls("translation service failed")
        message = error.get("message") or error.get("code") or "translation service failed"
        return cls(
            str(message),
            error_category=_optional_error_text(error.get("category") or error.get("error_category")),
            error_code=_optional_error_text(error.get("code") or error.get("error_code")),
            retry_strategy=_optional_error_text(error.get("retry_strategy")),
        )


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
        max_retries: int = DEFAULT_TRANSLATION_SERVICE_MAX_RETRIES,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._provider = provider
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
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
            max_retries=int(
                os.environ.get("TRANSLATION_SERVICE_MAX_RETRIES", str(DEFAULT_TRANSLATION_SERVICE_MAX_RETRIES))
            ),
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
                "max_retries": self._max_retries,
            }
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("translation service returned a non-object response")
        if payload.get("status") == "failed":
            raise TranslationServiceError.from_payload(payload)
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


def translation_item_id(item: TranslationItem) -> str:
    key = translation_cache_key(item)
    return f"{key.category}:{key.original_hash}"


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


def _optional_error_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
