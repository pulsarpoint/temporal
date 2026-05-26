from __future__ import annotations

import json
import os
import re
from typing import Any, Protocol

import httpx

from corpscout_translation_service.models import LLMTranslationItem, LLMTranslationRequest


DEFAULT_LLM_BASE_URL = "http://100.77.62.33:8888"
DEFAULT_LLM_MODEL = "qwen3:6b"


class LLMClient(Protocol):
    async def translate_terms(self, request: LLMTranslationRequest) -> dict[str, str]:
        ...


class OpenAICompatibleLLMClient:
    def __init__(self, *, timeout_seconds: float = 120) -> None:
        self._timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls) -> "OpenAICompatibleLLMClient":
        return cls(timeout_seconds=float(os.environ.get("TRANSLATION_LLM_TIMEOUT_SECONDS", "120")))

    async def translate_terms(self, request: LLMTranslationRequest) -> dict[str, str]:
        base_url = _provider_base_url(request.provider)
        api_key = _provider_api_key(request.provider)
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
            response = await client.post(
                f"{base_url}/chat/completions",
                headers=headers,
                json={
                    "model": request.model,
                    "messages": build_translation_messages(request),
                    "temperature": 0,
                    "max_tokens": translation_max_tokens(request.items),
                    "chat_template_kwargs": {"enable_thinking": False},
                },
            )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return parse_llm_translation_response(
            str(content),
            expected_ids={item.id for item in request.items},
            require_all=False,
        )


def default_provider() -> str:
    return os.environ.get("TRANSLATION_DEFAULT_PROVIDER") or "default"


def default_model() -> str:
    return os.environ.get("TRANSLATION_DEFAULT_MODEL") or os.environ.get("BRREG_TRANSLATION_MODEL") or DEFAULT_LLM_MODEL


def build_translation_messages(request: LLMTranslationRequest) -> list[dict[str, str]]:
    items_json = json.dumps(
        [
            {"id": item.id, "text": item.text, "category": item.category}
            for item in request.items
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return [
        {
            "role": "user",
            "content": (
                "/no_think\n"
                f"Translate {request.source_lang} business registry text to {request.target_lang}.\n"
                "Use each item's category as context.\n"
                'Return only JSON: {"translations":[{"id":"...","translation":"..."}]}\n'
                "Preserve every input id exactly. Include one translation per input item.\n"
                f"Items: {items_json}"
            ),
        }
    ]


def translation_max_tokens(items: list[LLMTranslationItem]) -> int:
    return min(4096, max(512, len(items) * 96))


def parse_llm_translation_response(
    content: str,
    *,
    expected_ids: set[str],
    require_all: bool = True,
) -> dict[str, str]:
    parsed = _load_json_with_repairs(_clean_json_content(content))
    translated = _translations_from_value(parsed, expected_ids)
    if require_all:
        missing = sorted(expected_ids - set(translated))
        if missing:
            raise ValueError(f"missing translations for ids: {', '.join(missing)}")
    return translated


def _translations_from_value(value: Any, expected_ids: set[str]) -> dict[str, str]:
    if isinstance(value, dict):
        translations = value.get("translations") or value.get("items")
        if isinstance(translations, list):
            return _translations_from_list(translations, expected_ids)
        nested = value.get("translations_json")
        if isinstance(nested, str):
            try:
                nested = json.loads(nested)
            except json.JSONDecodeError:
                nested = _load_json_with_repairs(nested)
        if isinstance(nested, list):
            return _translations_from_list(nested, expected_ids)
        return {
            key: str(item).strip()
            for key, item in value.items()
            if key in expected_ids and str(item).strip()
        }
    if isinstance(value, list):
        return _translations_from_list(value, expected_ids)
    return {}


def _translations_from_list(values: list[Any], expected_ids: set[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if not isinstance(value, dict):
            continue
        item_id = value.get("id")
        translated = value.get("translation") or value.get("translated_text") or value.get("text")
        if isinstance(item_id, str) and item_id in expected_ids and isinstance(translated, str) and translated.strip():
            result[item_id] = translated.strip()
    return result


def _load_json_with_repairs(content: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        from json_repair import repair_json

        return json.loads(repair_json(content))


def _clean_json_content(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()
    return text


def _provider_base_url(provider: str) -> str:
    key = _provider_env_key(provider)
    value = (
        os.environ.get(f"TRANSLATION_PROVIDER_{key}_BASE_URL")
        or os.environ.get("TRANSLATION_LLM_BASE_URL")
        or os.environ.get("LLM_BASE_URL")
        or DEFAULT_LLM_BASE_URL
    )
    return normalize_openai_api_base(value)


def normalize_openai_api_base(base_url: str) -> str:
    trimmed = base_url.strip().rstrip("/")
    if trimmed.endswith("/v1/chat/completions"):
        trimmed = trimmed[: -len("/v1/chat/completions")]
    if trimmed.endswith("/chat/completions"):
        trimmed = trimmed[: -len("/chat/completions")]
    if trimmed.endswith("/v1"):
        trimmed = trimmed[: -len("/v1")]
    return trimmed.rstrip("/") + "/v1"


def _provider_api_key(provider: str) -> str:
    key = _provider_env_key(provider)
    return (
        os.environ.get(f"TRANSLATION_PROVIDER_{key}_API_KEY")
        or os.environ.get("TRANSLATION_LLM_API_KEY")
        or os.environ.get("LLM_API_KEY")
        or ""
    )


def _provider_env_key(provider: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", provider).strip("_")
    return normalized.upper() or "DEFAULT"
