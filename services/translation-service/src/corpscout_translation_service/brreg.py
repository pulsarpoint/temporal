from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable

from corpscout_translation_service.models import LLMTranslationItem


TRANSLATION_PAYLOAD_SCHEMA_VERSION = "brreg.translation_terms.v1"


@dataclass(frozen=True)
class TranslationItem:
    category: str
    text: str


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


def translation_item_id(item: TranslationItem | LLMTranslationItem) -> str:
    normalized = item.text.strip().lower()
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"{item.category}:{digest}"


def to_llm_item(item: TranslationItem) -> LLMTranslationItem:
    return LLMTranslationItem(id=translation_item_id(item), category=item.category, text=item.text)


def build_translation_payload(
    *,
    raw_payload: dict[str, Any],
    items: Iterable[TranslationItem],
    translated_by_id: dict[str, str],
    model: str,
    prompt_version: str,
    source_lang: str,
    target_lang: str,
) -> dict[str, Any]:
    terms: list[dict[str, str]] = []
    for item in items:
        item_id = translation_item_id(item)
        terms.append(
            {
                "category": item.category,
                "original_text": item.text,
                "translated_text": translated_by_id[item_id],
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


def _append_text(items: list[TranslationItem], category: str, value: Any) -> None:
    if not isinstance(value, str):
        return
    text = value.strip()
    if text:
        items.append(TranslationItem(category=category, text=text))


def _deduplicate_items(items: list[TranslationItem]) -> list[TranslationItem]:
    seen: set[tuple[str, str]] = set()
    result: list[TranslationItem] = []
    for item in items:
        key = (item.category, item.text.strip().lower())
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result
