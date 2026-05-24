from __future__ import annotations

from corpscout_dagster.brreg.translation import (
    CachedTermTranslation,
    TranslationItem,
    build_translation_messages,
    build_translation_payload,
    extract_translation_items,
    parse_translation_response,
    translation_cache_key,
    translation_item_id,
)


def test_extract_translation_items_reads_brreg_business_terms() -> None:
    payload = {
        "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
        "institusjonellSektorkode": {"kode": "2100", "beskrivelse": "Private aksjeselskaper mv."},
        "naeringskode1": {"kode": "41.000", "beskrivelse": "Oppføring av bygninger"},
        "kapital": {"type": "Aksjekapital"},
        "aktivitet": ["Drive utleie av fast eiendom", ""],
        "vedtektsfestetFormaal": ["Kjøp og salg av aksjer."],
        "frivilligMvaRegistrertBeskrivelser": ["Utleier av bygg eller anlegg"],
    }

    items = extract_translation_items(payload)

    assert items == [
        TranslationItem(category="org_form", text="Aksjeselskap"),
        TranslationItem(category="sector_code", text="Private aksjeselskaper mv."),
        TranslationItem(category="industry_code", text="Oppføring av bygninger"),
        TranslationItem(category="capital_type", text="Aksjekapital"),
        TranslationItem(category="activity", text="Drive utleie av fast eiendom"),
        TranslationItem(category="statutory_purpose", text="Kjøp og salg av aksjer."),
        TranslationItem(category="vat_description", text="Utleier av bygg eller anlegg"),
    ]


def test_translation_cache_key_hashes_normalized_text() -> None:
    left = translation_cache_key(TranslationItem(category="activity", text="  Regnskapstjenester "))
    right = translation_cache_key(TranslationItem(category="activity", text="regnskapstjenester"))

    assert left == right
    assert left.category == "activity"
    assert len(left.original_hash) == 64


def test_build_translation_payload_uses_cached_terms() -> None:
    items = [
        TranslationItem(category="activity", text="Drive utleie av fast eiendom"),
        TranslationItem(category="org_form", text="Aksjeselskap"),
    ]
    cache = {
        translation_cache_key(items[0]): CachedTermTranslation(
            category="activity",
            original_text="Drive utleie av fast eiendom",
            translated_text="Engage in rental of real estate",
            model="qwen3:6b",
            prompt_version="v1",
        ),
        translation_cache_key(items[1]): CachedTermTranslation(
            category="org_form",
            original_text="Aksjeselskap",
            translated_text="Limited Liability Company",
            model="qwen3:6b",
            prompt_version="v1",
        ),
    }

    payload = build_translation_payload(
        raw_payload={"organisasjonsnummer": "810202572"},
        items=items,
        cached_translations=cache,
        model="qwen3:6b",
        prompt_version="v1",
    )

    assert payload == {
        "schema_version": "brreg.translation_terms.v1",
        "source_language": "no",
        "target_language": "en",
        "model": "qwen3:6b",
        "prompt_version": "v1",
        "organization_number": "810202572",
        "terms": [
            {
                "category": "activity",
                "original_text": "Drive utleie av fast eiendom",
                "translated_text": "Engage in rental of real estate",
            },
            {
                "category": "org_form",
                "original_text": "Aksjeselskap",
                "translated_text": "Limited Liability Company",
            },
        ],
    }


def test_build_translation_messages_uses_item_categories_for_mixed_batches() -> None:
    activity = TranslationItem(category="activity", text="Eie aksjer")
    capital = TranslationItem(category="capital_type", text="Aksjekapital")
    messages = build_translation_messages(
        category="mixed",
        items=[activity, capital],
        source_lang="no",
        target_lang="en",
        prompt_version="v1",
    )

    assert messages == [
        {
            "role": "user",
            "content": (
                "/no_think\n"
                "Translate no business registry text to en.\n"
                "Use each item's category as context.\n"
                'Return only JSON: {"translations":[{"id":"...","translation":"..."}]}\n'
                "Preserve every input id exactly. Include one translation per input item.\n"
                f'Items: [{{"id":"{translation_item_id(activity)}","text":"Eie aksjer","category":"activity"}},'
                f'{{"id":"{translation_item_id(capital)}","text":"Aksjekapital","category":"capital_type"}}]'
            ),
        }
    ]
    assert '"category":"activity"' in messages[0]["content"]
    assert '"category":"capital_type"' in messages[0]["content"]


def test_parse_translation_response_repairs_missing_translation_key() -> None:
    content = (
        '{"translations":[{"id":"t0","translation":"Providing accounting services."},'
        '{"id":"t1","Information technology consulting services."}]}'
    )

    result = parse_translation_response(content, {"t0", "t1"})

    assert result == {
        "t0": "Providing accounting services.",
        "t1": "Information technology consulting services.",
    }
