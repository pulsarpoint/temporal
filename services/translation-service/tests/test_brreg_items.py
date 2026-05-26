from __future__ import annotations

from corpscout_translation_service.brreg import extract_translation_items, translation_item_id

from tests.fixtures import brreg_raw_payload


def test_extracts_brreg_translation_terms_from_raw_payload() -> None:
    items = extract_translation_items(brreg_raw_payload())

    assert [(item.category, item.text) for item in items] == [
        ("org_form", "Aksjeselskap"),
        ("sector_code", "Private aksjeselskaper mv."),
        ("industry_code", "Oppføring av bygninger"),
        ("capital_type", "Aksjekapital"),
        ("activity", "Drive utleie av fast eiendom, maskiner og utstyr."),
        ("statutory_purpose", "Drive utleie av fast eiendom, maskiner og utstyr."),
    ]


def test_translation_item_ids_are_stable_and_category_scoped() -> None:
    items = extract_translation_items(brreg_raw_payload())
    ids = [translation_item_id(item) for item in items]

    assert len(ids) == len(set(ids))
    assert translation_item_id(items[4]) != translation_item_id(items[5])
    assert translation_item_id(items[0]) == "org_form:4371a971b6681abf840c767ae6637c3488bec4f756ff94045b01e1542ee78f74"
