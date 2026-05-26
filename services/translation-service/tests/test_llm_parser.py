from __future__ import annotations

import pytest

from corpscout_translation_service.llm import parse_llm_translation_response


def test_parse_llm_translation_response_accepts_fenced_json() -> None:
    content = """```json
    {"translations":[{"id":"a","translation":"hello"},{"id":"b","translation":"world"}]}
    ```"""

    assert parse_llm_translation_response(content, expected_ids={"a", "b"}) == {
        "a": "hello",
        "b": "world",
    }


def test_parse_llm_translation_response_repairs_trailing_comma() -> None:
    content = '{"translations":[{"id":"a","translation":"hello",},],}'

    assert parse_llm_translation_response(content, expected_ids={"a"}) == {"a": "hello"}


def test_parse_llm_translation_response_rejects_missing_expected_ids() -> None:
    content = '{"translations":[{"id":"a","translation":"hello"}]}'

    with pytest.raises(ValueError, match="missing translations"):
        parse_llm_translation_response(content, expected_ids={"a", "b"})
