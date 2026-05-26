from __future__ import annotations

from corpscout_translation_service.llm import normalize_openai_api_base


def test_normalize_openai_api_base_appends_v1() -> None:
    assert normalize_openai_api_base("http://llm.example:8888") == "http://llm.example:8888/v1"


def test_normalize_openai_api_base_accepts_existing_v1_or_chat_completion_url() -> None:
    assert normalize_openai_api_base("http://llm.example:8888/v1") == "http://llm.example:8888/v1"
    assert (
        normalize_openai_api_base("http://llm.example:8888/v1/chat/completions")
        == "http://llm.example:8888/v1"
    )
