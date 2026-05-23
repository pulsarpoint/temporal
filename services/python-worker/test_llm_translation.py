from __future__ import annotations

import json
import os
import socket
from urllib.parse import urlparse

import httpx
import pytest
import respx

from contracts import TranslateTermsInput, TranslationItem
from activities.llm_translation import (
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    DSPyTranslationService,
    parse_translation_payload,
    translate_terms_with_dspy,
    translation_max_tokens,
)


@pytest.mark.asyncio
async def test_translate_terms_uses_default_local_model():
    seen: list[tuple[str, str]] = []

    async def fake_runner(payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
        seen.append((model, base_url))
        return {"t0": "Share capital"}

    service = DSPyTranslationService(runner=fake_runner)

    result = await service.translate_terms(
        TranslateTermsInput(
            category="capital_type",
            items=[TranslationItem(id="t0", text="Aksjekapital")],
        )
    )

    assert seen == [("qwen3:6b", "http://100.77.62.33:8888")]
    assert result.model == "qwen3:6b"
    assert [(item.id, item.translation) for item in result.translations] == [("t0", "Share capital")]


@pytest.mark.asyncio
async def test_translate_terms_allows_task_model_override():
    seen: list[str] = []

    async def fake_runner(payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
        seen.append(model)
        return {"t0": "Limited company"}

    service = DSPyTranslationService(default_model="qwen3:6b", runner=fake_runner)

    result = await service.translate_terms(
        TranslateTermsInput(
            category="org_form",
            model="openai/custom-task-model",
            items=[TranslationItem(id="t0", text="Aksjeselskap")],
        )
    )

    assert seen == ["openai/custom-task-model"]
    assert result.model == "openai/custom-task-model"
    assert result.translations[0].translation == "Limited company"


@pytest.mark.asyncio
async def test_translate_terms_reports_missing_items_without_failing_activity():
    async def fake_runner(payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
        return {"t0": "Share capital"}

    service = DSPyTranslationService(runner=fake_runner)

    result = await service.translate_terms(
        TranslateTermsInput(
            category="capital_type",
            items=[
                TranslationItem(id="t0", text="Aksjekapital"),
                TranslationItem(id="t1", text="Aksjekapital annen"),
            ],
        )
    )

    assert [(item.id, item.translation) for item in result.translations] == [("t0", "Share capital")]
    assert [(failure.id, failure.error) for failure in result.failures] == [("t1", "missing translation")]


@pytest.mark.asyncio
async def test_translate_terms_retries_missing_items_once():
    seen: list[list[str]] = []

    async def fake_runner(payload: TranslateTermsInput, model: str, base_url: str) -> dict[str, str]:
        item_ids = [item.id for item in payload.items]
        seen.append(item_ids)
        if item_ids == ["t0", "t1"]:
            return {"t0": "Accounting services"}
        return {"t1": "Information technology consulting services"}

    service = DSPyTranslationService(runner=fake_runner)

    result = await service.translate_terms(
        TranslateTermsInput(
            category="activity",
            items=[
                TranslationItem(id="t0", text="Regnskapsjenester"),
                TranslationItem(id="t1", text="Konsulentvirksomhet tilknyttet informasjonsteknologi."),
            ],
        )
    )

    assert seen == [["t0", "t1"], ["t1"]]
    assert result.failures == []
    assert [(item.id, item.translation) for item in result.translations] == [
        ("t0", "Accounting services"),
        ("t1", "Information technology consulting services"),
    ]


@pytest.mark.asyncio
async def test_translate_terms_activity_uses_service_factory(monkeypatch):
    service = DSPyTranslationService(
        runner=lambda payload, model, base_url: {"t0": "Association/organization/entity"}
    )
    monkeypatch.setattr("activities.llm_translation.default_translation_service", lambda: service)

    result = await translate_terms_with_dspy(
        TranslateTermsInput(
            category="org_form",
            items=[TranslationItem(id="t0", text="Forening/lag/innretning")],
        )
    )

    assert result.translations[0].translation == "Association/organization/entity"


@pytest.mark.asyncio
@respx.mock
async def test_translate_terms_default_runner_uses_direct_chat_completion_api():
    route = respx.post("http://llm.test/v1/chat/completions").mock(
        return_value=httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": '{"translations":[{"id":"t0","translation":"Accounting services"}]}'
                        }
                    }
                ]
            },
        )
    )
    service = DSPyTranslationService(default_base_url="http://llm.test", default_model="qwen-test")

    result = await service.translate_terms(
        TranslateTermsInput(
            category="activity",
            source_lang="no",
            target_lang="en",
            items=[TranslationItem(id="t0", text="Regnskapsjenester")],
        )
    )

    assert result.translations[0].translation == "Accounting services"
    request = route.calls.last.request
    body = json.loads(request.content)
    assert body["model"] == "qwen-test"
    assert body["temperature"] == 0
    assert body["max_tokens"] == 512
    assert body["messages"] == [
        {
            "role": "user",
            "content": (
                "/no_think\n"
                "Translate no business registry activity text to en.\n"
                'Return only JSON: {"translations":[{"id":"...","translation":"..."}]}\n'
                "Preserve every input id exactly. Include one translation per input item.\n"
                'Items: [{"id":"t0","text":"Regnskapsjenester"}]'
            ),
        }
    ]


def test_translation_max_tokens_scales_with_batch_size():
    small_payload = TranslateTermsInput(
        category="activity",
        items=[TranslationItem(id="t0", text="Regnskapsjenester")],
    )
    large_payload = TranslateTermsInput(
        category="activity",
        items=[TranslationItem(id=f"t{index}", text="Regnskapsjenester") for index in range(50)],
    )

    assert translation_max_tokens(small_payload) == 512
    assert translation_max_tokens(large_payload) == 4096


def test_parse_translation_payload_repairs_missing_translation_key():
    content = (
        '{"translations":[{"id":"t0","translation":"Providing accounting services."},'
        '{"id":"t1","Information technology consulting services."}]}'
    )

    result = parse_translation_payload(content, {"t0", "t1"})

    assert result == {
        "t0": "Providing accounting services.",
        "t1": "Information technology consulting services.",
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_translate_terms_with_existing_model_and_real_brreg_data():
    pytest.importorskip("dspy")

    base_url = os.environ.get("LLM_INTEGRATION_BASE_URL") or os.environ.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL
    model = os.environ.get("LLM_INTEGRATION_MODEL") or os.environ.get("LLM_MODEL") or DEFAULT_LLM_MODEL
    skip_if_llm_is_unreachable(base_url, model)

    source_terms = {
        "t0": "Å yte regnskapsjenester, inkasso av fordringer og annen administrativ tjenesteyting.",
        "t1": "Konsulentvirksomhet tilknyttet informasjonsteknologi.",
    }
    service = DSPyTranslationService(default_base_url=base_url, default_model=model)
    result = await service.translate_terms(
        TranslateTermsInput(
            category="activity",
            items=[TranslationItem(id=item_id, text=text) for item_id, text in source_terms.items()],
        )
    )

    assert result.failures == []
    assert result.model == model

    translated = {item.id: item.translation for item in result.translations}
    assert set(translated) == set(source_terms)
    assert translated["t0"] != source_terms["t0"]
    assert translated["t1"] != source_terms["t1"]
    assert "regnskapsjenester" not in translated["t0"].lower()
    assert "konsulentvirksomhet" not in translated["t1"].lower()


def skip_if_llm_is_unreachable(base_url: str, model: str = "") -> None:
    parsed = urlparse(base_url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if not host:
        pytest.fail(f"invalid LLM base URL: {base_url}")
    try:
        with socket.create_connection((host, port), timeout=1.5):
            pass
    except OSError as exc:
        pytest.skip(f"local LLM is not reachable at {host}:{port}: {exc}")
    if not model:
        return
    try:
        response = httpx.get(f"{base_url.rstrip('/').removesuffix('/v1')}/v1/models", timeout=2)
        response.raise_for_status()
        available_models = {item.get("id") for item in response.json().get("data", []) if isinstance(item, dict)}
    except Exception as exc:
        pytest.skip(f"local LLM model list is not available at {base_url}: {exc}")
    if model not in available_models:
        pytest.skip(f"local LLM model {model!r} is not available; available models: {sorted(available_models)}")
