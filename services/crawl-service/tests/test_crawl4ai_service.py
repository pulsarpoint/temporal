from __future__ import annotations

import asyncio

import pytest

from corpscout_crawl_service.crawl4ai_service import (
    Crawl4AiRequest,
    Crawl4AiResponse,
    LlmConfig,
    llm_config_from_env,
    search_url_for_engine,
)


def test_search_url_for_engine_defaults_to_duckduckgo() -> None:
    assert search_url_for_engine("BORTIGARD AS Norway website") == (
        "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20Norway%20website"
    )


def test_search_url_for_engine_supports_yandex() -> None:
    assert search_url_for_engine("BORTIGARD AS Norway website", search_engine="yandex") == (
        "https://yandex.com/search/?text=BORTIGARD%20AS%20Norway%20website"
    )


def test_search_url_for_engine_rejects_unsupported_engine() -> None:
    with pytest.raises(ValueError, match="Unsupported search engine"):
        search_url_for_engine("BORTIGARD AS Norway website", search_engine="google")


def test_crawl4ai_service_requires_llm_model(monkeypatch) -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService, LlmConfigError

    monkeypatch.delenv("CRAWL_SERVICE_LLM_MODEL", raising=False)
    monkeypatch.setenv("CRAWL_SERVICE_LLM_BASE_URL", "http://100.77.62.33:8888")

    with pytest.raises(LlmConfigError, match="CRAWL_SERVICE_LLM_MODEL"):
        Crawl4AiService.from_env()


def test_crawl4ai_service_requires_llm_base_url(monkeypatch) -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService, LlmConfigError

    monkeypatch.setenv("CRAWL_SERVICE_LLM_MODEL", "qwen3:6b")
    monkeypatch.delenv("CRAWL_SERVICE_LLM_BASE_URL", raising=False)

    with pytest.raises(LlmConfigError, match="CRAWL_SERVICE_LLM_BASE_URL"):
        Crawl4AiService.from_env()


def test_llm_config_reads_api_key_from_file(monkeypatch, tmp_path) -> None:
    api_key_file = tmp_path / "api-key"
    api_key_file.write_text("secret-key\n")
    monkeypatch.setenv("CRAWL_SERVICE_LLM_MODEL", "deepseek-v4-flash")
    monkeypatch.setenv("CRAWL_SERVICE_LLM_BASE_URL", "https://api.deepseek.com")
    monkeypatch.delenv("CRAWL_SERVICE_LLM_API_KEY", raising=False)
    monkeypatch.setenv("CRAWL_SERVICE_LLM_API_KEY_FILE", str(api_key_file))

    config = llm_config_from_env()

    assert config.model == "deepseek-v4-flash"
    assert config.base_url == "https://api.deepseek.com/v1"
    assert config.api_key == "secret-key"


@pytest.mark.asyncio
async def test_crawl4ai_service_returns_markdown_without_llm() -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService

    service = Crawl4AiService(
        llm_config=_llm_config(),
        crawler_factory=lambda: _FakeCrawler(markdown="# Bortigard AS"),
    )

    response = await service.crawl(Crawl4AiRequest(url="https://www.bortigard.no/", llm_enabled=False))

    assert response.status == "succeeded"
    assert response.final_url == "https://www.bortigard.no/"
    assert response.markdown == "# Bortigard AS"
    assert response.markdown_hash
    assert response.links == ["https://www.bortigard.no/contact"]
    assert response.llm_output is None


@pytest.mark.asyncio
async def test_crawl4ai_service_returns_llm_output_when_enabled() -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService

    async def extractor(request: Crawl4AiRequest, response: Crawl4AiResponse):
        return {"candidates": [{"url": "https://www.bortigard.no/", "score": 82}]}

    service = Crawl4AiService(
        llm_config=_llm_config(),
        crawler_factory=lambda: _FakeCrawler(markdown="# Search results"),
        llm_extractor=extractor,
    )

    response = await service.crawl(
        Crawl4AiRequest(
            url="https://html.duckduckgo.com/html/?q=BORTIGARD",
            llm_enabled=True,
            llm_query="Find company domains.",
            llm_schema={"type": "object"},
        )
    )

    assert response.status == "succeeded"
    assert response.llm_output == {"candidates": [{"url": "https://www.bortigard.no/", "score": 82}]}


@pytest.mark.asyncio
async def test_crawl4ai_service_returns_structured_error_on_llm_failure() -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService

    async def extractor(request: Crawl4AiRequest, response: Crawl4AiResponse):
        return [
            {
                "error": True,
                "content": "litellm.BadRequestError: unsupported model",
            }
        ]

    service = Crawl4AiService(
        llm_config=_llm_config(),
        crawler_factory=lambda: _FakeCrawler(markdown="# Search results"),
        llm_extractor=extractor,
    )

    response = await service.crawl(
        Crawl4AiRequest(
            url="https://html.duckduckgo.com/html/?q=BORTIGARD",
            llm_enabled=True,
            llm_query="Find company domains.",
            llm_schema={"type": "object"},
        )
    )

    assert response.status == "failed"
    assert response.error is not None
    assert response.error["code"] == "llm_extraction_failed"
    assert response.error["detail"] == "litellm.BadRequestError: unsupported model"


@pytest.mark.asyncio
async def test_crawl4ai_service_returns_structured_error_on_crawl_failure() -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService

    class FailingCrawler:
        async def arun(self, *, url: str):
            raise RuntimeError("browser failed")

    service = Crawl4AiService(llm_config=_llm_config(), crawler_factory=lambda: FailingCrawler())

    response = await service.crawl(Crawl4AiRequest(url="https://example.no/", llm_enabled=False))

    assert response.status == "failed"
    assert response.error is not None
    assert response.error["code"] == "crawl_failed"
    assert response.error["detail"] == "browser failed"


@pytest.mark.asyncio
async def test_crawl4ai_service_returns_structured_error_on_failed_result() -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService

    class FailedResultCrawler:
        async def arun(self, *, url: str):
            return type(
                "CrawlResult",
                (),
                {
                    "url": url,
                    "success": False,
                    "error_message": "blocked by provider",
                    "markdown": "",
                    "links": {"external": [], "internal": []},
                    "metadata": {},
                },
            )()

    service = Crawl4AiService(llm_config=_llm_config(), crawler_factory=lambda: FailedResultCrawler())

    response = await service.crawl(Crawl4AiRequest(url="https://example.no/", llm_enabled=True))

    assert response.status == "failed"
    assert response.llm_output is None
    assert response.error is not None
    assert response.error["code"] == "crawl_failed"
    assert response.error["detail"] == "blocked by provider"


@pytest.mark.asyncio
async def test_crawl4ai_service_returns_structured_timeout_error() -> None:
    from corpscout_crawl_service.crawl4ai_service import Crawl4AiService

    class SlowCrawler:
        async def arun(self, *, url: str):
            await asyncio.sleep(5)

    service = Crawl4AiService(llm_config=_llm_config(), crawler_factory=lambda: SlowCrawler())

    response = await service.crawl(Crawl4AiRequest(url="https://example.no/", timeout_seconds=1))

    assert response.status == "failed"
    assert response.error is not None
    assert response.error["code"] == "crawl_timeout"
    assert response.error["detail"] == {"timeout_seconds": 1}


class _FakeCrawler:
    def __init__(self, *, markdown: str) -> None:
        self._markdown = markdown

    async def arun(self, *, url: str):
        return type(
            "CrawlResult",
            (),
            {
                "url": url,
                "markdown": self._markdown,
                "links": {
                    "external": [
                        {"href": "https://www.bortigard.no/contact"},
                    ],
                    "internal": [],
                },
                "metadata": {"title": "Bortigard"},
            },
        )()


def _llm_config() -> LlmConfig:
    return LlmConfig(model="qwen3:6b", base_url="http://100.77.62.33:8888/v1")
