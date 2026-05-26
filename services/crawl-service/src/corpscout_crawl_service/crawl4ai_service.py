from __future__ import annotations

import asyncio
import hashlib
import os
import time
import urllib.parse
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from corpscout_crawl_service.models import Crawl4AiRequest, Crawl4AiResponse


JsonObject = dict[str, Any]


LlmExtractor = Callable[[Crawl4AiRequest, Crawl4AiResponse], Awaitable[Any]]


class LlmConfigError(RuntimeError):
    pass


class LlmExtractionError(RuntimeError):
    pass


@dataclass(frozen=True)
class LlmConfig:
    model: str
    base_url: str
    api_key: str = ""


class Crawl4AiService:
    def __init__(
        self,
        *,
        llm_config: LlmConfig | None = None,
        crawler_factory: Callable[[], Any] | None = None,
        llm_extractor: LlmExtractor | None = None,
    ) -> None:
        self._llm_config = llm_config or llm_config_from_env()
        self._crawler_factory = crawler_factory
        self._llm_extractor = llm_extractor
        self._crawler_context = None
        self._crawler = None
        self._lock = asyncio.Lock()

    @classmethod
    def from_env(cls) -> "Crawl4AiService":
        return cls()

    async def start(self) -> None:
        await self._ensure_crawler()

    async def close(self) -> None:
        async with self._lock:
            crawler_context = self._crawler_context
            self._crawler_context = None
            self._crawler = None
            if crawler_context is not None:
                await crawler_context.__aexit__(None, None, None)

    async def crawl(self, request: Crawl4AiRequest) -> Crawl4AiResponse:
        started = time.monotonic()
        try:
            crawler = await self._ensure_crawler()
            crawl_result = await asyncio.wait_for(crawler.arun(url=request.url), timeout=request.timeout_seconds)
            response = _response_from_result(request=request, crawl_result=crawl_result, started=started)
            if response.status == "succeeded" and request.llm_enabled:
                try:
                    response.llm_output = await self._extract(request=request, response=response)
                except LlmExtractionError as exc:
                    response.status = "failed"
                    response.error = {
                        "code": "llm_extraction_failed",
                        "message": "Crawl4AI LLM extraction failed",
                        "detail": str(exc),
                    }
            return response
        except TimeoutError:
            return Crawl4AiResponse(
                url=request.url,
                final_url=request.url,
                status="failed",
                error={
                    "code": "crawl_timeout",
                    "message": "Crawl4AI crawl timed out",
                    "detail": {"timeout_seconds": request.timeout_seconds},
                },
                duration_ms=int((time.monotonic() - started) * 1000),
                metadata={"purpose": request.purpose, **request.metadata},
            )
        except Exception as exc:
            return Crawl4AiResponse(
                url=request.url,
                final_url=request.url,
                status="failed",
                error={"code": "crawl_failed", "message": "Crawl4AI crawl failed", "detail": str(exc)},
                duration_ms=int((time.monotonic() - started) * 1000),
                metadata={"purpose": request.purpose, **request.metadata},
            )

    async def _ensure_crawler(self):
        async with self._lock:
            if self._crawler is not None:
                return self._crawler
            if self._crawler_factory is not None:
                self._crawler = self._crawler_factory()
                return self._crawler

            from crawl4ai import AsyncWebCrawler  # type: ignore[import]

            self._crawler_context = AsyncWebCrawler(config=domain_crawler_browser_config_from_env())
            self._crawler = await self._crawler_context.__aenter__()
            return self._crawler

    async def _extract(self, *, request: Crawl4AiRequest, response: Crawl4AiResponse) -> Any:
        if self._llm_extractor is not None:
            return _normalize_llm_output(await self._llm_extractor(request, response))
        if not request.llm_query:
            return None

        from crawl4ai import LLMConfig, LLMExtractionStrategy

        strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider=f"openai/{self._llm_config.model}",
                api_token=self._llm_config.api_key,
                base_url=self._llm_config.base_url,
            ),
            instruction=request.llm_query,
            schema=request.llm_schema,
            extraction_type="schema" if request.llm_schema else "block",
            input_format="markdown",
            force_json_response=True,
            apply_chunking=False,
            extra_args=_llm_extra_args(),
        )
        blocks = await strategy.arun(response.final_url or request.url, [response.markdown or ""])
        return _normalize_llm_output(blocks)


def search_url_for_engine(search_term: str, *, search_engine: str | None = None) -> str:
    engine = (search_engine or "duckduckgo").strip().lower()
    encoded = urllib.parse.quote(search_term)
    if engine == "duckduckgo":
        return f"https://html.duckduckgo.com/html/?q={encoded}"
    if engine == "yandex":
        return f"https://yandex.com/search/?text={encoded}"
    raise ValueError(f"Unsupported search engine: {search_engine}")


def _response_from_result(*, request: Crawl4AiRequest, crawl_result, started: float) -> Crawl4AiResponse:
    markdown = _markdown_from_result(crawl_result)
    final_url = str(getattr(crawl_result, "url", None) or request.url)
    if getattr(crawl_result, "success", True) is False:
        return Crawl4AiResponse(
            url=request.url,
            final_url=final_url,
            status="failed",
            markdown=markdown or None,
            markdown_hash=hashlib.sha256(markdown.encode("utf-8")).hexdigest() if markdown else None,
            links=_result_links(crawl_result),
            error={
                "code": "crawl_failed",
                "message": "Crawl4AI crawl failed",
                "detail": str(getattr(crawl_result, "error_message", "") or ""),
            },
            duration_ms=int((time.monotonic() - started) * 1000),
            metadata={
                "purpose": request.purpose,
                "timeout_seconds": request.timeout_seconds,
                **request.metadata,
                **_metadata_from_result(crawl_result),
            },
        )
    return Crawl4AiResponse(
        url=request.url,
        final_url=final_url,
        status="succeeded",
        markdown=markdown,
        markdown_hash=hashlib.sha256(markdown.encode("utf-8")).hexdigest(),
        links=_result_links(crawl_result),
        duration_ms=int((time.monotonic() - started) * 1000),
        metadata={
            "purpose": request.purpose,
            "timeout_seconds": request.timeout_seconds,
            **request.metadata,
            **_metadata_from_result(crawl_result),
        },
    )


def _markdown_from_result(result) -> str:
    markdown = getattr(result, "markdown", "")
    if isinstance(markdown, str):
        return markdown.strip()
    for attr in ("raw_markdown", "fit_markdown", "markdown"):
        value = getattr(markdown, attr, None)
        if isinstance(value, str):
            return value.strip()
    return str(markdown or "").strip()


def _result_links(result) -> list[str]:
    links = getattr(result, "links", None)
    if not isinstance(links, dict):
        return []
    values: list[str] = []
    for key in ("external", "internal"):
        for link in links.get(key) or []:
            if isinstance(link, dict) and link.get("href"):
                values.append(str(link["href"]))
    unique: list[str] = []
    for value in values:
        if value not in unique:
            unique.append(value)
    return unique[:100]


def _metadata_from_result(result) -> JsonObject:
    metadata = getattr(result, "metadata", None)
    return metadata if isinstance(metadata, dict) else {}


def _normalize_llm_output(blocks: Any) -> Any:
    if isinstance(blocks, list):
        error_blocks = [block for block in blocks if isinstance(block, dict) and block.get("error") is True]
        if error_blocks:
            details = [
                str(block.get("content") or block.get("error_message") or block)
                for block in error_blocks
            ]
            raise LlmExtractionError("; ".join(details))
        clean_blocks = [block for block in blocks if isinstance(block, dict) and block.get("error") is not True]
        if len(clean_blocks) == 1:
            return clean_blocks[0]
        return {"items": clean_blocks}
    return blocks


def domain_crawler_browser_config_from_env():
    from crawl4ai import BrowserConfig  # type: ignore[import]

    chrome_channel = os.environ.get("DOMAIN_CRAWLER_CHROME_CHANNEL", "chromium").strip() or "chromium"
    return BrowserConfig(
        browser_type=os.environ.get("DOMAIN_CRAWLER_BROWSER_TYPE", "chromium").strip() or "chromium",
        headless=_env_bool("DOMAIN_CRAWLER_HEADLESS", default=True),
        chrome_channel=chrome_channel,
        channel=chrome_channel,
        ignore_https_errors=True,
        sleep_on_close=True,
        light_mode=_env_bool("DOMAIN_CRAWLER_LIGHT_MODE", default=True),
        use_managed_browser=_env_bool("DOMAIN_CRAWLER_USE_MANAGED_BROWSER", default=False),
        enable_stealth=_env_bool("DOMAIN_CRAWLER_ENABLE_STEALTH", default=True),
        verbose=False,
    )


def llm_config_from_env() -> LlmConfig:
    model = _required_env("CRAWL_SERVICE_LLM_MODEL")
    base_url = _required_env("CRAWL_SERVICE_LLM_BASE_URL")
    api_key = _optional_secret("CRAWL_SERVICE_LLM_API_KEY")
    return LlmConfig(model=model, base_url=normalize_openai_api_base(base_url), api_key=api_key)


def normalize_openai_api_base(base_url: str) -> str:
    trimmed = base_url.strip().rstrip("/")
    if trimmed.endswith("/v1/chat/completions"):
        trimmed = trimmed[: -len("/v1/chat/completions")]
    if trimmed.endswith("/chat/completions"):
        trimmed = trimmed[: -len("/chat/completions")]
    if trimmed.endswith("/v1"):
        trimmed = trimmed[: -len("/v1")]
    return trimmed.rstrip("/") + "/v1"


def _llm_extra_args() -> JsonObject:
    extra_args: JsonObject = {"temperature": 0, "max_tokens": 1000}
    if _env_bool("CRAWL_SERVICE_DISABLE_THINKING", default=True):
        extra_args["chat_template_kwargs"] = {"enable_thinking": False}
    return extra_args


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise LlmConfigError(f"{name} must be configured")
    return value.strip()


def _optional_secret(name: str) -> str:
    value = os.environ.get(name)
    if value is not None and value.strip():
        return value.strip()
    path = os.environ.get(f"{name}_FILE")
    if path is None or not path.strip():
        return ""
    try:
        with open(path.strip(), encoding="utf-8") as file:
            return file.read().strip()
    except OSError as exc:
        raise LlmConfigError(f"{name}_FILE could not be read") from exc
