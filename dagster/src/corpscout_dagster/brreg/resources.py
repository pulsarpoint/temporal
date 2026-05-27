from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg
from dagster import resource

from corpscout_dagster.brreg.asset_config import corpscout_database_url
from corpscout_dagster.brreg.crawl_service import CrawlServiceClient, HttpCrawlServiceClient
from corpscout_dagster.brreg.fx_rates import FxRateSet, load_ecb_rates_for_date, load_latest_ecb_rates
from corpscout_dagster.brreg.source import BrregBulkClient, BrregBulkRecordClient
from corpscout_dagster.brreg.translation_terms import (
    DEFAULT_LLM_MODEL,
    DEFAULT_PROMPT_VERSION,
    HttpTranslationServiceTermTranslator,
    TermTranslator,
)


@dataclass(frozen=True)
class PostgresResource:
    database_url: str
    connection_factory: Callable[[str], Any]


@dataclass(frozen=True)
class TranslationServiceResource:
    translator: TermTranslator
    model: str
    prompt_version: str


@dataclass(frozen=True)
class CrawlServiceResource:
    client: CrawlServiceClient


@dataclass(frozen=True)
class BrregBulkResource:
    client: BrregBulkRecordClient


@dataclass(frozen=True)
class FxResource:
    rate_date: str | None = None

    def load_rates(self, rate_date: str | None = None) -> FxRateSet:
        selected_rate_date = rate_date or self.rate_date
        if not selected_rate_date:
            return load_latest_ecb_rates()
        return load_ecb_rates_for_date(date.fromisoformat(selected_rate_date))


@resource
def postgres_resource(_context) -> PostgresResource:
    return PostgresResource(
        database_url=corpscout_database_url(),
        connection_factory=psycopg.connect,
    )


@resource
def translation_service_resource(_context) -> TranslationServiceResource:
    return TranslationServiceResource(
        translator=HttpTranslationServiceTermTranslator.from_env(),
        model=(
            os.environ.get("BRREG_TRANSLATION_MODEL")
            or os.environ.get("TRANSLATION_DEFAULT_MODEL")
            or DEFAULT_LLM_MODEL
        ),
        prompt_version=os.environ.get("BRREG_TRANSLATION_PROMPT_VERSION") or DEFAULT_PROMPT_VERSION,
    )


@resource
def crawl_service_resource(_context) -> CrawlServiceResource:
    return CrawlServiceResource(client=HttpCrawlServiceClient.from_env())


@resource
def brreg_bulk_resource(_context) -> BrregBulkResource:
    return BrregBulkResource(client=BrregBulkClient())


@resource
def fx_resource(_context) -> FxResource:
    return FxResource(rate_date=os.environ.get("BRREG_FX_RATE_DATE"))
