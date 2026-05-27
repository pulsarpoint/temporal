from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


RecordStatus = Literal["succeeded", "failed", "skipped"]
BatchStatus = Literal["succeeded", "partial", "failed"]


class LLMSelection(BaseModel):
    provider: str = Field(default="default", min_length=1)
    model: str | None = Field(default=None, min_length=1)


class BrregRecord(BaseModel):
    record_id: str = Field(min_length=1)
    organization_number: str = Field(min_length=1)
    raw_payload: dict[str, Any]


class BrregTranslateRequest(BaseModel):
    records: list[BrregRecord] = Field(min_length=1, max_length=1000)
    llm: LLMSelection = Field(default_factory=LLMSelection)
    prompt_version: str = Field(default="v1", min_length=1)
    source_lang: str = Field(default="no", min_length=2)
    target_lang: str = Field(default="en", min_length=2)
    max_retries: int = Field(default=3, ge=0, le=5)


class LLMTranslationItem(BaseModel):
    id: str
    category: str
    text: str


class LLMTranslationRequest(BaseModel):
    provider: str
    model: str
    prompt_version: str
    source_lang: str
    target_lang: str
    items: list[LLMTranslationItem] = Field(min_length=1)
    max_retries: int = Field(default=3, ge=0, le=5)


class LLMTermTranslation(BaseModel):
    id: str
    translation: str


class TranslationError(BaseModel):
    code: str
    message: str
    category: str | None = None
    retry_strategy: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class LLMTranslateResponse(BaseModel):
    schema_version: str = "translation-service.terms.v1"
    status: BatchStatus
    provider: str
    model: str
    prompt_version: str
    items_seen: int
    items_completed: int
    items_failed: int
    translations: list[LLMTermTranslation]
    missing_ids: list[str] = Field(default_factory=list)
    error: TranslationError | None = None
    duration_ms: int


class BrregRecordTranslationResult(BaseModel):
    record_id: str
    organization_number: str
    status: RecordStatus
    translated_payload: dict[str, Any] | None = None
    missing_terms: list[str] = Field(default_factory=list)
    error: TranslationError | None = None
    duration_ms: int


class BrregTranslateResponse(BaseModel):
    schema_version: str = "translation-service.brreg.v1"
    status: BatchStatus
    provider: str
    model: str
    prompt_version: str
    records_seen: int
    records_completed: int
    records_failed: int
    records_skipped: int
    duration_ms: int
    results: list[BrregRecordTranslationResult]
