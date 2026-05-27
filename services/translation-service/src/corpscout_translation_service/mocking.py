from __future__ import annotations

import hashlib
import os
import re
import time
from dataclasses import dataclass, field

from corpscout_translation_service.models import (
    LLMTermTranslation,
    LLMTranslateResponse,
    LLMTranslationRequest,
    TranslationError,
)


DEFAULT_MOCK_SEED = "brreg-e2e-v1"


@dataclass
class MockTranslationController:
    seed: str = DEFAULT_MOCK_SEED
    profile: str = "mixed"
    fail_once_keys: set[str] = field(default_factory=set)

    @classmethod
    def from_env(cls) -> "MockTranslationController":
        return cls(
            seed=os.environ.get("MOCK_SEED") or DEFAULT_MOCK_SEED,
            profile=os.environ.get("MOCK_PROFILE") or "mixed",
        )

    def reset(self) -> None:
        self.fail_once_keys.clear()

    def state(self) -> dict[str, object]:
        return {
            "seed": self.seed,
            "profile": self.profile,
            "fail_once_keys": sorted(self.fail_once_keys),
        }

    def translate_terms_response(self, request: LLMTranslationRequest) -> LLMTranslateResponse:
        started = time.monotonic()
        outcome = self._request_outcome(request)
        if outcome == "fail_once":
            key = self._request_key(request)
            if key not in self.fail_once_keys:
                self.fail_once_keys.add(key)
                return _failed_response(
                    request=request,
                    code="mock_fail_once",
                    message="Mock translation transient failure.",
                    category="transient_external",
                    retry_strategy="automatic",
                    started=started,
                )
        if outcome == "terminal":
            return _failed_response(
                request=request,
                code="mock_terminal_translation",
                message="Mock translation terminal failure.",
                category="invalid_llm_output",
                retry_strategy="change_model_or_prompt",
                started=started,
            )

        translations = [
            LLMTermTranslation(id=item.id, translation=f"[mock-en] {item.text}")
            for item in request.items
        ]
        return LLMTranslateResponse(
            status="succeeded",
            provider="mock",
            model=request.model,
            prompt_version=request.prompt_version,
            items_seen=len(request.items),
            items_completed=len(translations),
            items_failed=0,
            translations=translations,
            missing_ids=[],
            error=None,
            duration_ms=_elapsed_ms(started),
        )

    def _request_outcome(self, request: LLMTranslationRequest) -> str:
        outcomes = [_outcome_for_bucket(_bucket(self.seed, "translation", org)) for org in self._org_numbers(request)]
        if "fail_once" in outcomes:
            return "fail_once"
        if "terminal" in outcomes:
            return "terminal"
        return "success"

    def _request_key(self, request: LLMTranslationRequest) -> str:
        orgs = ",".join(self._org_numbers(request))
        ids = ",".join(sorted(item.id for item in request.items))
        return f"translation:{orgs}:{hashlib.sha256(ids.encode('utf-8')).hexdigest()[:12]}"

    def _org_numbers(self, request: LLMTranslationRequest) -> list[str]:
        orgs: list[str] = []
        for item in request.items:
            match = re.search(r"\b(\d{9})\b", f"{item.id} {item.text}")
            if match:
                orgs.append(match.group(1))
        if orgs:
            return sorted(set(orgs))
        request_fingerprint = ",".join(sorted(item.id for item in request.items))
        return [hashlib.sha256(request_fingerprint.encode("utf-8")).hexdigest()[:12]]


def mock_enabled_from_env() -> bool:
    return _truthy(os.environ.get("TRANSLATION_MOCK_ENABLED")) or os.environ.get("TRANSLATION_DEFAULT_PROVIDER") == "mock"


def _bucket(seed: str, task: str, organization_number: str) -> int:
    digest = hashlib.sha256(f"{seed}:{task}:{organization_number}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


def _outcome_for_bucket(bucket: int) -> str:
    if 80 <= bucket < 90:
        return "fail_once"
    if 90 <= bucket < 95:
        return "terminal"
    return "success"


def _failed_response(
    *,
    request: LLMTranslationRequest,
    code: str,
    message: str,
    category: str,
    retry_strategy: str,
    started: float,
) -> LLMTranslateResponse:
    return LLMTranslateResponse(
        status="failed",
        provider="mock",
        model=request.model,
        prompt_version=request.prompt_version,
        items_seen=len(request.items),
        items_completed=0,
        items_failed=len(request.items),
        translations=[],
        missing_ids=[item.id for item in request.items],
        error=TranslationError(
            code=code,
            message=message,
            category=category,
            retry_strategy=retry_strategy,
            detail={"mock": True},
        ),
        duration_ms=_elapsed_ms(started),
    )


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
