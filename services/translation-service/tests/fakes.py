from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from corpscout_translation_service.models import LLMTranslationItem, LLMTranslationRequest


@dataclass
class FakeLLMClient:
    missing_once_ids: set[str] = field(default_factory=set)
    always_missing_ids: set[str] = field(default_factory=set)
    fail_with: Exception | None = None
    calls: list[LLMTranslationRequest] = field(default_factory=list)

    async def translate_terms(self, request: LLMTranslationRequest) -> dict[str, str]:
        self.calls.append(request)
        if self.fail_with is not None:
            raise self.fail_with
        return {
            item.id: f"{item.text} EN"
            for item in request.items
            if not self._should_omit(item)
        }

    def _should_omit(self, item: LLMTranslationItem) -> bool:
        if item.id in self.always_missing_ids:
            return True
        if item.id in self.missing_once_ids:
            self.missing_once_ids.remove(item.id)
            return True
        return False


def translated_texts_for(items: Iterable[LLMTranslationItem]) -> dict[str, str]:
    return {item.id: f"{item.text} EN" for item in items}
