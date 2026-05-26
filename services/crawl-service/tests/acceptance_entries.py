from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from corpscout_crawl_service.models import BrregDomainDiscoveryRequest, DomainStatus


ACCEPTANCE_ENTRIES_PATH = Path(__file__).parent / "data" / "brreg_domain_acceptance_entries_10.json"


@dataclass(frozen=True)
class DomainDiscoveryAcceptanceCase:
    request: BrregDomainDiscoveryRequest
    expected_status: DomainStatus
    expected_best_domain: str | None


def load_domain_discovery_acceptance_cases() -> list[DomainDiscoveryAcceptanceCase]:
    values = json.loads(ACCEPTANCE_ENTRIES_PATH.read_text())
    return [
        DomainDiscoveryAcceptanceCase(
            request=BrregDomainDiscoveryRequest.model_validate(value["request"]),
            expected_status=value["expected"]["status"],
            expected_best_domain=value["expected"].get("best_domain"),
        )
        for value in values
    ]
