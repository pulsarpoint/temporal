from __future__ import annotations

from tests.acceptance_entries import load_domain_discovery_acceptance_cases


def test_acceptance_fixture_contains_at_least_10_curated_entries() -> None:
    cases = load_domain_discovery_acceptance_cases()

    assert len(cases) >= 10
    assert len({case.request.organization_number for case in cases}) == len(cases)
    assert all(case.expected_status for case in cases)
    assert all(case.expected_best_domain for case in cases if case.expected_status == "succeeded")
