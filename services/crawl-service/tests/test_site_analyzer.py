from __future__ import annotations

import pytest

from corpscout_crawl_service.site_analyzer import _parse_llm_json, _site_analysis_prompt


def test_parse_llm_json_extracts_fenced_object_after_reasoning_text() -> None:
    content = """
<think>Need to compare the legal entity with the page.</think>
```json
{
  "decision": "accepted",
  "score": 92,
  "site_type": "company_website",
  "relationship": "primary_web_presence",
  "owned_domain": true
}
```
"""

    parsed = _parse_llm_json(content, list_key=None)

    assert parsed["decision"] == "accepted"
    assert parsed["score"] == 92


def test_parse_llm_json_wraps_candidate_array_for_search() -> None:
    content = """
Here are the candidates:
[
  {"url": "https://example.no", "domain": "example.no", "score": 80}
]
"""

    parsed = _parse_llm_json(content, list_key="candidates")

    assert parsed == {"candidates": [{"url": "https://example.no", "domain": "example.no", "score": 80}]}


def test_parse_llm_json_error_includes_output_excerpt() -> None:
    with pytest.raises(ValueError, match="not parseable"):
        _parse_llm_json("I cannot answer this as JSON.", list_key=None)


def test_site_prompt_forbids_schema_echoing_and_includes_json_skeleton() -> None:
    prompt = _site_analysis_prompt(
        {
            "company_name": "ALSTRAY AS",
            "business_activity": ["Holdingselskap."],
            "industry_codes": ["68.200 Utleie av egen eller leid fast eiendom"],
        }
    )

    assert "Do not output schema" in prompt
    assert '"identity_signals"' in prompt
    assert '"activity_alignment"' in prompt
