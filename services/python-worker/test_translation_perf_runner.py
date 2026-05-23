from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def load_perf_runner_module():
    module_path = Path(__file__).parent / "testing" / "perf_translation_runner.py"
    spec = importlib.util.spec_from_file_location("perf_translation_runner", module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_load_corpus_reads_translation_cases(tmp_path):
    perf = load_perf_runner_module()
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "name": "activity-short",
                        "category": "activity",
                        "source_lang": "no",
                        "target_lang": "en",
                        "items": [
                            {
                                "id": "t0",
                                "text": "Regnskapsjenester",
                                "expected_any": ["accounting"],
                                "forbidden_any": ["regnskaps"],
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = perf.load_corpus(corpus_path)

    assert [case.name for case in cases] == ["activity-short"]
    assert cases[0].items[0].expected_any == ["accounting"]


def test_validate_translations_reports_missing_expected_and_source_copy():
    perf = load_perf_runner_module()
    case = perf.TranslationCase(
        name="activity-short",
        category="activity",
        source_lang="no",
        target_lang="en",
        items=[
            perf.CorpusItem(
                id="t0",
                text="Regnskapsjenester",
                expected_any=["accounting"],
                forbidden_any=["regnskaps"],
            )
        ],
    )

    errors = perf.validate_translations(case, {"t0": "Regnskapsjenester"})

    assert 't0 copied source text: "Regnskapsjenester"' in errors
    assert "t0 missing expected text: one of ['accounting']" in errors
    assert "t0 contains forbidden text: regnskaps" in errors


def test_append_result_writes_jsonl_record(tmp_path):
    perf = load_perf_runner_module()
    output_dir = tmp_path / ".perf-results"
    result = {
        "run_id": "run-1",
        "runner": "dspy",
        "case_count": 1,
        "total_seconds": 1.23,
    }

    output_path = perf.append_result(result, output_dir=output_dir)

    assert output_path == output_dir / "translation_perf_results.jsonl"
    assert json.loads(output_path.read_text(encoding="utf-8")) == result
