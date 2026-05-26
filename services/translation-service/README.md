# Corpscout Translation Service

Standalone OpenAI-compatible translation API for Corpscout enrichment pipelines.

## Run

```bash
make sync
make run
```

Health check:

```bash
curl http://localhost:8095/healthz
```

BRREG translation:

```bash
curl -X POST 'http://localhost:8095/v1/translate/brreg-records?provider=default&model=qwen3:6b' \
  -H 'content-type: application/json' \
  -d '{"records":[{"record_id":"record-1","organization_number":"810202572","raw_payload":{"organisasjonsnummer":"810202572","navn":"BORTIGARD AS","organisasjonsform":{"kode":"AS","beskrivelse":"Aksjeselskap"}}}]}'
```

Term-batch translation used by Dagster cache fills:

```bash
curl -X POST 'http://localhost:8095/v1/translate/terms?provider=default&model=qwen3:6b' \
  -H 'content-type: application/json' \
  -d '{"provider":"default","model":"default","prompt_version":"v1","source_lang":"no","target_lang":"en","items":[{"id":"org_form:example","category":"org_form","text":"Aksjeselskap"}]}'
```

## LLM Configuration

The service supports multiple OpenAI-compatible providers selected per request with query parameters:

- `provider`
- `model`
- `prompt_version`

Provider env vars:

```bash
TRANSLATION_DEFAULT_PROVIDER=local
TRANSLATION_DEFAULT_MODEL=qwen3:6b
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888
# Local LLM does not require a password/API key.
# TRANSLATION_PROVIDER_LOCAL_API_KEY=
TRANSLATION_PROVIDER_DEEPSEEK_BASE_URL=https://api.deepseek.com
TRANSLATION_PROVIDER_DEEPSEEK_API_KEY=...
```

Never pass API keys in query parameters.

## Tests

Normal test suite uses fake LLMs:

```bash
make test
```

The repository includes `tests/data/brreg_raw_records_300.json`, exported from `dagster_brreg.raw_records`, so the real LLM test uses actual BRREG payloads rather than synthetic records.

Refresh the fixture from a database:

```bash
CORPSCOUT_DATABASE_URL=postgresql://... \
uv run scripts/export_brreg_raw_records_fixture.py --limit 300
```

Opt-in real LLM stress test against the local LLM:

```bash
TRANSLATION_SERVICE_RUN_REAL_LLM_TESTS=1 \
TRANSLATION_SERVICE_TEST_PROVIDER=default \
TRANSLATION_SERVICE_TEST_MODEL=qwen3:6b \
TRANSLATION_LLM_BASE_URL=http://100.77.62.33:8888 \
make test-real
```
