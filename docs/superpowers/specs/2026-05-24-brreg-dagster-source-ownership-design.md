# BRREG Dagster Source Ownership Design

## Decision

Dagster owns BRREG ingestion and enrichment, including a BRREG working store for in-progress artifacts. Corpscout owns published storage, normalized source read models, suggestions, review, and approval.

The BRREG pipeline should not use Corpscout as an upstream source. Dagster should pull original BRREG data directly from BRREG, enrich it, and publish complete original and enhanced results into the Corpscout Postgres database. Corpscout should not know how BRREG orchestration works and should not periodically pull BRREG itself.

## Goals

- Pull BRREG original raw company records directly from BRREG public endpoints.
- Store original raw payloads in Dagster BRREG working tables during enrichment.
- Produce enhanced BRREG JSON that follows the Corpscout BRREG enhanced schema.
- Publish original raw payloads and enhanced payloads to Corpscout Postgres without using the Corpscout HTTP API after enrichment is complete.
- Let Corpscout unpack enhanced JSON into normalized `brreg_source_*` tables and create suggestions from those normalized tables.
- Make pipeline status, retries, and failures visible in Dagster.
- Keep the first implementation MVP-oriented and avoid preserving old orchestration compatibility unless it is cheaper than removing it.

## Non-Goals

- Corpscout will not orchestrate BRREG download, translation, domain enrichment, or financial enrichment.
- Dagster will not write company suggestions directly.
- Dagster will not call Corpscout HTTP endpoints for BRREG ingestion.
- CVR, Ariregister, GLEIF, and Companies House do not move in this first BRREG-specific change.

## Architecture

Dagster contains a BRREG graph with source-specific assets:

- `brreg_raw_source`: downloads BRREG original rows from BRREG.
- `brreg_working_raw_records`: upserts original rows into `dagster_brreg.raw_records`.
- `brreg_translated_inputs`: translates fields needed by the enhanced schema.
- `brreg_domain_enrichment`: discovers potential domains for each source company.
- `brreg_financial_enrichment`: pulls and normalizes available financial fields.
- `brreg_enhanced_records`: builds the final enhanced BRREG JSON and writes it to `dagster_brreg.enhanced_records`.
- `brreg_publish_enhanced_records`: publishes original raw plus final enhanced JSON to Corpscout Postgres and asks the database-side Corpscout ingestion contract to unpack enhanced JSON into normalized source tables.

Corpscout keeps its normalized tables:

- `brreg_company_raw_inputs`
- `brreg_enhanced_raw_inputs`
- `brreg_source_companies`
- `brreg_source_addresses`
- `brreg_source_industries`
- `brreg_source_capital`
- `brreg_source_domains`
- `brreg_source_financials`

The normalized `brreg_source_*` tables are Corpscout read models. Dagster stores in-progress raw and enhanced source facts in `dagster_brreg`, then publishes complete raw/enhanced outputs and invokes a database ingestion function or procedure that belongs to the Corpscout schema.

## Data Flow

1. Dagster downloads BRREG data from the BRREG website or bulk endpoint.
2. Dagster computes stable source identifiers and payload hashes.
3. Dagster upserts original rows into `dagster_brreg.raw_records`.
4. Dagster runs enrichment steps over the raw payload:
   - translation,
   - domain discovery,
   - financial extraction and USD conversion.
5. Dagster builds one enhanced JSON object per BRREG raw input.
6. Dagster inserts the enhanced JSON into `dagster_brreg.enhanced_records`.
7. Dagster publishes original raw plus enhanced JSON to Corpscout Postgres.
8. Dagster invokes the Corpscout database unpack contract in the same Postgres environment.
9. Corpscout reads normalized `brreg_source_*` tables for UI, filtering, review, and suggestion creation.

## Database Contract

Dagster writes to Corpscout Postgres using a dedicated database role. That role should be granted only the minimum privileges needed for BRREG ingestion:

- insert/update on `brreg_company_raw_inputs`,
- insert on `brreg_enhanced_raw_inputs`,
- execute on the BRREG enhanced unpack procedure,
- read any lookup tables needed for idempotency.

The unpack contract should be exposed as a database function or procedure, not a Corpscout HTTP API. The contract should accept:

- `raw_input_id`,
- `payload_hash`,
- `enhanced_payload`,
- `dagster_run_id`,
- metadata about the producer version.

The contract should return:

- enhanced raw input id,
- source company id,
- unpack status,
- validation or persistence errors in a controlled shape.

The unpack operation remains Corpscout-owned because it writes Corpscout normalized tables and must respect Corpscout constraints.

## Idempotency

Raw input writes are idempotent by BRREG organization number and payload hash.

Enhanced writes are idempotent by `raw_input_id`, `payload_hash`, and enhanced schema version. Re-running the same Dagster run should not duplicate normalized source records. A changed enhanced payload should supersede the previous normalized BRREG source records for that organization while keeping history in `brreg_enhanced_raw_inputs`.

## Status Model

Dagster is the source of orchestration status:

- which BRREG rows were downloaded,
- which rows were translated,
- which rows were domain enriched,
- which rows received financial enrichment,
- which rows were written to Corpscout,
- which rows failed and need retry.

Corpscout is the source of review status:

- normalized source visibility,
- suggestion creation,
- suggestion review,
- approval into central companies.

Corpscout can still display lifecycle fields from the database, but it should not contain BRREG-specific orchestration actions beyond triggering a Dagster run for a source or selected set.

## Error Handling

Dagster records step-level failures with enough metadata to retry only failed work. Source download failures, translation failures, enrichment failures, database write failures, and unpack failures should be separate observable events.

Database writes should be transactional at the smallest meaningful unit:

- raw input upsert per batch,
- enhanced raw input insert plus unpack per company or bounded batch.

If unpack fails, the enhanced raw input should remain available with a failed unpack status so the record can be retried without recomputing enrichment.

## Testing

Dagster tests should cover:

- BRREG source extraction parsing,
- raw input upsert SQL shape and idempotency,
- enhanced JSON validation,
- financial original-currency and USD fields,
- domain enrichment output mapping,
- database unpack invocation,
- retry behavior after failed write or failed unpack.

Corpscout tests should cover:

- the database unpack function/procedure,
- normalized `brreg_source_*` persistence,
- superseding old normalized BRREG rows,
- source UI reads from normalized tables,
- suggestion creation from normalized BRREG source data.

## Migration Plan

1. Keep the existing Corpscout BRREG tables as the storage target.
2. Add a database-level unpack contract for enhanced BRREG payloads.
3. Build the Dagster BRREG project with direct BRREG extraction and direct Corpscout Postgres writes.
4. Move BRREG translation, domain enrichment, and financial enrichment into Dagster assets.
5. Disable or remove the old Corpscout-triggered BRREG pull path once Dagster produces equivalent raw and enhanced rows.
6. Keep Corpscout suggestion/review workflows unchanged except that their upstream data comes from Dagster-produced normalized BRREG tables.

## First Implementation Slice

The first slice should be narrow:

- create the Dagster package structure,
- implement the BRREG bulk/source extraction asset,
- define the Corpscout Postgres writer interface,
- write original BRREG rows into `brreg_company_raw_inputs`,
- add tests with mocked BRREG input and mocked/database-isolated Postgres writes.

Translation, domain enrichment, financial enrichment, and enhanced unpack invocation should be separate follow-up slices.
