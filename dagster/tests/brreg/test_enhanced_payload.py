from __future__ import annotations

from corpscout_dagster.brreg.enhanced_payload import (
    BRREG_ENHANCED_SCHEMA_VERSION,
    build_brreg_enhanced_payload,
    enhanced_payload_hash,
)
from corpscout_dagster.brreg.working_store import DomainResultCandidateRow, RawTaskRecord


def test_build_brreg_enhanced_payload_matches_corpscout_contract() -> None:
    record = RawTaskRecord(
        id="00000000-0000-0000-0000-000000000010",
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        website=None,
        raw_payload={
            "organisasjonsnummer": "810202572",
            "navn": "BORTIGARD AS",
            "telefon": "33051963",
            "maalform": "Bokmål",
            "respons_klasse": "Enhet",
            "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
            "institusjonellSektorkode": {"kode": "2100", "beskrivelse": "Private aksjeselskaper mv."},
            "naeringskode1": {"kode": "41.000", "beskrivelse": "Oppføring av bygninger"},
            "hjelpeenhetskode": {"kode": "70.100", "beskrivelse": "Hovedkontortjenester"},
            "aktivitet": ["Drive utleie av fast eiendom."],
            "vedtektsfestetFormaal": ["Drive utleie av fast eiendom."],
            "kapital": {
                "type": "Aksjekapital",
                "belop": 81870.00,
                "valuta": "NOK",
                "innfortDato": "2012-07-09",
                "antallAksjer": 8187,
            },
            "forretningsadresse": {
                "adresse": ["Løkkeveien 18"],
                "postnummer": "3085",
                "poststed": "HOLMESTRAND",
                "kommune": "HOLMESTRAND",
                "kommunenummer": "3903",
                "land": "Norge",
                "landkode": "NO",
            },
            "stiftelsesdato": "1975-06-04",
            "registreringsdatoEnhetsregisteret": "1995-02-19",
            "registreringsdatoForetaksregisteret": "1989-09-05",
            "registreringsdatoMerverdiavgiftsregisteret": "1975-11-01",
            "registreringsdatoMerverdiavgiftsregisteretEnhetsregisteret": "1995-02-19",
            "vedtektsdato": "2018-06-27",
            "sisteInnsendteAarsregnskap": "2024",
            "konkurs": False,
            "erIKonsern": False,
            "underAvvikling": False,
            "underTvangsavviklingEllerTvangsopplosning": False,
            "harRegistrertAntallAnsatte": False,
            "registrertIMvaregisteret": True,
            "registrertIForetaksregisteret": True,
            "registrertIFrivillighetsregisteret": False,
            "registrertIStiftelsesregisteret": False,
            "registrertIPartiregisteret": False,
        },
    )

    payload = build_brreg_enhanced_payload(
        record=record,
        payload_hash="raw-payload-hash",
        translation_status="succeeded",
        translation_payload={
            "terms": [
                {
                    "category": "org_form",
                    "original_text": "Aksjeselskap",
                    "translated_text": "Limited Liability Company",
                },
                {
                    "category": "industry_code",
                    "original_text": "Oppføring av bygninger",
                    "translated_text": "Construction of buildings",
                },
                {
                    "category": "sector_code",
                    "original_text": "Private aksjeselskaper mv.",
                    "translated_text": "Private limited companies etc.",
                },
                {
                    "category": "capital_type",
                    "original_text": "Aksjekapital",
                    "translated_text": "Share capital",
                },
                {
                    "category": "activity",
                    "original_text": "Drive utleie av fast eiendom.",
                    "translated_text": "Engage in rental of real estate.",
                },
                {
                    "category": "statutory_purpose",
                    "original_text": "Drive utleie av fast eiendom.",
                    "translated_text": "Engage in rental of real estate.",
                },
            ]
        },
        domain_status="succeeded",
        domain_candidates=[
            DomainResultCandidateRow(
                domain="www.bortigard.no",
                normalized_domain="bortigard.no",
                score=95,
                signals=["website_field"],
                status="proposed",
                evidence={"website": "https://www.bortigard.no"},
                metadata={"source": "test"},
            )
        ],
        task_statuses={
            "translate": "succeeded",
            "domain_results": "succeeded",
            "currency_conversion": "succeeded",
        },
        currency_status="succeeded",
        financial_payload={
            "capital": {
                "original_amount": 81870.0,
                "original_currency": "NOK",
            },
        },
        usd_payload={
            "capital": {
                "amount_usd": 8868.64,
                "amount_usd_cents": 886864,
            },
        },
        fx_metadata={
            "source": "ECB",
            "rate_date": "2026-05-21",
            "capital": {
                "source_currency": "NOK",
                "target_currency": "USD",
                "source_rate_per_eur": 10.7075,
                "target_rate_per_eur": 1.1599,
            },
        },
        dagster_run_id="dagster-run-1",
    )

    assert payload["schema_version"] == BRREG_ENHANCED_SCHEMA_VERSION
    assert payload["enhancement"]["status"] == "partial"
    assert payload["enhancement"]["section_statuses"]["currency"] == "succeeded"
    assert payload["enhancement"]["section_statuses"]["financials"] == "not_available"
    assert payload["source_company"]["organization_number"] == "810202572"
    assert payload["source_company"]["organization_form_description_en"]["value"] == "Limited Liability Company"
    assert payload["source_company"]["activity_description_en"]["value"] == "Engage in rental of real estate."
    assert payload["addresses"][0]["address_type"] == "business"
    assert payload["industries"][0]["description_en"]["value"] == "Construction of buildings"
    assert payload["capital"]["capital_type_en"]["value"] == "Share capital"
    assert payload["capital"]["original_amount"] == 81870.0
    assert payload["capital"]["original_currency"] == "NOK"
    assert payload["capital"]["amount_usd"] == 8868.64
    assert payload["capital"]["amount_usd_cents"] == 886864
    assert payload["capital"]["fx_source"] == "ECB"
    assert payload["capital"]["fx_rate_date"] == "2026-05-21"
    assert payload["capital"]["fx_metadata"] == {
        "source_currency": "NOK",
        "target_currency": "USD",
        "source_rate_per_eur": 10.7075,
        "target_rate_per_eur": 1.1599,
    }
    assert payload["domains"][0]["normalized_domain"] == "bortigard.no"
    assert payload["domains"][0]["source"] == "dagster"
    assert payload["financials"] == []
    assert enhanced_payload_hash(payload) == enhanced_payload_hash(payload)
