from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from corpscout_dagster.brreg.fx_rates import FxRateSet
from corpscout_dagster.brreg.working_store import DomainProposalRow, RawTaskRecord


BRREG_ENHANCED_SCHEMA_VERSION = "brreg.enhanced.v1"


def build_brreg_enhanced_payload(
    *,
    record: RawTaskRecord,
    payload_hash: str,
    translation_status: str,
    translation_payload: dict[str, Any] | None,
    domain_status: str,
    domain_proposals: list[DomainProposalRow],
    task_statuses: dict[str, str],
    fx_rates: FxRateSet | None = None,
    dagster_run_id: str,
) -> dict[str, Any]:
    raw = record.raw_payload
    translations = _translation_lookup(translation_payload or {})
    section_statuses = {
        "source_company": "succeeded",
        "addresses": "succeeded",
        "industries": "succeeded",
        "capital": "succeeded" if isinstance(raw.get("kapital"), dict) else "not_available",
        "domains": _section_status_from_task(domain_status),
        "financials": "not_available",
    }
    now = _utc_now()
    return {
        "schema_version": BRREG_ENHANCED_SCHEMA_VERSION,
        "enhancement": {
            "status": _enhancement_status(section_statuses),
            "dagster_run_id": dagster_run_id,
            "started_at": now,
            "finished_at": now,
            "section_statuses": section_statuses,
        },
        "source_company": _source_company_section(
            raw=raw,
            record=record,
            translations=translations,
            translation_status=translation_status,
        ),
        "addresses": _address_sections(raw),
        "industries": _industry_sections(raw=raw, translations=translations, translation_status=translation_status),
        "capital": _capital_section(
            raw=raw,
            translations=translations,
            translation_status=translation_status,
            fx_rates=fx_rates,
        ),
        "domains": _domain_sections(domain_proposals),
        "financials": [],
    }


def enhanced_payload_hash(payload: dict[str, Any]) -> str:
    raw_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw_bytes).hexdigest()


def _source_company_section(
    *,
    raw: dict[str, Any],
    record: RawTaskRecord,
    translations: dict[tuple[str, str], str],
    translation_status: str,
) -> dict[str, Any]:
    org_form = _dict(raw.get("organisasjonsform"))
    sector = _dict(raw.get("institusjonellSektorkode"))
    activity_items = _string_list(raw.get("aktivitet"))
    purpose_items = _string_list(raw.get("vedtektsfestetFormaal"))
    return {
        "organization_number": str(raw.get("organisasjonsnummer") or record.organization_number),
        "name": str(raw.get("navn") or record.organization_name or record.organization_number),
        "registration_status": _registration_status(raw),
        "country_iso2": "NO",
        "website": record.website or _optional_str(raw.get("hjemmeside")),
        "phone": _optional_str(raw.get("telefon")),
        "organization_form_code": _optional_str(org_form.get("kode")),
        "organization_form_description": _optional_str(org_form.get("beskrivelse")),
        "organization_form_description_en": _translated_string(
            translations=translations,
            category="org_form",
            original=org_form.get("beskrivelse"),
            translation_status=translation_status,
        ),
        "language_code": _optional_str(raw.get("maalform")),
        "response_class": _optional_str(raw.get("respons_klasse")),
        "founded_date": _optional_str(raw.get("stiftelsesdato")),
        "unit_registry_registered_at": _optional_str(raw.get("registreringsdatoEnhetsregisteret")),
        "enterprise_registry_registered_at": _optional_str(raw.get("registreringsdatoForetaksregisteret")),
        "vat_registry_registered_at": _optional_str(raw.get("registreringsdatoMerverdiavgiftsregisteret")),
        "vat_registry_unit_registered_at": _optional_str(
            raw.get("registreringsdatoMerverdiavgiftsregisteretEnhetsregisteret")
        ),
        "articles_date": _optional_str(raw.get("vedtektsdato")),
        "last_annual_report_year": _optional_int(raw.get("sisteInnsendteAarsregnskap")),
        "activity_description": _join_text(activity_items),
        "activity_description_en": _translated_joined_string(
            translations=translations,
            category="activity",
            originals=activity_items,
            translation_status=translation_status,
        ),
        "statutory_purpose": _join_text(purpose_items),
        "statutory_purpose_en": _translated_joined_string(
            translations=translations,
            category="statutory_purpose",
            originals=purpose_items,
            translation_status=translation_status,
        ),
        "is_bankrupt": _optional_bool(raw.get("konkurs")),
        "is_in_group": _optional_bool(raw.get("erIKonsern")),
        "is_under_liquidation": _optional_bool(raw.get("underAvvikling")),
        "is_forced_dissolution": _optional_bool(raw.get("underTvangsavviklingEllerTvangsopplosning")),
        "has_registered_employees": _optional_bool(raw.get("harRegistrertAntallAnsatte")),
        "in_vat_register": _optional_bool(raw.get("registrertIMvaregisteret")),
        "in_business_register": _optional_bool(raw.get("registrertIForetaksregisteret")),
        "in_voluntary_register": _optional_bool(raw.get("registrertIFrivillighetsregisteret")),
        "in_foundation_register": _optional_bool(raw.get("registrertIStiftelsesregisteret")),
        "in_party_register": _optional_bool(raw.get("registrertIPartiregisteret")),
    }


def _address_sections(raw: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, address_type in [("forretningsadresse", "business"), ("postadresse", "postal")]:
        address = _dict(raw.get(key))
        if not address:
            continue
        rows.append(
            {
                "address_type": address_type,
                "street_lines": _string_list(address.get("adresse")),
                "postal_code": _optional_str(address.get("postnummer")),
                "city": _optional_str(address.get("poststed")),
                "municipality": _optional_str(address.get("kommune")),
                "municipality_number": _optional_str(address.get("kommunenummer")),
                "country": _optional_str(address.get("land")),
                "country_code": _optional_str(address.get("landkode")),
            }
        )
    return rows


def _industry_sections(
    *,
    raw: dict[str, Any],
    translations: dict[tuple[str, str], str],
    translation_status: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position, key in enumerate(["naeringskode1", "naeringskode2", "naeringskode3"], start=1):
        value = _dict(raw.get(key))
        if value:
            rows.append(_industry_row("industry", position, value, translations, translation_status))
    helper = _dict(raw.get("hjelpeenhetskode"))
    if helper:
        rows.append(_industry_row("helper_unit", 1, helper, translations, translation_status))
    sector = _dict(raw.get("institusjonellSektorkode"))
    if sector:
        rows.append(_industry_row("institutional_sector", 1, sector, translations, translation_status))
    return rows


def _industry_row(
    classification_type: str,
    position: int,
    value: dict[str, Any],
    translations: dict[tuple[str, str], str],
    translation_status: str,
) -> dict[str, Any]:
    category = "sector_code" if classification_type == "institutional_sector" else "industry_code"
    description = _optional_str(value.get("beskrivelse"))
    return {
        "classification_type": classification_type,
        "position": position,
        "code": str(value.get("kode") or ""),
        "description": description,
        "description_en": _translated_string(
            translations=translations,
            category=category,
            original=description,
            translation_status=translation_status,
        ),
    }


def _capital_section(
    *,
    raw: dict[str, Any],
    translations: dict[tuple[str, str], str],
    translation_status: str,
    fx_rates: FxRateSet | None,
) -> dict[str, Any] | None:
    capital = _dict(raw.get("kapital"))
    if not capital:
        return None
    capital_type = _optional_str(capital.get("type"))
    original_amount = _optional_decimal(capital.get("belop"))
    original_currency = _optional_str(capital.get("valuta"))
    amount_usd_cents = None
    amount_usd = None
    fx_metadata: dict[str, Any] = {}
    fx_source = None
    fx_rate_date = None
    if original_amount is not None or original_currency is not None:
        if original_amount is None or original_currency is None:
            raise ValueError("incomplete capital currency data")
        if fx_rates is None:
            raise ValueError("FX rates are required for BRREG capital conversion")
        amount_usd_cents = fx_rates.to_usd_cents(original_amount, original_currency)
        amount_usd = amount_usd_cents / 100
        fx_source = fx_rates.source
        fx_rate_date = fx_rates.rate_date
        fx_metadata = fx_rates.exchange_metadata(original_currency)

    return {
        "capital_type": capital_type,
        "capital_type_en": _translated_string(
            translations=translations,
            category="capital_type",
            original=capital_type,
            translation_status=translation_status,
        ),
        "original_amount": float(original_amount) if original_amount is not None else None,
        "original_currency": original_currency,
        "introduced_at": _optional_str(capital.get("innfortDato")),
        "share_count": _optional_int(capital.get("antallAksjer")),
        "amount_usd": amount_usd,
        "amount_usd_cents": amount_usd_cents,
        "fx_source": fx_source,
        "fx_rate_date": fx_rate_date,
        "fx_metadata": fx_metadata,
    }


def _domain_sections(proposals: list[DomainProposalRow]) -> list[dict[str, Any]]:
    return [
        {
            "domain": proposal.domain,
            "normalized_domain": proposal.normalized_domain,
            "best_signal": proposal.signals[0] if proposal.signals else "unknown",
            "confidence": proposal.score,
            "status": "active" if proposal.status in {"proposed", "accepted"} else proposal.status,
            "source": "dagster",
            "evidence": proposal.evidence,
            "metadata": proposal.metadata,
        }
        for proposal in proposals
    ]


def _translation_lookup(payload: dict[str, Any]) -> dict[tuple[str, str], str]:
    terms = payload.get("terms")
    if not isinstance(terms, list):
        return {}
    result: dict[tuple[str, str], str] = {}
    for term in terms:
        if not isinstance(term, dict):
            continue
        category = _optional_str(term.get("category"))
        original = _optional_str(term.get("original_text"))
        translated = _optional_str(term.get("translated_text"))
        if category and original and translated:
            result[(category, original)] = translated
    return result


def _translated_string(
    *,
    translations: dict[tuple[str, str], str],
    category: str,
    original: Any,
    translation_status: str,
) -> dict[str, Any] | None:
    original_text = _optional_str(original)
    if original_text is None:
        return None
    translated = translations.get((category, original_text))
    if translated:
        return {"value": translated, "status": "translated", "source": "translation", "confidence": 100, "error": None}
    status = "not_available" if translation_status in {"succeeded", "skipped"} else "not_done"
    return {"value": None, "status": status, "source": "translation", "confidence": None, "error": None}


def _translated_joined_string(
    *,
    translations: dict[tuple[str, str], str],
    category: str,
    originals: list[str],
    translation_status: str,
) -> dict[str, Any] | None:
    if not originals:
        return None
    translated = [translations.get((category, original)) for original in originals]
    if all(translated):
        return {
            "value": _join_text([str(item) for item in translated if item]),
            "status": "translated",
            "source": "translation",
            "confidence": 100,
            "error": None,
        }
    status = "not_available" if translation_status in {"succeeded", "skipped"} else "not_done"
    return {"value": None, "status": status, "source": "translation", "confidence": None, "error": None}


def _registration_status(raw: dict[str, Any]) -> str:
    if raw.get("konkurs") is True or raw.get("underAvvikling") is True:
        return "dissolved"
    return "active"


def _section_status_from_task(status: str) -> str:
    if status == "not_found":
        return "not_available"
    if status == "partial":
        return "partial"
    if status in {"succeeded", "skipped", "failed"}:
        return status
    return "not_done"


def _enhancement_status(section_statuses: dict[str, str]) -> str:
    if any(status == "failed" for status in section_statuses.values()):
        return "partial"
    if any(status in {"not_available", "skipped", "not_done", "partial"} for status in section_statuses.values()):
        return "partial"
    return "succeeded"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = _optional_str(value)
    return [text] if text else []


def _join_text(items: list[str]) -> str | None:
    if not items:
        return None
    return " ".join(items)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("amount must be numeric")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("amount must be numeric") from exc


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
