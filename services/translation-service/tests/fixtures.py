from __future__ import annotations

from typing import Any


def brreg_raw_payload(
    *,
    organization_number: str = "810202572",
    name: str = "BORTIGARD AS",
    activity: str = "Drive utleie av fast eiendom, maskiner og utstyr.",
    purpose: str = "Drive utleie av fast eiendom, maskiner og utstyr.",
) -> dict[str, Any]:
    return {
        "organisasjonsnummer": organization_number,
        "navn": name,
        "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
        "institusjonellSektorkode": {"kode": "2100", "beskrivelse": "Private aksjeselskaper mv."},
        "naeringskode1": {"kode": "41.000", "beskrivelse": "Oppføring av bygninger"},
        "kapital": {"type": "Aksjekapital", "belop": 81870.0, "valuta": "NOK"},
        "aktivitet": [activity],
        "vedtektsfestetFormaal": [purpose],
    }


def brreg_record_payload(index: int) -> dict[str, Any]:
    organization_number = f"81{index:07d}"
    return {
        "record_id": f"record-{index}",
        "organization_number": organization_number,
        "raw_payload": brreg_raw_payload(
            organization_number=organization_number,
            name=f"TEST COMPANY {index} AS",
            activity=f"Drive testaktivitet nummer {index} for programvare og rådgivning.",
            purpose=f"Utvikle og selge programvare og rådgivning nummer {index}.",
        ),
    }
