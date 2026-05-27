from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path


SEED = "brreg-e2e-v1"
FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "brreg_raw_records_1000.json.gz"


def main() -> None:
    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = [_payload(index) for index in range(1000)]
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.GzipFile(filename="", mode="wb", fileobj=FIXTURE_PATH.open("wb"), mtime=0) as gzip_file:
        gzip_file.write(encoded)


def _payload(index: int) -> dict:
    organization_number = f"{810000000 + index:09d}"
    translation_bucket = _bucket("translation", organization_number)
    company_name = f"MOCK BRREG COMPANY {index:04d} AS"
    payload = {
        "organisasjonsnummer": organization_number,
        "navn": company_name,
        "konkurs": False,
        "underAvvikling": False,
        "stiftelsesdato": f"20{index % 24:02d}-01-{(index % 27) + 1:02d}",
        "registreringsdatoEnhetsregisteret": f"20{index % 24:02d}-02-{(index % 27) + 1:02d}",
        "forretningsadresse": {
            "adresse": [f"Mockgata {index + 1}"],
            "postnummer": f"{1000 + (index % 8000):04d}",
            "poststed": "OSLO" if index % 3 == 0 else "BERGEN" if index % 3 == 1 else "TRONDHEIM",
            "kommune": "OSLO",
            "kommunenummer": "0301",
            "land": "Norge",
            "landkode": "NO",
        },
    }
    if index % 4 == 0:
        payload["hjemmeside"] = f"https://existing-{organization_number}.example.no"
    if translation_bucket < 95:
        payload.update(
            {
                "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                "naeringskode1": {"kode": "62.010", "beskrivelse": f"Programmeringstjenester {organization_number}"},
                "aktivitet": [f"Mock activity for organization {organization_number}"],
                "vedtektsfestetFormaal": [f"Mock purpose for organization {organization_number}"],
            }
        )
    capital = _capital(index)
    if capital is not None:
        payload["kapital"] = capital
    return payload


def _capital(index: int) -> dict | None:
    mod = index % 5
    if mod == 0:
        return None
    currency = "NOK" if mod in {1, 4} else "EUR" if mod == 2 else "USD"
    return {
        "belop": float(10000 + index * 17),
        "valuta": currency,
        "innfortDato": "2026-05-21",
    }


def _bucket(task: str, organization_number: str) -> int:
    digest = hashlib.sha256(f"{SEED}:{task}:{organization_number}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


if __name__ == "__main__":
    main()
