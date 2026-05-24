from __future__ import annotations

from datetime import date

import pytest

from corpscout_dagster.brreg.fx_rates import FxRateSet, parse_ecb_rates


ECB_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2026-05-20">
      <Cube currency="USD" rate="1.0900"/>
      <Cube currency="NOK" rate="11.5000"/>
    </Cube>
    <Cube time="2026-05-17">
      <Cube currency="USD" rate="1.0800"/>
      <Cube currency="NOK" rate="11.4000"/>
    </Cube>
  </Cube>
</gesmes:Envelope>"""


def test_parse_ecb_rates_for_date_uses_latest_official_rate_on_or_before_date() -> None:
    rates = parse_ecb_rates(ECB_FIXTURE, target_date=date(2026, 5, 19))

    assert rates.source == "ECB"
    assert rates.rate_date == "2026-05-17"
    assert rates.eur_per["EUR"] == 1.0
    assert rates.eur_per["USD"] == 1.08
    assert rates.eur_per["NOK"] == 11.4


def test_parse_ecb_rates_without_date_uses_first_rate_day() -> None:
    rates = parse_ecb_rates(ECB_FIXTURE)

    assert rates.rate_date == "2026-05-20"


def test_fx_rate_set_converts_original_currency_to_usd_cents() -> None:
    rates = FxRateSet(
        source="ECB",
        rate_date="2026-05-21",
        eur_per={
            "EUR": 1.0,
            "NOK": 10.7075,
            "USD": 1.1599,
        },
    )

    assert rates.to_usd_cents(81870, "NOK") == 886864
    assert rates.to_usd_cents(12.34, "USD") == 1234
    assert rates.exchange_metadata("NOK") == {
        "source_currency": "NOK",
        "target_currency": "USD",
        "source_rate_per_eur": 10.7075,
        "target_rate_per_eur": 1.1599,
    }


def test_fx_rate_set_rejects_invalid_conversion_inputs() -> None:
    rates = FxRateSet(source="ECB", rate_date="2026-05-21", eur_per={"EUR": 1.0, "USD": 1.1599})

    with pytest.raises(ValueError, match="negative amount"):
        rates.to_usd_cents(-1, "USD")

    with pytest.raises(ValueError, match='currency "NOK" not found'):
        rates.to_usd_cents(100, "NOK")

    with pytest.raises(ValueError, match="USD rate not found"):
        FxRateSet(source="ECB", rate_date="2026-05-21", eur_per={"EUR": 1.0}).to_usd_cents(100, "NOK")
