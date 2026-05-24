from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from xml.etree import ElementTree

import httpx


ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
ECB_HISTORICAL_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"


@dataclass(frozen=True)
class FxRateSet:
    source: str
    rate_date: str
    eur_per: dict[str, float]

    def to_usd_cents(self, amount: int | float | str | Decimal, currency: str) -> int:
        source_currency = currency.strip().upper()
        value = _decimal_amount(amount)
        if value < 0:
            raise ValueError("negative amount not supported")

        if source_currency == "USD":
            return _round_cents(value * Decimal("100"))

        usd_rate = self._positive_rate("USD", message="USD rate not found")
        source_rate = self._positive_rate(source_currency, message=f'currency "{source_currency}" not found')
        return _round_cents((value / source_rate) * usd_rate * Decimal("100"))

    def exchange_metadata(self, currency: str) -> dict[str, Any]:
        source_currency = currency.strip().upper()
        return {
            "source_currency": source_currency,
            "target_currency": "USD",
            "source_rate_per_eur": float(
                self._positive_rate(source_currency, message=f'currency "{source_currency}" not found')
            ),
            "target_rate_per_eur": float(self._positive_rate("USD", message="USD rate not found")),
        }

    def _positive_rate(self, currency: str, *, message: str) -> Decimal:
        rate = self.eur_per.get(currency)
        if rate is None:
            raise ValueError(message)
        try:
            value = Decimal(str(rate))
        except InvalidOperation as exc:
            raise ValueError(message) from exc
        if not value.is_finite() or value <= 0:
            raise ValueError(message)
        return value


def parse_ecb_rates(xml_text: str | bytes, *, target_date: date | None = None) -> FxRateSet:
    root = ElementTree.fromstring(xml_text)
    daily_rates = []
    for element in root.iter():
        if _local_name(element.tag) != "Cube":
            continue
        rate_date = element.attrib.get("time")
        if not rate_date:
            continue
        parsed_date = date.fromisoformat(rate_date)
        rates = {"EUR": 1.0}
        for child in element:
            if _local_name(child.tag) != "Cube":
                continue
            currency = child.attrib.get("currency")
            rate = child.attrib.get("rate")
            if currency and rate:
                rates[currency.upper()] = float(rate)
        daily_rates.append((parsed_date, rates))

    if not daily_rates:
        raise ValueError("ECB feed did not contain rates")

    if target_date is None:
        selected_date, selected_rates = daily_rates[0]
    else:
        candidates = [(rate_day, rates) for rate_day, rates in daily_rates if rate_day <= target_date]
        if not candidates:
            raise ValueError(f"ECB rate not found on or before {target_date.isoformat()}")
        selected_date, selected_rates = max(candidates, key=lambda item: item[0])

    return FxRateSet(source="ECB", rate_date=selected_date.isoformat(), eur_per=selected_rates)


def load_latest_ecb_rates() -> FxRateSet:
    return _load_ecb_rates(ECB_DAILY_URL)


def load_ecb_rates_for_date(rate_date: date) -> FxRateSet:
    return _load_ecb_rates(ECB_HISTORICAL_URL, target_date=rate_date)


def _load_ecb_rates(url: str, *, target_date: date | None = None) -> FxRateSet:
    with httpx.Client(timeout=30) as client:
        response = client.get(url)
        response.raise_for_status()
    return parse_ecb_rates(response.content, target_date=target_date)


def _round_cents(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _decimal_amount(value: int | float | str | Decimal) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("amount must be numeric")
    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("amount must be numeric") from exc
    if not amount.is_finite():
        raise ValueError("amount must be numeric")
    return amount


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
