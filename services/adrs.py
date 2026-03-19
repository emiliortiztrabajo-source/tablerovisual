from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from dashboard.models import AdrQuote


@dataclass(frozen=True)
class AdrPayload:
    company: str
    local_ticker: str
    usa_ticker: str
    price: Decimal
    daily_change: Decimal
    period_date: date


def fetch_adr_quotes(use_mock: bool = True) -> list[AdrPayload]:
    if not use_mock:
        raise NotImplementedError("Conecta aqui tu API real de ADRs.")

    today = date(2026, 3, 18)
    raw_data = [
        ("Grupo Financiero Galicia", "GGAL", "GGAL", "35.82", "2.42"),
        ("Banco Macro", "BMA", "BMA", "68.11", "-1.24"),
        ("YPF", "YPFD", "YPF", "27.58", "0.93"),
        ("Pampa Energia", "PAMP", "PAM", "48.90", "1.12"),
        ("Transportadora Gas del Sur", "TGSU2", "TGS", "32.10", "-0.58"),
        ("Telecom Argentina", "TECO2", "TEO", "9.84", "1.76"),
    ]
    return [
        AdrPayload(
            company=company,
            local_ticker=local_ticker,
            usa_ticker=usa_ticker,
            price=Decimal(price),
            daily_change=Decimal(daily_change),
            period_date=today,
        )
        for company, local_ticker, usa_ticker, price, daily_change in raw_data
    ]


def sync_adrs_data(use_mock: bool = True) -> int:
    payloads = fetch_adr_quotes(use_mock=use_mock)
    for payload in payloads:
        AdrQuote.objects.update_or_create(
            usa_ticker=payload.usa_ticker,
            date=payload.period_date,
            defaults={
                "company": payload.company,
                "local_ticker": payload.local_ticker,
                "value": payload.price,
                "daily_change": payload.daily_change,
            },
        )
    return len(payloads)


def get_dashboard_adrs() -> dict:
    adrs = list(AdrQuote.objects.order_by("company"))
    if not adrs:
        sync_adrs_data()
        adrs = list(AdrQuote.objects.order_by("company"))

    last_update = max((item.updated_at for item in adrs), default=None)
    return {"items": adrs, "last_update": last_update}
