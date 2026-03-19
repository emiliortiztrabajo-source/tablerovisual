from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from dashboard.models import InflationData


@dataclass(frozen=True)
class InflationPayload:
    month_label: str
    monthly_value: Decimal
    year_over_year: Decimal
    period_date: date


def fetch_inflation_series(use_mock: bool = True) -> list[InflationPayload]:
    if not use_mock:
        raise NotImplementedError("Conecta aqui tu API real de IPC.")

    raw_data = [
        ("Abr 2025", "8.8", "287.9", date(2025, 4, 1)),
        ("May 2025", "4.2", "276.4", date(2025, 5, 1)),
        ("Jun 2025", "4.6", "271.5", date(2025, 6, 1)),
        ("Jul 2025", "4.0", "263.4", date(2025, 7, 1)),
        ("Ago 2025", "4.1", "254.1", date(2025, 8, 1)),
        ("Sep 2025", "3.7", "246.8", date(2025, 9, 1)),
        ("Oct 2025", "3.2", "235.4", date(2025, 10, 1)),
        ("Nov 2025", "2.9", "226.7", date(2025, 11, 1)),
        ("Dic 2025", "2.7", "214.2", date(2025, 12, 1)),
        ("Ene 2026", "2.5", "198.6", date(2026, 1, 1)),
        ("Feb 2026", "2.2", "184.3", date(2026, 2, 1)),
        ("Mar 2026", "2.1", "171.8", date(2026, 3, 1)),
    ]
    return [
        InflationPayload(
            month_label=label,
            monthly_value=Decimal(monthly_value),
            year_over_year=Decimal(year_over_year),
            period_date=period_date,
        )
        for label, monthly_value, year_over_year, period_date in raw_data
    ]


def sync_inflation_data(use_mock: bool = True) -> int:
    payloads = fetch_inflation_series(use_mock=use_mock)
    for payload in payloads:
        InflationData.objects.update_or_create(
            date=payload.period_date,
            defaults={
                "value": payload.monthly_value,
                "month_label": payload.month_label,
                "year_over_year": payload.year_over_year,
            },
        )
    return len(payloads)


def get_dashboard_inflation_data() -> dict:
    series = list(InflationData.objects.order_by("date"))
    if not series:
        sync_inflation_data()
        series = list(InflationData.objects.order_by("date"))

    last_entry = series[-1]
    return {
        "latest_month": last_entry.value,
        "year_over_year": last_entry.year_over_year,
        "chart_labels": [item.month_label for item in series],
        "chart_values": [float(item.value) for item in series],
        "last_update": last_entry.updated_at,
    }
