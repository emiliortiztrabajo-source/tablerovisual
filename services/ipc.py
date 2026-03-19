from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import requests
import urllib3
from django.conf import settings
from django.utils import timezone

from dashboard.models import InflationData


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
IPC_HISTORICO_JSON_PATH = DATA_DIR / "ipc_historico.json"
IPC_SERIES_API_URL = "https://apis.datos.gob.ar/series/api/series/"
IPC_SOURCE_LABEL = "Datos Argentina / INDEC"
REQUEST_TIMEOUT_SECONDS = 25
IPC_INDEX_SERIES_ID = "145.3_INGNACNAL_DICI_M_15"
IPC_MONTHLY_VARIATION_SERIES_ID = "145.3_INGNACUAL_DICI_M_38"
IPC_MONTHS_TO_KEEP = 12

MONTH_LABELS = {
    1: "Ene",
    2: "Feb",
    3: "Mar",
    4: "Abr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Ago",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dic",
}


@dataclass(frozen=True)
class InflationPayload:
    month_label: str
    monthly_value: Decimal
    year_over_year: Decimal
    period_date: date


def _request(url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", REQUEST_TIMEOUT_SECONDS)
    kwargs.setdefault(
        "headers",
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            )
        },
    )
    try:
        response = requests.get(url, **kwargs)
        response.encoding = response.apparent_encoding or "utf-8"
        response.raise_for_status()
        return response
    except requests.exceptions.SSLError:
        response = requests.get(url, verify=False, **kwargs)
        response.encoding = response.apparent_encoding or "utf-8"
        response.raise_for_status()
        return response


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _month_label(period_date: date) -> str:
    return f"{MONTH_LABELS[period_date.month]} {period_date.year}"


def _round_percentage(value: float | Decimal | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _empty_payload() -> dict[str, Any]:
    return {
        "ultima_actualizacion": timezone.localtime().replace(microsecond=0).isoformat(),
        "fuente": IPC_SOURCE_LABEL,
        "fuente_url": IPC_SERIES_API_URL,
        "serie": [],
        "year_over_year": None,
    }


def read_ipc_historico_json() -> dict[str, Any] | None:
    if not IPC_HISTORICO_JSON_PATH.exists():
        return None
    try:
        with IPC_HISTORICO_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (OSError, json.JSONDecodeError):
        return None


def write_ipc_historico_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with IPC_HISTORICO_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def _parse_series_rows(data: list[list[Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in data:
        if len(item) < 3:
            continue

        raw_date, raw_index, raw_monthly = item[0], item[1], item[2]
        if not raw_date or raw_index is None or raw_monthly is None:
            continue

        period_date = datetime.strptime(str(raw_date), "%Y-%m-%d").date()
        rows.append(
            {
                "period_date": period_date,
                "index_value": float(raw_index),
                "monthly_value": round(float(raw_monthly) * 100, 2),
            }
        )

    rows.sort(key=lambda item: item["period_date"])
    return rows


def _build_historical_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if len(rows) < IPC_MONTHS_TO_KEEP + 12:
        raise RuntimeError("La serie oficial de IPC no tiene suficientes observaciones para calcular 12 meses.")

    selected_rows = rows[-IPC_MONTHS_TO_KEEP:]
    payload_rows: list[dict[str, Any]] = []
    indexes_by_date = {row["period_date"]: position for position, row in enumerate(rows)}

    for selected_row in selected_rows:
        current_index = indexes_by_date[selected_row["period_date"]]
        previous_year_row = rows[current_index - 12]
        year_over_year = ((selected_row["index_value"] / previous_year_row["index_value"]) - 1) * 100

        payload_rows.append(
            {
                "periodo": selected_row["period_date"].strftime("%Y-%m"),
                "label": _month_label(selected_row["period_date"]),
                "valor_mensual": _round_percentage(selected_row["monthly_value"]),
                "variacion_interanual": _round_percentage(year_over_year),
            }
        )

    return {
        "ultima_actualizacion": timezone.localtime().replace(microsecond=0).isoformat(),
        "fuente": IPC_SOURCE_LABEL,
        "fuente_url": (
            f"{IPC_SERIES_API_URL}?ids={IPC_INDEX_SERIES_ID},{IPC_MONTHLY_VARIATION_SERIES_ID}"
        ),
        "serie": payload_rows,
        "year_over_year": payload_rows[-1]["variacion_interanual"] if payload_rows else None,
    }


def fetch_inflation_series() -> list[InflationPayload]:
    response = _request(
        IPC_SERIES_API_URL,
        params={
            "ids": f"{IPC_INDEX_SERIES_ID},{IPC_MONTHLY_VARIATION_SERIES_ID}",
            "format": "json",
        },
    )
    payload = response.json()
    rows = _parse_series_rows(payload.get("data", []))
    historical_payload = _build_historical_payload(rows)

    items: list[InflationPayload] = []
    for item in historical_payload["serie"]:
        period_date = datetime.strptime(f"{item['periodo']}-01", "%Y-%m-%d").date()
        items.append(
            InflationPayload(
                month_label=str(item["label"]),
                monthly_value=Decimal(str(item["valor_mensual"])),
                year_over_year=Decimal(str(item["variacion_interanual"])),
                period_date=period_date,
            )
        )
    return items


def update_ipc_historico_json() -> dict[str, Any]:
    try:
        response = _request(
            IPC_SERIES_API_URL,
            params={
                "ids": f"{IPC_INDEX_SERIES_ID},{IPC_MONTHLY_VARIATION_SERIES_ID}",
                "format": "json",
            },
        )
        api_payload = response.json()
        rows = _parse_series_rows(api_payload.get("data", []))
        payload = _build_historical_payload(rows)
        write_ipc_historico_json(payload)
        logger.info("IPC historico: serie oficial actualizada")
        return {
            "payload": payload,
            "series_count": len(payload["serie"]),
            "method": "series_api",
        }
    except Exception as error:
        logger.warning("IPC historico: usando fallback JSON")
        cached = read_ipc_historico_json()
        if cached is not None:
            return {
                "payload": cached,
                "series_count": len(cached.get("serie", [])),
                "method": "fallback_json",
                "error": str(error),
            }
        return {
            "payload": _empty_payload(),
            "series_count": 0,
            "method": "empty_fallback",
            "error": str(error),
        }


def sync_inflation_payload(payload: dict[str, Any]) -> int:
    valid_dates: set[date] = set()
    for item in payload.get("serie", []):
        period_date = datetime.strptime(f"{item['periodo']}-01", "%Y-%m-%d").date()
        valid_dates.add(period_date)
        InflationData.objects.update_or_create(
            date=period_date,
            defaults={
                "value": Decimal(str(item["valor_mensual"])),
                "month_label": str(item["label"]),
                "year_over_year": Decimal(str(item.get("variacion_interanual") or 0)),
            },
        )

    if valid_dates:
        InflationData.objects.exclude(date__in=valid_dates).delete()

    return len(valid_dates)


def sync_inflation_data() -> int:
    result = update_ipc_historico_json()
    return sync_inflation_payload(result["payload"])


def get_dashboard_inflation_data() -> dict[str, Any]:
    cached = read_ipc_historico_json()
    if cached is None:
        cached = update_ipc_historico_json()["payload"]

    parsed_last_update = None
    raw_last_update = cached.get("ultima_actualizacion")
    if raw_last_update:
        try:
            parsed_last_update = datetime.fromisoformat(raw_last_update)
        except ValueError:
            parsed_last_update = None

    series = cached.get("serie", [])
    latest_month = series[-1]["valor_mensual"] if series else None
    year_over_year = cached.get("year_over_year")
    if year_over_year is None and series:
        year_over_year = series[-1].get("variacion_interanual")

    return {
        "latest_month": latest_month,
        "year_over_year": year_over_year,
        "chart_labels": [item.get("label") for item in series],
        "chart_values": [item.get("valor_mensual") for item in series],
        "last_update": parsed_last_update,
        "source": cached.get("fuente", IPC_SOURCE_LABEL),
    }
