from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import requests
from django.conf import settings
from django.utils import timezone


logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
ACCIONES_JSON_PATH = DATA_DIR / "acciones_internacionales.json"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
REQUEST_TIMEOUT_SECONDS = 20

INTERNATIONAL_STOCKS = [
    {"nombre": "Apple", "ticker": "AAPL"},
    {"nombre": "Microsoft", "ticker": "MSFT"},
    {"nombre": "Alphabet", "ticker": "GOOGL"},
    {"nombre": "Amazon", "ticker": "AMZN"},
    {"nombre": "Tesla", "ticker": "TSLA"},
    {"nombre": "Nvidia", "ticker": "NVDA"},
    {"nombre": "Meta", "ticker": "META"},
]


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _parse_market_datetime(timestamp: int | None) -> datetime | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.get_current_timezone())


def _normalize_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _format_stock_item(
    stock_config: dict[str, str],
    meta: dict[str, Any],
    fetched_at: datetime,
) -> dict[str, Any]:
    market_datetime = _parse_market_datetime(meta.get("regularMarketTime"))
    regular_market_price = _normalize_float(meta.get("regularMarketPrice"))
    previous_close = _normalize_float(meta.get("chartPreviousClose"))

    variation_percentage = None
    if regular_market_price is not None and previous_close not in (None, 0):
        variation_percentage = round(
            ((regular_market_price - previous_close) / previous_close) * 100,
            2,
        )

    return {
        "nombre": stock_config["nombre"],
        "ticker": stock_config["ticker"],
        "precio_actual": regular_market_price,
        "variacion_porcentual": variation_percentage,
        "moneda": meta.get("currency") or "USD",
        "fecha_dato": market_datetime.date().isoformat() if market_datetime else None,
        "fecha_scraping": fetched_at.isoformat(),
        "tipo_activo": "accion_internacional",
    }


def _empty_stock_item(stock_config: dict[str, str], fetched_at: datetime) -> dict[str, Any]:
    return {
        "nombre": stock_config["nombre"],
        "ticker": stock_config["ticker"],
        "precio_actual": None,
        "variacion_porcentual": None,
        "moneda": "USD",
        "fecha_dato": None,
        "fecha_scraping": fetched_at.isoformat(),
        "tipo_activo": "accion_internacional",
    }


def _fetch_stock_item(
    stock_config: dict[str, str],
    fetched_at: datetime,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    request_callable = session.get if session is not None else requests.get
    response = request_callable(
        YAHOO_CHART_URL.format(ticker=stock_config["ticker"]),
        params={"interval": "1d", "range": "5d", "includePrePost": "false"},
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            )
        },
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("chart", {}).get("result") or []
    if not result:
        raise ValueError(f"No se obtuvo respuesta válida para {stock_config['ticker']}.")

    meta = result[0].get("meta") or {}
    return _format_stock_item(stock_config, meta, fetched_at)


def read_acciones_internacionales_json() -> dict[str, Any] | None:
    if not ACCIONES_JSON_PATH.exists():
        return None
    try:
        with ACCIONES_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (OSError, json.JSONDecodeError):
        return None


def write_acciones_internacionales_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with ACCIONES_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def update_acciones_internacionales_json() -> dict[str, Any]:
    fetched_at = timezone.localtime().replace(microsecond=0)
    cached_payload = read_acciones_internacionales_json()
    cached_by_ticker = {
        item.get("ticker"): item for item in (cached_payload or {}).get("acciones", [])
    }

    acciones: list[dict[str, Any]] = []
    live_count = 0
    used_partial_fallback = False
    errors: list[str] = []

    with requests.Session() as session:
        for stock_config in INTERNATIONAL_STOCKS:
            try:
                acciones.append(_fetch_stock_item(stock_config, fetched_at, session))
                live_count += 1
            except Exception as error:
                logger.warning(
                    "Acciones internacionales: fallo al obtener %s",
                    stock_config["ticker"],
                )
                errors.append(f"{stock_config['ticker']}: {error}")
                cached_item = cached_by_ticker.get(stock_config["ticker"])
                if cached_item is not None:
                    acciones.append(cached_item)
                    used_partial_fallback = True
                else:
                    acciones.append(_empty_stock_item(stock_config, fetched_at))
                    used_partial_fallback = True

    if live_count == 0 and cached_payload is not None:
        logger.warning("Acciones internacionales: usando fallback JSON completo")
        return {
            "payload": cached_payload,
            "processed_count": len(cached_payload.get("acciones", [])),
            "source": cached_payload.get("fuente", "Yahoo Finance chart"),
            "fallback_mode": "full",
            "used_fallback": True,
            "errors": errors,
        }

    if live_count == 0:
        logger.warning("Acciones internacionales: sin datos disponibles y sin cache previo")
        return {
            "payload": {
                "ultima_actualizacion": fetched_at.isoformat(),
                "fuente": "Yahoo Finance chart",
                "acciones": [],
            },
            "processed_count": 0,
            "source": "Yahoo Finance chart",
            "fallback_mode": "empty",
            "used_fallback": True,
            "errors": errors,
        }

    payload = {
        "ultima_actualizacion": fetched_at.isoformat(),
        "fuente": "Yahoo Finance chart",
        "acciones": acciones,
    }
    write_acciones_internacionales_json(payload)
    return {
        "payload": payload,
        "processed_count": len(acciones),
        "source": payload["fuente"],
        "fallback_mode": "partial" if used_partial_fallback else "none",
        "used_fallback": used_partial_fallback,
        "errors": errors,
    }


def get_dashboard_acciones_internacionales() -> dict[str, Any]:
    cached_payload = read_acciones_internacionales_json()
    if cached_payload is None:
        return {
            "items": [],
            "last_update": None,
            "source": None,
        }

    parsed_last_update = None
    raw_last_update = cached_payload.get("ultima_actualizacion")
    if raw_last_update:
        try:
            parsed_last_update = datetime.fromisoformat(raw_last_update)
        except ValueError:
            parsed_last_update = None

    normalized_items: list[dict[str, Any]] = []
    for item in cached_payload.get("acciones", []):
        enriched_item = dict(item)
        variation = enriched_item.get("variacion_porcentual")
        enriched_item["is_positive"] = variation is not None and variation >= 0
        enriched_item["is_negative"] = variation is not None and variation < 0
        normalized_items.append(enriched_item)

    return {
        "items": normalized_items,
        "last_update": parsed_last_update,
        "source": cached_payload.get("fuente"),
    }
