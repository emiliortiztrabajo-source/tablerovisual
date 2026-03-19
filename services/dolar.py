from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from django.conf import settings
from django.utils import timezone

from dashboard.models import DollarQuote


BNA_PERSONAS_URL = "https://www.bna.com.ar/Personas"
DATA_DIR = settings.BASE_DIR / "data"
DOLLAR_JSON_PATH = DATA_DIR / "dolar_blue.json"
REQUEST_TIMEOUT_SECONDS = 15


@dataclass(frozen=True)
class DollarPayload:
    buy_value: Decimal
    sell_value: Decimal
    external_updated_at: datetime
    source: str


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _decimal_from_bna(value: str) -> Decimal:
    normalized = value.replace(".", "").replace(",", ".").strip()
    return Decimal(normalized)


def _serialize_payload(payload: DollarPayload) -> dict:
    return {
        "compra": float(payload.buy_value),
        "venta": float(payload.sell_value),
        "ultima_actualizacion": payload.external_updated_at.isoformat(),
    }


def _deserialize_payload(data: dict, source: str) -> DollarPayload:
    raw_timestamp = data.get("ultima_actualizacion")
    if not raw_timestamp:
        raise ValueError("El JSON local del dolar no contiene una fecha válida.")
    parsed_timestamp = datetime.fromisoformat(raw_timestamp)
    if timezone.is_naive(parsed_timestamp):
        parsed_timestamp = timezone.make_aware(
            parsed_timestamp,
            timezone.get_current_timezone(),
        )

    return DollarPayload(
        buy_value=Decimal(str(data.get("compra", 0))),
        sell_value=Decimal(str(data.get("venta", 0))),
        external_updated_at=parsed_timestamp,
        source=source,
    )


def read_dollar_json() -> DollarPayload | None:
    if not DOLLAR_JSON_PATH.exists():
        return None

    try:
        with DOLLAR_JSON_PATH.open("r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        return _deserialize_payload(data, source="json_fallback")
    except (json.JSONDecodeError, ValueError, TypeError):
        return None


def write_dollar_json(payload: DollarPayload) -> None:
    _ensure_data_dir()
    with DOLLAR_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(_serialize_payload(payload), json_file, ensure_ascii=False, indent=2)


def extract_bna_quote_from_html(html: str) -> DollarPayload:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    compact_text = re.sub(r"\s+", " ", text)

    quote_match = re.search(
        r"Cotizaci[oó]n Billetes.*?Dolar U\.S\.A\s+([\d.,]+)\s+([\d.,]+).*?Hora Actualizaci[oó]n:\s*([\d:]+)",
        compact_text,
        flags=re.IGNORECASE,
    )
    if quote_match is None:
        raise ValueError("No se pudo localizar la cotización de Dolar U.S.A en BNA Personas.")

    buy_value = _decimal_from_bna(quote_match.group(1))
    sell_value = _decimal_from_bna(quote_match.group(2))
    time_label = quote_match.group(3)

    today = timezone.localdate()
    parsed_time = datetime.strptime(time_label, "%H:%M").time()
    external_updated_at = timezone.make_aware(
        datetime.combine(today, parsed_time),
        timezone.get_current_timezone(),
    )

    return DollarPayload(
        buy_value=buy_value,
        sell_value=sell_value,
        external_updated_at=external_updated_at,
        source="bna_scraping",
    )


def fetch_dollar_quote_requests() -> DollarPayload:
    response = requests.get(
        BNA_PERSONAS_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            )
        },
    )
    response.raise_for_status()
    return extract_bna_quote_from_html(response.text)


def fetch_dollar_quote_browser() -> DollarPayload:
    """
    Fallback preparado para contenido dinámico.

    Implementación sugerida:
    - Selenium: abrir la página, esperar el bloque de cotizaciones y leer el texto renderizado.
    - Playwright: navegar a BNA Personas, esperar al selector de cotizaciones y parsear el DOM final.

    Este proyecto no fuerza esas dependencias todavía para no romper el arranque local.
    """
    raise NotImplementedError(
        "Fallback browser pendiente. Puedes implementarlo con Selenium o Playwright si BNA cambia a contenido dinámico."
    )


def fetch_dollar_quote() -> DollarPayload:
    try:
        payload = fetch_dollar_quote_requests()
        write_dollar_json(payload)
        return payload
    except Exception:
        json_payload = read_dollar_json()
        if json_payload is not None:
            return json_payload

        try:
            payload = fetch_dollar_quote_browser()
            write_dollar_json(payload)
            return payload
        except Exception as browser_error:
            raise RuntimeError(
                "No se pudo obtener la cotización desde BNA ni desde el fallback local."
            ) from browser_error


def refresh_dollar_json() -> DollarPayload:
    payload = fetch_dollar_quote_requests()
    write_dollar_json(payload)
    return payload


def _calculate_variation(current_quote: DollarQuote) -> Decimal:
    previous_quote = (
        DollarQuote.objects.exclude(pk=current_quote.pk)
        .order_by("-external_updated_at", "-updated_at")
        .first()
    )
    if previous_quote is None or previous_quote.sell_value == 0:
        return Decimal("0.00")

    variation = ((current_quote.sell_value - previous_quote.sell_value) / previous_quote.sell_value) * Decimal("100")
    return variation.quantize(Decimal("0.01"))


def sync_dollar_quote() -> DollarQuote:
    payload = fetch_dollar_quote()
    quote, _ = DollarQuote.objects.update_or_create(
        date=payload.external_updated_at.date(),
        defaults={
            "value": payload.sell_value,
            "buy_value": payload.buy_value,
            "sell_value": payload.sell_value,
            "variation_daily": Decimal("0.00"),
            "source": payload.source,
            "external_updated_at": payload.external_updated_at,
        },
    )
    quote.variation_daily = _calculate_variation(quote)
    quote.save(update_fields=["variation_daily", "updated_at"])
    return quote


def get_dashboard_dollar_data() -> dict:
    payload = read_dollar_json()

    if payload is None:
        quote = sync_dollar_quote()
        return {
            "buy_value": quote.buy_value,
            "sell_value": quote.sell_value,
            "variation_daily": quote.variation_daily,
            "last_update": quote.external_updated_at,
            "source": quote.source,
        }

    latest_quote = DollarQuote.objects.order_by("-external_updated_at", "-updated_at").first()
    source = payload.source
    variation_daily = Decimal("0.00")
    if latest_quote is not None and latest_quote.external_updated_at == payload.external_updated_at:
        source = latest_quote.source
        variation_daily = latest_quote.variation_daily

    return {
        "buy_value": payload.buy_value,
        "sell_value": payload.sell_value,
        "variation_daily": variation_daily,
        "last_update": payload.external_updated_at,
        "source": source,
    }
