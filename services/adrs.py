from __future__ import annotations

import importlib.util
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import requests
import urllib3
from bs4 import BeautifulSoup
from django.conf import settings
from django.utils import timezone

from dashboard.models import AdrQuote


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

IOL_ADRS_URL = "https://iol.invertironline.com/mercado/cotizaciones/argentina"
DATA_DIR = settings.BASE_DIR / "data"
ADRS_JSON_PATH = DATA_DIR / "adrs.json"
REQUEST_TIMEOUT_SECONDS = 25

PRIMARY_ADR_TARGETS = [
    {"ticker": "YPF", "preferred_symbols": ["YPFDD", "YPFD"]},
    {"ticker": "GGAL", "preferred_symbols": ["GGALD", "GGAL"]},
    {"ticker": "BMA", "preferred_symbols": ["BMA.D", "BMA"]},
    {"ticker": "SUPV", "preferred_symbols": ["SUPVD", "SUPV"]},
    {"ticker": "BBAR", "preferred_symbols": ["BBARD", "BBAR"]},
    {"ticker": "PAM", "preferred_symbols": ["PAMPD", "PAMP"]},
    {"ticker": "TGS", "preferred_symbols": ["TGSUD", "TGSU2"]},
    {"ticker": "TEO", "preferred_symbols": ["TECOD", "TECO2"]},
    {"ticker": "CRESY", "preferred_symbols": ["CRESD", "CRES"]},
    {"ticker": "IRS", "preferred_symbols": ["IRSAD"]},
]


@dataclass(frozen=True)
class AdrPayload:
    company: str
    local_ticker: str
    usa_ticker: str
    price: Decimal
    daily_change: Decimal
    period_date: date
    quote_updated_at: datetime
    currency: str
    opening_price: Decimal | None = None
    previous_close: Decimal | None = None
    traded_amount: Decimal | None = None
    source: str = IOL_ADRS_URL


@dataclass(frozen=True)
class DashboardAdrItem:
    company: str
    local_ticker: str
    usa_ticker: str
    value: Decimal
    daily_change: Decimal
    currency: str
    currency_symbol: str


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


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_number(value: str | None) -> Decimal | None:
    text = _clean_text(value)
    if not text or text == "-":
        return None

    text = text.replace("US$", "").replace("$", "").replace("%", "").replace(" ", "")

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", "")

    try:
        return Decimal(text)
    except Exception:
        return None


def _currency_from_symbol(symbol: str) -> str:
    normalized = symbol.upper()
    if normalized in {
        "YPFDD",
        "GGALD",
        "BMA.D",
        "SUPVD",
        "BBARD",
        "PAMPD",
        "TGSUD",
        "TECOD",
        "CRESD",
        "IRSAD",
    }:
        return "USD"
    return "ARS"


def _currency_symbol(currency: str) -> str:
    return "US$" if currency == "USD" else "$"


def _extract_table_rows(html: str) -> tuple[dict[str, dict[str, Any]], int]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.cotizacionestb")
    if table is None:
        raise ValueError("No se encontro la tabla de cotizaciones de IOL.")

    rows_by_symbol: dict[str, dict[str, Any]] = {}
    parsed_rows = 0

    for row in table.find_all("tr"):
        symbol_link = row.select_one('td[data-field="IDTitulo"] a[data-symbol]')
        if symbol_link is None:
            continue

        cells = row.find_all("td")
        if len(cells) < 11:
            continue

        symbol = _clean_text(symbol_link.get("data-symbol")).upper()
        company_span = symbol_link.select_one("span")
        company = _clean_text(
            symbol_link.get("title")
            or (company_span.get_text(" ", strip=True) if company_span is not None else symbol)
        )

        # IOL expone la mayor parte de la tabla con data-field estables; usamos
        # esos atributos y dejamos una caida a indices solo donde hace falta.
        price_cell = row.select_one('td[data-field="UltimoPrecio"]')
        variation_cell = row.select_one('td[data-field="Variacion"]')
        opening_cell = row.select_one('td[data-field="Apertura"]')
        previous_close_cell = row.select_one('td[data-field="UltimoCierre"]')
        traded_amount_cell = row.select_one('td[data-field="MontoOperado"]')

        rows_by_symbol[symbol] = {
            "empresa": company,
            "ticker_local": symbol,
            "precio_actual": _normalize_number(
                price_cell.get_text(" ", strip=True) if price_cell is not None else cells[1].get_text(" ", strip=True)
            ),
            "variacion_diaria": _normalize_number(
                variation_cell.get_text(" ", strip=True)
                if variation_cell is not None
                else cells[2].get_text(" ", strip=True)
            ),
            "apertura": _normalize_number(
                opening_cell.get_text(" ", strip=True)
                if opening_cell is not None
                else cells[7].get_text(" ", strip=True)
            ),
            "cierre_anterior": _normalize_number(
                previous_close_cell.get_text(" ", strip=True)
                if previous_close_cell is not None
                else cells[10].get_text(" ", strip=True)
            ),
            "monto_operado": _normalize_number(
                traded_amount_cell.get_text(" ", strip=True)
                if traded_amount_cell is not None
                else cells[11].get_text(" ", strip=True)
            )
            if len(cells) > 11 or traded_amount_cell is not None
            else None,
            "moneda": _currency_from_symbol(symbol),
        }
        parsed_rows += 1

    return rows_by_symbol, parsed_rows


def _fetch_iol_html_with_requests() -> tuple[str, str]:
    response = _request(IOL_ADRS_URL)
    return response.text, "requests_html"


def _fetch_iol_html_with_browser() -> tuple[str, str] | None:
    if importlib.util.find_spec("playwright") is not None:
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(IOL_ADRS_URL, wait_until="networkidle", timeout=30000)
                html = page.content()
                browser.close()
            return html, "browser_fallback"
        except Exception:
            logger.warning("ADRs IOL: fallo fallback con Playwright")

    if importlib.util.find_spec("selenium") is not None:
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options

            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            driver = webdriver.Chrome(options=options)
            try:
                driver.get(IOL_ADRS_URL)
                html = driver.page_source
            finally:
                driver.quit()
            return html, "browser_fallback"
        except Exception:
            logger.warning("ADRs IOL: fallo fallback con Selenium")

    return None


def _build_selected_payload(
    rows_by_symbol: dict[str, dict[str, Any]],
    scraping_time: datetime,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for target in PRIMARY_ADR_TARGETS:
        selected_row = None
        # Priorizamos las especies dolarizadas cuando existen para mantener una
        # lectura mas cercana al bloque de ADRs, con fallback a la especie local.
        for symbol in target["preferred_symbols"]:
            selected_row = rows_by_symbol.get(symbol.upper())
            if selected_row is not None:
                break

        if selected_row is None:
            continue

        item = {
            "empresa": selected_row["empresa"],
            "ticker": target["ticker"],
            "ticker_local": selected_row["ticker_local"],
            "precio_actual": float(selected_row["precio_actual"])
            if selected_row["precio_actual"] is not None
            else None,
            "variacion_diaria": float(selected_row["variacion_diaria"])
            if selected_row["variacion_diaria"] is not None
            else None,
            "moneda": selected_row["moneda"],
            "fecha_actualizacion": scraping_time.isoformat(),
            "fuente": IOL_ADRS_URL,
            "apertura": float(selected_row["apertura"])
            if selected_row["apertura"] is not None
            else None,
            "cierre_anterior": float(selected_row["cierre_anterior"])
            if selected_row["cierre_anterior"] is not None
            else None,
            "monto_operado": float(selected_row["monto_operado"])
            if selected_row["monto_operado"] is not None
            else None,
        }
        items.append(item)
    return items


def read_adrs_json() -> dict[str, Any] | None:
    if not ADRS_JSON_PATH.exists():
        return None
    try:
        with ADRS_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (OSError, json.JSONDecodeError):
        return None


def write_adrs_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with ADRS_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def scrape_adrs() -> dict[str, Any]:
    html, method = _fetch_iol_html_with_requests()
    try:
        rows_by_symbol, found_count = _extract_table_rows(html)
    except Exception:
        browser_result = _fetch_iol_html_with_browser()
        if browser_result is None:
            raise
        html, method = browser_result
        rows_by_symbol, found_count = _extract_table_rows(html)

    scraping_time = timezone.localtime().replace(microsecond=0)
    adrs = _build_selected_payload(rows_by_symbol, scraping_time)

    payload = {
        "ultima_actualizacion": scraping_time.isoformat(),
        "fuente": IOL_ADRS_URL,
        "adrs": adrs,
    }
    return {
        "payload": payload,
        "found_count": found_count,
        "saved_count": len(adrs),
        "method": method,
    }


def update_adrs_json() -> dict[str, Any]:
    try:
        result = scrape_adrs()
        write_adrs_json(result["payload"])
        logger.info("ADRs IOL: scraping actualizado")
        return result
    except Exception as error:
        logger.warning("ADRs IOL: usando fallback JSON")
        cached = read_adrs_json()
        if cached is not None:
            return {
                "payload": cached,
                "found_count": len(cached.get("adrs", [])),
                "saved_count": len(cached.get("adrs", [])),
                "method": "fallback_json",
                "error": str(error),
            }
        return {
            "payload": {
                "ultima_actualizacion": timezone.localtime().replace(microsecond=0).isoformat(),
                "fuente": IOL_ADRS_URL,
                "adrs": [],
            },
            "found_count": 0,
            "saved_count": 0,
            "method": "empty_fallback",
            "error": str(error),
        }


def _payload_item_to_model(item: dict[str, Any]) -> AdrPayload | None:
    price = _normalize_number(str(item.get("precio_actual"))) if item.get("precio_actual") is not None else None
    daily_change = (
        _normalize_number(str(item.get("variacion_diaria")))
        if item.get("variacion_diaria") is not None
        else None
    )
    raw_update = item.get("fecha_actualizacion")
    if not raw_update or price is None or daily_change is None:
        return None

    quote_updated_at = datetime.fromisoformat(str(raw_update))
    if timezone.is_naive(quote_updated_at):
        quote_updated_at = timezone.make_aware(quote_updated_at, timezone.get_current_timezone())

    opening_price = (
        _normalize_number(str(item.get("apertura")))
        if item.get("apertura") is not None
        else None
    )
    previous_close = (
        _normalize_number(str(item.get("cierre_anterior")))
        if item.get("cierre_anterior") is not None
        else None
    )
    traded_amount = (
        _normalize_number(str(item.get("monto_operado")))
        if item.get("monto_operado") is not None
        else None
    )

    return AdrPayload(
        company=str(item.get("empresa") or item.get("ticker") or "N/D"),
        local_ticker=str(item.get("ticker_local") or item.get("ticker") or "N/D"),
        usa_ticker=str(item.get("ticker") or "N/D"),
        price=price,
        daily_change=daily_change,
        period_date=quote_updated_at.date(),
        quote_updated_at=quote_updated_at,
        currency=str(item.get("moneda") or "USD"),
        opening_price=opening_price,
        previous_close=previous_close,
        traded_amount=traded_amount,
        source=str(item.get("fuente") or IOL_ADRS_URL),
    )


def sync_adrs_payloads(items: list[dict[str, Any]]) -> int:
    payloads = [
        payload
        for payload in (
            _payload_item_to_model(item)
            for item in items
        )
        if payload is not None
    ]

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


def sync_adrs_data() -> int:
    result = update_adrs_json()
    return sync_adrs_payloads(result["payload"].get("adrs", []))


def get_dashboard_adrs() -> dict[str, Any]:
    cached = read_adrs_json()
    if cached is None:
        cached = update_adrs_json()["payload"]

    last_update = None
    raw_last_update = cached.get("ultima_actualizacion")
    if raw_last_update:
        try:
            last_update = datetime.fromisoformat(raw_last_update)
        except ValueError:
            last_update = None

    items: list[DashboardAdrItem] = []
    for item in cached.get("adrs", []):
        value = _normalize_number(str(item.get("precio_actual"))) if item.get("precio_actual") is not None else None
        daily_change = (
            _normalize_number(str(item.get("variacion_diaria")))
            if item.get("variacion_diaria") is not None
            else None
        )
        currency = str(item.get("moneda") or "USD")
        if value is None or daily_change is None:
            continue

        items.append(
            DashboardAdrItem(
                company=str(item.get("empresa") or item.get("ticker") or "N/D"),
                local_ticker=str(item.get("ticker_local") or item.get("ticker") or "N/D"),
                usa_ticker=str(item.get("ticker") or "N/D"),
                value=value,
                daily_change=daily_change,
                currency=currency,
                currency_symbol=_currency_symbol(currency),
            )
        )

    return {
        "items": items,
        "last_update": last_update,
        "source": cached.get("fuente", IOL_ADRS_URL),
    }
