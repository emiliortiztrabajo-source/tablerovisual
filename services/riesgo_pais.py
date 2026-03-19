from __future__ import annotations

import importlib.util
import json
import logging
import re
from datetime import datetime
from typing import Any

import requests
import urllib3
from bs4 import BeautifulSoup
from django.conf import settings
from django.utils import timezone


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
RIESGO_PAIS_JSON_PATH = DATA_DIR / "riesgo_pais.json"
RIESGO_PAIS_PAGE_URL = "https://www.ambito.com/contenidos/riesgo-pais.html"
DEFAULT_MARKETS_BASE_URL = "https://mercados.ambito.com/"
DEFAULT_INDEX_PATH = "/riesgopais"
REQUEST_TIMEOUT_SECONDS = 25


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


def _normalize_number(raw_value: Any) -> float | None:
    if raw_value in (None, ""):
        return None

    text = str(raw_value).strip()
    text = text.replace("%", "").replace("+", "").replace("\xa0", " ")
    text = re.sub(r"\s+", "", text)

    if not text:
        return None

    negative = text.startswith("-")
    text = text.lstrip("-")

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
        value = float(text)
    except ValueError:
        return None

    if negative:
        value *= -1

    return round(value, 2)


def _normalize_integer_like(value: float | None) -> int | float | None:
    if value is None:
        return None
    return int(value) if float(value).is_integer() else value


def _parse_date(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None

    text = raw_value.strip()
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y"):
        try:
            parsed = datetime.strptime(text, fmt)
            return timezone.make_aware(parsed, timezone.get_current_timezone())
        except ValueError:
            continue
    return None


def _build_endpoint(base_url: str, index_path: str, endpoint_path: str) -> str:
    return f"{base_url.rstrip('/')}/{index_path.strip('/')}/{endpoint_path.strip('/')}"


def _discover_endpoints(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    endpoints: dict[str, str] = {}

    for block in soup.select(".indicador[data-url][data-indice][data-ep]"):
        endpoint_path = block.get("data-ep")
        base_url = block.get("data-url") or DEFAULT_MARKETS_BASE_URL
        index_path = block.get("data-indice") or DEFAULT_INDEX_PATH
        if not endpoint_path:
            continue

        key = endpoint_path.strip("/").replace("-", "_")
        endpoints[key] = _build_endpoint(base_url, index_path, endpoint_path)

    endpoints.setdefault(
        "variacion_ultimo",
        _build_endpoint(DEFAULT_MARKETS_BASE_URL, DEFAULT_INDEX_PATH, "/variacion-ultimo"),
    )
    endpoints.setdefault(
        "jornada",
        _build_endpoint(DEFAULT_MARKETS_BASE_URL, DEFAULT_INDEX_PATH, "/jornada"),
    )
    endpoints.setdefault(
        "historico",
        _build_endpoint(DEFAULT_MARKETS_BASE_URL, DEFAULT_INDEX_PATH, "/historico"),
    )
    return endpoints


def _fetch_page_html_with_requests() -> tuple[str, str]:
    response = _request(RIESGO_PAIS_PAGE_URL)
    return response.text, "requests_page"


def _fetch_page_html_with_browser() -> tuple[str, str] | None:
    if importlib.util.find_spec("playwright") is not None:
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(RIESGO_PAIS_PAGE_URL, wait_until="networkidle", timeout=30000)
                html = page.content()
                browser.close()
            return html, "browser_page"
        except Exception:
            logger.warning("Riesgo Pais: fallo fallback con Playwright")

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
                driver.get(RIESGO_PAIS_PAGE_URL)
                html = driver.page_source
            finally:
                driver.quit()
            return html, "browser_page"
        except Exception:
            logger.warning("Riesgo Pais: fallo fallback con Selenium")

    return None


def _request_json(url: str) -> dict[str, Any]:
    response = _request(url)
    return response.json()


def read_riesgo_pais_json() -> dict[str, Any] | None:
    if not RIESGO_PAIS_JSON_PATH.exists():
        return None
    try:
        with RIESGO_PAIS_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (OSError, json.JSONDecodeError):
        return None


def write_riesgo_pais_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with RIESGO_PAIS_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def _empty_payload() -> dict[str, Any]:
    scraping_time = timezone.localtime().replace(microsecond=0).isoformat()
    return {
        "ultima_actualizacion": scraping_time,
        "fuente": RIESGO_PAIS_PAGE_URL,
        "riesgo_pais": {
            "valor": None,
            "variacion": None,
            "cierre_anterior": None,
            "fecha_scraping": scraping_time,
        },
    }


def scrape_riesgo_pais() -> dict[str, Any]:
    html, method = _fetch_page_html_with_requests()
    endpoints = _discover_endpoints(html)
    if "variacion_ultimo" not in endpoints or "jornada" not in endpoints:
        browser_result = _fetch_page_html_with_browser()
        if browser_result is not None:
            html, method = browser_result
            endpoints = _discover_endpoints(html)

    variation_payload = _request_json(endpoints["variacion_ultimo"])
    jornada_payload = _request_json(endpoints["jornada"])

    valor = _normalize_number(variation_payload.get("ultimo"))
    variacion = _normalize_number(variation_payload.get("variacion"))
    cierre_anterior = _normalize_number(jornada_payload.get("valor"))
    variacion_puntos = _normalize_number(jornada_payload.get("varpesos"))

    if cierre_anterior is None and valor is not None and variacion_puntos is not None:
        cierre_anterior = valor - variacion_puntos

    source_timestamp = _parse_date(jornada_payload.get("fecha")) or _parse_date(
        variation_payload.get("fecha")
    )
    scraping_time = timezone.localtime().replace(microsecond=0)

    payload = {
        "ultima_actualizacion": scraping_time.isoformat(),
        "fuente": RIESGO_PAIS_PAGE_URL,
        "riesgo_pais": {
            "valor": _normalize_integer_like(valor),
            "variacion": _normalize_integer_like(variacion),
            "cierre_anterior": _normalize_integer_like(cierre_anterior),
            "fecha_scraping": (source_timestamp or scraping_time).isoformat(),
        },
    }

    return {
        "payload": payload,
        "method": f"{method}_plus_market_json",
    }


def update_riesgo_pais_json() -> dict[str, Any]:
    try:
        result = scrape_riesgo_pais()
        write_riesgo_pais_json(result["payload"])
        logger.info("Riesgo Pais: scraping actualizado")
        return result
    except Exception as error:
        logger.warning("Riesgo Pais: usando fallback JSON")
        cached = read_riesgo_pais_json()
        if cached is not None:
            return {
                "payload": cached,
                "method": "fallback_json",
                "error": str(error),
            }
        return {
            "payload": _empty_payload(),
            "method": "empty_fallback",
            "error": str(error),
        }


def get_dashboard_riesgo_pais() -> dict[str, Any]:
    cached = read_riesgo_pais_json()
    if cached is None:
        cached = update_riesgo_pais_json()["payload"]

    parsed_last_update = None
    raw_last_update = cached.get("ultima_actualizacion")
    if raw_last_update:
        try:
            parsed_last_update = datetime.fromisoformat(raw_last_update)
        except ValueError:
            parsed_last_update = None

    riesgo_pais = cached.get("riesgo_pais", {})
    return {
        "valor": riesgo_pais.get("valor"),
        "variacion": riesgo_pais.get("variacion"),
        "cierre_anterior": riesgo_pais.get("cierre_anterior"),
        "fecha_scraping": riesgo_pais.get("fecha_scraping"),
        "last_update": parsed_last_update,
        "source": cached.get("fuente", RIESGO_PAIS_PAGE_URL),
    }
