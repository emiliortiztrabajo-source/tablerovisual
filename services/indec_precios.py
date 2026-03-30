from __future__ import annotations

import importlib.util
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import requests
import urllib3
from bs4 import BeautifulSoup
from django.conf import settings
from django.utils import timezone


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
INDEC_PRECIOS_JSON_PATH = DATA_DIR / "indec_precios.json"
INDEC_PRECIOS_SOURCE_URL = "https://www.indec.gob.ar/indec/web/Nivel3-Tema-3-5"
INDEC_BASE_URL = "https://www.indec.gob.ar/"
REQUEST_TIMEOUT_SECONDS = 25

CACHE_TTL = timedelta(hours=24)

INDICATOR_CONFIG = {
    "ipc": {
        "indicador": "IPC",
        "titulo": "Precios al consumidor",
        "match": "precios al consumidor",
    },
    "ipim": {
        "indicador": "IPIM",
        "titulo": "Precios mayoristas",
        "match": "precios mayoristas",
    },
}


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


def _normalize_text(value: str | None) -> str:
    return _clean_text(value).casefold()


def _normalize_percentage(value: str | None) -> float | None:
    text = _clean_text(value).replace("%", "").replace(" ", "")
    if not text:
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    if not text:
        return None
    try:
        return round(float(text), 2)
    except ValueError:
        return None


def _empty_indicator(key: str) -> dict[str, Any]:
    config = INDICATOR_CONFIG[key]
    return {
        "indicador": config["indicador"],
        "titulo": config["titulo"],
        "variacion_mensual": None,
        "periodo": None,
        "nivel": None,
    }


def _empty_payload() -> dict[str, Any]:
    return {
        "ultima_actualizacion": timezone.localtime().replace(microsecond=0).isoformat(),
        "fuente": INDEC_PRECIOS_SOURCE_URL,
        "ipc": _empty_indicator("ipc"),
        "ipim": _empty_indicator("ipim"),
    }


def _extract_partial_path(main_html: str) -> str:
    soup = BeautifulSoup(main_html, "html.parser")
    vista_carga = soup.select_one("#VistaCarga")
    if vista_carga is not None and vista_carga.get("value"):
        return vista_carga["value"].strip()

    match = re.search(r'id="VistaCarga"[^>]*value="([^"]+)"', main_html)
    if match:
        return match.group(1).strip()

    return "Nivel3/Tema/3/5"


def _fetch_partial_html_with_requests() -> tuple[str, str]:
    main_html = _request(INDEC_PRECIOS_SOURCE_URL).text
    if "Precios al consumidor" in main_html and "Precios mayoristas" in main_html:
        return main_html, INDEC_PRECIOS_SOURCE_URL

    partial_path = _extract_partial_path(main_html)
    partial_url = urljoin(INDEC_BASE_URL, partial_path.lstrip("/"))
    partial_html = _request(partial_url).text
    return partial_html, partial_url


def _fetch_partial_html_with_browser() -> tuple[str, str] | None:
    if importlib.util.find_spec("playwright") is not None:
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(INDEC_PRECIOS_SOURCE_URL, wait_until="networkidle", timeout=30000)
                html = page.content()
                browser.close()
            return html, INDEC_PRECIOS_SOURCE_URL
        except Exception:
            logger.warning("INDEC precios: fallo fallback con Playwright")

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
                driver.get(INDEC_PRECIOS_SOURCE_URL)
                html = driver.page_source
            finally:
                driver.quit()
            return html, INDEC_PRECIOS_SOURCE_URL
        except Exception:
            logger.warning("INDEC precios: fallo fallback con Selenium")

    return None


def _parse_indicator_block(block: Any, key: str) -> dict[str, Any]:
    config = INDICATOR_CONFIG[key]
    title_node = block.select_one(".font-2")
    value_node = block.select_one(".font-1")
    period_node = block.select_one(".font-33")
    level_node = block.select_one(".font-111")

    title = _clean_text(title_node.get_text(" ", strip=True) if title_node else config["titulo"])
    return {
        "indicador": config["indicador"],
        "titulo": title or config["titulo"],
        "variacion_mensual": _normalize_percentage(
            value_node.get_text(" ", strip=True) if value_node else None
        ),
        "periodo": _clean_text(period_node.get_text(" ", strip=True) if period_node else None) or None,
        "nivel": _clean_text(level_node.get_text(" ", strip=True) if level_node else None) or None,
    }


def _extract_indicators_from_html(html: str, source_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    payload = _empty_payload()
    payload["fuente"] = INDEC_PRECIOS_SOURCE_URL

    indicator_blocks = soup.select(".indicadores-inicio .col-centered")
    for block in indicator_blocks:
        title_node = block.select_one(".font-2")
        block_title = _normalize_text(title_node.get_text(" ", strip=True) if title_node else "")
        for key, config in INDICATOR_CONFIG.items():
            if config["match"] in block_title:
                payload[key] = _parse_indicator_block(block, key)

    return payload


def _has_required_indicators(payload: dict[str, Any]) -> bool:
    return (
        payload.get("ipc", {}).get("variacion_mensual") is not None
        and payload.get("ipim", {}).get("variacion_mensual") is not None
    )


def write_indec_precios_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with INDEC_PRECIOS_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def read_indec_precios_json() -> dict[str, Any] | None:
    if not INDEC_PRECIOS_JSON_PATH.exists():
        return None
    try:
        with INDEC_PRECIOS_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (OSError, json.JSONDecodeError):
        return None


def scrape_indec_precios() -> dict[str, Any]:
    html, source_url = _fetch_partial_html_with_requests()
    method = "requests_partial"
    if "Precios al consumidor" not in html or "Precios mayoristas" not in html:
        browser_result = _fetch_partial_html_with_browser()
        if browser_result is not None:
            html, source_url = browser_result
            method = "browser_fallback"

    payload = _extract_indicators_from_html(html, source_url)
    if not _has_required_indicators(payload):
        raise RuntimeError("No se pudieron detectar IPC e IPIM en la pagina de INDEC.")
    return {
        "payload": payload,
        "method": method,
        "source_url": source_url,
    }


def update_indec_precios_json() -> dict[str, Any]:
    try:
        result = scrape_indec_precios()
        write_indec_precios_json(result["payload"])
        logger.info("INDEC precios: scraping actualizado")
        return result
    except Exception as error:
        logger.warning("INDEC precios: usando fallback JSON")
        cached = read_indec_precios_json()
        if cached is not None:
            return {
                "payload": cached,
                "method": "fallback_json",
                "source_url": cached.get("fuente", INDEC_PRECIOS_SOURCE_URL),
                "error": str(error),
            }
        return {
            "payload": _empty_payload(),
            "method": "empty_fallback",
            "source_url": INDEC_PRECIOS_SOURCE_URL,
            "error": str(error),
        }


def _is_cache_stale(cached: dict | None) -> bool:
    if cached is None:
        return True
    raw = cached.get("ultima_actualizacion")
    if not raw:
        return True
    try:
        ts = datetime.fromisoformat(raw)
        if ts.tzinfo is None:
            from django.utils import timezone as tz
            ts = tz.make_aware(ts)
        from django.utils import timezone as tz
        return (tz.now() - ts) >= CACHE_TTL
    except (ValueError, TypeError):
        return True


def get_dashboard_indec_precios() -> dict[str, Any]:
    cached = read_indec_precios_json()
    if _is_cache_stale(cached):
        cached = update_indec_precios_json()["payload"]

    parsed_last_update = None
    raw_last_update = cached.get("ultima_actualizacion")
    if raw_last_update:
        try:
            parsed_last_update = datetime.fromisoformat(raw_last_update)
        except ValueError:
            parsed_last_update = None

    return {
        "ipc": cached.get("ipc", _empty_indicator("ipc")),
        "ipim": cached.get("ipim", _empty_indicator("ipim")),
        "source_url": cached.get("fuente", INDEC_PRECIOS_SOURCE_URL),
        "last_update": parsed_last_update,
    }
