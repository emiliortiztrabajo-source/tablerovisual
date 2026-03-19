from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests
import urllib3
from bs4 import BeautifulSoup
from django.conf import settings
from django.utils import timezone


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

PROVINCIA_FONDOS_URL = "https://www.provinciafondos.com.ar/nuestros-fondos"
DEFAULT_API_BASE_URL = "https://provincia-prod-api.teocoop.site"
DATA_DIR = settings.BASE_DIR / "data"
PROVINCIA_FONDOS_JSON_PATH = DATA_DIR / "provincia_fondos.json"
REQUEST_TIMEOUT_SECONDS = 30


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
        response.encoding = "utf-8"
        response.raise_for_status()
        return response
    except requests.exceptions.SSLError:
        response = requests.get(url, verify=False, **kwargs)
        response.encoding = "utf-8"
        response.raise_for_status()
        return response


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("%", "").replace("$", "").replace("\xa0", " ")
    text = re.sub(r"\s+", "", text)

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif text.count(",") == 1 and text.count(".") == 0:
        text = text.replace(",", ".")
    else:
        text = text.replace(",", "")

    try:
        return float(text)
    except ValueError:
        return None


def _normalize_percentage(value: Any) -> float | None:
    return _normalize_number(value)


def _normalize_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return text


def _get_scraping_timestamp() -> str:
    return timezone.localtime().replace(microsecond=0).isoformat()


def read_provincia_fondos_json() -> dict[str, Any] | None:
    if not PROVINCIA_FONDOS_JSON_PATH.exists():
        return None
    try:
        with PROVINCIA_FONDOS_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (json.JSONDecodeError, OSError):
        return None


def write_provincia_fondos_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with PROVINCIA_FONDOS_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def fetch_primary_page_html() -> str:
    return _request(PROVINCIA_FONDOS_URL).text


def extract_fund_links_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    seen: set[str] = set()

    for anchor in soup.select('a[href^="/nuestros-fondos/"], a[href*="/nuestros-fondos/"]'):
        href = anchor.get("href")
        if not href:
            continue
        absolute_url = urljoin(PROVINCIA_FONDOS_URL, href)
        if absolute_url != PROVINCIA_FONDOS_URL and absolute_url not in seen:
            seen.add(absolute_url)
            links.append(absolute_url)
    return links


def discover_api_base_url(html: str) -> str:
    script_paths = re.findall(r'<script[^>]+src="([^"]+)"', html)
    for script_path in script_paths:
        if "157-" not in script_path and "layout-" not in script_path and "nuestros-fondos/page-" not in script_path:
            continue
        try:
            script_url = urljoin(PROVINCIA_FONDOS_URL, script_path)
            script_text = _request(script_url).text
        except requests.RequestException:
            continue

        match = re.search(r"https://provincia-[a-z-]+-api\.teocoop\.site/?", script_text)
        if match:
            return match.group(0).rstrip("/")
    return DEFAULT_API_BASE_URL


def fetch_funds_catalog(api_base_url: str) -> list[dict[str, Any]]:
    response = _request(f"{api_base_url}/api/our-founds")
    payload = response.json()
    return payload.get("data", [])


def fetch_latest_quota_part(num_fondo: Any, clase_fondo: str | None, api_base_url: str) -> dict[str, Any] | None:
    if num_fondo in (None, "") or not clase_fondo:
        return None

    params = {
        "filters[numero_fondo][$eq]": str(num_fondo),
        "filters[clase_fondo][$eq]": clase_fondo,
        "sort[0]": "fecha:desc",
        "pagination[page]": "1",
        "pagination[pageSize]": "1",
    }
    response = _request(f"{api_base_url}/api/cuota-partes", params=params)
    payload = response.json()
    data = payload.get("data", [])
    return data[0] if data else None


def build_fund_public_url(document_id: str | None) -> str | None:
    if not document_id:
        return None
    return urljoin(PROVINCIA_FONDOS_URL + "/", document_id)


def fetch_fund_page_details(url_fondo: str | None) -> dict[str, Any]:
    if not url_fondo:
        return {}
    try:
        html = _request(url_fondo).text
    except requests.RequestException:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    compact = re.sub(r"\s+", " ", text)

    detected_currency = None
    if "dólares" in compact.lower() or "usd" in compact.lower():
        detected_currency = "USD"
    elif "pesos" in compact.lower() or "ars" in compact.lower():
        detected_currency = "ARS"

    return {"moneda_detectada": detected_currency}


def normalize_fund_record(
    fund: dict[str, Any],
    quota_part: dict[str, Any] | None,
    scraping_timestamp: str,
    individual_url: str | None,
    page_details: dict[str, Any],
) -> dict[str, Any]:
    clases = fund.get("clase_fondo") or []
    clase_fondo = clases[0].get("clase") if clases else None
    caracteristicas = fund.get("caracteristicas_fondos") or []
    tipo = caracteristicas[0].get("value") if caracteristicas else None

    moneda = _safe_text(fund.get("moneda"))
    if moneda:
        moneda_upper = moneda.upper()
        if "PESO" in moneda_upper:
            moneda = "ARS"
        elif "DOLAR" in moneda_upper or "USD" in moneda_upper:
            moneda = "USD"
    elif page_details.get("moneda_detectada"):
        moneda = page_details["moneda_detectada"]

    return {
        "nombre_fondo": _safe_text(fund.get("name")),
        "numero_fondo": _safe_text(fund.get("numero_fondo")),
        "url_fondo": individual_url,
        "valor_cuotaparte": _normalize_number((quota_part or {}).get("valor_cuota_parte")),
        "variacion_diaria": _normalize_percentage(fund.get("variacionDiaria")),
        "fecha_dato": _normalize_date((quota_part or {}).get("fecha") or fund.get("informationAt")),
        "fecha_scraping": scraping_timestamp,
        "moneda": moneda,
        "tipo": _safe_text(tipo),
        "clase_fondo": _safe_text(clase_fondo),
    }


def calculate_extra_metrics(numero_fondo: Any, clase_fondo: str | None, api_base_url: str) -> dict[str, float | None]:
    if numero_fondo in (None, "") or not clase_fondo:
        return {
            "variacion_acumulada_7_dias": None,
            "variacion_acumulada_30_dias": None,
            "promedio_geometrico_diario": None,
        }

    params = {
        "filters[numero_fondo][$eq]": str(numero_fondo),
        "filters[clase_fondo][$eq]": clase_fondo,
        "sort[0]": "fecha:desc",
        "pagination[page]": "1",
        "pagination[pageSize]": "31",
    }
    try:
        response = _request(f"{api_base_url}/api/cuota-partes", params=params)
        series = response.json().get("data", [])
    except requests.RequestException:
        series = []

    values = [_normalize_number(item.get("valor_cuota_parte")) for item in series]
    values = [value for value in values if value is not None]
    if len(values) < 2:
        return {
            "variacion_acumulada_7_dias": None,
            "variacion_acumulada_30_dias": None,
            "promedio_geometrico_diario": None,
        }

    current = values[0]

    def accumulated(days: int) -> float | None:
        if len(values) <= days:
            return None
        previous = values[days]
        if previous in (None, 0):
            return None
        return round(((current / previous) - 1) * 100, 4)

    daily_returns = []
    for index in range(len(values) - 1):
        newer = values[index]
        older = values[index + 1]
        if older == 0:
            continue
        daily_returns.append(newer / older)

    geometric = None
    if daily_returns:
        product = 1.0
        for value in daily_returns:
            product *= value
        geometric = round(((product ** (1 / len(daily_returns))) - 1) * 100, 6)

    return {
        "variacion_acumulada_7_dias": accumulated(7),
        "variacion_acumulada_30_dias": accumulated(30),
        "promedio_geometrico_diario": geometric,
    }


def scrape_provincia_fondos_with_requests() -> dict[str, Any]:
    html = fetch_primary_page_html()
    links_from_html = extract_fund_links_from_html(html)
    api_base_url = discover_api_base_url(html)
    funds_catalog = fetch_funds_catalog(api_base_url)
    scraping_timestamp = _get_scraping_timestamp()

    fondos: list[dict[str, Any]] = []
    processed_count = 0

    for fund in funds_catalog:
        document_id = fund.get("documentId")
        url_fondo = build_fund_public_url(document_id)
        if links_from_html and url_fondo not in links_from_html:
            links_from_html.append(url_fondo)

        page_details = fetch_fund_page_details(url_fondo)

        clases = fund.get("clase_fondo") or []
        clase_fondo = clases[0].get("clase") if clases else None
        quota_part = fetch_latest_quota_part(fund.get("numero_fondo"), clase_fondo, api_base_url)

        normalized = normalize_fund_record(
            fund=fund,
            quota_part=quota_part,
            scraping_timestamp=scraping_timestamp,
            individual_url=url_fondo,
            page_details=page_details,
        )
        fondos.append(normalized)
        if normalized["nombre_fondo"]:
            processed_count += 1

    payload = {
        "ultima_actualizacion": scraping_timestamp,
        "fuente": PROVINCIA_FONDOS_URL,
        "fondos": fondos,
    }
    return {
        "payload": payload,
        "funds_found": len(funds_catalog),
        "funds_processed": processed_count,
        "method": "requests_api",
    }


def scrape_provincia_fondos_with_browser() -> dict[str, Any]:
    """
    Fallback preparado para contenido dinámico.

    Estrategia sugerida:
    - Selenium o Playwright para abrir /nuestros-fondos.
    - Esperar al render de las cards y links de detalle.
    - Extraer documentId, nombre y luego consultar cuotapartes.
    """
    raise NotImplementedError(
        "Fallback browser pendiente. Implementar con Selenium o Playwright si la API deja de ser accesible."
    )


def update_provincia_fondos_json() -> dict[str, Any]:
    try:
        result = scrape_provincia_fondos_with_requests()
        write_provincia_fondos_json(result["payload"])
        logger.info("Provincia Fondos: scraping actualizado con requests")
        return result
    except Exception as requests_error:
        logger.warning("Provincia Fondos: fallo requests, intentando fallback JSON")
        cached = read_provincia_fondos_json()
        if cached is not None:
            return {
                "payload": cached,
                "funds_found": len(cached.get("fondos", [])),
                "funds_processed": len(cached.get("fondos", [])),
                "method": "fallback_json",
                "error": str(requests_error),
            }

        try:
            browser_result = scrape_provincia_fondos_with_browser()
            write_provincia_fondos_json(browser_result["payload"])
            logger.info("Provincia Fondos: scraping actualizado con browser fallback")
            return browser_result
        except Exception as browser_error:
            logger.error("Provincia Fondos: sin datos disponibles")
            raise RuntimeError("No se pudo actualizar Provincia Fondos.") from browser_error


def get_dashboard_provincia_fondos() -> dict[str, Any]:
    cached = read_provincia_fondos_json()
    if cached is None:
        result = update_provincia_fondos_json()
        cached = result["payload"]

    fondos = cached.get("fondos", [])
    last_update_raw = cached.get("ultima_actualizacion")
    last_update = None
    if last_update_raw:
        try:
            last_update = datetime.fromisoformat(last_update_raw)
        except ValueError:
            last_update = None
    return {
        "summary": {
            "count": len(fondos),
            "last_update": last_update,
            "source": cached.get("fuente"),
        },
        "items": fondos,
    }
