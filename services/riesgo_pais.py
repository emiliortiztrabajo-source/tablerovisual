from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any

import requests
import urllib3
from django.conf import settings
from django.utils import timezone


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
RIESGO_PAIS_JSON_PATH = DATA_DIR / "riesgo_pais.json"
RIESGO_PAIS_PAGE_URL = "https://www.dolarito.ar/indices/riesgo-pais"
RIESGO_PAIS_HISTORY_PAGE_URL = (
    "https://www.dolarito.ar/indices/riesgo-pais/historico/anual/{year}"
)
RIESGO_PAIS_API_URL = "https://api.dolarito.ar/api/frontend/indices/riesgoPais"
RIESGO_PAIS_SOURCE_LABEL = "Dolarito"
REQUEST_TIMEOUT_SECONDS = 30
CACHE_TTL = timedelta(hours=12)
SELENIUM_WAIT_SECONDS = 25
CHROME_BINARY_CANDIDATES = (
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
)
DOLARITO_AUTH_CLIENT = "f7d471ab0a4ff2b7947759d985ed1db0"
CHART_MAX_POINTS = 180


def _request(url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", REQUEST_TIMEOUT_SECONDS)
    headers = kwargs.setdefault("headers", {})
    headers.setdefault(
        "User-Agent",
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
        ),
    )
    headers.setdefault("Accept-Language", "es-AR,es;q=0.9,en;q=0.8")

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
    text = "".join(text.split())

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
    return int(value) if float(value).is_integer() else round(value, 2)


def _parse_dolarito_date(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    try:
        return datetime.strptime(raw_value.strip(), "%d-%m-%Y")
    except ValueError:
        return None


def _get_first_existing_path(*candidates: str) -> str | None:
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def _fetch_dolarito_history_with_requests() -> dict[str, Any]:
    response = _request(
        RIESGO_PAIS_API_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Referer": RIESGO_PAIS_PAGE_URL,
            "Origin": "https://www.dolarito.ar",
            "auth-client": DOLARITO_AUTH_CLIENT,
        },
    )
    if "application/json" not in response.headers.get("content-type", ""):
        raise ValueError("Dolarito API no devolvio JSON.")

    payload = response.json()
    if not isinstance(payload, dict) or not payload:
        raise ValueError("Dolarito API devolvio un payload vacio.")
    return payload


def _fetch_dolarito_history_with_selenium() -> dict[str, Any]:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError as exc:
        raise RuntimeError("Selenium no esta disponible en el entorno actual.") from exc

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1600,2200")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    chrome_binary = os.environ.get("GOOGLE_CHROME_BIN") or _get_first_existing_path(
        *CHROME_BINARY_CANDIDATES
    )
    if chrome_binary:
        options.binary_location = chrome_binary

    driver = webdriver.Chrome(options=options)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        page_urls = [
            RIESGO_PAIS_PAGE_URL,
            RIESGO_PAIS_HISTORY_PAGE_URL.format(year=timezone.localdate().year),
        ]
        last_error: Exception | None = None

        for page_url in page_urls:
            driver.get(page_url)
            deadline = time.monotonic() + SELENIUM_WAIT_SECONDS

            while time.monotonic() < deadline:
                time.sleep(1.0)
                logs = driver.get_log("performance")
                for entry in logs:
                    try:
                        message = json.loads(entry["message"]).get("message", {})
                    except json.JSONDecodeError:
                        continue

                    if message.get("method") != "Network.responseReceived":
                        continue

                    params = message.get("params", {})
                    response = params.get("response", {})
                    if response.get("url") != RIESGO_PAIS_API_URL:
                        continue
                    if int(response.get("status", 0) or 0) != 200:
                        continue

                    request_id = params.get("requestId")
                    if not request_id:
                        continue

                    try:
                        body = driver.execute_cdp_cmd(
                            "Network.getResponseBody",
                            {"requestId": request_id},
                        )
                    except Exception as exc:
                        last_error = exc
                        continue

                    raw_body = body.get("body", "")
                    if body.get("base64Encoded"):
                        raw_body = base64.b64decode(raw_body).decode("utf-8")

                    payload = json.loads(raw_body)
                    if not isinstance(payload, dict) or not payload:
                        raise ValueError("Dolarito devolvio un historico vacio.")
                    return payload

            last_error = RuntimeError(
                f"No se pudo capturar la respuesta de Dolarito desde {page_url}."
            )

        raise last_error or RuntimeError("No se pudo obtener el historico de Dolarito.")
    finally:
        driver.quit()


def _build_historical_series(raw_payload: dict[str, Any]) -> list[dict[str, Any]]:
    historical_series: list[dict[str, Any]] = []

    for raw_date, raw_value in raw_payload.items():
        parsed_date = _parse_dolarito_date(str(raw_date))
        normalized_value = _normalize_number(raw_value)
        if parsed_date is None or normalized_value is None:
            continue

        historical_series.append(
            {
                "fecha": parsed_date.strftime("%Y-%m-%d"),
                "valor": _normalize_integer_like(normalized_value),
            }
        )

    historical_series.sort(key=lambda item: item["fecha"])
    return historical_series


def _build_payload(historical_series: list[dict[str, Any]]) -> dict[str, Any]:
    scraping_time = timezone.localtime().replace(microsecond=0)
    latest_item = historical_series[-1] if historical_series else {}
    previous_item = historical_series[-2] if len(historical_series) > 1 else {}

    latest_value = _normalize_number(latest_item.get("valor"))
    previous_value = _normalize_number(previous_item.get("valor"))

    variacion = None
    variacion_puntos = None
    if latest_value is not None and previous_value not in (None, 0):
        variacion = ((latest_value - previous_value) / previous_value) * 100
        variacion_puntos = latest_value - previous_value

    latest_date = latest_item.get("fecha")
    source_timestamp = scraping_time.isoformat()
    if latest_date:
        parsed_latest = datetime.strptime(latest_date, "%Y-%m-%d")
        source_timestamp = timezone.make_aware(
            datetime.combine(parsed_latest.date(), datetime_time.min),
            timezone.get_current_timezone(),
        ).isoformat()

    return {
        "ultima_actualizacion": scraping_time.isoformat(),
        "fuente": RIESGO_PAIS_PAGE_URL,
        "source_label": RIESGO_PAIS_SOURCE_LABEL,
        "api_url": RIESGO_PAIS_API_URL,
        "riesgo_pais": {
            "valor": _normalize_integer_like(latest_value),
            "variacion": _normalize_integer_like(variacion),
            "cierre_anterior": _normalize_integer_like(previous_value),
            "variacion_puntos": _normalize_integer_like(variacion_puntos),
            "fecha_dato": latest_date,
            "fecha_scraping": source_timestamp,
        },
        "historico": historical_series,
    }


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
        "source_label": RIESGO_PAIS_SOURCE_LABEL,
        "api_url": RIESGO_PAIS_API_URL,
        "riesgo_pais": {
            "valor": None,
            "variacion": None,
            "cierre_anterior": None,
            "variacion_puntos": None,
            "fecha_dato": None,
            "fecha_scraping": scraping_time,
        },
        "historico": [],
    }


def _is_cache_stale(cache: dict[str, Any] | None) -> bool:
    if cache is None:
        return True
    if not cache.get("historico"):
        return True

    raw_timestamp = cache.get("ultima_actualizacion")
    if not raw_timestamp:
        return True

    try:
        parsed_timestamp = datetime.fromisoformat(raw_timestamp)
        if parsed_timestamp.tzinfo is None:
            parsed_timestamp = timezone.make_aware(parsed_timestamp)
        return (timezone.now() - parsed_timestamp) >= CACHE_TTL
    except (TypeError, ValueError):
        return True


def _get_cached_or_updated_payload() -> dict[str, Any]:
    cached = read_riesgo_pais_json()
    if _is_cache_stale(cached):
        try:
            return update_riesgo_pais_json()["payload"]
        except Exception:
            logger.warning("Riesgo Pais: no se pudo refrescar, usando cache existente.")
    return cached or _empty_payload()


def scrape_riesgo_pais() -> dict[str, Any]:
    attempts: list[tuple[str, Any]] = [
        ("dolarito_api_requests", _fetch_dolarito_history_with_requests),
        ("dolarito_api_browser", _fetch_dolarito_history_with_selenium),
    ]
    errors: list[str] = []

    for method_name, fetcher in attempts:
        try:
            raw_payload = fetcher()
            historical_series = _build_historical_series(raw_payload)
            if not historical_series:
                raise ValueError("El historico de Dolarito llego vacio.")
            return {
                "payload": _build_payload(historical_series),
                "method": method_name,
            }
        except Exception as error:
            logger.warning("Riesgo Pais: fallo %s", method_name)
            errors.append(f"{method_name}: {error}")

    raise RuntimeError(" | ".join(errors))


def update_riesgo_pais_json() -> dict[str, Any]:
    try:
        result = scrape_riesgo_pais()
        write_riesgo_pais_json(result["payload"])
        logger.info("Riesgo Pais: scraping actualizado desde Dolarito")
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


def get_riesgo_pais_chart_history(
    cached_payload: dict[str, Any] | None = None,
    max_points: int = CHART_MAX_POINTS,
) -> dict[str, list[Any]]:
    cached = cached_payload or _get_cached_or_updated_payload()
    historical_series = (cached.get("historico") or [])[-max_points:]

    labels: list[str] = []
    values: list[Any] = []
    for item in historical_series:
        fecha = item.get("fecha")
        valor = item.get("valor")
        if not fecha or valor is None:
            continue

        try:
            parsed_date = datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError:
            continue

        labels.append(parsed_date.strftime("%d/%m"))
        values.append(valor)

    return {
        "labels": labels,
        "values": values,
    }


def get_dashboard_riesgo_pais() -> dict[str, Any]:
    cached = _get_cached_or_updated_payload()

    parsed_last_update = None
    raw_last_update = cached.get("ultima_actualizacion")
    if raw_last_update:
        try:
            parsed_last_update = datetime.fromisoformat(raw_last_update)
            if parsed_last_update.tzinfo is None:
                parsed_last_update = timezone.make_aware(parsed_last_update)
        except ValueError:
            parsed_last_update = None

    chart_history = get_riesgo_pais_chart_history(cached_payload=cached)
    riesgo_pais = cached.get("riesgo_pais", {})
    return {
        "valor": riesgo_pais.get("valor"),
        "variacion": riesgo_pais.get("variacion"),
        "cierre_anterior": riesgo_pais.get("cierre_anterior"),
        "variacion_puntos": riesgo_pais.get("variacion_puntos"),
        "fecha_dato": riesgo_pais.get("fecha_dato"),
        "fecha_scraping": riesgo_pais.get("fecha_scraping"),
        "last_update": parsed_last_update,
        "source": cached.get("fuente", RIESGO_PAIS_PAGE_URL),
        "source_label": cached.get("source_label", RIESGO_PAIS_SOURCE_LABEL),
        "chart_labels": chart_history["labels"],
        "chart_values": chart_history["values"],
    }
