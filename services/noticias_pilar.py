from __future__ import annotations

import json
import logging
import re
import warnings
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus

import requests
import urllib3
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from django.conf import settings
from django.utils import timezone


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
NOTICIAS_JSON_PATH = DATA_DIR / "noticias_pilar.json"
REQUEST_TIMEOUT_SECONDS = 25
MAX_NEWS_ITEMS = 5

PILAR_DIARIO_RSS = [
    ("https://www.pilaradiario.com/rss/pages/ultimas-noticias.xml", "Pilar a Diario"),
    ("https://www.pilaradiario.com/rss/pages/locales.xml", "Pilar a Diario"),
]

GOOGLE_NEWS_RSS_QUERIES = [
    ("Pilar Municipalidad de Pilar obras salud seguridad", "Google News"),
    ("Pilar transito educacion economia local servicios publicos", "Google News"),
]

OFFICIAL_PILAR_PAGES = [
    ("https://pilar.gov.ar/gobierno-digital/", "Municipio Pilar"),
    ("https://pilar.gov.ar/seguridad/", "Municipio Pilar"),
    ("https://pilar.gov.ar/novedades/", "Municipio Pilar"),
]

SOURCE_PRIORITY = {
    "Municipio Pilar": 7,
    "Pilar a Diario": 6,
    "Google News": 3,
    "Clarin": 4,
    "La Nacion": 4,
    "Infobae": 4,
    "Ambito": 4,
}

THEME_KEYWORDS = {
    "seguridad": ["seguridad", "policial", "delito", "guardia urbana", "patrullero"],
    "salud": ["salud", "hospital", "centro de salud", "vacuna"],
    "obras": ["obra", "obras", "pavimento", "asfalto", "infraestructura", "plaza"],
    "servicios": ["servicio", "servicios", "agua", "luz", "transporte", "transito"],
    "educacion": ["educacion", "escuela", "colegio", "jardin", "universidad"],
    "economia": ["economia", "empleo", "comercio", "empresa", "industria"],
    "gobierno": ["municipalidad", "municipio", "intendente", "achaval", "concejo"],
    "eventos": ["evento", "festival", "cultura", "deporte", "agenda"],
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


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value.strip()
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_text(value: str) -> str:
    lowered = value.casefold()
    replacements = str.maketrans(
        {
            "á": "a",
            "é": "e",
            "í": "i",
            "ó": "o",
            "ú": "u",
            "ü": "u",
            "ñ": "n",
        }
    )
    lowered = lowered.translate(replacements)
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _short_summary(value: str, length: int = 150) -> str:
    text = _clean_text(value)
    if len(text) <= length:
        return text
    return text[: length - 3].rstrip() + "..."


def _parse_date(value: str | None) -> tuple[str | None, datetime | None]:
    if not value:
        return None, None

    raw_value = value.strip()
    try:
        parsed = parsedate_to_datetime(raw_value)
        if parsed.tzinfo is None:
            parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
        return parsed.date().isoformat(), parsed
    except (TypeError, ValueError):
        pass

    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y",
        "%d %b %Y",
        "%d %b, %Y",
        "%d %B, %Y",
    ):
        try:
            parsed = datetime.strptime(raw_value, fmt)
            parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
            return parsed.date().isoformat(), parsed
        except ValueError:
            continue

    return raw_value, None


def _detect_theme(text: str) -> str:
    lowered = _normalize_text(text)
    for theme, keywords in THEME_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return theme
    return "general"


def _score_news_item(item: dict[str, Any]) -> int:
    title = _normalize_text(item["titulo"])
    summary = _normalize_text(item["resumen_corto"])
    full_text = f"{title} {summary}"
    score = SOURCE_PRIORITY.get(item["fuente"], 1)

    if "pilar" in title:
        score += 5
    elif "pilar" in full_text:
        score += 3

    if any(keyword in full_text for keyword in ("municipalidad", "municipio", "intendente", "achaval")):
        score += 4

    if item["tema"] in {"seguridad", "salud", "obras", "servicios", "educacion", "economia"}:
        score += 3
    elif item["tema"] == "general":
        score += 1

    if item.get("_parsed_date") is None:
        score -= 2

    return score


def _deduplicate_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_items: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _normalize_text(item["titulo"])
        existing = unique_items.get(key)
        if existing is None or item["relevancia"] > existing["relevancia"]:
            unique_items[key] = item
    return list(unique_items.values())


def _google_news_url(query: str) -> str:
    return (
        f"https://news.google.com/rss/search?q={quote_plus(query)}"
        "&hl=es-419&gl=AR&ceid=AR:es-419"
    )


def fetch_rss_items(url: str, fallback_source: str) -> list[dict[str, Any]]:
    response = _request(url)
    root = ET.fromstring(response.text)
    items: list[dict[str, Any]] = []

    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title"))
        link = _clean_text(item.findtext("link"))
        description = _clean_text(item.findtext("description"))
        source_node = item.find("source")
        source = _clean_text(source_node.text if source_node is not None else None) or fallback_source
        fecha_publicacion, parsed_date = _parse_date(item.findtext("pubDate"))
        tema = _detect_theme(f"{title} {description}")

        normalized_item = {
            "titulo": title,
            "fuente": source,
            "url": link,
            "fecha_publicacion": fecha_publicacion,
            "resumen_corto": _short_summary(description or title),
            "tema": tema,
            "_parsed_date": parsed_date,
        }
        normalized_item["relevancia"] = _score_news_item(normalized_item)
        items.append(normalized_item)

    return items


def fetch_official_pilar_news() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    date_pattern = re.compile(r"\b\d{1,2}\s+[A-Za-zÁÉÍÓÚáéíóúñÑ]+,?\s+\d{4}\b")

    for url, source in OFFICIAL_PILAR_PAGES:
        try:
            html = _request(url).text
        except requests.RequestException:
            continue

        soup = BeautifulSoup(html, "html.parser")
        titles = soup.find_all(["h2", "h3", "h4"], string=True)
        for title_node in titles:
            title = _clean_text(title_node.get_text(" ", strip=True))
            if len(title) < 18:
                continue
            if "leer mas" in _normalize_text(title):
                continue

            nearby_text = _clean_text(title_node.parent.get_text(" ", strip=True))
            lowered_text = _normalize_text(nearby_text)
            if "pilar" not in lowered_text and "achaval" not in lowered_text:
                continue

            date_match = date_pattern.search(nearby_text)
            fecha_publicacion, parsed_date = _parse_date(date_match.group(0) if date_match else None)
            tema = _detect_theme(f"{title} {nearby_text}")

            item = {
                "titulo": title,
                "fuente": source,
                "url": url,
                "fecha_publicacion": fecha_publicacion,
                "resumen_corto": _short_summary(nearby_text),
                "tema": tema,
                "_parsed_date": parsed_date,
            }
            item["relevancia"] = _score_news_item(item)
            items.append(item)

    return items


def _sort_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            item["relevancia"],
            item["_parsed_date"].timestamp() if item["_parsed_date"] else 0,
        ),
        reverse=True,
    )


def _build_json_payload(items: list[dict[str, Any]]) -> dict[str, Any]:
    top_items = items[:MAX_NEWS_ITEMS]
    dominant_theme = Counter(item["tema"] for item in top_items).most_common(1)
    return {
        "ultima_actualizacion": timezone.localtime().replace(microsecond=0).isoformat(),
        "cantidad_noticias": len(top_items),
        "tema_dominante": dominant_theme[0][0] if dominant_theme else "general",
        "noticias": [
            {
                "titulo": item["titulo"],
                "fuente": item["fuente"],
                "url": item["url"],
                "fecha_publicacion": item["fecha_publicacion"],
                "resumen_corto": item["resumen_corto"],
                "tema": item["tema"],
                "relevancia": item["relevancia"],
            }
            for item in top_items
        ],
    }


def write_noticias_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with NOTICIAS_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def read_noticias_json() -> dict[str, Any] | None:
    if not NOTICIAS_JSON_PATH.exists():
        return None
    try:
        with NOTICIAS_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (OSError, json.JSONDecodeError):
        return None


def scrape_noticias_pilar() -> dict[str, Any]:
    all_items: list[dict[str, Any]] = []

    for url, source in PILAR_DIARIO_RSS:
        try:
            all_items.extend(fetch_rss_items(url, source))
        except Exception:
            logger.warning("Noticias Pilar: fallo feed %s", url)

    for query, _source in GOOGLE_NEWS_RSS_QUERIES:
        try:
            all_items.extend(fetch_rss_items(_google_news_url(query), "Google News"))
        except Exception:
            logger.warning("Noticias Pilar: fallo Google News para %s", query)

    try:
        all_items.extend(fetch_official_pilar_news())
    except Exception:
        logger.warning("Noticias Pilar: fallo scraping oficial")

    cutoff_date = timezone.now() - timedelta(days=120)
    filtered_items: list[dict[str, Any]] = []
    for item in all_items:
        title_and_summary = _normalize_text(f"{item['titulo']} {item['resumen_corto']}")
        has_pilar_signal = (
            "pilar" in title_and_summary
            or "achaval" in title_and_summary
            or item["fuente"] == "Municipio Pilar"
        )
        is_recent = item["_parsed_date"] is not None and item["_parsed_date"] >= cutoff_date
        if has_pilar_signal and is_recent:
            filtered_items.append(item)

    deduped_items = _deduplicate_news(filtered_items)
    sorted_items = _sort_news(deduped_items)
    payload = _build_json_payload(sorted_items)

    return {
        "payload": payload,
        "count": payload["cantidad_noticias"],
        "tema_dominante": payload["tema_dominante"],
        "method": "rss_plus_html",
    }


def update_noticias_pilar_json() -> dict[str, Any]:
    try:
        result = scrape_noticias_pilar()
        write_noticias_json(result["payload"])
        logger.info("Noticias Pilar: scraping actualizado")
        return result
    except Exception as error:
        logger.warning("Noticias Pilar: usando fallback JSON")
        cached = read_noticias_json()
        if cached is not None:
            return {
                "payload": cached,
                "count": cached.get("cantidad_noticias", 0),
                "tema_dominante": cached.get("tema_dominante", "general"),
                "method": "fallback_json",
                "error": str(error),
            }
        raise


def get_dashboard_noticias_pilar() -> dict[str, Any]:
    cached = read_noticias_json()
    if cached is None:
        cached = update_noticias_pilar_json()["payload"]

    parsed_last_update = None
    raw_last_update = cached.get("ultima_actualizacion")
    if raw_last_update:
        try:
            parsed_last_update = datetime.fromisoformat(raw_last_update)
        except ValueError:
            parsed_last_update = None

    return {
        "items": cached.get("noticias", []),
        "count": cached.get("cantidad_noticias", 0),
        "tema_dominante": cached.get("tema_dominante", "general"),
        "last_update": parsed_last_update,
    }
