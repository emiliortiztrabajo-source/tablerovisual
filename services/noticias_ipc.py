from __future__ import annotations

import json
import logging
import re
import unicodedata
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
NOTICIAS_IPC_JSON_PATH = DATA_DIR / "noticias_ipc.json"
REQUEST_TIMEOUT_SECONDS = 25
MIN_NEWS_ITEMS = 5
MAX_NEWS_ITEMS = 6
RECENT_NEWS_DAYS = 120
ARTICLE_ENRICH_LIMIT = 14

GOOGLE_NEWS_RSS_QUERIES = [
    '"IPC interanual" Argentina INDEC',
    '"inflacion interanual" Argentina INDEC',
    '"variacion interanual" IPC Argentina INDEC',
    'site:indec.gob.ar "indice de precios al consumidor" Argentina',
    'site:ambito.com "inflacion interanual" Argentina IPC',
    'site:infobae.com "inflacion interanual" Argentina IPC',
    'site:lanacion.com.ar "inflacion interanual" Argentina IPC',
    'site:cronista.com "inflacion interanual" Argentina IPC',
    'site:clarin.com "inflacion interanual" Argentina IPC',
    'site:elpais.com/argentina "inflacion interanual" Argentina IPC',
]

SOURCE_PRIORITY = {
    "indec": 16,
    "reuters": 14,
    "la nacion": 12,
    "lanacion": 12,
    "clarin": 12,
    "ambito": 12,
    "ambito financiero": 12,
    "infobae": 12,
    "el cronista": 11,
    "cronista": 11,
    "perfil": 10,
    "el pais": 10,
    "forbes argentina": 9,
    "cadena 3": 8,
    "pagina12": 8,
    "pagina 12": 8,
    "analisis digital": 7,
    "pilar de todos": 6,
    "yahoo noticias": 5,
}

INTERANUAL_KEYWORDS = (
    "ipc interanual",
    "inflacion interanual",
    "variacion interanual",
    "interanual",
    "ultimos 12 meses",
    "doce meses",
)

RELEVANT_KEYWORDS = (
    "ipc",
    "inflacion",
    "indice de precios al consumidor",
    "precios al consumidor",
    "indec",
    "argentina",
    "nacional",
)

GENERIC_PENALTY_KEYWORDS = (
    "trabajadores",
    "mayorista",
    "caba",
    "ciudad",
    "cordoba",
    "supermercado",
    "canasta",
    "construccion",
)

PERCENT_PATTERN = re.compile(r"(?<!\d)(\d{1,3}(?:[.,]\d{1,2})?)\s*%")
VALUE_AFTER_KEYWORD_PATTERNS = (
    re.compile(
        r"(?:ipc|inflacion|variacion|indice de precios al consumidor)"
        r"[^.%]{0,90}?interanual[^0-9]{0,20}(?P<value>\d{1,3}(?:[.,]\d{1,2})?)\s*%",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<value>\d{1,3}(?:[.,]\d{1,2})?)\s*%[^.%]{0,60}"
        r"(?:interanual|en los ultimos 12 meses|en doce meses)",
        re.IGNORECASE,
    ),
    re.compile(
        r"acumul(?:o|a)\s+(?P<value>\d{1,3}(?:[.,]\d{1,2})?)\s*%[^.%]{0,60}"
        r"(?:interanual|en los ultimos 12 meses|en doce meses)",
        re.IGNORECASE,
    ),
)


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


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("ascii", "ignore").decode("ascii")


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value.strip()
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    lowered = _strip_accents(value).casefold()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _normalize_source(value: str | None) -> str:
    text = _clean_text(value)
    return text.replace("  ", " ").strip() or "Fuente no identificada"


def _short_summary(value: str, length: int = 190) -> str:
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
        "%Y-%m-%dT%H:%M:%S%z",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d %b %Y",
        "%d %B %Y",
    ):
        try:
            parsed = datetime.strptime(raw_value, fmt)
            parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
            return parsed.date().isoformat(), parsed
        except ValueError:
            continue

    return raw_value, None


def _normalize_percentage(raw_value: str | None) -> float | None:
    if not raw_value:
        return None
    try:
        value = float(raw_value.replace(",", "."))
    except ValueError:
        return None
    if value <= 0 or value > 500:
        return None
    return round(value, 2)


def _title_without_source(title: str, source: str) -> str:
    cleaned = _clean_text(title)
    normalized_source = _normalize_text(source)
    for separator in (" - ", " | "):
        parts = cleaned.rsplit(separator, 1)
        if len(parts) == 2 and _normalize_text(parts[1]) == normalized_source:
            return parts[0].strip()
    return cleaned


def _google_news_url(query: str) -> str:
    return (
        f"https://news.google.com/rss/search?q={quote_plus(query)}"
        "&hl=es-419&gl=AR&ceid=AR:es-419"
    )


def _source_score(source: str) -> int:
    normalized_source = _normalize_text(source)
    best_score = 2
    for key, score in SOURCE_PRIORITY.items():
        if key in normalized_source:
            best_score = max(best_score, score)
    return best_score


def _days_old(parsed_date: datetime | None) -> int | None:
    if parsed_date is None:
        return None
    delta = timezone.now() - parsed_date
    return max(0, delta.days)


def _extract_article_metadata(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    metadata: dict[str, str] = {}

    def set_if_missing(key: str, value: str | None) -> None:
        cleaned = _clean_text(value)
        if cleaned and key not in metadata:
            metadata[key] = cleaned

    title_node = soup.find("title")
    set_if_missing("title", title_node.get_text(" ", strip=True) if title_node else None)

    for selector in (
        ('meta[property="og:title"]', "content", "title"),
        ('meta[name="title"]', "content", "title"),
        ('meta[name="description"]', "content", "summary"),
        ('meta[property="og:description"]', "content", "summary"),
        ('meta[name="twitter:description"]', "content", "summary"),
        ('meta[property="article:published_time"]', "content", "published"),
        ('meta[name="publish-date"]', "content", "published"),
        ('meta[name="date"]', "content", "published"),
    ):
        node = soup.select_one(selector[0])
        if node is not None:
            set_if_missing(selector[2], node.get(selector[1]))

    paragraph_selectors = (
        "article p",
        "[itemprop='articleBody'] p",
        ".article-body p",
        ".story-body p",
        ".story p",
        ".entry-content p",
        "main p",
    )
    paragraphs: list[str] = []
    seen_paragraphs: set[str] = set()
    for selector in paragraph_selectors:
        for node in soup.select(selector):
            paragraph = _clean_text(node.get_text(" ", strip=True))
            normalized_paragraph = _normalize_text(paragraph)
            if len(paragraph) < 60 or normalized_paragraph in seen_paragraphs:
                continue
            seen_paragraphs.add(normalized_paragraph)
            paragraphs.append(paragraph)
            if len(paragraphs) >= 8:
                break
        if paragraphs:
            break

    if paragraphs:
        metadata["body"] = " ".join(paragraphs)

    return metadata


def _extract_interannual_value(texts: list[str]) -> float | None:
    candidates: list[tuple[int, float]] = []
    for index, raw_text in enumerate(texts):
        text = _clean_text(raw_text)
        if not text:
            continue

        search_text = _strip_accents(text)
        search_text_lower = search_text.casefold()

        for pattern in VALUE_AFTER_KEYWORD_PATTERNS:
            for match in pattern.finditer(search_text):
                value = _normalize_percentage(match.group("value"))
                if value is None:
                    continue
                score = 130 - (index * 10)
                context = _normalize_text(search_text[max(0, match.start() - 40): match.end() + 40])
                if "ipc" in context or "inflacion" in context:
                    score += 15
                candidates.append((score, value))

        for keyword in INTERANUAL_KEYWORDS:
            start = 0
            while True:
                keyword_index = search_text_lower.find(keyword, start)
                if keyword_index == -1:
                    break
                window_start = max(0, keyword_index - 130)
                window_end = min(len(search_text), keyword_index + 180)
                window = search_text[window_start:window_end]
                for match in PERCENT_PATTERN.finditer(window):
                    value = _normalize_percentage(match.group(1))
                    if value is None:
                        continue
                    local_context = _normalize_text(
                        window[max(0, match.start() - 20): min(len(window), match.end() + 35)]
                    )
                    distance = abs((window_start + match.start()) - keyword_index)
                    score = 85 - min(distance, 70) - (index * 8)
                    if "mensual" in local_context and "interanual" not in local_context:
                        score -= 25
                    if "ipc" in local_context or "inflacion" in local_context:
                        score += 10
                    candidates.append((score, value))
                start = keyword_index + len(keyword)

    if not candidates:
        return None
    best_score, best_value = max(candidates, key=lambda item: (item[0], item[1]))
    if best_score < 20:
        return None
    return round(best_value, 1)


def _score_news_item(item: dict[str, Any]) -> int:
    title = _normalize_text(item["titulo"])
    summary = _normalize_text(item["resumen_corto"])
    article_text = _normalize_text(item.get("_article_text", ""))
    full_text = " ".join(part for part in (title, summary, article_text) if part)

    score = _source_score(item["fuente"])

    if "ipc" in title:
        score += 14
    elif "ipc" in full_text:
        score += 8

    if "interanual" in title:
        score += 16
    elif "interanual" in full_text:
        score += 10

    if "inflacion" in title or "indice de precios al consumidor" in title:
        score += 8
    elif "inflacion" in full_text:
        score += 4

    if "argentina" in full_text or "indec" in full_text or "nacional" in full_text:
        score += 7

    detected_value = item.get("ipc_interanual_detectado")
    if detected_value is not None:
        score += 13
        if detected_value >= 10:
            score += 2

    days_old = _days_old(item.get("_parsed_date"))
    if days_old is None:
        score -= 4
    elif days_old <= 2:
        score += 15
    elif days_old <= 7:
        score += 11
    elif days_old <= 30:
        score += 7
    elif days_old <= 90:
        score += 2
    else:
        score -= 6

    penalty_hits = sum(1 for keyword in GENERIC_PENALTY_KEYWORDS if keyword in title)
    score -= penalty_hits * 4

    if "trabajadores" in full_text and "indec" not in full_text:
        score -= 6
    if "mayorista" in full_text and "ipc" not in full_text:
        score -= 6

    return score


def _is_candidate_relevant(item: dict[str, Any]) -> bool:
    full_text = _normalize_text(
        " ".join(
            part
            for part in (item["titulo"], item["resumen_corto"], item.get("_article_text", ""))
            if part
        )
    )
    if not full_text:
        return False

    has_topic_signal = any(keyword in full_text for keyword in RELEVANT_KEYWORDS)
    has_interannual_signal = any(keyword in full_text for keyword in INTERANUAL_KEYWORDS)
    if item.get("ipc_interanual_detectado") is not None:
        has_interannual_signal = True

    if not has_topic_signal or not has_interannual_signal:
        return False

    if "argentina" not in full_text and "indec" not in full_text and "nacional" not in full_text:
        return False

    return True


def _deduplicate_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_items: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _normalize_text(f"{item['fuente']} {item['titulo']}")
        existing = unique_items.get(key)
        if existing is None or item["relevancia"] > existing["relevancia"]:
            unique_items[key] = item
    return list(unique_items.values())


def _sort_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            item["relevancia"],
            item["_parsed_date"].timestamp() if item.get("_parsed_date") else 0,
        ),
        reverse=True,
    )


def _enrich_with_article_content(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("_article_enriched"):
        return item
    item["_article_enriched"] = True

    try:
        response = _request(item["url"], allow_redirects=True)
    except requests.RequestException:
        return item

    item["url"] = response.url or item["url"]
    metadata = _extract_article_metadata(response.text)

    if metadata.get("title"):
        item["titulo"] = _title_without_source(metadata["title"], item["fuente"])

    if metadata.get("summary"):
        preferred_summary = _short_summary(metadata["summary"])
        if len(preferred_summary) > len(item["resumen_corto"]):
            item["resumen_corto"] = preferred_summary

    article_text = " ".join(
        part for part in (metadata.get("summary"), metadata.get("body"), item["resumen_corto"]) if part
    )
    item["_article_text"] = article_text

    if item.get("_parsed_date") is None and metadata.get("published"):
        fecha_publicacion, parsed_date = _parse_date(metadata["published"])
        item["fecha_publicacion"] = fecha_publicacion
        item["_parsed_date"] = parsed_date

    item["ipc_interanual_detectado"] = _extract_interannual_value(
        [item["titulo"], item["resumen_corto"], article_text]
    )
    item["relevancia"] = _score_news_item(item)
    return item


def fetch_google_news_items(query: str) -> list[dict[str, Any]]:
    response = _request(_google_news_url(query))
    root = ET.fromstring(response.text)
    items: list[dict[str, Any]] = []

    for item in root.findall(".//item"):
        source_node = item.find("source")
        source = _normalize_source(source_node.text if source_node is not None else "Google News")
        title = _title_without_source(_clean_text(item.findtext("title")), source)
        description = _clean_text(item.findtext("description"))
        link = _clean_text(item.findtext("link"))
        fecha_publicacion, parsed_date = _parse_date(item.findtext("pubDate"))

        normalized_item = {
            "titulo": title,
            "fuente": source,
            "url": link,
            "fecha_publicacion": fecha_publicacion,
            "resumen_corto": _short_summary(description or title),
            "ipc_interanual_detectado": _extract_interannual_value([title, description]),
            "_parsed_date": parsed_date,
            "_article_text": "",
            "_source_query": query,
        }
        normalized_item["relevancia"] = _score_news_item(normalized_item)
        items.append(normalized_item)

    return items


def _supplement_with_cached_items(
    current_items: list[dict[str, Any]],
    cached_payload: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if cached_payload is None or len(current_items) >= MIN_NEWS_ITEMS:
        return current_items

    merged_items = list(current_items)
    current_keys = {_normalize_text(f"{item['fuente']} {item['titulo']}") for item in current_items}

    for cached_item in cached_payload.get("noticias", []):
        key = _normalize_text(f"{cached_item.get('fuente')} {cached_item.get('titulo')}")
        if key in current_keys:
            continue
        merged_items.append(
            {
                "titulo": cached_item.get("titulo", ""),
                "fuente": cached_item.get("fuente", "Fuente no identificada"),
                "url": cached_item.get("url", ""),
                "fecha_publicacion": cached_item.get("fecha_publicacion"),
                "resumen_corto": cached_item.get("resumen_corto", ""),
                "ipc_interanual_detectado": cached_item.get("ipc_interanual_detectado"),
                "relevancia": cached_item.get("relevancia", 0),
                "_parsed_date": _parse_date(cached_item.get("fecha_publicacion"))[1],
                "_article_text": "",
            }
        )
        current_keys.add(key)
        if len(merged_items) >= MIN_NEWS_ITEMS:
            break

    return merged_items


def _confidence_level(consolidated_value: float | None, coincidences: int, total_items: int) -> str:
    if consolidated_value is None or coincidences <= 0:
        return "sin-dato"
    if coincidences >= 4:
        return "alta"
    if coincidences >= 2 and coincidences / max(total_items, 1) >= 0.4:
        return "media"
    return "baja"


def _build_json_payload(items: list[dict[str, Any]]) -> dict[str, Any]:
    top_items = items[:MAX_NEWS_ITEMS]
    detected_values = [
        round(float(item["ipc_interanual_detectado"]), 1)
        for item in top_items
        if item.get("ipc_interanual_detectado") is not None
    ]
    counts = Counter(detected_values)

    consolidated_value: float | None = None
    coincidences = 0
    confidence_level = "sin-dato"

    if counts:
        most_common = counts.most_common()
        best_value, best_count = most_common[0]
        tied_values = [value for value, count in most_common if count == best_count]
        if best_count > 1 or len(tied_values) == 1:
            consolidated_value = best_value
            coincidences = best_count
        confidence_level = _confidence_level(consolidated_value, coincidences, len(top_items))

    return {
        "ultima_actualizacion": timezone.localtime().replace(microsecond=0).isoformat(),
        "dato_consolidado": consolidated_value,
        "coincidencias": coincidences,
        "total_noticias_analizadas": len(top_items),
        "nivel_confianza": confidence_level,
        "noticias": [
            {
                "titulo": item["titulo"],
                "fuente": item["fuente"],
                "url": item["url"],
                "fecha_publicacion": item["fecha_publicacion"],
                "resumen_corto": item["resumen_corto"],
                "ipc_interanual_detectado": item.get("ipc_interanual_detectado"),
                "relevancia": item["relevancia"],
            }
            for item in top_items
        ],
    }


def write_noticias_ipc_json(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    with NOTICIAS_IPC_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=2)


def read_noticias_ipc_json() -> dict[str, Any] | None:
    if not NOTICIAS_IPC_JSON_PATH.exists():
        return None
    try:
        with NOTICIAS_IPC_JSON_PATH.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (OSError, json.JSONDecodeError):
        return None


def scrape_noticias_ipc() -> dict[str, Any]:
    raw_items: list[dict[str, Any]] = []
    feeds_consulted = len(GOOGLE_NEWS_RSS_QUERIES)

    for query in GOOGLE_NEWS_RSS_QUERIES:
        try:
            raw_items.extend(fetch_google_news_items(query))
        except Exception:
            logger.warning("Noticias IPC: fallo Google News para %s", query)

    cutoff_date = timezone.now() - timedelta(days=RECENT_NEWS_DAYS)
    filtered_items: list[dict[str, Any]] = []
    for item in raw_items:
        parsed_date = item.get("_parsed_date")
        if parsed_date is not None and parsed_date < cutoff_date:
            continue
        if _is_candidate_relevant(item):
            filtered_items.append(item)

    deduped_items = _deduplicate_news(filtered_items)
    preliminarily_sorted = _sort_news(deduped_items)

    enriched_items: list[dict[str, Any]] = []
    for index, item in enumerate(preliminarily_sorted):
        current_item = dict(item)
        if index < ARTICLE_ENRICH_LIMIT and current_item.get("ipc_interanual_detectado") is None:
            current_item = _enrich_with_article_content(current_item)
        current_item["relevancia"] = _score_news_item(current_item)
        if _is_candidate_relevant(current_item):
            enriched_items.append(current_item)

    deduped_enriched_items = _deduplicate_news(enriched_items)
    sorted_items = _sort_news(deduped_enriched_items)
    cached_payload = read_noticias_ipc_json()
    if not sorted_items and cached_payload is not None:
        return {
            "payload": cached_payload,
            "feeds_consulted": feeds_consulted,
            "found_count": len(raw_items),
            "candidate_count": len(filtered_items),
            "used_count": cached_payload.get("total_noticias_analizadas", 0),
            "method": "fallback_json",
        }

    final_items = _supplement_with_cached_items(sorted_items, cached_payload)
    final_items = _sort_news(final_items)
    payload = _build_json_payload(final_items)
    method = "rss_plus_cache" if len(final_items) > len(sorted_items) else "rss_plus_article_parsing"

    return {
        "payload": payload,
        "feeds_consulted": feeds_consulted,
        "found_count": len(raw_items),
        "candidate_count": len(filtered_items),
        "used_count": payload["total_noticias_analizadas"],
        "method": method,
    }


def update_noticias_ipc_json() -> dict[str, Any]:
    try:
        result = scrape_noticias_ipc()
        if result["method"] == "fallback_json":
            logger.warning("Noticias IPC: usando fallback JSON")
            return result
        if result["payload"]["total_noticias_analizadas"] == 0:
            raise RuntimeError("No se pudieron reunir noticias relevantes de IPC.")
        write_noticias_ipc_json(result["payload"])
        logger.info("Noticias IPC: scraping actualizado")
        return result
    except Exception as error:
        logger.warning("Noticias IPC: usando fallback JSON")
        cached = read_noticias_ipc_json()
        if cached is not None:
            return {
                "payload": cached,
                "feeds_consulted": len(GOOGLE_NEWS_RSS_QUERIES),
                "found_count": 0,
                "candidate_count": 0,
                "used_count": cached.get("total_noticias_analizadas", 0),
                "method": "fallback_json",
                "error": str(error),
            }
        raise


def get_dashboard_noticias_ipc() -> dict[str, Any]:
    cached = read_noticias_ipc_json()
    if cached is None:
        cached = update_noticias_ipc_json()["payload"]

    parsed_last_update = None
    raw_last_update = cached.get("ultima_actualizacion")
    if raw_last_update:
        try:
            parsed_last_update = datetime.fromisoformat(raw_last_update)
        except ValueError:
            parsed_last_update = None

    dato_consolidado = cached.get("dato_consolidado")
    coincidencias = cached.get("coincidencias", 0)
    total_noticias = cached.get("total_noticias_analizadas", 0)
    confidence_level = cached.get(
        "nivel_confianza",
        _confidence_level(dato_consolidado, coincidencias, total_noticias),
    )

    items: list[dict[str, Any]] = []
    for item in cached.get("noticias", []):
        normalized_item = dict(item)
        detected_value = normalized_item.get("ipc_interanual_detectado")
        normalized_item["ipc_display"] = (
            f"{float(detected_value):.1f}%"
            if detected_value is not None
            else "Sin dato claro"
        )
        items.append(normalized_item)

    return {
        "items": items,
        "dato_consolidado": dato_consolidado,
        "dato_consolidado_display": (
            f"{float(dato_consolidado):.1f}%"
            if dato_consolidado is not None
            else "Sin consenso"
        ),
        "coincidencias": coincidencias,
        "count": total_noticias,
        "confidence_level": confidence_level,
        "last_update": parsed_last_update,
    }
