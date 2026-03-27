from __future__ import annotations

import json
import logging
import re
import unicodedata
import warnings
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning, Tag
from django.conf import settings
from django.utils import timezone


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
NOTICIAS_JSON_PATH = DATA_DIR / "noticias_pilar.json"
REQUEST_TIMEOUT_SECONDS = 25
MAX_NEWS_ITEMS = 5
RECENT_NEWS_DAYS = 2
CACHE_TTL_MINUTES = 30
ARTICLE_ENRICH_LIMIT = 28
ARTICLE_ENRICH_WORKERS = 6
MIN_BODY_CHARS = 160

DIRECT_FEED_SOURCES = [
    ("https://www.pilaradiario.com/rss/pages/ultimas-noticias.xml", "Pilar a Diario"),
    ("https://www.pilaradiario.com/rss/pages/locales.xml", "Pilar a Diario"),
    ("https://norteonline.com.ar/feed/", "NorteOnline"),
    ("https://pilardetodosapiv3.eleco.com.ar/feed-notes", "Pilar de Todos"),
]

HTML_SOURCE_PAGES = [
    ("https://www.pilaradiario.com/", "Pilar a Diario"),
    ("https://norteonline.com.ar/", "NorteOnline"),
    ("https://pilardetodos.com.ar/", "Pilar de Todos"),
    ("https://diarioresumen.com.ar/", "Diario Resumen"),
    ("https://www.zonanortehoy.com/", "Zona Norte Hoy"),
    ("https://diariolaprimera.com.ar/", "Diario La Primera"),
    ("https://termometro.com.ar/", "Termometro"),
]

GOOGLE_NEWS_RSS_QUERIES = [
    '"Pilar" "Buenos Aires"',
    '"Municipio de Pilar"',
    '"partido de Pilar"',
    '"Pilar" Achaval',
    '"Del Viso" Pilar',
    '"Presidente Derqui" Pilar',
]

SOURCE_PRIORITY = {
    "pilar a diario": 16,
    "pilar de todos": 15,
    "norteonline": 13,
    "diario resumen": 12,
    "diarioresumen": 12,
    "radio x pilar": 11,
    "zona norte hoy": 10,
    "diario la primera": 10,
    "diariolaprimera": 10,
    "termometro": 9,
    "clarin": 8,
    "la nacion": 8,
    "infobae": 8,
    "ambito": 8,
    "perfil": 7,
    "tn": 7,
    "cronos noticias": 7,
    "infocielo": 7,
}

BLOCKED_SOURCE_KEYWORDS = (
    "google news",
    "gob.ar",
    "gobierno",
    "municipio",
    "ministerio",
    "boletin oficial",
    "tyc sports",
    "espn",
    "copaargentina",
    "youtube",
    "facebook",
    "instagram",
    "x.com",
    "twitter",
)

LOCAL_SOURCE_KEYWORDS = (
    "pilar a diario",
    "pilar de todos",
    "norteonline",
    "diario resumen",
    "diarioresumen",
    "radio x pilar",
    "zona norte hoy",
    "diario la primera",
    "diariolaprimera",
    "termometro",
)

THEME_KEYWORDS = {
    "seguridad": ["seguridad", "policial", "delito", "robo", "guardia urbana", "allanamiento"],
    "salud": ["salud", "hospital", "vacuna", "medico", "sanitario"],
    "obras": ["obra", "obras", "asfalto", "infraestructura", "pavimento", "plaza"],
    "servicios": ["servicio", "servicios", "agua", "luz", "transporte", "transito", "clima"],
    "educacion": ["educacion", "escuela", "colegio", "universidad", "aulas"],
    "economia": ["economia", "empleo", "comercio", "industria", "empresa", "paritarias"],
    "gobierno": ["municipio", "municipal", "intendente", "achaval", "concejo", "sesiones"],
    "eventos": ["evento", "festival", "cultura", "agenda", "deporte"],
}

STRONG_LOCAL_KEYWORDS = (
    "municipio de pilar",
    "partido de pilar",
    "pilar buenos aires",
    "pilar bonaerense",
    "federico achaval",
    "achaval",
    "del viso",
    "presidente derqui",
    "manuel alberti",
    "villa rosa",
    "lagomarsino",
    "fatima",
    "villa astolfi",
    "villa verde",
)

LOCALITY_KEYWORDS = (
    "pilar",
    "del viso",
    "presidente derqui",
    "manuel alberti",
    "villa rosa",
    "lagomarsino",
    "fatima",
    "villa astolfi",
    "villa verde",
    "km 50",
    "ruta 8",
    "panamericana",
)

NEGATIVE_CONTEXT_KEYWORDS = (
    "pilar sordo",
    "pilar rubio",
    "pilar del rio",
    "pilar cisneros",
    "pilar fundamental",
    "pilares del",
)

GENERIC_PENALTY_KEYWORDS = (
    "clima hoy",
    "pronostico",
    "anses",
    "cuenta dni",
    "semana santa",
    "correo de lectores",
    "bioetanol",
    "horoscopo",
    "loteria",
)

SPORTS_PENALTY_KEYWORDS = (
    "real pilar",
    "banfield",
    "copa argentina",
    "torneo",
    "gol",
    "partido de futbol",
    "libertadores",
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
    lowered = _strip_accents(_clean_text(value)).casefold()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _normalize_source(value: str | None, fallback: str = "Fuente no identificada") -> str:
    cleaned = _clean_text(value)
    return cleaned.replace("  ", " ").strip() or fallback


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
        return timezone.localtime(parsed).date().isoformat(), parsed
    except (TypeError, ValueError):
        pass

    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d %b %Y",
        "%d %b, %Y",
        "%d %B %Y",
        "%d %B, %Y",
    ):
        try:
            parsed = datetime.strptime(raw_value, fmt)
            if parsed.tzinfo is None:
                parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
            return timezone.localtime(parsed).date().isoformat(), parsed
        except ValueError:
            continue

    return raw_value, None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _calendar_days_old(parsed_date: datetime | None) -> int | None:
    if parsed_date is None:
        return None
    local_date = timezone.localtime(parsed_date).date()
    return (timezone.localdate() - local_date).days


def _is_recent_news(parsed_date: datetime | None) -> bool:
    days_old = _calendar_days_old(parsed_date)
    return days_old is not None and 0 <= days_old < RECENT_NEWS_DAYS


def _title_without_source(title: str, source: str) -> str:
    cleaned = _clean_text(title)
    normalized_source = _normalize_text(source)
    for separator in (" - ", " | ", " :: "):
        parts = cleaned.rsplit(separator, 1)
        if len(parts) == 2 and _normalize_text(parts[1]) == normalized_source:
            return parts[0].strip()
    return cleaned


def _canonicalize_url(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return value.rstrip("/")
    cleaned = parsed._replace(query="", fragment="")
    return cleaned.geturl().rstrip("/")


def _google_news_url(query: str) -> str:
    return (
        f"https://news.google.com/rss/search?q={quote_plus(query)}"
        "&hl=es-419&gl=AR&ceid=AR:es-419"
    )


def _source_score(source: str) -> int:
    normalized_source = _normalize_text(source)
    score = 2
    for key, value in SOURCE_PRIORITY.items():
        if key in normalized_source:
            score = max(score, value)
    return score


def _is_diario_source(source: str) -> bool:
    normalized_source = _normalize_text(source)
    if not normalized_source:
        return False
    if any(keyword in normalized_source for keyword in BLOCKED_SOURCE_KEYWORDS):
        return False
    if any(keyword in normalized_source for keyword in SOURCE_PRIORITY):
        return True
    return any(keyword in normalized_source for keyword in ("diario", "noticias", "online"))


def _looks_like_local_source(source: str) -> bool:
    normalized_source = _normalize_text(source)
    return any(keyword in normalized_source for keyword in LOCAL_SOURCE_KEYWORDS)


def _detect_theme(text: str) -> str:
    lowered = _normalize_text(text)
    for theme, keywords in THEME_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return theme
    return "general"


def _extract_json_ld_items(soup: BeautifulSoup) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    for node in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        raw_text = node.string or node.get_text(" ", strip=True)
        if not raw_text:
            continue
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            continue

        candidates = parsed if isinstance(parsed, list) else [parsed]
        for candidate in candidates:
            if isinstance(candidate, dict) and isinstance(candidate.get("@graph"), list):
                for child in candidate["@graph"]:
                    if isinstance(child, dict):
                        collected.append(child)
            elif isinstance(candidate, dict):
                collected.append(candidate)
    return collected


def _extract_article_metadata(html: str, base_url: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    metadata: dict[str, str] = {}

    def set_if_missing(key: str, value: str | None) -> None:
        cleaned = _clean_text(value)
        if cleaned and key not in metadata:
            metadata[key] = cleaned

    title_node = soup.find("title")
    set_if_missing("title", title_node.get_text(" ", strip=True) if title_node else None)

    for json_ld_item in _extract_json_ld_items(soup):
        raw_type = json_ld_item.get("@type")
        if isinstance(raw_type, list):
            item_type = _normalize_text(" ".join(str(value) for value in raw_type))
        else:
            item_type = _normalize_text(str(raw_type or ""))

        if any(token in item_type for token in ("newsarticle", "article", "reportage")):
            set_if_missing("title", json_ld_item.get("headline"))
            set_if_missing("summary", json_ld_item.get("description"))
            set_if_missing("published", json_ld_item.get("datePublished"))
            set_if_missing("body", json_ld_item.get("articleBody"))
            set_if_missing("url", json_ld_item.get("url"))

            publisher = json_ld_item.get("publisher")
            if isinstance(publisher, dict):
                set_if_missing("source", publisher.get("name"))

            author = json_ld_item.get("author")
            if isinstance(author, dict):
                set_if_missing("author", author.get("name"))

    for selector, attribute, key in (
        ('meta[property="og:title"]', "content", "title"),
        ('meta[name="title"]', "content", "title"),
        ('meta[property="og:description"]', "content", "summary"),
        ('meta[name="description"]', "content", "summary"),
        ('meta[name="twitter:description"]', "content", "summary"),
        ('meta[property="article:published_time"]', "content", "published"),
        ('meta[name="publish-date"]', "content", "published"),
        ('meta[name="date"]', "content", "published"),
        ('meta[property="og:url"]', "content", "url"),
        ('link[rel="canonical"]', "href", "url"),
        ('meta[property="og:site_name"]', "content", "source"),
        ('meta[name="author"]', "content", "author"),
    ):
        node = soup.select_one(selector)
        if node is not None:
            set_if_missing(key, node.get(attribute))

    paragraph_selectors = (
        "article p",
        "[itemprop='articleBody'] p",
        ".article-body p",
        ".entry-content p",
        ".story-body p",
        ".post-content p",
        ".single-content p",
        ".nota p",
        "main p",
    )

    paragraphs: list[str] = []
    seen_paragraphs: set[str] = set()
    for selector in paragraph_selectors:
        for node in soup.select(selector):
            paragraph = _clean_text(node.get_text(" ", strip=True))
            normalized = _normalize_text(paragraph)
            if len(paragraph) < 60 or normalized in seen_paragraphs:
                continue
            seen_paragraphs.add(normalized)
            paragraphs.append(paragraph)
            if len(paragraphs) >= 10:
                break
        if len(paragraphs) >= 4:
            break

    if paragraphs and len(" ".join(paragraphs)) >= MIN_BODY_CHARS:
        metadata["body"] = " ".join(paragraphs)

    metadata.setdefault("url", base_url)
    return metadata


def _extract_entry_link(entry: ET.Element) -> str:
    for tag_name in ("link", "{http://www.w3.org/2005/Atom}link"):
        for node in entry.findall(tag_name):
            href = node.get("href")
            rel = node.get("rel")
            if href and rel in (None, "", "alternate"):
                return _clean_text(href)
    for tag_name in ("link", "{http://www.w3.org/2005/Atom}link"):
        text_value = entry.findtext(tag_name)
        if text_value:
            return _clean_text(text_value)
    return ""


def _extract_feed_entries(root: ET.Element) -> list[ET.Element]:
    rss_items = root.findall(".//item")
    if rss_items:
        return rss_items
    return root.findall(".//{http://www.w3.org/2005/Atom}entry")


def fetch_feed_items(url: str, fallback_source: str) -> list[dict[str, Any]]:
    response = _request(url)
    root = ET.fromstring(response.text)
    items: list[dict[str, Any]] = []

    for entry in _extract_feed_entries(root):
        title = _clean_text(entry.findtext("title") or entry.findtext("{http://www.w3.org/2005/Atom}title"))
        summary = _clean_text(
            entry.findtext("description")
            or entry.findtext("summary")
            or entry.findtext("{http://www.w3.org/2005/Atom}summary")
            or entry.findtext("{http://www.w3.org/2005/Atom}content")
        )
        source_node = entry.find("source")
        source = _normalize_source(source_node.text if source_node is not None else None, fallback_source)
        link = _extract_entry_link(entry)

        raw_date = (
            entry.findtext("pubDate")
            or entry.findtext("published")
            or entry.findtext("updated")
            or entry.findtext("{http://www.w3.org/2005/Atom}published")
            or entry.findtext("{http://www.w3.org/2005/Atom}updated")
        )
        fecha_publicacion, parsed_date = _parse_date(raw_date)

        normalized_item = {
            "titulo": _title_without_source(title, source),
            "fuente": source,
            "url": link,
            "fecha_publicacion": fecha_publicacion,
            "resumen_corto": _short_summary(summary or title),
            "tema": _detect_theme(f"{title} {summary}"),
            "relevancia": 0,
            "_parsed_date": parsed_date,
            "_article_text": "",
            "_source_origin": url,
        }
        normalized_item["relevancia"] = _score_news_item(normalized_item)
        items.append(normalized_item)

    return items


def fetch_rss_items(url: str, fallback_source: str) -> list[dict[str, Any]]:
    return fetch_feed_items(url, fallback_source)


def _same_registered_domain(candidate_url: str, page_url: str) -> bool:
    candidate_host = urlparse(candidate_url).netloc.removeprefix("www.")
    page_host = urlparse(page_url).netloc.removeprefix("www.")
    return bool(candidate_host) and candidate_host == page_host


def _looks_like_article_url(candidate_url: str, page_url: str) -> bool:
    if not candidate_url:
        return False
    parsed = urlparse(candidate_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return False
    if not _same_registered_domain(candidate_url, page_url):
        return False

    path = parsed.path.lower().rstrip("/")
    if not path or path in ("", "/"):
        return False
    if any(
        segment in path
        for segment in (
            "/feed",
            "/tag/",
            "/tags/",
            "/author/",
            "/search",
            "/buscar",
            "/wp-json",
            "/wp-content",
            "/page/",
        )
    ):
        return False

    path_parts = [part for part in path.split("/") if part]
    if len(path_parts) == 1 and len(path_parts[0]) < 18:
        return False

    return True


def _extract_nearby_date(node: Tag) -> tuple[str | None, datetime | None]:
    current: Tag | None = node
    for _ in range(4):
        if current is None:
            break
        time_node = current.find("time")
        if time_node is not None:
            raw_value = time_node.get("datetime") or time_node.get_text(" ", strip=True)
            parsed = _parse_date(raw_value)
            if parsed[1] is not None:
                return parsed
        current = current.parent if isinstance(current.parent, Tag) else None
    return None, None


def fetch_html_listing_items(url: str, fallback_source: str) -> list[dict[str, Any]]:
    response = _request(url)
    soup = BeautifulSoup(response.text, "html.parser")
    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    selectors = (
        "article a[href]",
        "main a[href]",
        "section a[href]",
        "h2 a[href]",
        "h3 a[href]",
    )

    for selector in selectors:
        for node in soup.select(selector):
            href = urljoin(url, node.get("href", ""))
            href = _canonicalize_url(href)
            if href in seen_urls or not _looks_like_article_url(href, url):
                continue

            title = _clean_text(
                node.get("title") or node.get("aria-label") or node.get_text(" ", strip=True)
            )
            if len(title) < 25:
                continue

            parent_text = _clean_text(node.parent.get_text(" ", strip=True)) if node.parent else title
            fecha_publicacion, parsed_date = _extract_nearby_date(node)

            item = {
                "titulo": title,
                "fuente": fallback_source,
                "url": href,
                "fecha_publicacion": fecha_publicacion,
                "resumen_corto": _short_summary(parent_text or title),
                "tema": _detect_theme(f"{title} {parent_text}"),
                "relevancia": 0,
                "_parsed_date": parsed_date,
                "_article_text": "",
                "_source_origin": url,
            }
            item["relevancia"] = _score_news_item(item)
            items.append(item)
            seen_urls.add(href)

            if len(items) >= 16:
                return items

    return items


def _local_signal_score(text: str) -> int:
    normalized = _normalize_text(text)
    if not normalized:
        return 0
    if any(keyword in normalized for keyword in NEGATIVE_CONTEXT_KEYWORDS):
        return -20

    score = 0
    if "pilar" in normalized:
        score += 10

    strong_hits = sum(1 for keyword in STRONG_LOCAL_KEYWORDS if keyword in normalized)
    locality_hits = sum(1 for keyword in LOCALITY_KEYWORDS if keyword in normalized)
    score += strong_hits * 5
    score += locality_hits * 2

    if "municipio de pilar" in normalized or "partido de pilar" in normalized:
        score += 6

    return score


def _has_basic_pilar_signal(item: dict[str, Any]) -> bool:
    text = _normalize_text(f"{item['titulo']} {item['resumen_corto']}")
    if _local_signal_score(text) >= 10:
        return True
    return _looks_like_local_source(item["fuente"]) and "pilar" in text


def _is_candidate_relevant(item: dict[str, Any]) -> bool:
    if not _is_diario_source(item["fuente"]):
        return False

    if not _is_recent_news(item.get("_parsed_date")):
        return False

    if "news.google.com" in urlparse(item.get("url", "")).netloc:
        return False

    full_text = _normalize_text(
        " ".join(
            part
            for part in (item["titulo"], item["resumen_corto"], item.get("_article_text", ""))
            if part
        )
    )
    if not full_text:
        return False

    if _local_signal_score(full_text) < 10:
        return False

    if item.get("_article_text") and len(item["_article_text"]) < MIN_BODY_CHARS:
        return False

    return True


def _score_news_item(item: dict[str, Any]) -> int:
    title = _normalize_text(item.get("titulo"))
    summary = _normalize_text(item.get("resumen_corto"))
    article_text = _normalize_text(item.get("_article_text", ""))
    full_text = " ".join(part for part in (title, summary, article_text) if part)

    score = _source_score(item.get("fuente", ""))
    score += _local_signal_score(full_text)

    if article_text:
        score += 6
    elif "news.google.com" in urlparse(item.get("url", "")).netloc:
        score -= 10

    theme = item.get("tema")
    if theme in {"seguridad", "salud", "obras", "economia", "gobierno"}:
        score += 4
    elif theme == "general":
        score += 1

    days_old = _calendar_days_old(item.get("_parsed_date"))
    if days_old is None:
        score -= 6
    elif days_old == 0:
        score += 24
    elif days_old == 1:
        score += 18
    else:
        score -= 25

    if any(keyword in full_text for keyword in GENERIC_PENALTY_KEYWORDS):
        score -= 6

    if any(keyword in full_text for keyword in SPORTS_PENALTY_KEYWORDS):
        score -= 8

    return score


def _deduplicate_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_items: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _normalize_text(
            f"{item.get('fuente')} {_canonicalize_url(item.get('url'))} {item.get('titulo')}"
        )
        existing = unique_items.get(key)
        if existing is None or item["relevancia"] > existing["relevancia"]:
            unique_items[key] = item
    return list(unique_items.values())


def _sort_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            item["_parsed_date"].timestamp() if item.get("_parsed_date") else 0,
            item["relevancia"],
            _source_score(item.get("fuente", "")),
        ),
        reverse=True,
    )


def _enrich_with_article_content(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("_article_enriched"):
        return item

    current_item = dict(item)
    current_item["_article_enriched"] = True

    try:
        response = _request(current_item["url"], allow_redirects=True)
    except requests.RequestException:
        return current_item

    final_url = response.url or current_item["url"]
    current_item["url"] = _canonicalize_url(final_url)
    if "news.google.com" in urlparse(current_item["url"]).netloc:
        return current_item

    metadata = _extract_article_metadata(response.text, current_item["url"])

    if metadata.get("url"):
        current_item["url"] = _canonicalize_url(metadata["url"])

    if metadata.get("source") and _is_diario_source(metadata["source"]):
        current_item["fuente"] = metadata["source"]

    if metadata.get("title"):
        current_item["titulo"] = _title_without_source(metadata["title"], current_item["fuente"])

    if metadata.get("summary"):
        candidate_summary = _short_summary(metadata["summary"])
        if len(candidate_summary) >= len(current_item.get("resumen_corto", "")):
            current_item["resumen_corto"] = candidate_summary

    article_text = " ".join(
        part
        for part in (
            metadata.get("summary"),
            metadata.get("body"),
            current_item.get("resumen_corto"),
        )
        if part
    )
    current_item["_article_text"] = article_text

    if metadata.get("published"):
        fecha_publicacion, parsed_date = _parse_date(metadata["published"])
        if parsed_date is not None:
            current_item["fecha_publicacion"] = fecha_publicacion
            current_item["_parsed_date"] = parsed_date

    current_item["tema"] = _detect_theme(
        " ".join(
            part
            for part in (
                current_item.get("titulo"),
                current_item.get("resumen_corto"),
                current_item.get("_article_text", ""),
            )
            if part
        )
    )
    current_item["relevancia"] = _score_news_item(current_item)
    return current_item


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


def _collect_seed_items() -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []

    for url, source in DIRECT_FEED_SOURCES:
        try:
            all_items.extend(fetch_feed_items(url, source))
        except Exception:
            logger.warning("Noticias Pilar: fallo feed %s", url)

    for url, source in HTML_SOURCE_PAGES:
        try:
            all_items.extend(fetch_html_listing_items(url, source))
        except Exception:
            logger.warning("Noticias Pilar: fallo portada %s", url)

    for query in GOOGLE_NEWS_RSS_QUERIES:
        try:
            all_items.extend(fetch_feed_items(_google_news_url(query), "Google News"))
        except Exception:
            logger.warning("Noticias Pilar: fallo Google News para %s", query)

    filtered_items: list[dict[str, Any]] = []
    for item in all_items:
        if not item.get("url") or not _is_diario_source(item["fuente"]):
            continue
        if not _has_basic_pilar_signal(item):
            continue
        filtered_items.append(item)

    return _deduplicate_news(filtered_items)


def _enrich_candidates(seed_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    preliminarily_sorted = sorted(
        seed_items,
        key=lambda item: (
            _looks_like_local_source(item["fuente"]),
            item["relevancia"],
            item["_parsed_date"].timestamp() if item.get("_parsed_date") else 0,
        ),
        reverse=True,
    )

    items_to_enrich = preliminarily_sorted[:ARTICLE_ENRICH_LIMIT]
    if not items_to_enrich:
        return []

    enriched_items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=ARTICLE_ENRICH_WORKERS) as executor:
        for current_item in executor.map(_enrich_with_article_content, items_to_enrich):
            if _is_candidate_relevant(current_item):
                current_item["relevancia"] = _score_news_item(current_item)
                enriched_items.append(current_item)

    return enriched_items


def scrape_noticias_pilar() -> dict[str, Any]:
    seed_items = _collect_seed_items()
    enriched_items = _enrich_candidates(seed_items)
    final_items = _sort_news(_deduplicate_news(enriched_items))
    payload = _build_json_payload(final_items)

    return {
        "payload": payload,
        "count": payload["cantidad_noticias"],
        "tema_dominante": payload["tema_dominante"],
        "method": "feeds_plus_html_plus_article_analysis",
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


def _should_refresh_cache(cached: dict[str, Any] | None) -> bool:
    if cached is None:
        return True
    parsed_last_update = _parse_iso_datetime(cached.get("ultima_actualizacion"))
    if parsed_last_update is None:
        return True
    return timezone.now() - parsed_last_update >= timedelta(minutes=CACHE_TTL_MINUTES)


def get_dashboard_noticias_pilar() -> dict[str, Any]:
    cached = read_noticias_json()
    if _should_refresh_cache(cached):
        try:
            cached = update_noticias_pilar_json()["payload"]
        except Exception:
            cached = cached or {"noticias": [], "cantidad_noticias": 0, "tema_dominante": "general"}

    parsed_last_update = None
    raw_last_update = cached.get("ultima_actualizacion") if cached else None
    if raw_last_update:
        parsed_last_update = _parse_iso_datetime(raw_last_update)

    return {
        "items": cached.get("noticias", []) if cached else [],
        "count": cached.get("cantidad_noticias", 0) if cached else 0,
        "tema_dominante": cached.get("tema_dominante", "general") if cached else "general",
        "last_update": parsed_last_update,
    }
