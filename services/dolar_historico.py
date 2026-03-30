"""
Servicio de historial de cotización dólar blue.
Fuente primaria: scraping de dolarhoy.com (hasta 1 año).
Fuente secundaria: base de datos local (DollarQuote).
Caché: data/dolar_historico.json — se refresca una vez por día.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta

import requests
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
HISTORY_JSON_PATH = DATA_DIR / "dolar_historico.json"
HISTORY_SOURCE_URL = "https://dolarhoy.com/historico-dolar-blue"
REQUEST_TIMEOUT = 20
REFRESH_INTERVAL = timedelta(hours=12)


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _parse_js_date(raw: str) -> str | None:
    """
    Parsea fechas tipo:
      "Tue Sep 30 2025 23:26:09 GMT-0300 (Argentina Standard Time)"
    Devuelve "YYYY-MM-DD" o None si falla.
    """
    try:
        parts = raw.strip().split()
        # parts[0]=Day parts[1]=Mon parts[2]=DD parts[3]=YYYY
        return datetime.strptime(f"{parts[1]} {parts[2]} {parts[3]}", "%b %d %Y").strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        return None


def _scrape_dolarhoy() -> list[dict]:
    """
    Scrapea dolarhoy.com y extrae los arrays arrayDatos / arrayDatos2
    (venta y compra respectivamente, o viceversa).
    Devuelve lista de {fecha, compra, venta} ordenada por fecha.
    """
    resp = requests.get(
        HISTORY_SOURCE_URL,
        timeout=REQUEST_TIMEOUT,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Accept-Language": "es-AR,es;q=0.9",
        },
    )
    resp.raise_for_status()
    html = resp.text

    # Extrae todos los bloques arrayDatosX.push({x:"...", y:N})
    pattern = re.compile(
        r'(arrayDatos\d*)\s*\.push\s*\(\s*\{\s*x\s*:\s*"([^"]+)"\s*,\s*y\s*:\s*([\d.]+)\s*\}\s*\)',
        re.IGNORECASE,
    )
    matches = pattern.findall(html)

    # Agrupa por nombre de array → {fecha: valor}
    arrays: dict[str, dict[str, float]] = {}
    for array_name, raw_date, raw_value in matches:
        fecha = _parse_js_date(raw_date)
        if not fecha:
            continue
        arrays.setdefault(array_name, {})
        arrays[array_name][fecha] = float(raw_value)

    if not arrays:
        return []

    array_names = sorted(arrays.keys())
    # Convención dolarhoy: primer array = venta, segundo = compra (si existe)
    arr_venta = arrays[array_names[0]]
    arr_compra = arrays[array_names[1]] if len(array_names) > 1 else {}

    all_dates = sorted(set(arr_venta.keys()) | set(arr_compra.keys()))
    result = []
    for fecha in all_dates:
        venta = arr_venta.get(fecha)
        compra = arr_compra.get(fecha)
        if venta is None:
            continue
        # Si no hay compra, estimamos spread típico (~$20 menos)
        if compra is None:
            compra = round(venta - 20, 2)
        result.append({"fecha": fecha, "compra": compra, "venta": venta})

    return result


def _get_history_from_db(days: int = 180) -> list[dict]:
    """Fallback: usa DollarQuote de la base de datos."""
    from dashboard.models import DollarQuote
    cutoff = (timezone.now() - timedelta(days=days)).date()
    quotes = (
        DollarQuote.objects
        .filter(date__gte=cutoff)
        .order_by("date", "-updated_at")
        .values("date", "buy_value", "sell_value")
    )
    seen: set = set()
    result = []
    for q in quotes:
        d = q["date"].strftime("%Y-%m-%d")
        if d in seen:
            continue
        seen.add(d)
        result.append({
            "fecha": d,
            "compra": float(q["buy_value"]),
            "venta": float(q["sell_value"]),
        })
    return result


def _read_cache() -> dict | None:
    if not HISTORY_JSON_PATH.exists():
        return None
    try:
        with HISTORY_JSON_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_cache(data: list[dict]) -> None:
    _ensure_data_dir()
    payload = {
        "ultima_actualizacion": timezone.now().isoformat(),
        "datos": data,
    }
    with HISTORY_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _is_cache_stale(cache: dict | None) -> bool:
    if cache is None:
        return True
    raw = cache.get("ultima_actualizacion")
    if not raw:
        return True
    try:
        ts = datetime.fromisoformat(raw)
        if ts.tzinfo is None:
            ts = timezone.make_aware(ts)
        return (timezone.now() - ts) >= REFRESH_INTERVAL
    except (ValueError, TypeError):
        return True


def get_dollar_chart_history() -> dict:
    """
    Retorna {labels, compra, venta} para el gráfico del dashboard.
    Prioriza caché → scraping dolarhoy → DB local.
    """
    cache = _read_cache()

    if _is_cache_stale(cache):
        try:
            datos = _scrape_dolarhoy()
            if datos:
                _write_cache(datos)
                cache = {"datos": datos}
            else:
                raise ValueError("scraping devolvió lista vacía")
        except Exception as exc:
            logger.warning("dolar_historico: scraping falló (%s), usando DB", exc)
            datos = _get_history_from_db()
            if datos:
                _write_cache(datos)
                cache = {"datos": datos}

    datos = (cache or {}).get("datos") or []

    # Si el caché tiene datos pero son pocos, complementar con DB
    if len(datos) < 10:
        datos = _get_history_from_db()

    labels = [d["fecha"][5:][:5].replace("-", "/") for d in datos]   # "MM/DD" → "MM/DD"
    # Convertir a formato "DD/MM"
    labels_fmt = []
    for d in datos:
        parts = d["fecha"].split("-")  # ["YYYY", "MM", "DD"]
        labels_fmt.append(f"{parts[2]}/{parts[1]}")

    return {
        "labels": labels_fmt,
        "compra": [d["compra"] for d in datos],
        "venta": [d["venta"] for d in datos],
    }
