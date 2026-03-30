"""
Historial de precios de combustibles YPF — fuente: surtidores.com.ar/precios/
Caché: data/naftas_historico.json — refresco diario.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

DATA_DIR = settings.BASE_DIR / "data"
NAFTAS_JSON_PATH = DATA_DIR / "naftas_historico.json"
SOURCE_URL = "https://surtidores.com.ar/precios/"
REQUEST_TIMEOUT = 20
CACHE_TTL = timedelta(hours=24)

MONTH_NAMES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}
MONTH_LABELS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

FUEL_LABELS = {
    "super": ["super", "súper"],
    "premium": ["premium"],
    "gasoil": ["gasoil", "gas oil", "diesel"],
}

# Solo nos interesan estos 3
TARGET_FUELS = ["super", "premium", "gasoil"]


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _classify_fuel(text: str) -> str | None:
    t = text.casefold().strip()
    for key, variants in FUEL_LABELS.items():
        if any(t == v or t.startswith(v) for v in variants):
            return key
    return None


def _scrape_surtidores() -> list[dict]:
    resp = requests.get(
        SOURCE_URL,
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
    soup = BeautifulSoup(resp.text, "html.parser")
    lines = [ln.strip() for ln in soup.get_text(separator="\n").splitlines()]

    year_re = re.compile(r"^(201[89]|202\d)$")  # 2018-2029
    num_re = re.compile(r"^\d[\d.,]*$")

    data_by_year: dict[int, dict[str, list[float]]] = {}
    current_year: int | None = None
    current_fuel: str | None = None

    for line in lines:
        if not line:
            continue

        # ¿Es un año?
        m = year_re.match(line)
        if m:
            current_year = int(m.group(1))
            current_fuel = None
            data_by_year.setdefault(current_year, {})
            continue

        if current_year is None:
            continue

        # ¿Es un nombre de mes? (ignorar, son los headers de columna)
        if line.casefold() in MONTH_NAMES_ES:
            continue

        # ¿Es texto alfabético? → puede ser un tipo de combustible o un encabezado desconocido
        if re.match(r"^[A-Za-záéíóúüñÁÉÍÓÚÜÑ\s]+$", line):
            fuel_key = _classify_fuel(line)
            if fuel_key is not None:
                current_fuel = fuel_key
                data_by_year[current_year].setdefault(current_fuel, [])
            else:
                current_fuel = None  # sección desconocida (ej: "Euro") → no recolectar
            continue

        # ¿Es un número?
        if current_fuel and num_re.match(line):
            try:
                value = float(line.replace(",", "."))
                data_by_year[current_year][current_fuel].append(value)
            except ValueError:
                pass

    if not data_by_year:
        raise ValueError("No se encontraron datos de precios en la página.")

    # Construir serie mensual ordenada
    records: list[dict] = []
    for year in sorted(data_by_year.keys()):
        fuels = data_by_year[year]
        if not fuels:
            continue
        n_months = min(max((len(v) for v in fuels.values()), default=0), 12)
        for month_idx in range(n_months):
            month = month_idx + 1
            label = f"{MONTH_LABELS[month_idx]}/{str(year)[2:]}"
            record: dict = {"fecha": f"{year}-{month:02d}-01", "label": label}
            for key in TARGET_FUELS:
                vals = fuels.get(key, [])
                record[key] = vals[month_idx] if month_idx < len(vals) else None
            records.append(record)

    return records


def _read_cache() -> dict | None:
    if not NAFTAS_JSON_PATH.exists():
        return None
    try:
        with NAFTAS_JSON_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_cache(datos: list[dict]) -> None:
    _ensure_data_dir()
    payload = {
        "ultima_actualizacion": timezone.now().isoformat(),
        "fuente": SOURCE_URL,
        "datos": datos,
    }
    with NAFTAS_JSON_PATH.open("w", encoding="utf-8") as f:
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
        return (timezone.now() - ts) >= CACHE_TTL
    except (ValueError, TypeError):
        return True


def get_naftas_chart_history() -> dict:
    """
    Retorna {labels, super, premium, gasoil} para el gráfico del modal.
    Últimos 36 meses (3 años).
    """
    cache = _read_cache()

    if _is_cache_stale(cache):
        try:
            datos = _scrape_surtidores()
            if datos:
                _write_cache(datos)
                cache = {"datos": datos}
            else:
                raise ValueError("Scraping devolvió lista vacía.")
        except Exception as exc:
            logger.warning("naftas_historico: scraping falló (%s), usando caché", exc)

    datos = (cache or {}).get("datos") or []
    datos = datos[-36:]

    return {
        "labels":  [d["label"] for d in datos],
        "super":   [d.get("super") for d in datos],
        "premium": [d.get("premium") for d in datos],
        "gasoil":  [d.get("gasoil") for d in datos],
    }
