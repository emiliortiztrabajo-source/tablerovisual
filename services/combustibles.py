from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import requests
import urllib3
from django.db.models import Max

from dashboard.models import FuelPrice


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CKAN_BASE_URL = "https://datos.energia.gob.ar/api/3/action"
FUEL_DATASET_ID = "precios-en-surtidor"
DEFAULT_PROVINCE = "BUENOS AIRES"
DATASTORE_PAGE_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 30

COMPANIES = ["YPF", "Shell", "Axion"]
COMPANY_BRAND_MAP = {
    "YPF": "YPF",
    "Shell": "SHELL C.A.P.S.A.",
    "Axion": "AXION",
}
PROVINCES = [
    ("BUENOS AIRES", "Buenos Aires"),
    ("CIUDAD AUTONOMA DE BUENOS AIRES", "CABA"),
    ("CATAMARCA", "Catamarca"),
    ("CHACO", "Chaco"),
    ("CHUBUT", "Chubut"),
    ("CORDOBA", "Cordoba"),
    ("CORRIENTES", "Corrientes"),
    ("ENTRE RIOS", "Entre Rios"),
    ("FORMOSA", "Formosa"),
    ("JUJUY", "Jujuy"),
    ("LA PAMPA", "La Pampa"),
    ("LA RIOJA", "La Rioja"),
    ("MENDOZA", "Mendoza"),
    ("MISIONES", "Misiones"),
    ("NEUQUEN", "Neuquen"),
    ("RIO NEGRO", "Rio Negro"),
    ("SALTA", "Salta"),
    ("SAN JUAN", "San Juan"),
    ("SAN LUIS", "San Luis"),
    ("SANTA CRUZ", "Santa Cruz"),
    ("SANTA FE", "Santa Fe"),
    ("SANTIAGO DEL ESTERO", "Santiago del Estero"),
    ("TIERRA DEL FUEGO", "Tierra del Fuego"),
    ("TUCUMAN", "Tucuman"),
]


@dataclass(frozen=True)
class FuelPayload:
    company: str
    province: str
    fuel_type: str
    value: Decimal
    period_date: date


def _ckan_request(action: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{CKAN_BASE_URL}/{action}"
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.SSLError:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
        response.raise_for_status()
        payload = response.json()

    if not payload.get("success"):
        raise RuntimeError(f"La API CKAN devolvio error en {action}.")
    return payload["result"]


def get_fuel_dataset_metadata() -> dict[str, Any]:
    return _ckan_request("package_show", {"id": FUEL_DATASET_ID})


def get_fuel_resource_id() -> str:
    metadata = get_fuel_dataset_metadata()
    resources = metadata.get("resources", [])
    active_resources = [resource for resource in resources if resource.get("datastore_active")]
    current_resource = next(
        (
            resource
            for resource in active_resources
            if "vigentes" in (resource.get("name") or "").lower()
        ),
        active_resources[0],
    )
    return current_resource["id"]


def inspect_fuel_fields() -> dict[str, Any]:
    resource_id = get_fuel_resource_id()
    result = _ckan_request(
        "datastore_search",
        {"resource_id": resource_id, "limit": 1},
    )
    records = result.get("records", [])
    first_record = records[0] if records else {}
    return {
        "resource_id": resource_id,
        "fields": [field["id"] for field in result.get("fields", [])],
        "first_record": first_record,
    }


def _normalize_company(company: str) -> str:
    if company not in COMPANY_BRAND_MAP:
        raise ValueError(f"Empresa no soportada: {company}")
    return company


def _normalize_province(province: str) -> str:
    valid_values = {value for value, _label in PROVINCES}
    return province if province in valid_values else DEFAULT_PROVINCE


def _classify_fuel(product_name: str) -> str | None:
    normalized = product_name.casefold()
    if "premium" in normalized:
        return "premium"
    if "super" in normalized or "súper" in normalized:
        return "super"
    if "gas oil" in normalized or "gasoil" in normalized:
        return "gasoil"
    return None


def _decimal_average(values: list[Decimal]) -> Decimal:
    average = sum(values) / Decimal(len(values))
    return average.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def fetch_fuel_records(company: str, province: str) -> list[dict[str, Any]]:
    normalized_company = _normalize_company(company)
    normalized_province = _normalize_province(province)
    resource_id = get_fuel_resource_id()
    filters = {
        "provincia": normalized_province,
        "empresabandera": COMPANY_BRAND_MAP[normalized_company],
    }

    offset = 0
    total = None
    records: list[dict[str, Any]] = []
    while total is None or offset < total:
        result = _ckan_request(
            "datastore_search",
            {
                "resource_id": resource_id,
                "limit": DATASTORE_PAGE_SIZE,
                "offset": offset,
                "filters": json.dumps(filters),
            },
        )
        batch = result.get("records", [])
        total = result.get("total", 0)
        records.extend(batch)
        if not batch:
            break
        offset += len(batch)
    return records


def fetch_fuel_prices(company: str = "YPF", province: str = DEFAULT_PROVINCE) -> list[FuelPayload]:
    records = fetch_fuel_records(company=company, province=province)
    grouped_prices: dict[str, list[Decimal]] = defaultdict(list)
    latest_timestamp: datetime | None = None

    for record in records:
        fuel_type = _classify_fuel(str(record.get("producto", "")))
        if fuel_type is None:
            continue

        price_value = record.get("precio")
        if price_value in (None, ""):
            continue

        grouped_prices[fuel_type].append(Decimal(str(price_value)))

        timestamp_value = record.get("fecha_vigencia")
        if timestamp_value:
            timestamp = _parse_timestamp(str(timestamp_value))
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp

    if latest_timestamp is None:
        latest_timestamp = datetime.now()

    payloads: list[FuelPayload] = []
    for fuel_type in ("super", "premium", "gasoil"):
        if grouped_prices[fuel_type]:
            payloads.append(
                FuelPayload(
                    company=company,
                    province=province,
                    fuel_type=fuel_type,
                    value=_decimal_average(grouped_prices[fuel_type]),
                    period_date=latest_timestamp.date(),
                )
            )
    return payloads


def sync_fuel_prices(company: str = "YPF", province: str = DEFAULT_PROVINCE) -> int:
    payloads = fetch_fuel_prices(company=company, province=province)
    for payload in payloads:
        FuelPrice.objects.update_or_create(
            company=payload.company,
            province=payload.province,
            fuel_type=payload.fuel_type,
            date=payload.period_date,
            defaults={"value": payload.value},
        )
    return len(payloads)


def _get_cached_fuel_prices(company: str, province: str):
    return FuelPrice.objects.filter(company=company, province=province).order_by("fuel_type", "-date", "-updated_at")


def get_dashboard_fuel_prices(company: str, province: str = DEFAULT_PROVINCE) -> dict[str, Any]:
    normalized_company = _normalize_company(company)
    normalized_province = _normalize_province(province)

    try:
        sync_fuel_prices(company=normalized_company, province=normalized_province)
        source = "api_ckan"
    except Exception:
        source = "cache_local"

    prices = _get_cached_fuel_prices(normalized_company, normalized_province)
    latest_by_type: dict[str, FuelPrice] = {}
    for price in prices:
        latest_by_type.setdefault(price.fuel_type, price)

    latest_items = list(latest_by_type.values())
    last_update = max((item.updated_at for item in latest_items), default=None)
    effective_date = prices.aggregate(last_date=Max("date"))["last_date"]

    return {
        "super": latest_by_type.get("super"),
        "premium": latest_by_type.get("premium"),
        "gasoil": latest_by_type.get("gasoil"),
        "last_update": last_update,
        "effective_date": effective_date,
        "province": normalized_province,
        "source": source,
    }
