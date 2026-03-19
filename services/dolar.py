from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal

import requests
from django.conf import settings
from django.utils import timezone

from dashboard.models import DollarQuote


BLUE_SOURCE_URL = "https://dolarapi.com/v1/dolares/blue"
DATA_DIR = settings.BASE_DIR / "data"
DOLLAR_JSON_PATH = DATA_DIR / "dolar_blue.json"
REQUEST_TIMEOUT_SECONDS = 15
AUTO_REFRESH_INTERVAL = timedelta(minutes=10)


@dataclass(frozen=True)
class DollarPayload:
    buy_value: Decimal
    sell_value: Decimal
    external_updated_at: datetime
    fetched_at: datetime
    source: str
    source_label: str
    source_url: str
    status: str = "live"
    previous_buy_value: Decimal | None = None
    previous_sell_value: Decimal | None = None
    previous_updated_at: datetime | None = None
    error_message: str | None = None


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_aware(value: datetime) -> datetime:
    if timezone.is_naive(value):
        return timezone.make_aware(value, timezone.get_current_timezone())
    return value.astimezone(timezone.get_current_timezone())


def _parse_timestamp(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    normalized = raw_value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return _ensure_aware(parsed)


def _serialize_payload(payload: DollarPayload) -> dict:
    previous_snapshot = None
    if (
        payload.previous_buy_value is not None
        and payload.previous_sell_value is not None
        and payload.previous_updated_at is not None
    ):
        previous_snapshot = {
            "compra": float(payload.previous_buy_value),
            "venta": float(payload.previous_sell_value),
            "ultima_actualizacion": payload.previous_updated_at.isoformat(),
        }

    return {
        "compra": float(payload.buy_value),
        "venta": float(payload.sell_value),
        "ultima_actualizacion": payload.external_updated_at.isoformat(),
        "ultima_sincronizacion": payload.fetched_at.isoformat(),
        "fuente": payload.source,
        "fuente_nombre": payload.source_label,
        "fuente_url": payload.source_url,
        "estado": payload.status,
        "anterior": previous_snapshot,
    }


def _deserialize_payload(data: dict, default_status: str = "cached") -> DollarPayload:
    external_updated_at = _parse_timestamp(data.get("ultima_actualizacion"))
    if external_updated_at is None:
        raise ValueError("El JSON local del dolar no contiene una fecha válida.")

    fetched_at = _parse_timestamp(data.get("ultima_sincronizacion")) or external_updated_at

    previous_snapshot = data.get("anterior") or {}
    previous_updated_at = _parse_timestamp(previous_snapshot.get("ultima_actualizacion"))

    return DollarPayload(
        buy_value=Decimal(str(data.get("compra", 0))),
        sell_value=Decimal(str(data.get("venta", 0))),
        external_updated_at=external_updated_at,
        fetched_at=fetched_at,
        source=str(data.get("fuente", "dolarapi_blue")),
        source_label=str(data.get("fuente_nombre", "DolarApi Blue")),
        source_url=str(data.get("fuente_url", BLUE_SOURCE_URL)),
        status=str(data.get("estado", default_status)),
        previous_buy_value=(
            Decimal(str(previous_snapshot.get("compra")))
            if previous_snapshot.get("compra") is not None
            else None
        ),
        previous_sell_value=(
            Decimal(str(previous_snapshot.get("venta")))
            if previous_snapshot.get("venta") is not None
            else None
        ),
        previous_updated_at=previous_updated_at,
    )


def read_dollar_json() -> DollarPayload | None:
    if not DOLLAR_JSON_PATH.exists():
        return None

    try:
        with DOLLAR_JSON_PATH.open("r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        return _deserialize_payload(data)
    except (json.JSONDecodeError, ValueError, TypeError, OSError):
        return None


def write_dollar_json(payload: DollarPayload) -> None:
    _ensure_data_dir()
    with DOLLAR_JSON_PATH.open("w", encoding="utf-8") as json_file:
        json.dump(_serialize_payload(payload), json_file, ensure_ascii=False, indent=2)


def _fetch_blue_quote_from_api() -> DollarPayload:
    response = requests.get(
        BLUE_SOURCE_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            )
        },
    )
    response.raise_for_status()
    data = response.json()

    external_updated_at = _parse_timestamp(data.get("fechaActualizacion"))
    if external_updated_at is None:
        raise ValueError("La fuente blue no devolvió una fecha válida.")

    return DollarPayload(
        buy_value=Decimal(str(data["compra"])),
        sell_value=Decimal(str(data["venta"])),
        external_updated_at=external_updated_at,
        fetched_at=timezone.now(),
        source="dolarapi_blue",
        source_label="DolarApi Blue",
        source_url=BLUE_SOURCE_URL,
        status="live",
    )


def _merge_with_previous_snapshot(
    current_payload: DollarPayload,
    previous_payload: DollarPayload | None,
) -> DollarPayload:
    if previous_payload is None:
        return current_payload

    if (
        current_payload.buy_value == previous_payload.buy_value
        and current_payload.sell_value == previous_payload.sell_value
        and current_payload.external_updated_at == previous_payload.external_updated_at
    ):
        return replace(
            current_payload,
            previous_buy_value=previous_payload.previous_buy_value,
            previous_sell_value=previous_payload.previous_sell_value,
            previous_updated_at=previous_payload.previous_updated_at,
        )

    return replace(
        current_payload,
        previous_buy_value=previous_payload.buy_value,
        previous_sell_value=previous_payload.sell_value,
        previous_updated_at=previous_payload.external_updated_at,
    )


def _calculate_variation_from_payload(payload: DollarPayload) -> Decimal:
    if payload.previous_sell_value in (None, Decimal("0"), Decimal("0.00")):
        return Decimal("0.00")

    variation = (
        (payload.sell_value - payload.previous_sell_value) / payload.previous_sell_value
    ) * Decimal("100")
    return variation.quantize(Decimal("0.01"))


def _is_payload_stale(payload: DollarPayload | None) -> bool:
    if payload is None:
        return True
    return (timezone.now() - payload.fetched_at) >= AUTO_REFRESH_INTERVAL


def _sync_payload_to_db(payload: DollarPayload) -> DollarQuote:
    variation_daily = _calculate_variation_from_payload(payload)
    quote = (
        DollarQuote.objects.filter(
            external_updated_at=payload.external_updated_at,
            buy_value=payload.buy_value,
            sell_value=payload.sell_value,
        )
        .order_by("-updated_at")
        .first()
    )

    if quote is None:
        quote = DollarQuote.objects.create(
            date=payload.external_updated_at.date(),
            value=payload.sell_value,
            buy_value=payload.buy_value,
            sell_value=payload.sell_value,
            variation_daily=variation_daily,
            source=payload.source_label,
            external_updated_at=payload.external_updated_at,
        )
        return quote

    fields_to_update: list[str] = []
    if quote.value != payload.sell_value:
        quote.value = payload.sell_value
        fields_to_update.append("value")
    if quote.variation_daily != variation_daily:
        quote.variation_daily = variation_daily
        fields_to_update.append("variation_daily")
    if quote.source != payload.source_label:
        quote.source = payload.source_label
        fields_to_update.append("source")
    if fields_to_update:
        fields_to_update.append("updated_at")
        quote.save(update_fields=fields_to_update)

    return quote


def refresh_dollar_json() -> DollarPayload:
    previous_payload = read_dollar_json()
    live_payload = _fetch_blue_quote_from_api()
    merged_payload = _merge_with_previous_snapshot(live_payload, previous_payload)
    write_dollar_json(merged_payload)
    _sync_payload_to_db(merged_payload)
    return merged_payload


def update_dollar_json() -> dict:
    try:
        payload = refresh_dollar_json()
        return {
            "payload": payload,
            "method": "live_api",
        }
    except Exception as error:
        cached_payload = read_dollar_json()
        if cached_payload is None:
            raise

        fallback_payload = replace(
            cached_payload,
            status="fallback_json",
            error_message=str(error),
        )
        _sync_payload_to_db(fallback_payload)
        return {
            "payload": fallback_payload,
            "method": "fallback_json",
            "error": str(error),
        }


def sync_dollar_quote() -> DollarQuote:
    result = update_dollar_json()
    payload = result["payload"]
    return _sync_payload_to_db(payload)


def _payload_to_dashboard_data(payload: DollarPayload) -> dict:
    is_fallback = payload.status == "fallback_json"
    status_label = "Fallback local" if is_fallback else "Fuente en vivo"
    return {
        "buy_value": payload.buy_value,
        "sell_value": payload.sell_value,
        "variation_daily": _calculate_variation_from_payload(payload),
        "last_update": payload.external_updated_at,
        "last_sync": payload.fetched_at,
        "source": payload.source,
        "source_label": payload.source_label,
        "source_url": payload.source_url,
        "status": payload.status,
        "status_label": status_label,
        "is_fallback": is_fallback,
        "error_message": payload.error_message,
    }


def get_dashboard_dollar_data() -> dict:
    cached_payload = read_dollar_json()

    if _is_payload_stale(cached_payload):
        try:
            live_payload = refresh_dollar_json()
            return _payload_to_dashboard_data(live_payload)
        except Exception as error:
            if cached_payload is not None:
                fallback_payload = replace(
                    cached_payload,
                    status="fallback_json",
                    error_message=str(error),
                )
                return _payload_to_dashboard_data(fallback_payload)

            latest_quote = DollarQuote.objects.order_by("-external_updated_at", "-updated_at").first()
            if latest_quote is not None:
                payload_from_db = DollarPayload(
                    buy_value=latest_quote.buy_value,
                    sell_value=latest_quote.sell_value,
                    external_updated_at=latest_quote.external_updated_at,
                    fetched_at=latest_quote.updated_at,
                    source="db_fallback",
                    source_label=latest_quote.source or "Histórico local",
                    source_url=BLUE_SOURCE_URL,
                    status="fallback_db",
                )
                return _payload_to_dashboard_data(payload_from_db)
            raise

    return _payload_to_dashboard_data(cached_payload)
