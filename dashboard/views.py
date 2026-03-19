import unicodedata

from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from services.adrs import get_dashboard_adrs
from services.combustibles import COMPANIES, DEFAULT_PROVINCE, get_dashboard_fuel_prices
from services.dolar import get_dashboard_dollar_data
from services.ipc import get_dashboard_inflation_data
from services.noticias_pilar import get_dashboard_noticias_pilar
from services.provincia_fondos import get_dashboard_provincia_fondos


PRIORITY_FUND_NAME = "1822 RAICES INVERSION"


def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(normalized.upper().split())


def _prioritize_raices_inversion(items: list[dict]) -> list[dict]:
    priority_items = [
        item for item in items if _normalize_name(item.get("nombre_fondo")) == PRIORITY_FUND_NAME
    ]
    remaining_items = [
        item for item in items if _normalize_name(item.get("nombre_fondo")) != PRIORITY_FUND_NAME
    ]
    return priority_items + remaining_items


def _mark_priority(items: list[dict]) -> list[dict]:
    marked_items = []
    for item in items:
        enriched_item = dict(item)
        enriched_item["is_priority"] = _normalize_name(item.get("nombre_fondo")) == PRIORITY_FUND_NAME
        marked_items.append(enriched_item)
    return marked_items


def _build_fuel_companies() -> list[dict]:
    fuel_companies = []
    for company in COMPANIES:
        fuel_data = get_dashboard_fuel_prices(company, DEFAULT_PROVINCE)
        fuel_companies.append(
            {
                "empresa": company.upper(),
                "provincia": DEFAULT_PROVINCE.title(),
                "super": float(fuel_data["super"].value) if fuel_data.get("super") else None,
                "premium": float(fuel_data["premium"].value) if fuel_data.get("premium") else None,
                "gasoil": float(fuel_data["gasoil"].value) if fuel_data.get("gasoil") else None,
                "fuente": fuel_data.get("source", "N/D"),
                "last_update": fuel_data.get("last_update"),
            }
        )
    return fuel_companies


@login_required
def dashboard_view(request):
    dollar = get_dashboard_dollar_data()
    inflation = get_dashboard_inflation_data()
    fuel_companies = _build_fuel_companies()
    fuels = fuel_companies[0] if fuel_companies else None
    adrs = get_dashboard_adrs()
    provincia_fondos = get_dashboard_provincia_fondos()
    noticias_pilar = get_dashboard_noticias_pilar()

    provincia_fondos["items"] = _mark_priority(
        _prioritize_raices_inversion(provincia_fondos["items"])
    )

    top_adrs_resumen = sorted(
        adrs["items"],
        key=lambda item: abs(item.daily_change),
        reverse=True,
    )[:5]
    top_fondos_resumen = provincia_fondos["items"][:5]

    fuel_updates = [item["last_update"] for item in fuel_companies if item.get("last_update") is not None]
    last_updates = [
        dollar.get("last_update"),
        inflation.get("last_update"),
        adrs.get("last_update"),
        provincia_fondos["summary"].get("last_update"),
        noticias_pilar.get("last_update"),
        *fuel_updates,
    ]
    general_last_update = max((value for value in last_updates if value is not None), default=None)

    context = {
        "dollar": dollar,
        "inflation": inflation,
        "fuel_companies": fuel_companies,
        "fuels": fuels,
        "adrs": adrs,
        "provincia_fondos": provincia_fondos,
        "top_adrs_resumen": top_adrs_resumen,
        "top_fondos_resumen": top_fondos_resumen,
        "general_last_update": general_last_update,
        "priority_fund_name": PRIORITY_FUND_NAME,
        "noticias_pilar": noticias_pilar["items"],
        "noticias_count": noticias_pilar["count"],
        "tema_dominante": noticias_pilar["tema_dominante"],
        "ultima_actualizacion_noticias": noticias_pilar["last_update"],
    }
    return render(request, "dashboard.html", context)
