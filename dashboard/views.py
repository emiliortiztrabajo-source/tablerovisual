import unicodedata

from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from services.adrs import get_dashboard_adrs
from services.acciones_internacionales import get_dashboard_acciones_internacionales
from services.combustibles import COMPANIES, DEFAULT_PROVINCE, get_dashboard_fuel_prices
from services.dolar import get_dashboard_dollar_data
from services.indec_precios import get_dashboard_indec_precios
from services.ipc import get_dashboard_inflation_data
from services.noticias_ipc import get_dashboard_noticias_ipc
from services.noticias_pilar import get_dashboard_noticias_pilar
from services.provincia_fondos import get_dashboard_provincia_fondos
from services.riesgo_pais import get_dashboard_riesgo_pais


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
    acciones_internacionales = get_dashboard_acciones_internacionales()
    provincia_fondos = get_dashboard_provincia_fondos()
    indec_precios = get_dashboard_indec_precios()
    riesgo_pais = get_dashboard_riesgo_pais()
    noticias_pilar = get_dashboard_noticias_pilar()
    noticias_ipc = get_dashboard_noticias_ipc()
    primary_ipc_interannual = noticias_ipc["dato_consolidado"]
    primary_ipc_source = "Noticias relevantes"
    if primary_ipc_interannual is None and inflation.get("year_over_year") is not None:
        primary_ipc_interannual = float(inflation["year_over_year"])
        primary_ipc_source = "Serie local"

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
        riesgo_pais.get("last_update"),
        adrs.get("last_update"),
        acciones_internacionales.get("last_update"),
        provincia_fondos["summary"].get("last_update"),
        indec_precios.get("last_update"),
        noticias_pilar.get("last_update"),
        noticias_ipc.get("last_update"),
        *fuel_updates,
    ]
    general_last_update = max((value for value in last_updates if value is not None), default=None)

    context = {
        "dollar": dollar,
        "inflation": inflation,
        "fuel_companies": fuel_companies,
        "fuels": fuels,
        "adrs": adrs,
        "acciones_internacionales": acciones_internacionales["items"],
        "provincia_fondos": provincia_fondos,
        "top_adrs_resumen": top_adrs_resumen,
        "top_fondos_resumen": top_fondos_resumen,
        "general_last_update": general_last_update,
        "priority_fund_name": PRIORITY_FUND_NAME,
        "ultima_actualizacion_acciones": acciones_internacionales["last_update"],
        "ipc_mensual": indec_precios["ipc"].get("variacion_mensual"),
        "ipc_periodo": indec_precios["ipc"].get("periodo"),
        "ipim_mensual": indec_precios["ipim"].get("variacion_mensual"),
        "ipim_periodo": indec_precios["ipim"].get("periodo"),
        "riesgo_pais_valor": riesgo_pais.get("valor"),
        "riesgo_pais_variacion": riesgo_pais.get("variacion"),
        "riesgo_pais_cierre_anterior": riesgo_pais.get("cierre_anterior"),
        "riesgo_pais_last_update": riesgo_pais.get("last_update"),
        "indec_precios_last_update": indec_precios["last_update"],
        "primary_ipc_interannual": primary_ipc_interannual,
        "primary_ipc_source": primary_ipc_source,
        "noticias_pilar": noticias_pilar["items"],
        "noticias_count": noticias_pilar["count"],
        "tema_dominante": noticias_pilar["tema_dominante"],
        "ultima_actualizacion_noticias": noticias_pilar["last_update"],
        "noticias_ipc": noticias_ipc["items"],
        "noticias_ipc_count": noticias_ipc["count"],
        "ipc_noticias_dato_consolidado": noticias_ipc["dato_consolidado_display"],
        "ipc_noticias_coincidencias": noticias_ipc["coincidencias"],
        "ipc_noticias_confianza": noticias_ipc["confidence_level"],
        "ipc_noticias_confianza_display": noticias_ipc["confidence_level"].replace("-", " "),
        "ultima_actualizacion_noticias_ipc": noticias_ipc["last_update"],
    }
    return render(request, "dashboard.html", context)
