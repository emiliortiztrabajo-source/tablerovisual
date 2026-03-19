from django.shortcuts import render

from services.adrs import get_dashboard_adrs
from services.combustibles import COMPANIES, DEFAULT_PROVINCE, PROVINCES, get_dashboard_fuel_prices
from services.dolar import get_dashboard_dollar_data
from services.ipc import get_dashboard_inflation_data
from services.provincia_fondos import get_dashboard_provincia_fondos


def dashboard_view(request):
    selected_company = request.GET.get("empresa", "YPF")
    if selected_company not in COMPANIES:
        selected_company = "YPF"

    valid_provinces = {value for value, _label in PROVINCES}
    selected_province = request.GET.get("provincia", DEFAULT_PROVINCE)
    if selected_province not in valid_provinces:
        selected_province = DEFAULT_PROVINCE

    dollar = get_dashboard_dollar_data()
    inflation = get_dashboard_inflation_data()
    fuels = get_dashboard_fuel_prices(selected_company, selected_province)
    adrs = get_dashboard_adrs()
    provincia_fondos = get_dashboard_provincia_fondos()

    top_adrs_resumen = sorted(
        adrs["items"],
        key=lambda item: abs(item.daily_change),
        reverse=True,
    )[:5]
    top_fondos_resumen = sorted(
        provincia_fondos["items"],
        key=lambda item: abs(item.get("variacion_diaria") or 0),
        reverse=True,
    )[:5]

    last_updates = [
        dollar.get("last_update"),
        inflation.get("last_update"),
        fuels.get("last_update"),
        adrs.get("last_update"),
        provincia_fondos["summary"].get("last_update"),
    ]
    general_last_update = max((value for value in last_updates if value is not None), default=None)

    context = {
        "dollar": dollar,
        "inflation": inflation,
        "selected_company": selected_company,
        "selected_province": selected_province,
        "companies": COMPANIES,
        "provinces": PROVINCES,
        "fuels": fuels,
        "adrs": adrs,
        "provincia_fondos": provincia_fondos,
        "top_adrs_resumen": top_adrs_resumen,
        "top_fondos_resumen": top_fondos_resumen,
        "general_last_update": general_last_update,
    }
    return render(request, "dashboard.html", context)
