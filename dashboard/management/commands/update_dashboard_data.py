from django.core.management.base import BaseCommand

from services.adrs import sync_adrs_data
from services.combustibles import COMPANIES, DEFAULT_PROVINCE, sync_fuel_prices
from services.dolar import sync_dollar_quote
from services.ipc import sync_inflation_data


class Command(BaseCommand):
    help = "Actualiza datos del dashboard financiero con scraping y mocks."

    def handle(self, *args, **options):
        dollar = sync_dollar_quote()
        inflation_count = sync_inflation_data()
        fuel_count = sum(
            sync_fuel_prices(company=company, province=DEFAULT_PROVINCE)
            for company in COMPANIES
        )
        adrs_count = sync_adrs_data()

        self.stdout.write(self.style.SUCCESS("Datos actualizados correctamente."))
        self.stdout.write(
            f"Dolar: {dollar.sell_value} ({dollar.source}) | IPC meses: {inflation_count} | "
            f"Combustibles: {fuel_count} | ADRs: {adrs_count}"
        )
