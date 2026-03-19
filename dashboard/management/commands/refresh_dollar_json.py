from django.core.management.base import BaseCommand

from services.dolar import DOLLAR_JSON_PATH, refresh_dollar_json


class Command(BaseCommand):
    help = "Actualiza manualmente data/dolar_blue.json desde BNA Personas."

    def handle(self, *args, **options):
        payload = refresh_dollar_json()
        self.stdout.write(self.style.SUCCESS("JSON del dolar actualizado correctamente."))
        self.stdout.write(
            f"Archivo: {DOLLAR_JSON_PATH} | Compra: {payload.buy_value} | Venta: {payload.sell_value}"
        )
