from django.core.management.base import BaseCommand

from services.dolar import DOLLAR_JSON_PATH, refresh_dollar_json


class Command(BaseCommand):
    help = "Actualiza manualmente data/dolar_blue.json desde una fuente real de dólar blue."

    def handle(self, *args, **options):
        payload = refresh_dollar_json()
        previous_sell = (
            f"{payload.previous_sell_value}"
            if payload.previous_sell_value is not None
            else "N/D"
        )
        self.stdout.write(self.style.SUCCESS("JSON del dolar actualizado correctamente."))
        self.stdout.write(
            f"Archivo: {DOLLAR_JSON_PATH} | Compra: {payload.buy_value} | Venta: {payload.sell_value}"
        )
        self.stdout.write(
            f"Anterior venta: {previous_sell} | Fuente: {payload.source_label} | "
            f"Actualizacion real: {payload.external_updated_at.isoformat()} | "
            f"Sincronizacion local: {payload.fetched_at.isoformat()}"
        )
