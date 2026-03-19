from django.core.management.base import BaseCommand

from services.adrs import sync_adrs_payloads, update_adrs_json


class Command(BaseCommand):
    help = "Actualiza cotizaciones reales de ADRs/acciones argentinas desde InvertirOnline."

    def handle(self, *args, **options):
        result = update_adrs_json()
        saved_count = sync_adrs_payloads(result["payload"].get("adrs", []))

        self.stdout.write(self.style.SUCCESS("ADRs actualizados correctamente."))
        self.stdout.write(f"Activos encontrados: {result['found_count']}")
        self.stdout.write(f"Activos guardados: {saved_count}")
        self.stdout.write(f"Fecha/hora: {result['payload'].get('ultima_actualizacion')}")
        self.stdout.write(f"Fuente: {result['payload'].get('fuente')}")
        self.stdout.write(f"Metodo: {result['method']}")
