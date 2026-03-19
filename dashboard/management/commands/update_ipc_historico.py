from django.core.management.base import BaseCommand

from services.ipc import sync_inflation_payload, update_ipc_historico_json


class Command(BaseCommand):
    help = "Actualiza el JSON de IPC historico real y sincroniza la serie local."

    def handle(self, *args, **options):
        result = update_ipc_historico_json()
        synced_count = sync_inflation_payload(result["payload"])
        payload = result["payload"]
        serie = payload.get("serie", [])
        ultimo = serie[-1] if serie else {}

        self.stdout.write(self.style.SUCCESS("IPC historico actualizado correctamente."))
        self.stdout.write(f"Meses guardados: {len(serie)}")
        self.stdout.write(f"Meses sincronizados: {synced_count}")
        self.stdout.write(
            f"Ultimo periodo: {ultimo.get('label')} | Valor mensual: {ultimo.get('valor_mensual')}"
        )
        self.stdout.write(f"Interanual: {payload.get('year_over_year')}")
        self.stdout.write(f"Fuente: {payload.get('fuente')}")
        self.stdout.write(f"Metodo: {result['method']}")
