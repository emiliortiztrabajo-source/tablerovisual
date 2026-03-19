from django.core.management.base import BaseCommand

from services.indec_precios import update_indec_precios_json


class Command(BaseCommand):
    help = "Actualiza el JSON de IPC e IPIM mensuales desde INDEC."

    def handle(self, *args, **options):
        result = update_indec_precios_json()
        payload = result["payload"]
        ipc = payload["ipc"]
        ipim = payload["ipim"]

        self.stdout.write(self.style.SUCCESS("INDEC precios actualizados correctamente."))
        self.stdout.write(
            f"IPC: {ipc['variacion_mensual']}% | Periodo: {ipc['periodo']} | Nivel: {ipc['nivel']}"
        )
        self.stdout.write(
            f"IPIM: {ipim['variacion_mensual']}% | Periodo: {ipim['periodo']} | Nivel: {ipim['nivel']}"
        )
        self.stdout.write(f"Fuente principal: {payload['fuente']}")
        if result["source_url"] != payload["fuente"]:
            self.stdout.write(f"Origen resuelto: {result['source_url']}")
        self.stdout.write(f"Metodo: {result['method']}")
