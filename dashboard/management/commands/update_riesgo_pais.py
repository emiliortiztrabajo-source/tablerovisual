from django.core.management.base import BaseCommand

from services.riesgo_pais import update_riesgo_pais_json


class Command(BaseCommand):
    help = "Actualiza el JSON de Riesgo Pais desde Dolarito."

    def handle(self, *args, **options):
        result = update_riesgo_pais_json()
        riesgo_pais = result["payload"]["riesgo_pais"]

        self.stdout.write(self.style.SUCCESS("Riesgo Pais actualizado correctamente."))
        self.stdout.write(f"Valor: {riesgo_pais.get('valor')}")
        self.stdout.write(f"Variacion: {riesgo_pais.get('variacion')}")
        self.stdout.write(f"Cierre anterior: {riesgo_pais.get('cierre_anterior')}")
        self.stdout.write(f"Fecha/hora: {result['payload'].get('ultima_actualizacion')}")
        self.stdout.write(f"Metodo: {result['method']}")
