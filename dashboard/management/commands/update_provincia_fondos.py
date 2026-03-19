from django.core.management.base import BaseCommand

from services.provincia_fondos import update_provincia_fondos_json


class Command(BaseCommand):
    help = "Actualiza el JSON diario de Provincia Fondos."

    def handle(self, *args, **options):
        result = update_provincia_fondos_json()
        self.stdout.write(self.style.SUCCESS("Provincia Fondos actualizado correctamente."))
        self.stdout.write(
            f"Fondos encontrados: {result['funds_found']} | "
            f"Procesados: {result['funds_processed']} | "
            f"Metodo: {result['method']}"
        )
