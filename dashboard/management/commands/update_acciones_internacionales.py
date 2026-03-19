from django.core.management.base import BaseCommand

from services.acciones_internacionales import update_acciones_internacionales_json


class Command(BaseCommand):
    help = "Actualiza el cache diario de acciones internacionales."

    def handle(self, *args, **options):
        result = update_acciones_internacionales_json()
        payload = result["payload"]

        self.stdout.write(self.style.SUCCESS("Acciones internacionales actualizadas correctamente."))
        self.stdout.write(f"Acciones procesadas: {result['processed_count']}")
        self.stdout.write(f"Actualizacion: {payload.get('ultima_actualizacion')}")
        self.stdout.write(f"Fuente: {result['source']}")
        self.stdout.write(f"Fallback: {result['fallback_mode']}")
        if result["errors"]:
            self.stdout.write(f"Errores: {len(result['errors'])}")
