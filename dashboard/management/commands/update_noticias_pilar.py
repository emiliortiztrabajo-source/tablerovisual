from django.core.management.base import BaseCommand

from services.noticias_pilar import update_noticias_pilar_json


class Command(BaseCommand):
    help = "Actualiza el JSON de noticias relevantes de Pilar."

    def handle(self, *args, **options):
        result = update_noticias_pilar_json()
        self.stdout.write(self.style.SUCCESS("Noticias de Pilar actualizadas correctamente."))
        self.stdout.write(
            f"Noticias: {result['count']} | "
            f"Tema dominante: {result['tema_dominante']} | "
            f"Metodo: {result['method']}"
        )
