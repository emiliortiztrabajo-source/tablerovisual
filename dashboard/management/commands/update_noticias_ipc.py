from django.core.management.base import BaseCommand

from services.noticias_ipc import update_noticias_ipc_json


class Command(BaseCommand):
    help = "Actualiza el JSON de noticias relevantes sobre IPC interanual de Argentina."

    def handle(self, *args, **options):
        result = update_noticias_ipc_json()
        payload = result["payload"]
        dato_consolidado = payload.get("dato_consolidado")
        dato_display = f"{float(dato_consolidado):.1f}%" if dato_consolidado is not None else "Sin consenso"

        self.stdout.write(self.style.SUCCESS("Noticias IPC actualizadas correctamente."))
        self.stdout.write(f"Fuentes consultadas: {result['feeds_consulted']}")
        self.stdout.write(f"Noticias encontradas: {result['found_count']}")
        self.stdout.write(f"Noticias candidatas: {result['candidate_count']}")
        self.stdout.write(f"Noticias usadas: {result['used_count']}")
        self.stdout.write(f"Dato consolidado: {dato_display}")
        self.stdout.write(f"Coincidencias: {payload.get('coincidencias', 0)}")
        self.stdout.write(f"Metodo: {result['method']}")
