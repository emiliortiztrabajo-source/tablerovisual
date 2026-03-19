from django.core.management.base import BaseCommand

from services.adrs import sync_adrs_payloads, update_adrs_json
from services.acciones_internacionales import update_acciones_internacionales_json
from services.combustibles import COMPANIES, DEFAULT_PROVINCE, sync_fuel_prices
from services.dolar import update_dollar_json
from services.indec_precios import update_indec_precios_json
from services.ipc import sync_inflation_payload, update_ipc_historico_json
from services.noticias_ipc import update_noticias_ipc_json
from services.noticias_pilar import update_noticias_pilar_json
from services.provincia_fondos import update_provincia_fondos_json
from services.riesgo_pais import update_riesgo_pais_json


class Command(BaseCommand):
    help = "Actualiza todos los modulos reales del dashboard usando fuentes en vivo y caches JSON."

    def _log_success(self, message: str) -> None:
        self.stdout.write(self.style.SUCCESS(message))

    def _log_warning(self, message: str) -> None:
        self.stdout.write(self.style.WARNING(message))

    def handle(self, *args, **options):
        warnings_count = 0

        try:
            dollar_result = update_dollar_json()
            dollar_payload = dollar_result["payload"]
            self._log_success(
                "Dolar blue | "
                f"venta: {dollar_payload.sell_value} | "
                f"fuente: {dollar_payload.source_label} | "
                f"metodo: {dollar_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"Dolar blue | error: {error}")

        fuel_count = 0
        fuel_errors = 0
        for company in COMPANIES:
            try:
                fuel_count += sync_fuel_prices(company=company, province=DEFAULT_PROVINCE)
            except Exception as error:
                fuel_errors += 1
                warnings_count += 1
                self._log_warning(f"Combustibles {company} | error: {error}")
        self._log_success(
            "Combustibles | "
            f"registros: {fuel_count} | "
            f"fuente: api_ckan | "
            f"fallas: {fuel_errors}"
        )

        try:
            adrs_result = update_adrs_json()
            adrs_saved = sync_adrs_payloads(adrs_result["payload"].get("adrs", []))
            self._log_success(
                "ADRs | "
                f"guardados: {adrs_saved} | "
                f"fuente: {adrs_result['payload'].get('fuente')} | "
                f"metodo: {adrs_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"ADRs | error: {error}")

        try:
            indec_result = update_indec_precios_json()
            ipc = indec_result["payload"]["ipc"]
            ipim = indec_result["payload"]["ipim"]
            self._log_success(
                "INDEC precios | "
                f"IPC: {ipc.get('variacion_mensual')} | "
                f"IPIM: {ipim.get('variacion_mensual')} | "
                f"metodo: {indec_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"INDEC precios | error: {error}")

        try:
            ipc_result = update_ipc_historico_json()
            ipc_count = sync_inflation_payload(ipc_result["payload"])
            self._log_success(
                "IPC historico | "
                f"meses: {ipc_count} | "
                f"interanual: {ipc_result['payload'].get('year_over_year')} | "
                f"metodo: {ipc_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"IPC historico | error: {error}")

        try:
            acciones_result = update_acciones_internacionales_json()
            self._log_success(
                "Acciones internacionales | "
                f"procesadas: {acciones_result['processed_count']} | "
                f"fuente: {acciones_result['source']} | "
                f"fallback: {acciones_result['fallback_mode']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"Acciones internacionales | error: {error}")

        try:
            noticias_pilar_result = update_noticias_pilar_json()
            self._log_success(
                "Noticias Pilar | "
                f"cantidad: {noticias_pilar_result['count']} | "
                f"tema: {noticias_pilar_result['tema_dominante']} | "
                f"metodo: {noticias_pilar_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"Noticias Pilar | error: {error}")

        try:
            noticias_ipc_result = update_noticias_ipc_json()
            self._log_success(
                "Noticias IPC | "
                f"usadas: {noticias_ipc_result['used_count']} | "
                f"dato: {noticias_ipc_result['payload'].get('dato_consolidado')} | "
                f"metodo: {noticias_ipc_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"Noticias IPC | error: {error}")

        try:
            provincia_result = update_provincia_fondos_json()
            self._log_success(
                "Provincia Fondos | "
                f"procesados: {provincia_result['funds_processed']} | "
                f"fuente: {provincia_result['payload'].get('fuente')} | "
                f"metodo: {provincia_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"Provincia Fondos | error: {error}")

        try:
            riesgo_result = update_riesgo_pais_json()
            riesgo = riesgo_result["payload"]["riesgo_pais"]
            self._log_success(
                "Riesgo Pais | "
                f"valor: {riesgo.get('valor')} | "
                f"variacion: {riesgo.get('variacion')} | "
                f"metodo: {riesgo_result['method']}"
            )
        except Exception as error:
            warnings_count += 1
            self._log_warning(f"Riesgo Pais | error: {error}")

        if warnings_count:
            self._log_warning(
                f"Actualizacion general finalizada con advertencias: {warnings_count}"
            )
        else:
            self._log_success("Actualizacion general completada sin advertencias.")
