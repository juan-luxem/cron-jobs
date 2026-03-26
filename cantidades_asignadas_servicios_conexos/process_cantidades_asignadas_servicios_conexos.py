import logging

from config import ENV
from global_utils.get_download_folder import get_download_folder
from global_utils.notify_error import notify_error

from .extract_data_from_csv import process_all_csv_files_with_api

logging.basicConfig(level=logging.INFO)


def process_cantidades_asignadas_servicios_conexos(
    market_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """
    Processes Cantidades Asignadas de Servicios Conexos data for the specified market type (MDA or MTR).

    Args:
        market_type (str): 'MDA' or 'MTR'
        start_date (str, optional): Start date (YYYY-MM-DD)
        end_date (str, optional): End date (YYYY-MM-DD)
    """
    if market_type not in ["MDA", "MTR"]:
        notify_error(
            f"[Cantidades Asignadas SC] Tipo de mercado invalido: '{market_type}'. Debe ser 'MDA' o 'MTR'."
        )
        return

    API_URL = str(ENV.API_URL)
    API_ENDPOINT = f"{API_URL}api/v1/cantidades-asignadas-servicios-conexos/bulk?market={market_type}"

    download_folder = get_download_folder(start_date=start_date, end_date=end_date)
    date_range_info = f"{start_date} - {end_date}" if start_date and end_date else "fecha actual (modo cron)"

    logging.info(
        f"Starting Cantidades Asignadas de Servicios Conexos {market_type} data processing ({date_range_info})"
    )
    logging.info(f"Download folder: {download_folder}")
    logging.info(f"API endpoint: {API_ENDPOINT}")

    summary = process_all_csv_files_with_api(
        download_folder, API_ENDPOINT, start_date=start_date, end_date=end_date
    )

    if "error" in summary:
        notify_error(
            f"[Cantidades Asignadas SC] Error al procesar archivos {market_type} ({date_range_info}): {summary['error']}"
        )
    else:
        logging.info(f"Final Summary for {market_type} ({date_range_info}):")
        logging.info(f"  Processed: {summary['processed']}/{summary['total']}")
        logging.info(f"  Failed: {summary['failed']}/{summary['total']}")
        logging.info(f"  Remaining: {summary['remaining']}")

        if summary["failed"] > 0:
            notify_error(
                f"[Cantidades Asignadas SC] Fallo en el procesamiento {market_type} ({date_range_info}): "
                f"{summary['failed']} de {summary['total']} archivos fallaron."
            )

    return summary
