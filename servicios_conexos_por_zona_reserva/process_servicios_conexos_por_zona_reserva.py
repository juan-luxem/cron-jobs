import logging
import os

from config import ENV
from global_utils.get_download_folder import get_download_folder
from global_utils.notify_error import notify_error

from .extract_data_from_csv import process_all_csv_files_with_api

logging.basicConfig(level=logging.INFO)


def process_servicios_conexos_por_zona_reserva(
    market_type: str, start_date: str | None = None, end_date: str | None = None
):
    """
    Processes Servicios Conexos por Zona de Reserva data for the specified market type (MDA).

    Args:
        market_type (str): 'MDA'
    """
    if market_type not in ["MDA"]:
        logging.error(f"❌ Invalid market type: {market_type}. Use 'MDA'.")
        return

    API_URL = str(ENV.API_URL)
    API_ENDPOINT = f"{API_URL}api/v1/servicios-conexos-por-zona-reserva/bulk?"

    download_folder = get_download_folder(start_date=start_date, end_date=end_date)
    os.makedirs(download_folder, exist_ok=True)

    logging.info(
        f"🚀 Starting Servicios Conexos por Zona de Reserva {market_type} data processing"
    )
    logging.info(f"📁 Download folder: {download_folder}")
    logging.info(f"🌐 API endpoint: {API_ENDPOINT}")

    if not os.path.exists(download_folder):
        logging.error(f"❌ Download folder not found: {download_folder}")
        notify_error(
            f"Error en process_servicios_conexos_por_zona_reserva ({market_type}): Download folder not found"
        )
        return

    summary = process_all_csv_files_with_api(
        download_folder=download_folder,
        endpoint_url=API_ENDPOINT,
        start_date=start_date,
        end_date=end_date,
    )

    if "error" in summary:
        logging.error(f"❌ Processing failed: {summary['error']}")
        notify_error(
            f"Error en process_servicios_conexos_por_zona_reserva ({market_type}): {summary['error']}"
        )
    else:
        logging.info(f"🎯 Final Summary for {market_type}:")
        logging.info(
            f"   ✅ Processed: {summary.get('processed', 0)}/{summary.get('total', 0)}"
        )
        logging.info(
            f"   ❌ Failed: {summary.get('failed', 0)}/{summary.get('total', 0)}"
        )
        logging.info(f"   📂 Remaining: {summary.get('remaining', 0)}")

        if (
            summary.get("processed", 0) == summary.get("total", 0)
            and summary.get("remaining", 0) == 0
        ):
            logging.info(f"🎉 All {market_type} files processed successfully!")
        elif summary.get("failed", 0) > 0:
            logging.warning(f"⚠️ Some {market_type} files failed to process")
            notify_error(
                f"Fallo en process_servicios_conexos_por_zona_reserva ({market_type}): {summary.get('failed', 0)} de {summary.get('total', 0)} archivos."
            )

    return summary
