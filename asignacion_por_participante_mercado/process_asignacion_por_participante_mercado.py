import logging
import os

from config import ENV
from global_utils import get_download_folder
from global_utils.notify_error import notify_error

from .extract_data_from_csv import process_all_csv_files_with_api

logging.basicConfig(level=logging.INFO)


def process_asignacion_por_participante_mercado(
    market_type="MDA", start_date=None, end_date=None
):
    """
    Processes Asignación por Participante del Mercado data.
    """

    # Setup paths and URLs
    API_URL = str(ENV.API_URL)  # Convert to string if it's an HttpUrl
    API_ENDPOINT = f"{API_URL}api/v1/asignacion-por-participante-mercado/bulk?"

    download_folder = get_download_folder(start_date=start_date, end_date=end_date)

    logging.info(f"📁 Download folder: {download_folder}")
    logging.info(f"🌐 API endpoint: {API_ENDPOINT}")

    if not os.path.exists(download_folder):
        notify_error(f"❌ Download folder not found: {download_folder}")
        return

    # Process all CSV files and send to API
    summary = process_all_csv_files_with_api(
        download_folder, API_ENDPOINT, start_date=start_date, end_date=end_date
    )

    # Log final summary
    if "error" in summary:
        notify_error(
            f"Error en process_asignacion_por_participante_mercado: {summary['error']}"
        )
    else:
        logging.info(f"✅ Processed: {summary['processed']}/{summary['total']}")
        logging.info(f"❌ Failed: {summary['failed']}/{summary['total']}")
        logging.info(f"📂 Remaining: {summary['remaining']}")

        if summary["processed"] == summary["total"] and summary["remaining"] == 0:
            logging.info("🎉 All files processed successfully!")
        elif summary["failed"] > 0:
            logging.warning("⚠️ Some files failed to process")
            notify_error(
                f"Fallo en process_asignacion_por_participante_mercado: {summary['failed']} de {summary['total']} archivos."
            )

    return summary
