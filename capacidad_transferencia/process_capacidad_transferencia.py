import logging

from capacidad_transferencia.extract_data_from_csv import (
    process_all_csv_files_with_api,
)
from config import ENV
from global_utils.get_download_folder import get_download_folder
from global_utils.notify_error import notify_error


def process_capacidad_transferencia_data(
    start_date: str | None = None, end_date: str | None = None
) -> None:
    """
    Processes Capacidad de Transferencia MDA data.

    Args:
        start_date (str, optional): Start date for filtering files (YYYY-MM-DD)
        end_date (str, optional): End date for filtering files (YYYY-MM-DD)
    """
    # Setup paths and URLs
    API_URL = str(ENV.API_URL)
    if not API_URL.endswith("/"):
        API_URL += "/"
    API_ENDPOINT = f"{API_URL}api/v1/capacidad-transferencia-mda/bulk"
    download_folder = get_download_folder(start_date=start_date, end_date=end_date)
    date_range_info = f"{start_date} - {end_date}" if start_date and end_date else "fecha actual (modo cron)"

    logging.info("🚀 Starting Capacidad de Transferencia MDA data processing")
    logging.info(f"📂 Download folder: {download_folder}")
    logging.info(f"🔗 API endpoint: {API_ENDPOINT}")

    # Process all CSV files and send to API
    summary = process_all_csv_files_with_api(
        download_folder, API_ENDPOINT, start_date=start_date, end_date=end_date
    )

    # Log final summary
    if "error" in summary:
        notify_error(
            f"[Capacidad de Transferencia] Error al procesar archivos CSV. "
            f"Rango: {date_range_info}. "
            f"Detalle: {summary['error']}"
        )
    else:
        logging.info("📊 Final Summary:")
        logging.info(
            f"✅ Processed: {summary.get('processed', 0)}/{summary.get('total', 0)}"
        )
        logging.info(f"❌ Failed: {summary.get('failed', 0)}/{summary.get('total', 0)}")
        logging.info(f"⏳ Remaining: {summary.get('remaining', 0)}")

        if (
            summary.get("processed", 0) == summary.get("total", 0)
            and summary.get("remaining", 0) == 0
            and summary.get("total", 0) > 0
        ):
            logging.info("✅ All files processed successfully!")
        elif summary.get("failed", 0) > 0:
            notify_error(
                f"[Capacidad de Transferencia] Fallo al procesar {summary.get('failed', 0)} de {summary.get('total', 0)} archivos. "
                f"Rango: {date_range_info}. "
                f"Archivos restantes en carpeta: {summary.get('remaining', 0)}."
            )
        elif summary.get("total", 0) == 0:
            logging.info("ℹ️ No files were found to process.")
