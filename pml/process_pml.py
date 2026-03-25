import logging

from config import ENV
from global_utils import get_download_folder
from global_utils.notify_error import notify_error
from pml.extract_data_from_csv import process_all_csv_files_with_api

logging.basicConfig(level=logging.INFO)


def process_pml_data(
    market_type: str, start_date: str | None = None, end_date: str | None = None
):
    """
    Processes PML data for the specified market type (MDA or MTR).

    Args:
        market_type (str): 'MDA' or 'MTR'
        start_date (str, optional): Start date for processing (bulk mode)
        end_date (str, optional): End date for processing (bulk mode)
    """
    if market_type not in ["MDA", "MTR"]:
        notify_error(f"[PML] Tipo de mercado invalido: '{market_type}'. Se esperaba 'MDA' o 'MTR'.")
        return

    # Setup paths and URLs
    API_URL = str(ENV.API_URL)  # Convert to string if it's an HttpUrl
    API_ENDPOINT = f"{API_URL}api/v1/pml/bulk?market={market_type}"

    download_folder = get_download_folder(start_date=start_date, end_date=end_date)
    date_range_info = f"{start_date} - {end_date}" if start_date and end_date else "fecha actual (modo cron)"

    logging.info(f"🚀 Starting PML {market_type} data processing")
    logging.info(f"📁 Download folder: {download_folder}")
    logging.info(f"🌐 API endpoint: {API_ENDPOINT}")

    # Process all CSV files and send to API
    summary = process_all_csv_files_with_api(
        download_folder, API_ENDPOINT, start_date=start_date, end_date=end_date
    )

    # Log final summary
    if "error" in summary:
        notify_error(
            f"[PML {market_type}] Error al procesar archivos CSV. "
            f"Rango: {date_range_info}. "
            f"Detalle: {summary['error']}"
        )
    else:
        logging.info(f"🎯 Final Summary for {market_type}:")
        logging.info(f"   ✅ Processed: {summary['processed']}/{summary['total']}")
        logging.info(f"   ❌ Failed: {summary['failed']}/{summary['total']}")
        logging.info(f"   📂 Remaining: {summary['remaining']}")

        if summary["processed"] == summary["total"] and summary["remaining"] == 0:
            logging.info(f"🎉 All {market_type} files processed successfully!")
        elif summary["failed"] > 0:
            notify_error(
                f"[PML {market_type}] Fallo al procesar {summary['failed']} de {summary['total']} archivos. "
                f"Rango: {date_range_info}. "
                f"Archivos restantes en carpeta: {summary['remaining']}."
            )

    return summary
