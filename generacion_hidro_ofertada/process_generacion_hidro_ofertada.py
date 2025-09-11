from config import ENV
import os
import logging
from .extract_data_from_csv import process_all_csv_files_with_api

logging.basicConfig(level=logging.INFO)


def process_generacion_hidro_ofertada(market_type: str):
    """
    Processes Ofertas de Venta – Hidroeléctricas data for the specified market type (MDA or MTR).

    Args:
        market_type (str): 'MDA' or 'MTR'
    """
    if market_type not in ["MDA", "MTR"]:
        logging.error(f"❌ Invalid market type: {market_type}. Use 'MDA' or 'MTR'.")
        return

    # Setup paths and URLs
    API_URL = str(ENV.API_URL)  # Convert to string if it's an HttpUrl
    API_ENDPOINT = (
        f"{API_URL}api/v1/generacion-hidro-ofertada/bulk?market={market_type}"
    )

    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    logging.info(
        f"🚀 Starting Ofertas de Venta – Hidroeléctricas {market_type} data processing"
    )
    logging.info(f"📁 Download folder: {download_folder}")
    logging.info(f"🌐 API endpoint: {API_ENDPOINT}")

    if not os.path.exists(download_folder):
        logging.error(f"❌ Download folder not found: {download_folder}")
        return

    # Process all CSV files and send to API
    summary = process_all_csv_files_with_api(download_folder, API_ENDPOINT)

    # Log final summary
    if "error" in summary:
        logging.error(f"❌ Processing failed: {summary['error']}")
    else:
        logging.info(f"🎯 Final Summary for {market_type}:")
        logging.info(f"   ✅ Processed: {summary['processed']}/{summary['total']}")
        logging.info(f"   ❌ Failed: {summary['failed']}/{summary['total']}")
        logging.info(f"   📂 Remaining: {summary['remaining']}")

        if summary["processed"] == summary["total"] and summary["remaining"] == 0:
            logging.info(f"🎉 All {market_type} files processed successfully!")
        elif summary["failed"] > 0:
            logging.warning(f"⚠️ Some {market_type} files failed to process")

    return summary
