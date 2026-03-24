import logging
import os

from config import ENV
from global_utils import get_download_folder
from global_utils.send_telegram_message import send_telegram_message
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
        logging.error(f"❌ Invalid market type: {market_type}. Use 'MDA' or 'MTR'.")
        return

    # Setup paths and URLs
    API_URL = str(ENV.API_URL)  # Convert to string if it's an HttpUrl
    API_ENDPOINT = f"{API_URL}api/v1/pml/bulk?market={market_type}"
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID

    download_folder = get_download_folder(start_date=start_date, end_date=end_date)
    os.makedirs(download_folder, exist_ok=True)

    logging.info(f"🚀 Starting PML {market_type} data processing")
    logging.info(f"📁 Download folder: {download_folder}")
    logging.info(f"🌐 API endpoint: {API_ENDPOINT}")

    if not os.path.exists(download_folder):
        logging.error(f"❌ Download folder not found: {download_folder}")
        send_telegram_message(
            bot_token,
            chat_id,
            f"Error en process_pml_data ({market_type}): Download folder not found",
        )
        return

    # Process all CSV files and send to API
    summary = process_all_csv_files_with_api(
        download_folder, API_ENDPOINT, start_date=start_date, end_date=end_date
    )

    # Log final summary
    if "error" in summary:
        logging.error(f"❌ Processing failed: {summary['error']}")
        send_telegram_message(
            bot_token,
            chat_id,
            f"Error en process_pml_data ({market_type}): {summary['error']}",
        )
    else:
        logging.info(f"🎯 Final Summary for {market_type}:")
        logging.info(f"   ✅ Processed: {summary['processed']}/{summary['total']}")
        logging.info(f"   ❌ Failed: {summary['failed']}/{summary['total']}")
        logging.info(f"   📂 Remaining: {summary['remaining']}")

        if summary["processed"] == summary["total"] and summary["remaining"] == 0:
            logging.info(f"🎉 All {market_type} files processed successfully!")
        elif summary["failed"] > 0:
            logging.warning(f"⚠️ Some {market_type} files failed to process")
            send_telegram_message(
                bot_token,
                chat_id,
                f"Fallo en process_pml_data ({market_type}): {summary['failed']} de {summary['total']} archivos.",
            )

    return summary
