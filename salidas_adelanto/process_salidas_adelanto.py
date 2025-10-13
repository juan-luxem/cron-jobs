from config import ENV
import os
import logging
from salidas_adelanto.extract_data_from_csv import process_all_csv_files_with_api

logging.basicConfig(level=logging.INFO)

def process_salidas_adelanto():
    """
    Processes Salidas Adelanto (Scheduled Outages) data.
    """
    # Setup paths and URLs
    API_URL = str(ENV.API_URL)  # Convert to string if it's an HttpUrl
    API_ENDPOINT = f"{API_URL}api/v1/salidas-forzadas/bulk/insert?batch_size=1000&validate=true"
    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    logging.info(f"🚀 Starting Salidas Adelanto data processing")
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
        logging.info(f"🎯 Final Summary for Salidas Adelanto:")
        logging.info(f"   ✅ Processed: {summary['processed']}/{summary['total']}")
        logging.info(f"   ❌ Failed: {summary['failed']}/{summary['total']}")
        logging.info(f"   📂 Remaining: {summary['remaining']}")
        
        if summary['processed'] == summary['total'] and summary['remaining'] == 0:
            logging.info(f"🎉 All Salidas Adelanto files processed successfully!")
        elif summary['failed'] > 0:
            logging.warning(f"⚠️ Some Salidas Adelanto files failed to process")

    return summary