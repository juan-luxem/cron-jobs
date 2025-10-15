from config import ENV
import os
import logging
from global_utils import send_telegram_message

logging.basicConfig(level=logging.INFO)

def delete_csv_files_after_process():
    """
    Deletes all CSV files in the specified download folder.
    """
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID
    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    if not os.path.exists(download_folder):
        logging.error(f"❌ Download folder not found: {download_folder}")
        send_telegram_message(
            bot_token,
            chat_id,
            f"Error in delete_csv_files_after_process: Download folder not found",
        )
        return

    if os.path.exists(download_folder):
        files = os.listdir(download_folder)
        if len(files) > 0:
            for file in files:
                file_path = os.path.join(download_folder, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    logging.info(f"Removed file: {file_path}")
        else:
                logging.info("No files to remove.")
