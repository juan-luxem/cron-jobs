import os
import logging
import pandas as pd
from datetime import datetime
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from utils import (
    extract_date,
    parse_spanish_date,
    check_date_exists,
    preprocess_csv,
    send_dataframe_to_api,
    get_sistema_from_filename,
)

API_TARGET_SOURCE_PMLMDA = "pml_mda"


# Example usage:

def get_pml_mda_from_files():
    load_dotenv()
    API_URL = os.getenv("API_URL")

    """
    Get PML MDA from files.
    """
    # logging.info("Starting PML MDA process")
    # # Get the current date
    # today = datetime.now()
    # # Get the current date in the format YYYYMMDD
    # today_str = today.strftime("%Y%m%d")
    # # Get the current time in the format HHMMSS
    # now_str = today.strftime("%H%M%S")
    
    # Get the path to the files
    path = os.path.join(os.path.dirname(__file__), "files")
    
    # Check if the path exists
    if not os.path.exists(path):
        logging.error(f"Path {path} does not exist")
        return
    
    # Get the list of files in the path
    files = os.listdir(path)

    if not files:
        logging.error(f"No files found in path {path}")
        return
    
    # Loop through the files and process them
    for file in files:
        # Check if the file is a CSV file
        if file.endswith(".csv"):
            # Get the full path to the file
            file_path = os.path.join(path, file)
            print(file)
            full_path = os.path.join(path, file)
            date= extract_date(full_path)
            dt, target_date_str_iso = parse_spanish_date(date)
            if not dt or not target_date_str_iso:
                # delete_csv_files(".")
                return 
            print(date)
            print("dt", dt)
            print("target_date_str_iso", target_date_str_iso)

            sistema = get_sistema_from_filename(file)
            df = preprocess_csv(file_path=full_path, system_name=sistema)
            df["Fecha"] = dt
            df = df.rename(
                columns={
                    "Clave del nodo": "Clave",
                    "Precio marginal local": "PML",
                    "Componente de energia": "Energia",
                    "Componente de perdidas": "Perdidas",
                    "Componente de congestion": "Congestion",
                }
            )
            BATCH_SIZE = (
                # 100  # <<< Choose a batch size (e.g., 100, 250, 500) - Tune this!
                # 800  # <<< Choose a batch size (e.g., 100, 250, 500) - Tune this!
                # 8000  # <<< Choose a batch size (e.g., 100, 250, 500) - Tune this!
                22000  # <<< Choose a batch size (e.g., 100, 250, 500) - Tune this!
                # 12000  # <<< Choose a batch size (e.g., 100, 250, 500) - Tune this!
            )
            num_records = len(df)
            num_batches = (
                num_records + BATCH_SIZE - 1
            ) // BATCH_SIZE  # Calculate needed batches

            overall_success = True  # Track if all batches succeeded
            records_successfully_sent_count = 0
            logging.info("--- Starting API Upload ---")
            logging.info(f"Total records to send: {num_records}")
            logging.info(f"Batch size: {BATCH_SIZE}")
            logging.info(f"Number of batches: {num_batches}")

            for i in range(num_batches):
                start_index = i * BATCH_SIZE
                # end_index calculation ensures we don't go past the end of the DataFrame
                end_index = min(start_index + BATCH_SIZE, num_records)

                # Select the batch from the DataFrame using row indices
                df_batch = df.iloc[start_index:end_index]

                logging.info(
                    f"--- Sending Batch {i + 1}/{num_batches} (Records {start_index + 1}-{end_index}) ---"
                )

                if df_batch.empty:
                    logging.warning(f"Batch {i + 1} is empty, skipping.")
                    continue

                # Call the existing function with the smaller batch DataFrame
                batch_success = send_dataframe_to_api(
                    df_batch, API_URL, API_TARGET_SOURCE_PMLMDA
                )
                if batch_success:
                    logging.info(f"Batch {i + 1}/{num_batches} sent successfully.")
                    records_successfully_sent_count += len(df_batch)
                else:
                    logging.error(
                        f"Failed to send Batch {i + 1}/{num_batches}. Check logs above."
                    )
                    overall_success = False
            logging.info("--- API Upload Summary ---")
            logging.info(f"Total records from processed files: {num_records}")
            logging.info(
                f"Total records successfully sent in batches: {records_successfully_sent_count}"
            )
            if overall_success and records_successfully_sent_count == num_records:
                logging.info("All batches appear to have been sent successfully.")
                # Optional: Save the combined CSV locally only if everything was sent
                # Send Telegram Success Message
                message = (
                    f"PML_MTR Script Success: Successfully uploaded {records_successfully_sent_count} "
                    f"records for date {target_date_str_iso}."
                )
                # send_telegram_message(bot_token, chat_id, message)
                # delete_csv_files(".")  # Replace "." with the target directory path
                print(
                    f"All batches appear to have been sent successfully. "
                    f"Total records successfully sent: {records_successfully_sent_count}"
                )
                if os.path.exists(file_path):
                    logging.info(f"File {file_path} deleted after processing.")
                    os.remove(file_path)
                else:
                    logging.error(f"File {file_path} not found for deletion.")
                logging.info(f"Deleted file {file_path} after processing.")
                print(
                    f"Deleted file {file_path} after processing."
                )
                # return True  # Indicate overall success
            else:
                logging.error(
                    "One or more batches failed to send, or record counts mismatch."
                )
                # Send Telegram Alert
                message = (
                    f"PML_MDA Script Error: Date mismatch between files "
                    f"{target_date_str_iso} and API. "
                    # f"SIN({date_sin}), BCA({date_bca}), BCS({date_bcs}). Aborting upload."
                )
                # send_telegram_message(bot_token, chat_id, message)
                # delete_csv_files(".")  # Replace "." with the target directory path
                print(
                    f"One or more batches failed to send, or record counts mismatch. "
                    f"Total records successfully sent: {records_successfully_sent_count}"
                )
                return False  # Indicate failure

            # Process the file
            # print(f"Processing file {file_path}")
            # process_file(file_path)
        else:
            logging.error(f"File {file} is not a CSV file")

get_pml_mda_from_files()