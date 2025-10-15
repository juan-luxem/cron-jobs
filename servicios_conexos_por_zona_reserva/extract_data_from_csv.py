from config import ENV
import os
import logging
import pandas as pd
from typing import Dict, List
from global_utils import (
    send_data_in_chunks,
    find_header_row,
    clean_column_names,
    extract_fecha_operacion_from_filename,
    extract_sistema_from_filename,
    send_telegram_message
)
from io import StringIO

logging.basicConfig(level=logging.INFO)


def rename_columns_to_target_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns to match the target structure for ServiciosConexosPorZonaDeReserva data.
    """
    column_mapping = {
        "Zona de Reserva": "ZonaReserva",
        "Hora": "HoraOperacion",
        "Reserva de Regulacion Secundaria (MW)": "ReservaRegulacionSecundaria_MW",
        "Reserva Rodante de 10 minutos (MW)": "ReservaRodante10Min_MW",
        "Reserva de 10 minutos (MW)": "Reserva10Min_MW",
        "Reserva Suplementaria (MW)": "ReservaSuplementaria_MW",
    }
    df_renamed = df.rename(columns=column_mapping)
    return df_renamed


def process_csv_file(file_path: str) -> List[Dict]:
    """
    Processes a single ServiciosConexosPorZonaDeReserva CSV file and returns a list of dictionaries with the target structure.
    """
    filename = os.path.basename(file_path)

    # Extract Sistema and FechaOperacion from filename
    sistema = extract_sistema_from_filename(filename)
    fecha_operacion = extract_fecha_operacion_from_filename(filename)

    if not sistema:
        logging.error(f"Could not extract Sistema from filename: {filename}")
        return []

    if not fecha_operacion:
        logging.error(f"Could not extract FechaOperacion from filename: {filename}")
        return []

    logging.info(f"Processing file: {filename}")
    logging.info(f"Sistema: {sistema}")
    logging.info(f"FechaOperacion: {fecha_operacion}")

    # Read CSV file with error handling for mixed column counts
    try:
        # First, read the file line by line to handle mixed structures
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="latin-1") as f:
                content = f.read()
        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")
            return []

    # Split into lines
    lines = content.split("\n")

    # Create a temporary DataFrame to use find_header_row function
    temp_df = pd.DataFrame([line.split(",") for line in lines if line][:25])

    # Find the line with the headers for AsignacionPorParticipanteMercado
    header_line_idx = find_header_row(temp_df, "Zona de Reserva", "Hora")
    logging.info(f"Header line index: {header_line_idx}")

    if header_line_idx == -1:
        logging.warning(f"Could not find header line in file: {filename}")
        return []

    # Create a temporary file with only the data part (headers + data rows)
    data_lines = lines[header_line_idx:]
    temp_csv_content = "\n".join(data_lines)

    # Now read this as a proper CSV
    try:
        df = pd.read_csv(StringIO(temp_csv_content), encoding="utf-8")
    except Exception as e:
        logging.error(f"Error parsing CSV data from {filename}: {e}")
        return []

    # Clean column names
    df.columns = clean_column_names(df.columns)

    # Rename columns to match target structure
    df = rename_columns_to_target_structure(df)

    # Add Sistema and FechaOperacion to each row
    df["Sistema"] = sistema
    df["FechaOperacion"] = fecha_operacion

    # Convert to list of dictionaries
    result = []
    for _, row in df.iterrows():
        # Skip empty rows
        if pd.isna(row.get("HoraOperacion")) or row.get("HoraOperacion") == "":
            continue

        try:
            record = {
                "FechaOperacion": fecha_operacion,
                "ZonaReserva": str(row.get("ZonaReserva", "")).strip(),
                "HoraOperacion": int(float(str(row.get("HoraOperacion", 0)))),
                "Sistema": sistema,
                "ReservaRegulacionSecundaria_MW": float(
                    str(row.get("ReservaRegulacionSecundaria_MW", 0)).replace(",", "")
                ),
                "ReservaRodante10Min_MW": float(
                    str(row.get("ReservaRodante10Min_MW", 0)).replace(",", "")
                ),
                "Reserva10Min_MW": float(
                    str(row.get("Reserva10Min_MW", 0)).replace(",", "")
                ),
                "ReservaSuplementaria_MW": float(
                    str(row.get("ReservaSuplementaria_MW", 0)).replace(",", "")
                ),
            }
            result.append(record)
        except (ValueError, TypeError) as e:
            logging.warning(f"Error processing row in {filename}: {row}. Error: {e}")
            continue

    logging.info(f"Processed {len(result)} records from {filename}")
    return result


def process_and_send_csv_file(file_path: str, endpoint_url: str) -> bool:
    """
    Processes a single CSV file, sends data to API in chunks, and deletes file if successful.
    Returns True if successful, False otherwise.
    """
    filename = os.path.basename(file_path)
    logging.info(f"\n🔄 Processing file: {filename}")

    # Process the CSV file
    processed_data = process_csv_file(file_path)
    logging.info(f"Processed {len(processed_data)} records from {filename}")

    if not processed_data:
        logging.error(f"❌ No data extracted from {filename}")
        return False

    # Send data to API in chunks
    success = send_data_in_chunks(processed_data, endpoint_url, chunk_size=2000)

    if success:
        try:
            # Delete the file after successful API call
            os.remove(file_path)
            logging.info(f"✅ File {filename} processed and deleted successfully")
            return True
        except OSError as e:
            logging.warning(
                f"⚠️ Data sent successfully but failed to delete file {filename}: {e}"
            )
            return True  # Still consider this a success since data was sent
    else:
        logging.error(f"❌ Failed to process {filename} - file kept for retry")
        return False


def process_all_csv_files_with_api(
    download_folder: str, endpoint_url: str
) -> Dict[str, int]:
    """
    Processes all CSV files in the download folder, sends to API, and deletes successful files.
    Validates that exactly 3 CSV files are present (one for each system: SIN, BCS, BCA).
    Returns a summary of processed vs failed files.
    """
    # Get all CSV files in the download folder
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID
    csv_files = [f for f in os.listdir(download_folder) if f.endswith(".csv")]

    # Validate exactly 3 CSV files
    if len(csv_files) != 3:
        error_msg = f"❌ Expected exactly 3 CSV files (one for each system: SIN, BCS, BCA), but found {len(csv_files)} files"
        logging.error(error_msg)
        send_telegram_message(
            bot_token,
            chat_id,
            f"Error en process_all_csv_files_with_api: {error_msg}",
        )
        if len(csv_files) == 0:
            logging.info("ℹ️ No CSV files found in download folder")
        else:
            logging.info(f"📁 Found files: {csv_files}")
        return {
            "processed": 0,
            "failed": 0,
            "total": len(csv_files),
            "error": error_msg,
        }

    logging.info(f"📁 Found {len(csv_files)} CSV files to process (validation passed)")

    # Verify we have one file for each system
    found_systems = set()
    for csv_file in csv_files:
        sistema = extract_sistema_from_filename(csv_file)
        if sistema:
            found_systems.add(sistema)

    expected_systems = {"SIN", "BCS", "BCA"}
    if found_systems != expected_systems:
        missing_systems = expected_systems - found_systems
        extra_systems = found_systems - expected_systems
        error_msg = f"❌ System validation failed. Missing: {missing_systems}, Extra: {extra_systems}"
        logging.error(error_msg)
        send_telegram_message(
            bot_token,
            chat_id,
            f"Error en process_all_csv_files_with_api: {error_msg}",
        )
        return {
            "processed": 0,
            "failed": 0,
            "total": len(csv_files),
            "error": error_msg,
        }

    logging.info(f"✅ System validation passed: Found files for {found_systems}")

    processed_count = 0
    failed_count = 0

    for csv_file in csv_files:
        file_path = os.path.join(download_folder, csv_file)

        success = process_and_send_csv_file(file_path, endpoint_url)

        if success:
            processed_count += 1
        else:
            failed_count += 1

    # Check if download folder is empty
    remaining_files = [f for f in os.listdir(download_folder) if f.endswith(".csv")]

    logging.info(f"\n📊 Processing Summary:")
    logging.info(f"✅ Successfully processed: {processed_count}")
    logging.info(f"❌ Failed: {failed_count}")
    logging.info(f"📂 Remaining CSV files: {len(remaining_files)}")

    if len(remaining_files) == 0:
        logging.info("🎉 Download folder is now empty!")
    else:
        logging.warning(f"⚠️ {len(remaining_files)} files remain in download folder")
        for file in remaining_files:
            logging.info(f"   - {file}")

    return {
        "processed": processed_count,
        "failed": failed_count,
        "total": len(csv_files),
        "remaining": len(remaining_files),
    }
