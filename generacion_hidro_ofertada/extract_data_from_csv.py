from config import ENV
import os
import logging
import pandas as pd
from typing import Dict, List
from global_utils import (send_data_in_chunks, find_header_row, clean_column_names, extract_fecha_operacion_from_filename, extract_sistema_from_filename, send_telegram_message)
from io import StringIO

logging.basicConfig(level=logging.INFO)

def rename_columns_to_target_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns to match the target structure for Hidroelectricas data.
    """
    # This mapping helps to reduce cyclomatic complexity by avoiding a long if/elif chain.
    name_map = {
        'Codigo': 'Codigo',
        'Estatus asignacion': 'EstatusAsignacion',
        'Hora': 'HoraOperacion',
        'Limite de despacho maximo (MW)': 'LimiteDespachoMaximo_MW',
        'Limite de despacho minimo (MW)': 'LimiteDespachoMinimo_MW',
        'Reserva rodante 10 min (MW)': 'ReservaRodante10Min_MW',
        'Costo Reserva rodante 10 min ($/MW)': 'CostoReservaRodante10Min_MW',
        'Reserva no rodante 10 min (MW)': 'ReservaNoRodante10Min_MW',
        'Costo Reserva no rodante 10 min ($/MW)': 'CostoReservaNoRodante10Min_MW',
        'Reserva rodante suplementaria (MW)': 'ReservaRodanteSuplementaria_MW',
        'Costo Reserva rodante suplementaria ($/MW)': 'CostoReservaRodanteSuplementaria_MW',
        'Reserva no rodante suplementaria (MW)': 'ReservaNoRodanteSuplementaria_MW',
        'Costo Reserva no rodante suplementaria ($/MW)': 'CostoReservaNoRodanteSuplementaria_MW',
        'Reserva regulacion secundaria (MW)': 'ReservaRegulacionSecundaria_MW',
        'Costo Reserva regulacion secundaria ($/MW)': 'CostoReservaRegulacionSecundaria_MW',
    }

    rename_dict = {}
    for col in df.columns:
        clean_col = str(col).strip().replace('"', '').strip()
        if clean_col in name_map:
            rename_dict[col] = name_map[clean_col]

    return df.rename(columns=rename_dict)

def process_csv_file(file_path: str) -> List[Dict]:
    """
    Processes a single Hidroelectricas CSV file and returns a list of dictionaries with the target structure.
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
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, 'r', encoding='latin-1') as f:
                content = f.read()
        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")
            return []

    # Split into lines
    lines = content.split('\n')

    # Create a temporary DataFrame to use find_header_row function
    temp_df = pd.DataFrame([line.split(',') for line in lines[:25]])  # Only first 25 lines to find header

    # Find the line with the headers for Servicios Conexos
    header_line_idx = find_header_row(temp_df, "Codigo", "Estatus asignacion")
    logging.info(f"Header line index: {header_line_idx}")

    if header_line_idx == -1:
        logging.warning(f"Could not find header line in file: {filename}")
        return []

    # Create a temporary file with only the data part (headers + data rows)
    data_lines = lines[header_line_idx:]
    temp_csv_content = '\n'.join(data_lines)

    # Now read this as a proper CSV
    try:
        df = pd.read_csv(StringIO(temp_csv_content), encoding='utf-8')
    except Exception as e:
        logging.error(f"Error parsing CSV data from {filename}: {e}")
        return []

    # Clean column names
    df.columns = clean_column_names(df.columns)

    # Rename columns to match target structure
    df = rename_columns_to_target_structure(df)

    # Add Sistema and FechaOperacion to each row
    df['Sistema'] = sistema
    df['FechaOperacion'] = fecha_operacion

    # Convert to list of dictionaries
    result = []
    for _, row in df.iterrows():
        # Skip empty rows
        if pd.isna(row.get('HoraOperacion')) or row.get('HoraOperacion') == '':
            continue

        try:
            record = {
                'FechaOperacion': fecha_operacion,
                'Codigo': str(row.get('Codigo', '')).strip().replace('"', ''),
                'HoraOperacion': int(float(str(row.get('HoraOperacion', 0)))) ,
                'Sistema': sistema,
                'EstatusAsignacion': str(row.get('EstatusAsignacion', '')).strip().replace('"', ''),
                'LimiteDespachoMaximo_MW': float(str(row.get('LimiteDespachoMaximo_MW', 0)).replace(',', '')),
                'LimiteDespachoMinimo_MW': float(str(row.get('LimiteDespachoMinimo_MW', 0)).replace(',', '')),
                'ReservaRodante10Min_MW': float(str(row.get('ReservaRodante10Min_MW', 0)).replace(',', '')),
                'CostoReservaRodante10Min_MW': float(str(row.get('CostoReservaRodante10Min_MW', 0)).replace(',', '')),
                'ReservaNoRodante10Min_MW': float(str(row.get('ReservaNoRodante10Min_MW', 0)).replace(',', '')),
                'CostoReservaNoRodante10Min_MW': float(str(row.get('CostoReservaNoRodante10Min_MW', 0)).replace(',', '')),
                'ReservaRodanteSuplementaria_MW': float(str(row.get('ReservaRodanteSuplementaria_MW', 0)).replace(',', '')),
                'CostoReservaRodanteSuplementaria_MW': float(str(row.get('CostoReservaRodanteSuplementaria_MW', 0)).replace(',', '')),
                'ReservaNoRodanteSuplementaria_MW': float(str(row.get('ReservaNoRodanteSuplementaria_MW', 0)).replace(',', '')),
                'CostoReservaNoRodanteSuplementaria_MW': float(str(row.get('CostoReservaNoRodanteSuplementaria_MW', 0)).replace(',', '')),
                'ReservaRegulacionSecundaria_MW': float(str(row.get('ReservaRegulacionSecundaria_MW', 0)).replace(',', '')),
                'CostoReservaRegulacionSecundaria_MW': float(str(row.get('CostoReservaRegulacionSecundaria_MW', 0)).replace(',', '')),
            }
            result.append(record)
        except (ValueError, TypeError) as e:
            logging.warning(f"Error processing row in {filename}: {e}")
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
            logging.warning(f"⚠️ Data sent successfully but failed to delete file {filename}: {e}")
            return True  # Still consider this a success since data was sent
    else:
        logging.error(f"❌ Failed to process {filename} - file kept for retry")
        return False

def process_all_csv_files_with_api(download_folder: str, endpoint_url: str):
    """
    Processes all CSV files in the download folder, sends to API, and deletes successful files.
    Returns a summary of processed vs failed files.
    """
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID

    # Get all CSV files in the download folder
    csv_files = [f for f in os.listdir(download_folder) if f.endswith('.csv')]

    # Validate exactly 1 CSV file
    if len(csv_files) != 1:
        error_msg = f"❌ Expected exactly 1 CSV files (for system: SIN), but found {len(csv_files)} files"
        logging.error(error_msg)
        send_telegram_message(
            bot_token,
            chat_id,
            error_msg
        )
        # Clean up files on validation error
        for csv_file in csv_files:
            file_path = os.path.join(download_folder, csv_file)
            try:
                os.remove(file_path)
                logging.info(f"Removed file after validation error: {file_path}")
            except OSError as e:
                logging.warning(f"Failed to remove file {file_path}: {e}")
        if len(csv_files) == 0:
            logging.info("ℹ️ No CSV files found in download folder")
        else:
            logging.info(f"📁 Found files: {csv_files}")
        return {"processed": 0, "failed": 0, "total": len(csv_files), "error": error_msg}

    logging.info(f"📁 Found {len(csv_files)} CSV files to process (validation passed)")

    # Verify we have one file for each system
    found_systems = set()
    for csv_file in csv_files:
        sistema = extract_sistema_from_filename(csv_file)
        if sistema:
            found_systems.add(sistema)

    expected_systems = {'SIN'}
    if found_systems != expected_systems:
        missing_systems = expected_systems - found_systems
        extra_systems = found_systems - expected_systems
        error_msg = f"❌ System validation failed. Missing: {missing_systems}, Extra: {extra_systems}"
        logging.error(error_msg)
        send_telegram_message(
            bot_token,
            chat_id,
            error_msg
        )
        # Clean up files on validation error
        for csv_file in csv_files:
            file_path = os.path.join(download_folder, csv_file)
            try:
                os.remove(file_path)
                logging.info(f"Removed file after validation error: {file_path}")
            except OSError as e:
                logging.warning(f"Failed to remove file {file_path}: {e}")
        return {"processed": 0, "failed": 0, "total": len(csv_files), "error": error_msg}

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
    remaining_files = [f for f in os.listdir(download_folder) if f.endswith('.csv')]

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
        "remaining": len(remaining_files)
    }
