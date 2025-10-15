from config import ENV
import os
import logging
import pandas as pd
from typing import Dict, List
from config import ENV
from global_utils.send_data_in_chunks import send_data_in_chunks
from global_utils.find_header_row import find_header_row
from global_utils.clean_column_names import clean_column_names
from global_utils.extract_fecha_operacion_from_filename import extract_fecha_operacion_from_filename
from global_utils.extract_sistema_from_file import extract_sistema_from_filename
from global_utils.send_telegram_message import send_telegram_message
from io import StringIO

logging.basicConfig(level=logging.INFO)

def rename_columns_to_target_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns to match the target structure for Generacion Ofertada data.
    Maps CSV columns to the comprehensive database schema with all power blocks and reserve services.
    Assumes columns have already been cleaned by clean_column_names().
    """
    import re
    
    # Create a mapping dictionary for column renaming
    column_mapping = {}
    
    for col in df.columns:
        # Column should already be cleaned by clean_column_names()
        # Just convert to lowercase for comparison
        clean_col = str(col).lower()
        
        # Basic identification fields
        if clean_col == 'codigo':
            column_mapping[col] = 'Codigo'
        elif clean_col == 'hora':
            column_mapping[col] = 'HoraOperacion'
        elif 'estatus' in clean_col and 'asignacion' in clean_col:
            column_mapping[col] = 'EstatusAsignacion'
            
        # Dispatch limits
        elif 'limite' in clean_col and 'despacho' in clean_col and 'maximo' in clean_col:
            column_mapping[col] = 'LimiteDespachoMaximo_MW'
        elif 'limite' in clean_col and 'despacho' in clean_col and 'minimo' in clean_col:
            column_mapping[col] = 'LimiteDespachoMinimo_MW'
        elif 'costo' in clean_col and 'operacion' in clean_col and 'potencia' in clean_col and 'minima' in clean_col:
            column_mapping[col] = 'CostoOperacionPotenciaMinima'
            
        # Power blocks - handle "Bloque de Potencia XX (MW)" pattern
        elif 'bloque' in clean_col and 'potencia' in clean_col and '(mw)' in clean_col:
            # Extract block number using regex
            block_match = re.search(r'bloque\s+de\s+potencia\s+(\d+)', clean_col)
            if block_match:
                block_num = block_match.group(1).zfill(2)  # Format as 01, 02, etc.
                column_mapping[col] = f'BloquePotencia{block_num}_MW'
                
        # Power block costs - handle "Costo Incremental de generacion Bloque XX ($/MWh)" pattern
        elif 'costo' in clean_col and 'incremental' in clean_col and 'generacion' in clean_col and 'bloque' in clean_col:
            # Extract block number using regex
            block_match = re.search(r'bloque\s+(\d+)', clean_col)
            if block_match:
                block_num = block_match.group(1).zfill(2)  # Format as 01, 02, etc.
                column_mapping[col] = f'CostoIncrementalBloque{block_num}_MWh'
        
        # Reserve services - Reserva rodante 10 min (NOT suplementaria, NOT "no rodante")
        elif ('reserva' in clean_col and 'rodante' in clean_col and '10' in clean_col and 'min' in clean_col and 
              'suplementaria' not in clean_col and 'no' not in clean_col):
            if 'costo' in clean_col:
                column_mapping[col] = 'CostoReservaRodante10Min_MW'
            else:
                column_mapping[col] = 'ReservaRodante10Min_MW'
                
        # Reserve services - Reserva no rodante 10 min (must have "no" in it)
        elif ('reserva' in clean_col and 'no' in clean_col and 'rodante' in clean_col and '10' in clean_col and 'min' in clean_col and 
              'suplementaria' not in clean_col):
            if 'costo' in clean_col:
                column_mapping[col] = 'CostoReservaNoRodante10Min_MW'
            else:
                column_mapping[col] = 'ReservaNoRodante10Min_MW'
                
        # Reserve services - Reserva rodante suplementaria (NOT "no rodante")
        elif ('reserva' in clean_col and 'rodante' in clean_col and 'suplementaria' in clean_col and 
              'no' not in clean_col):
            if 'costo' in clean_col:
                column_mapping[col] = 'CostoReservaRodanteSuplementaria_MW'
            else:
                column_mapping[col] = 'ReservaRodanteSuplementaria_MW'
                
        # Reserve services - Reserva no rodante suplementaria (must have "no" in it)
        elif ('reserva' in clean_col and 'no' in clean_col and 'rodante' in clean_col and 'suplementaria' in clean_col):
            if 'costo' in clean_col:
                column_mapping[col] = 'CostoReservaNoRodanteSuplementaria_MW'
            else:
                column_mapping[col] = 'ReservaNoRodanteSuplementaria_MW'
                
        # Reserve services - Reserva regulacion secundaria
        elif 'reserva' in clean_col and 'regulacion' in clean_col and 'secundaria' in clean_col:
            if 'costo' in clean_col:
                column_mapping[col] = 'CostoReservaRegulacionSecundaria_MW'
            else:
                column_mapping[col] = 'ReservaRegulacionSecundaria_MW'
    
    # Apply the mapping
    df_renamed = df.rename(columns=column_mapping)
    
    # Log the mapping for debugging
    if column_mapping:
        logging.info(f"Successfully mapped {len(column_mapping)} columns:")
        for original, mapped in column_mapping.items():
            logging.info(f"  '{original}' → '{mapped}'")
    else:
        logging.warning("❌ No column mappings found - check CSV structure")
        logging.info(f"Available columns: {list(df.columns)}")
    
    return df_renamed

def process_csv_file(file_path: str) -> List[Dict]:
    """
    Processes a single Generacion Ofertada CSV file and returns a list of dictionaries with the target structure.
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
    
    # Find the line with the headers for Generacion Ofertada
    header_line_idx = find_header_row(temp_df, "Codigo", "Hora")
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
            # Helper function to safely convert values
            def safe_float(value, default=0.0):
                try:
                    if pd.isna(value) or value == '' or value is None:
                        return default
                    return float(str(value).replace(',', ''))
                except (ValueError, TypeError):
                    return default
            
            def safe_int(value, default=0):
                try:
                    if pd.isna(value) or value == '' or value is None:
                        return default
                    return int(float(str(value)))
                except (ValueError, TypeError):
                    return default
            
            def safe_string(value, default=''):
                try:
                    if pd.isna(value) or value is None:
                        return default
                    return str(value).strip().replace('"', '')
                except:
                    return default
            
            record = {
                # Basic identification
                'FechaOperacion': fecha_operacion,
                'Codigo': safe_string(row.get('Codigo', '')),
                'HoraOperacion': safe_int(row.get('HoraOperacion', 0)),
                'Sistema': sistema,
                'EstatusAsignacion': safe_string(row.get('EstatusAsignacion', 'ECO')),
                
                # Dispatch limits
                'LimiteDespachoMaximo_MW': safe_float(row.get('LimiteDespachoMaximo_MW', 0)),
                'LimiteDespachoMinimo_MW': safe_float(row.get('LimiteDespachoMinimo_MW', 0)),
                'CostoOperacionPotenciaMinima': safe_float(row.get('CostoOperacionPotenciaMinima', 0)),
                
                # Power blocks (11 blocks)
                'BloquePotencia01_MW': safe_float(row.get('BloquePotencia01_MW', 0)),
                'CostoIncrementalBloque01_MWh': safe_float(row.get('CostoIncrementalBloque01_MWh', 0)),
                'BloquePotencia02_MW': safe_float(row.get('BloquePotencia02_MW', 0)),
                'CostoIncrementalBloque02_MWh': safe_float(row.get('CostoIncrementalBloque02_MWh', 0)),
                'BloquePotencia03_MW': safe_float(row.get('BloquePotencia03_MW', 0)),
                'CostoIncrementalBloque03_MWh': safe_float(row.get('CostoIncrementalBloque03_MWh', 0)),
                'BloquePotencia04_MW': safe_float(row.get('BloquePotencia04_MW', 0)),
                'CostoIncrementalBloque04_MWh': safe_float(row.get('CostoIncrementalBloque04_MWh', 0)),
                'BloquePotencia05_MW': safe_float(row.get('BloquePotencia05_MW', 0)),
                'CostoIncrementalBloque05_MWh': safe_float(row.get('CostoIncrementalBloque05_MWh', 0)),
                'BloquePotencia06_MW': safe_float(row.get('BloquePotencia06_MW', 0)),
                'CostoIncrementalBloque06_MWh': safe_float(row.get('CostoIncrementalBloque06_MWh', 0)),
                'BloquePotencia07_MW': safe_float(row.get('BloquePotencia07_MW', 0)),
                'CostoIncrementalBloque07_MWh': safe_float(row.get('CostoIncrementalBloque07_MWh', 0)),
                'BloquePotencia08_MW': safe_float(row.get('BloquePotencia08_MW', 0)),
                'CostoIncrementalBloque08_MWh': safe_float(row.get('CostoIncrementalBloque08_MWh', 0)),
                'BloquePotencia09_MW': safe_float(row.get('BloquePotencia09_MW', 0)),
                'CostoIncrementalBloque09_MWh': safe_float(row.get('CostoIncrementalBloque09_MWh', 0)),
                'BloquePotencia10_MW': safe_float(row.get('BloquePotencia10_MW', 0)),
                'CostoIncrementalBloque10_MWh': safe_float(row.get('CostoIncrementalBloque10_MWh', 0)),
                'BloquePotencia11_MW': safe_float(row.get('BloquePotencia11_MW', 0)),
                'CostoIncrementalBloque11_MWh': safe_float(row.get('CostoIncrementalBloque11_MWh', 0)),
                
                # Reserve services
                'ReservaRodante10Min_MW': safe_float(row.get('ReservaRodante10Min_MW', 0)),
                'CostoReservaRodante10Min_MW': safe_float(row.get('CostoReservaRodante10Min_MW', 0)),
                'ReservaNoRodante10Min_MW': safe_float(row.get('ReservaNoRodante10Min_MW', 0)),
                'CostoReservaNoRodante10Min_MW': safe_float(row.get('CostoReservaNoRodante10Min_MW', 0)),
                'ReservaRodanteSuplementaria_MW': safe_float(row.get('ReservaRodanteSuplementaria_MW', 0)),
                'CostoReservaRodanteSuplementaria_MW': safe_float(row.get('CostoReservaRodanteSuplementaria_MW', 0)),
                'ReservaNoRodanteSuplementaria_MW': safe_float(row.get('ReservaNoRodanteSuplementaria_MW', 0)),
                'CostoReservaNoRodanteSuplementaria_MW': safe_float(row.get('CostoReservaNoRodanteSuplementaria_MW', 0)),
                'ReservaRegulacionSecundaria_MW': safe_float(row.get('ReservaRegulacionSecundaria_MW', 0)),
                'CostoReservaRegulacionSecundaria_MW': safe_float(row.get('CostoReservaRegulacionSecundaria_MW', 0))
            }
            
            # Validate that we have at least the basic required fields
            if not record['Codigo'] or record['HoraOperacion'] == 0:
                logging.warning(f"Skipping row with missing required fields: Codigo={record['Codigo']}, HoraOperacion={record['HoraOperacion']}")
                continue
                
            result.append(record)
            
        except Exception as e:
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
    Validates that exactly 3 CSV files are present (one for each system: SIN, BCS, BCA).
    Returns a summary of processed vs failed files.
    """
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID
    
    # Get all CSV files in the download folder
    csv_files = [f for f in os.listdir(download_folder) if f.endswith('.csv')]
    
    # Validate exactly 3 CSV files
    if len(csv_files) != 3:
        error_msg = f"❌ Expected exactly 3 CSV files (one for each system: SIN, BCS, BCA), but found {len(csv_files)} files"
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
    
    expected_systems = {'SIN', 'BCS', 'BCA'}
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