import os
import logging
from typing import Dict, List
import pandas as pd
from global_utils import (send_data_in_chunks, find_header_row,clean_column_names,extract_fecha_operacion_from_row )
from io import StringIO

logging.basicConfig(level=logging.INFO)

def rename_columns_to_target_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns to match the target structure for Salidas Adelanto data.
    """
    # Create a mapping dictionary for column renaming
    column_mapping = {}
    
    for col in df.columns:
        clean_col = str(col).strip().replace('"', '').strip()
        if 'Tipo de Elemento' in clean_col:
            column_mapping[col] = 'TipoElemento'
        elif 'Tecnologia - Tension' in clean_col:
            column_mapping[col] = 'TecnologiaTension'
        elif 'Gerencia de Control Regional' in clean_col:
            column_mapping[col] = 'GerenciaControlRegional'
        elif 'Unidad de Medida' in clean_col:
            column_mapping[col] = 'UnidadMedida'
        # Date columns (like "13/10/2025") will be handled separately in processing
    
    df_renamed = df.rename(columns=column_mapping)
    return df_renamed

def process_csv_file(file_path: str) -> List[Dict]:
    """
    Processes a single Salidas en Adelanto SEN CSV file and returns a list of dictionaries with the target structure.
    """
    filename = os.path.basename(file_path)
    
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
    
    # Find the line with the headers
    header_line_idx = find_header_row(temp_df, "Tipo de Elemento", "Tecnologia - Tension")
    logging.info(f"Header line index: {header_line_idx}")

    # Extract fecha_publicacion from filename (from "v2025 10 12" part)
    fecha_publicacion = extract_fecha_operacion_from_row(str(temp_df), "Fecha de Publicacion:")
    
    if not fecha_publicacion:
        logging.error(f"Could not extract FechaPublicacion from filename: {filename}")
        return []

    logging.info(f"Processing file: {filename}")
    logging.info(f"FechaPublicacion: {fecha_publicacion}")


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
    
    # fecha_publicacion is already extracted at the top of the function
    
    # Get date columns (skip the first 4 metadata columns)
    date_columns = []
    for col in df.columns[4:]:
        if '/' in str(col):  # Date columns like "13/10/2025"
            date_columns.append(col)
    
    logging.info(f"Found date columns: {date_columns}")
    
    # Convert to list of dictionaries
    result = []
    for _, row in df.iterrows():
        # Skip empty rows
        if pd.isna(row.get('TipoElemento')) or str(row.get('TipoElemento', '')).strip() == '':
            continue
            
        # Get base data for this row
        tipo_elemento = str(row.get('TipoElemento', '')).strip().replace('"', '')
        tecnologia_tension = str(row.get('TecnologiaTension', '')).strip().replace('"', '')
        gerencia_control_regional = str(row.get('GerenciaControlRegional', '')).strip().replace('"', '')
        unidad_medida = str(row.get('UnidadMedida', '')).strip().replace('"', '')
        
        # Process each date column as a separate record
        for date_col in date_columns:
            try:
                valor_str = str(row[date_col]).strip().replace(',', '')
                
                # Skip only if completely empty or invalid
                if valor_str in ['', 'nan']:
                    continue
                    
                valor = float(valor_str)  # This will include 0.0 values
                
                # Convert date column to YYYY-MM-DD format
                # Date columns are like "13/10/2025"
                day, month, year = date_col.split('/')
                fecha_op_for_record = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                
                record = {
                    'fecha_operacion': fecha_op_for_record,
                    'tipo_elemento': tipo_elemento,
                    'tecnologia_tension': tecnologia_tension,
                    'gerencia_control_regional': gerencia_control_regional,
                    'fecha_publicacion': fecha_publicacion,
                    'unidad_medida': unidad_medida,
                    'valor': valor
                }
                result.append(record)
                
            except (ValueError, TypeError) as e:
                logging.warning(f"Error processing value '{row[date_col]}' for date {date_col} in {filename}: {e}")
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

def process_all_csv_files_with_api(download_folder: str, endpoint_url: str) -> Dict[str, int]:
    """
    Processes one file in download folder, sends to API, and deletes successful files.
        Returns a summary of processed vs failed files.
    """
    # Get all CSV files in the download folder
    csv_files = [f for f in os.listdir(download_folder) if f.endswith('.csv')]
    
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