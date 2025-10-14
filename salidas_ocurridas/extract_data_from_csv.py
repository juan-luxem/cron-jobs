import os
import logging
from typing import Dict, List
import pandas as pd
from global_utils import (send_data_in_chunks, find_header_row, clean_column_names, extract_fecha_operacion_from_row)
from io import StringIO

logging.basicConfig(level=logging.INFO)

def rename_columns_to_target_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns to match the target structure for Salidas Ocurridas data.
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
        # Type columns (like "Forzadas 12/10/2025", "Por Programa 12/10/2025") will be handled separately in processing
    
    df_renamed = df.rename(columns=column_mapping)
    return df_renamed

def process_csv_file(file_path: str) -> List[Dict]:
    """
    Processes a single Salidas Ocurridas SEN CSV file and returns a list of dictionaries with the target structure.
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
    
    # Extract fecha_publicacion from CSV content
    fecha_publicacion = extract_fecha_operacion_from_row(content, "Fecha de Publicacion:")
    
    if not fecha_publicacion:
        logging.error(f"Could not extract FechaPublicacion from CSV content: {filename}")
        return []

    logging.info(f"Processing file: {filename}")
    logging.info(f"FechaPublicacion: {fecha_publicacion}")
    
    # Split into lines
    lines = content.split('\n')
    
    # Create a temporary DataFrame to use find_header_row function
    temp_df = pd.DataFrame([line.split(',') for line in lines[:25]])  # Only first 25 lines to find header
    
    # Find the line with the headers
    header_line_idx = find_header_row(temp_df, "Tipo de Elemento", "Tecnologia - Tension")
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
    
    # Get type columns (skip the first 4 metadata columns)
    # Look for "Forzadas" and "Por Programa" columns
    type_columns = []
    fecha_operacion = None
    
    for col in df.columns[4:]:
        col_str = str(col).strip()
        if 'Forzadas' in col_str or 'Por Programa' in col_str:
            type_columns.append(col)
            # Extract fecha_operacion from first type column found (they should have same date)
            if not fecha_operacion:
                # Extract date from column like "Forzadas 12/10/2025"
                import re
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', col_str)
                if date_match:
                    date_str = date_match.group(1)
                    day, month, year = date_str.split('/')
                    fecha_operacion = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    logging.info(f"Found type columns: {type_columns}")
    logging.info(f"Extracted fecha_operacion: {fecha_operacion}")
    
    if not fecha_operacion:
        logging.error(f"Could not extract fecha_operacion from type columns in {filename}")
        return []
    
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
        
        # Process each type column as a separate record
        for type_col in type_columns:
            try:
                valor_str = str(row[type_col]).strip().replace(',', '')
                
                # Skip only if completely empty or invalid
                if valor_str in ['', 'nan']:
                    continue
                    
                valor = float(valor_str)  # This will include 0.0 values
                
                # Determine tipo_medida from column name
                col_str = str(type_col).strip()
                if 'Forzadas' in col_str:
                    tipo_medida = 'Forzada'
                elif 'Por Programa' in col_str:
                    tipo_medida = 'Programada'
                else:
                    logging.warning(f"Unknown type column: {col_str}")
                    continue
                
                record = {
                    'fecha_operacion': fecha_operacion,
                    'tipo_elemento': tipo_elemento,
                    'tecnologia_tension': tecnologia_tension,
                    'gerencia_control_regional': gerencia_control_regional,
                    'tipo_medida': tipo_medida,  # NEW FIELD
                    'fecha_publicacion': fecha_publicacion,
                    'unidad_medida': unidad_medida,
                    'valor': valor
                }
                result.append(record)
                
            except (ValueError, TypeError) as e:
                logging.warning(f"Error processing value '{row[type_col]}' for type {type_col} in {filename}: {e}")
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
    if not os.path.exists(download_folder):
        logging.error(f"❌ Download folder not found: {download_folder}")
        return {"processed": 0, "failed": 0, "total": 0, "error": "Download folder not found"}
    
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