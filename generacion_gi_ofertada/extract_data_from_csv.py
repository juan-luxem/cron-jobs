import os
import re
import logging
import requests
from datetime import datetime
from typing import List, Dict
from pandas import DataFrame, to_datetime
import pandas as pd

def extract_system_from_filename(filename: str) -> str | None:
    """
    Extract the system (BCS, BCA, or SIN) from the filename.
    
    Args:
        filename (str): The filename to extract system from
        
    Returns:
        str | None: The system name (BCS, BCA, or SIN) or None if not found
        
    Example:
        filename = "ProgGenGI BCS MTR_Expost Dia 2025-05-08 v2025 07 07_01 20 01"
        returns: "BCS"
    """
    try:
        # Remove file extension if present
        filename_no_ext = os.path.splitext(filename)[0]
        
        # Look for system patterns in the filename
        # Pattern 1: After "ProgGenGI" (most common case)
        match = re.search(r'ProgGenGI\s+(BCS|BCA|SIN)\s+', filename_no_ext, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        # Pattern 2: Just look for the systems anywhere in the filename
        systems = ['BCS', 'BCA', 'SIN']
        for system in systems:
            if re.search(rf'\b{system}\b', filename_no_ext, re.IGNORECASE):
                return system.upper()
        
        logging.warning(f"Could not extract system from filename: {filename}")
        return None
        
    except Exception as e:
        logging.error(f"Error extracting system from filename {filename}: {e}")
        return None

def find_sistema_row(df: DataFrame) -> int:
    """
    Finds the row index where the 'SISTEMA' column is present.
    """
    sistema_row_idx = None
    for idx, row in df.iterrows():
        if idx > 12:  # Limit to first 12 rows to avoid long processing
            break
        rows = row.values[0].replace('"', '').split(",")
        if 'Codigo' in rows and ('Hora' in rows or ' Hora' in rows) and (' Potencia Media (MW)' in rows or 'Potencia Media (MW)' in rows):
            sistema_row_idx = idx + 1
            return sistema_row_idx
    return sistema_row_idx if sistema_row_idx is not None else -1

def get_dates_in_file(df: DataFrame) -> str | None:
    date = None

    for col in df.columns:
        for cell in df[col].astype(str):
            import re
            # Extract Fecha de Publicacion
            match_date = re.search(r'Fecha:\s*(\d{2}/[a-z]{3}/\d{4})', cell, re.IGNORECASE)
            if match_date and not date:
                date = match_date.group(1)
        if date:
            break
    if date:
        # Map Spanish month abbreviations to English
        month_map = {
            'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr', 'may': 'May', 'jun': 'Jun',
            'jul': 'Jul', 'ago': 'Aug', 'sep': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec'
        }
        for es, en in month_map.items():
            if f"/{es}/" in date:
                date = date.replace(f"/{es}/", f"/{en}/")
                break
        formatted_date = to_datetime(date, format='%d/%b/%Y').strftime('%Y-%m-%d')
    else:
        logging.warning("No date found in the file.")
        return None
    return formatted_date


def extract_data_from_csv(file_path: str) -> List[Dict]:
    """
    Extract data from CSV file and convert to the required format.
    """
    try:
        # Read CSV with pandas to handle the data section
        df = pd.read_csv(file_path, encoding='utf-8', sep=';')

        # Extract operation date
        dia_operacion = get_dates_in_file(df)
        print(f"Extracted operation date: {dia_operacion}")
        if not dia_operacion:
            logging.error(f"Could not extract date from {file_path}")
            return []

        skip_rows_index = find_sistema_row(df)
        print(f"Skip rows index: {skip_rows_index}")
        if skip_rows_index is None or skip_rows_index < 0:
            logging.info("No SISTEMA row found in the file.")
            return
    
        if skip_rows_index == 0: # This would mean headers are at -1, which is an error
            logging.error("Error: skip_rows_index is 0, header row index would be invalid.")
            return

        # Extract the system from the filename
        system = extract_system_from_filename(os.path.basename(file_path))
        print(f"Extracted system: {system}")
        if not system:
            logging.error(f"Could not extract system from filename: {file_path}")
            return []

        # Extract data from the DataFrame
        df = pd.read_csv(file_path, encoding='utf-8', sep=',', skiprows=skip_rows_index)
        df.columns = df.columns.str.strip()

        # Define column mapping
        column_mapping = {
            'Codigo': 'Codigo',
            'Hora': 'HoraOperacion',
            'Potencia Media (MW)': 'PotenciaMedia_MW'
        }
        
        # Check if all required columns exist
        missing_columns = []
        for col in column_mapping.keys():
            if col not in df.columns:
                missing_columns.append(col)
        
        if missing_columns:
            logging.error(f"Missing columns in {file_path}: {missing_columns}")
            return []
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        data_list = []
        for _, row in df.iterrows():
            try:
                record = {
                    'DiaOperacion': dia_operacion,
                    'Sistema': system,
                    "Codigo": str(row['Codigo']).strip(),  # Ensure Codigo is a string
                    'HoraOperacion': int(row['HoraOperacion']),
                    'PotenciaMedia_MW': float(row['PotenciaMedia_MW']),
                    'Fecha_Creacion': datetime.now().isoformat(sep=' '),  # Add creation date
                    'Fecha_Actualizacion': datetime.now().isoformat(sep=' ')  # Add update date
                }
                data_list.append(record)
            except (ValueError, TypeError) as e:
                logging.warning(f"Error converting row data in {file_path}: {e}")
                continue
        
        logging.info(f"Extracted {len(data_list)} records from {file_path}")
        return data_list
        
    except Exception as e:
        logging.error(f"Error processing CSV file {file_path}: {e}")
        return []

def process_all_csv_files(download_folder: str) -> List[Dict]:
    """
    Process all CSV files in the download folder and return combined data.
    """
    if not os.path.exists(download_folder):
        logging.error(f"Download folder does not exist: {download_folder}")
        return []
    
    csv_files = [f for f in os.listdir(download_folder) if f.endswith('.csv')]
    
    if not csv_files:
        logging.warning(f"No CSV files found in {download_folder}")
        return []
    
    logging.info(f"Found {len(csv_files)} CSV files to process")
    
    if len(csv_files) != 3:
        logging.warning(f"Expected 3 CSV files, found {len(csv_files)}. This may cause issues in processing.")
        return []
    
    all_data = []
    for csv_file in csv_files:
        file_path = os.path.join(download_folder, csv_file)
        logging.info(f"Processing file: {csv_file}")
        
        file_data = extract_data_from_csv(file_path)
        if file_data:
            all_data.extend(file_data)
            logging.info(f"Added {len(file_data)} records from {csv_file}")
        else:
            logging.warning(f"No data extracted from {csv_file}")
    
    logging.info(f"Total records extracted: {len(all_data)}")
    return all_data


def send_data_to_endpoint(data: List[Dict], endpoint_url: str = "") -> bool:
    """
    Send the extracted data to a specified endpoint via POST request.
    """
    if not endpoint_url:
        logging.warning("No endpoint URL provided. Skipping POST request.")
        return False
    
    if not data:
        logging.warning("No data to send.")
        return False
    
    try:
        headers = {
            'Content-Type': 'application/json'
        }
        
        response = requests.post(endpoint_url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200 or response.status_code == 201:
            print(f"Successfully sent {len(data)} records to endpoint")
            logging.info(f"Successfully sent {len(data)} records to endpoint")
            return True
        else:
            logging.error(f"Failed to send data. Status code: {response.status_code}, Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending POST request: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error sending data: {e}")
        return False
