import os
import pandas as pd
from pandas import DataFrame
from datetime import datetime
import logging
import requests

# --- Helper Function (place this before your extract_data_from_file function or import it) ---
def safe_to_float(value, default=None, field_name_for_logging="<unknown_field>"):
    """
    Safely converts a value to a float, suitable for Numeric database fields.
    Handles None, NaN, strings like '---', empty strings, and strings with commas as thousands separators.
    """
    if pd.isna(value):  # Handles Python None, numpy.nan, pandas.NaT
        return default
    
    if isinstance(value, (int, float)):
        return float(value) # Ensure it's a float type
        
    if isinstance(value, str):
        cleaned_value = value.strip().replace(',', '') # Remove leading/trailing whitespace and commas
        if cleaned_value == '---' or cleaned_value == '':
            return default
        try:
            return float(cleaned_value)
        except ValueError:
            logging.warning(f"Warning: Could not convert string '{value}' (cleaned: '{cleaned_value}') to float for field '{field_name_for_logging}'. Returning default.")
            # In a real application, use logging.warning(...)
            return default
            
    logging.warning(f"Warning: Unhandled type '{type(value)}' for value '{value}' in field '{field_name_for_logging}'. Returning default.")
    return default
# if match_liq and not liquidacion_number and not dia_operacion:
def get_dates_and_liq_in_file(df: DataFrame):
    date = None
    liquidacion_number = None
    dia_operacion = None

    for col in df.columns:
        for cell in df[col].astype(str):
            import re
            # Extract Fecha de Publicacion
            match_date = re.search(r'Fecha de Publicacion:\s*(\d{2}/[a-z]{3}/\d{4})', cell, re.IGNORECASE)
            if match_date and not date:
                date = match_date.group(1)
            # Extract LIQUIDACION and Dia de Operacion
            match_liq = re.search(r'LIQUIDACION\s+(\d+)\s+\(Dia de Operacion:\s*([\d/]+)\)', cell)
            if match_liq and not liquidacion_number and not dia_operacion:
                liquidacion_number = match_liq.group(1)
                dia_operacion = match_liq.group(2)
        if date and liquidacion_number and dia_operacion:
            break
    if date:
        print(f"Found date: {date}")
        # Map Spanish month abbreviations to English
        month_map = {
            'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr', 'may': 'May', 'jun': 'Jun',
            'jul': 'Jul', 'ago': 'Aug', 'sep': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec'
        }
        for es, en in month_map.items():
            if f"/{es}/" in date:
                date = date.replace(f"/{es}/", f"/{en}/")
                break
        print(f"Found date: {date}")
        formatted_date = pd.to_datetime(date, format='%d/%b/%Y').strftime('%Y-%m-%d')
        print(f"Formatted date: {formatted_date}")
    else:
        logging.warning("No date found in the file.")
        return None
    return formatted_date, liquidacion_number, dia_operacion

def find_sistema_row(df: DataFrame) -> int:
    """
    Finds the row index where the 'SISTEMA' column is present.
    """
    sistema_row_idx = None
    for idx, row in df.iterrows():
        if idx > 20:  # Limit to first 20 rows to avoid long processing
            break
        rows = row.values[0].replace('"', '').split(",")
        if 'Sistema' in rows and (' Area' in rows or 'Area' in rows) and (' Generacion (MWh)' in rows or 'Generacion (MWh)' in rows):
            sistema_row_idx = idx + 1
            return sistema_row_idx
    return sistema_row_idx if sistema_row_idx is not None else -1


def extract_data_from_file(download_folder: str) -> str:
    """
    Extracts the file name from the given file path and formats it.
    """
    if not os.path.exists(download_folder):
        logging.error("Files not found in credentials path")
        print("Files not found in credentials path")
        return
    
    # Get files fom the download folder
    files = os.listdir(download_folder)
    print(f"Files found in download folder: {files}")
    if len(files) == 0:
        logging.error("No files found in the download folder")
        return

    all_transformed_records = [] # To store records from all processed files
    for _, file in enumerate(files):
        logging.info(f"Processing file: {file}")
        # Get full path of the file
        file_path = os.path.join(download_folder, file)
        # Check if the file exists
        if not os.path.exists(file_path):
            logging.error(f"File does not exist: {file_path}")
            return

        df = pd.read_csv(file_path, encoding='utf-8', sep=';')
        # Search for the string in all cells and extract the date
        formatted_date, liquidacion_number, dia_operacion = get_dates_and_liq_in_file(df)
        skip_rows_index = find_sistema_row(df)
        if skip_rows_index is None or skip_rows_index < 0:
            logging.info("No SISTEMA row found in the file.")
            return
        
        if skip_rows_index == 0: # This would mean headers are at -1, which is an error
            logging.error("Error: skip_rows_index is 0, header row index would be invalid.")
            return

        new_df = pd.read_csv(file_path, encoding='utf-8', sep=',', skiprows=skip_rows_index)
        new_df.columns = new_df.columns.str.strip()
        new_df = new_df.rename(
            columns={
            "Sistema": "Sistema",
            "Area": "Area",
            "Hora": "Hora",
            "Generacion (MWh)": "Generacion_MWh",
            "Importacion Total (MWh)":  "Importacion_Total_MWh",
            "Exportacion Total (MWh)": "Exportacion_Total_MWh",
            "Intercambio neto entre Gerencias (MWh)": "Intercambio_Neto_Entre_Gerencias_MWh",
            "Estimacion de Demanda por Balance (MWh)": "Estimacion_Demanda_Por_Balance_MWh",
            "Perdidas (MWh)": "Perdidas (MWh)"
            }
        )

        current_timestamp_iso = datetime.now().isoformat()
        file_records_count = 0
        for index, row in new_df.iterrows():
            try:
                hora_val = None
                hora_raw = row.get("Hora") # Uses renamed column "Hora"
                if pd.notna(hora_raw):
                    try:
                        hora_val = int(float(str(hora_raw).strip()))
                        # You might want to validate hora_val here (e.g., 1-24 or 0-23)
                        # For DemandaRealBalanceRecord, if Hora is 1-24 and DB needs 0-23:
                        # if hora_val == 24: hora_val = 0 # Example adjustment
                    except ValueError:
                        logging.error(f"    Warning: Row {index+skip_rows_index+1}: Could not convert Hora '{hora_raw}' to int for {dia_operacion}.")

                record = {
                    "DiaOperacion": dia_operacion,
                    "Sistema": str(row.get("Sistema", "")).strip(),
                    "Area": str(row.get("Area", "")).strip(),
                    "Hora": hora_val,
                    "Generacion_MWh": safe_to_float(row.get("Generacion_MWh"), field_name_for_logging=f"{file_path} R{index} Gen_MWh"),
                    "Importacion_Total_MWh": safe_to_float(row.get("Importacion_Total_MWh"), field_name_for_logging=f"{file_path} R{index} Imp_MWh"),
                    "Exportacion_Total_MWh": safe_to_float(row.get("Exportacion_Total_MWh"), field_name_for_logging=f"{file_path} R{index} Exp_MWh"),
                    "Intercambio_Neto_Entre_Gerencias_MWh": safe_to_float(row.get("Intercambio_Neto_Entre_Gerencias_MWh"), field_name_for_logging=f"{file_path} R{index} IntNeto_MWh"),
                    "Estimacion_Demanda_Por_Balance_MWh": safe_to_float(row.get("Estimacion_Demanda_Por_Balance_MWh"), field_name_for_logging=f"{file_path} R{index} EstDem_MWh"),
                    # "Perdidas_MWh": safe_to_float(row.get("Perdidas_MWh")), # If you add Perdidas_MWh to your model
                    "Liq": liquidacion_number,
                    "FechaPublicacion": formatted_date,
                    "FechaCreacion": current_timestamp_iso,
                    "FechaActualizacion": current_timestamp_iso
                }
                all_transformed_records.append(record)
                file_records_count += 1
            except Exception as e_row:
                logging.error(f"    Error processing row {index} in file {file_path}. Error: {e_row}")
                logging.error(f"    Problematic row data: {row.to_dict()}")
                continue # Skip to the next row in this file


    return all_transformed_records