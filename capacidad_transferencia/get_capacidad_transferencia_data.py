import time
import glob
import requests
import pandas as pd
import os
import logging

def get_capacidad_transferencia_data(csv_directory):
    # --- Configuration ---
    # *** IMPORTANT: Ensure this endpoint accepts a LIST of records in the JSON body ***
    API_URL = os.getenv("API_URL")
    API_ENDPOINT = f"{API_URL}/api/v1/capacidad_transferencia"
    # Directory containing the CSV files
    # Number of rows to skip at the beginning of each CSV
    CSV_SEPARATOR = ',' 

    # --- Header Mapping (CSV Header -> API JSON Key / SQL Column) ---
    # Keys: MUST exactly match the header strings in the CSV file after skipping rows
    # Values: MUST exactly match the JSON keys expected by the API (likely matching SQL columns)
    HEADER_MAP = {
        # Check for leading/trailing spaces in your actual CSV headers!
        'Sistema ': 'Sistema',
        ' Fecha': 'FechaOperacion', # Assuming CSV header has leading space
        ' Enlace': 'Enlace',         # Assuming CSV header has leading space
        ' Horario': 'Horario',       # Assuming CSV header has leading space
        # Map the long CSV headers to the shorter SQL column names
        ' Capacidad de Transferencia Disponible para Importacion Comercial (MWh)': 'CapTransDisImpComMwh',
        ' Capacidad Reservada para Importacion de Energia inadvertida (MWh)': 'CapResImpEneInadMwh',
        ' Capacidad Reservada para Importacion por Confiabilidad (MWh)': 'CapResImpConfMWh',
        ' Capacidad Absoluta de Transferencia Disponible para Importacion (MWh)': 'CapAbsTransDisImpMWh',
        ' Capacidad de Transferencia Disponible para Exportacion Comercial (MWh)': 'CapTransDisExpComMwh',
        ' Capacidad Reservada para Exportacion de Energia Inadvertida (MWh)': 'CapResExpEneInaMwh',
        ' Capacidad Reservada para Exportacion por Confiabilidad (MWh)': 'CapResExpConfMwh',
        ' Capacidad Absoluta de Transferencia Disponible para Exportacion (MWh)': 'CapAbsTransDisExpMwh'
        # NOTE: FechaCreacion, FechaActualizacion, Id are likely handled by the API/database
    }

    # --- Script Logic ---
    csv_pattern = os.path.join(csv_directory, '*.csv')
    csv_files = glob.glob(csv_pattern)

    logging.info(f"Found {len(csv_files)} CSV files to process.")

    # Set headers for the API request
    headers = {
        "Content-Type": "application/json",
        # Add any other required headers, like Authorization, here
        # "Authorization": "Bearer YOUR_API_TOKEN"
    }
    files_processed = 0
    files_succeeded = 0
    files_failed = 0

    if isinstance(csv_files, list):
        for file_path in csv_files:
            files_processed += 1
            csv_skiprows = 0  # Adjust if you need to skip more rows
        # --- Dynamically find the header row ---
            with open(file_path, 'r', encoding='utf-8') as f:
                csv_skiprows = 0
                for _ in range(7):
                    next(f)

                for i, line in enumerate(f, start=7):
                    csv_skiprows = i
                    print(f"Checking line {i}: {line.strip()}")
                    if "Sistema" in line:
                        break

            logging.info(f"Skipping {csv_skiprows} rows to find header in {os.path.basename(file_path)}")
            print(f"Skipping {csv_skiprows} rows to find header in {os.path.basename(file_path)}")
            print("-----------------------------------")

            try:
                # Read CSV, skipping initial rows and specifying separator
                # Use skipinitialspace=True if headers might have inconsistent leading spaces
                df = pd.read_csv(
                    file_path,
                    skiprows=csv_skiprows,
                    sep=CSV_SEPARATOR,
                    skipinitialspace=True # Handles potential extra spaces after delimiter
                )
                # --- Data Preprocessing ---
                
                # Strip leading/trailing whitespace from headers IN THE DATAFRAME
                # This makes mapping more robust if CSV spacing is inconsistent
                df.columns = df.columns.str.strip()

                # Adjust the HEADER_MAP keys if you stripped spaces above
                # Create a temporary map with stripped keys for renaming
                header_map_stripped_keys = {k.strip(): v for k, v in HEADER_MAP.items()}

                # 1. Check if all expected CSV headers (now stripped) are present
                missing_headers = [h for h in header_map_stripped_keys.keys() if h not in df.columns]
                if missing_headers:
                    logging.warning(f"Missing expected headers in {os.path.basename(file_path)}: {missing_headers}")
                    files_failed += 1
                    continue # Skip this file

                # 2. Rename columns to match API keys using the stripped-key map
                df.rename(columns=header_map_stripped_keys, inplace=True)

                # 3. Select only the columns that correspond to our API keys
                api_columns = list(header_map_stripped_keys.values())
                # Ensure all target API columns exist after renaming
                df_processed = df[[col for col in api_columns if col in df.columns]].copy()

                # 4. Convert Date Format ('DD/MM/YYYY' -> 'YYYY-MM-DD')
                if 'FechaOperacion' in df_processed.columns:
                    # Try parsing with expected format, coerce errors to NaT (Not a Time)
                    df_processed['FechaOperacion'] = pd.to_datetime(df_processed['FechaOperacion'], format='%d/%m/%Y', errors='coerce')
                    # Store original count before dropping invalid dates
                    original_count = len(df_processed)
                    # Drop rows where date conversion failed (NaT)
                    df_processed.dropna(subset=['FechaOperacion'], inplace=True)
                    if len(df_processed) < original_count:
                        logging.warning(f"  WARNING: Dropped {original_count - len(df_processed)} rows due to invalid date format in FechaOperacion.")
                    # Convert valid dates to 'YYYY-MM-DD' string format for JSON
                    df_processed['FechaOperacion'] = df_processed['FechaOperacion'].dt.strftime('%Y-%m-%d')
                else:
                    logging.warning(f"  WARNING: 'FechaOperacion' column not found in {os.path.basename(file_path)}. Cannot process dates.")
                    files_failed += 1
                    continue # Skip if date is essential

                # 5. Ensure numeric types for columns expected as numbers by API
                # Adjust list based on your SQL schema (INT columns)
                numeric_cols = [
                    'Horario', 'CapTransDisImpComMwh', 'CapResImpEneInadMwh',
                    'CapResImpConfMWh', 'CapAbsTransDisImpMWh', 'CapTransDisExpComMwh',
                    'CapResExpEneInaMwh', 'CapResExpConfMwh', 'CapAbsTransDisExpMwh'
                ]
                for col in numeric_cols:
                    if col in df_processed.columns:
                        # Coerce errors will turn non-numeric values into NaN/NaT
                        df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
                
                # 6. Handle Potential NaN values -> Convert to None for JSON
                df_processed = df_processed.astype(object).where(pd.notnull(df_processed), None)

                # --- Payload Creation and API Call ---

                if df_processed.empty:
                    logging.warning(f"  WARNING: No valid data rows found in {os.path.basename(file_path)} after processing.")
                    continue

                # 7. Convert entire DataFrame to list of dictionaries (records)
                payload_list = df_processed.to_dict(orient='records')
                logging.info(f"Prepared {len(payload_list)} rows to send to API from {os.path.basename(file_path)}.")

                # 8. Make the SINGLE POST request for the entire file's data
                try:
                    response = requests.post(API_ENDPOINT, headers=headers, json=payload_list) # Send the list

                    # Check response status for the batch
                    # Success might be 200 OK, 201 Created, or 207 Multi-Status depending on API design
                    if 200 <= response.status_code < 300:
                        logging.info(f"  SUCCESS: File data sent successfully (Status: {response.status_code}). Response: {response.text[:200]}...")
                        files_succeeded += 1
                    # Handle potential batch-specific errors (e.g., 422 Unprocessable Entity if some records failed validation)
                    elif response.status_code == 400:
                        logging.error(f"  FAILED: Bad Request (400) - Issue with payload structure or headers? Response: {response.text[:300]}...")
                        files_failed += 1
                    elif response.status_code == 409: # How the API handles conflicts in batch needs clarification
                        logging.warning(f"  WARNING/FAILED: Conflict (409) - Some/all records might already exist. Response: {response.text[:300]}...")
                        # Decide if this counts as success or failure based on requirements
                        files_failed += 1 # Defaulting to failure for simplicity
                    elif response.status_code == 422: # Often used for validation errors in batch requests
                        logging.error(f"  FAILED: Unprocessable Entity (422) - Validation errors in batch? Response: {response.text[:300]}...")
                        files_failed += 1
                    else:
                        logging.error(f"  FAILED: Unexpected status code {response.status_code}. Response: {response.text[:300]}...")
                        files_failed += 1

                except requests.exceptions.RequestException as e:
                    logging.error(f"  ERROR: API Request Exception - {e}")
                    files_failed += 1
                    # Optional: add a longer sleep or break if connection fails repeatedly
                    time.sleep(5) # Wait longer before processing next file on connection error

                # Optional: Add a small delay between processing files
                # time.sleep(0.5) # Sleep for 500 milliseconds

            except FileNotFoundError:
                logging.error(f"  ERROR: File not found: {file_path}")
                files_failed += 1
            except pd.errors.EmptyDataError:
                logging.error(f"  ERROR: Empty CSV file: {file_path}")
                # Decide if this is a failure or just skipped
            except ValueError as e:
                logging.error(f"  ERROR: Value Error processing file {os.path.basename(file_path)}: {e}")
                files_failed += 1
            except Exception as e:
                logging.error(f"  ERROR: An unexpected error occurred processing file {os.path.basename(file_path)}: {e}")
                files_failed += 1