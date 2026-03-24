import logging
import os
from io import StringIO
from typing import Dict, List

import pandas as pd

from global_utils import (
    clean_column_names,
    extract_fecha_operacion_from_filename,
    extract_sistema_from_filename,
    find_header_row,
    send_data_in_chunks,
)
from global_utils.notify_error import notify_error

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

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="latin-1") as f:
                content = f.read()
        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")
            return []

    lines = content.split("\n")
    temp_df = pd.DataFrame([line.split(",") for line in lines if line][:25])
    header_line_idx = find_header_row(temp_df, "Zona de Reserva", "Hora")

    if header_line_idx == -1:
        logging.warning(f"Could not find header line in file: {filename}")
        return []

    data_lines = lines[header_line_idx:]
    temp_csv_content = "\n".join(data_lines)

    try:
        df = pd.read_csv(StringIO(temp_csv_content), encoding="utf-8")
    except Exception as e:
        logging.error(f"Error parsing CSV data from {filename}: {e}")
        return []

    df.columns = clean_column_names(df.columns)
    df = rename_columns_to_target_structure(df)

    df["Sistema"] = sistema
    df["FechaOperacion"] = fecha_operacion

    result = []
    for _, row in df.iterrows():
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

    # Remove duplicates
    initial_count = len(result)
    seen_keys = {}
    deduplicated_result = []

    for record in result:
        key = (
            record["FechaOperacion"],
            record["ZonaReserva"],
            record["HoraOperacion"],
            record["Sistema"],
        )
        if key not in seen_keys:
            seen_keys[key] = record

    deduplicated_result = sorted(
        seen_keys.values(), key=lambda x: (x["ZonaReserva"], x["HoraOperacion"])
    )
    duplicates_removed = initial_count - len(deduplicated_result)
    if duplicates_removed > 0:
        logging.info(f"Removed {duplicates_removed} duplicate entries from {filename}")

    logging.info(f"Processed {len(deduplicated_result)} records from {filename}")
    return deduplicated_result


def process_and_send_csv_file(file_path: str, endpoint_url: str) -> bool:
    """
    Processes a single CSV file, sends data to API in chunks, and deletes file if successful.
    """
    filename = os.path.basename(file_path)
    logging.info(f"Processing file: {filename}")

    processed_data = process_csv_file(file_path)
    if not processed_data:
        logging.error(f"No data extracted from {filename}")
        return False

    success = send_data_in_chunks(processed_data, endpoint_url, chunk_size=2000)

    if success:
        try:
            os.remove(file_path)
            logging.info(f"File {filename} processed and deleted successfully")
            return True
        except OSError as e:
            logging.warning(
                f"Data sent successfully but failed to delete file {filename}: {e}"
            )
            return True
    else:
        msg = f"Failed to send data to API in chunks for {filename}"
        logging.error(msg)
        notify_error(msg)
        return False


def process_all_csv_files_with_api(
    download_folder: str,
    endpoint_url: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Dict[str, int]:
    """
    Processes all CSV files in the download folder, sends to API, and deletes successful files.
    Validates exactly 3 CSV files (SIN, BCS, BCA) only if start_date and end_date are None.
    """
    csv_files = [f for f in os.listdir(download_folder) if f.endswith(".csv")]

    if start_date is None and end_date is None:
        if len(csv_files) != 3:
            error_msg = f"Expected exactly 3 CSV files (one for each system: SIN, BCS, BCA), but found {len(csv_files)} files"
            logging.error(error_msg)
            notify_error(
                f"Error en process_all_csv_files_with_api: {error_msg}",
            )
            for csv_file in csv_files:
                try:
                    file_path = os.path.join(download_folder, csv_file)
                    os.remove(file_path)
                    logging.info(f"Cleaned up incorrect file: {csv_file}")
                except Exception as e:
                    logging.warning(f"Could not delete file {csv_file}: {e}")
            return {
                "processed": 0,
                "failed": 0,
                "total": len(csv_files),
                "error": error_msg,
            }

        found_systems = set()
        for csv_file in csv_files:
            sistema = extract_sistema_from_filename(csv_file)
            if sistema:
                found_systems.add(sistema)

        expected_systems = {"SIN", "BCS", "BCA"}
        if found_systems != expected_systems:
            missing_systems = expected_systems - found_systems
            extra_systems = found_systems - expected_systems
            error_msg = f"System validation failed. Missing: {missing_systems}, Extra: {extra_systems}"
            logging.error(error_msg)
            notify_error(
                f"Error en process_all_csv_files_with_api: {error_msg}",
            )
            for csv_file in csv_files:
                try:
                    file_path = os.path.join(download_folder, csv_file)
                    os.remove(file_path)
                    logging.info(f"Cleaned up invalid file: {csv_file}")
                except Exception as e:
                    logging.warning(f"Could not delete file {csv_file}: {e}")
            return {
                "processed": 0,
                "failed": 0,
                "total": len(csv_files),
                "error": error_msg,
            }

        logging.info(f"System validation passed: Found files for {found_systems}")
    else:
        logging.info(
            f"Bulk Processing Mode: Found {len(csv_files)} CSV files to process."
        )

    processed_count = 0
    failed_count = 0

    for csv_file in csv_files:
        file_path = os.path.join(download_folder, csv_file)
        success = process_and_send_csv_file(file_path, endpoint_url)
        if success:
            processed_count += 1
        else:
            failed_count += 1

    remaining_files = [f for f in os.listdir(download_folder) if f.endswith(".csv")]
    logging.info(f"Processing Summary:")
    logging.info(f"Successfully processed: {processed_count}")
    logging.info(f"Failed: {failed_count}")
    logging.info(f"Remaining CSV files: {len(remaining_files)}")

    return {
        "processed": processed_count,
        "failed": failed_count,
        "total": len(csv_files),
        "remaining": len(remaining_files),
    }
