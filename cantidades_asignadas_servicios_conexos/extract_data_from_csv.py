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
    notify_error,
    send_data_in_chunks,
)

logging.basicConfig(level=logging.INFO)


def rename_columns_to_target_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns to match the target structure for Cantidades Asignadas de Servicios Conexos data.
    """
    column_mapping = {}

    for col in df.columns:
        clean_col = str(col).strip().replace('"', "").strip()
        if "Zona de Reserva" in clean_col:
            column_mapping[col] = "ZonaReserva"
        elif "Hora" in clean_col:
            column_mapping[col] = "HoraOperacion"
        elif "RT Reserva Regulacion (MW)" in clean_col:
            column_mapping[col] = "RTReservaRegulacion_MW"
        elif "RT Reserva Rodante 10 (MW)" in clean_col:
            column_mapping[col] = "RTReservaRodante_10_MW"
        elif "RT Reserva 10 (MW)" in clean_col:
            column_mapping[col] = "RTReserva_10_MW"
        elif "RT Reserva Suplementaria (MW)" in clean_col:
            column_mapping[col] = "RTReservaSuplementaria_MW"

    df_renamed = df.rename(columns=column_mapping)
    return df_renamed


def process_csv_file(file_path: str) -> List[Dict]:
    """
    Processes a single Cantidades Asignadas de Servicios Conexos CSV file
    and returns a list of dictionaries with the target structure.
    """
    filename = os.path.basename(file_path)

    sistema = extract_sistema_from_filename(filename)
    fecha_operacion = extract_fecha_operacion_from_filename(filename)

    if not sistema:
        notify_error(f"[Cantidades Asignadas SC] No se pudo extraer el Sistema del nombre del archivo: '{filename}'")
        return []

    if not fecha_operacion:
        notify_error(f"[Cantidades Asignadas SC] No se pudo extraer la FechaOperacion del nombre del archivo: '{filename}'")
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
            notify_error(f"[Cantidades Asignadas SC] Error al leer el archivo '{filename}' con encoding latin-1: {e}")
            return []
    except Exception as e:
        notify_error(f"[Cantidades Asignadas SC] Error inesperado al abrir el archivo '{filename}': {e}")
        return []

    lines = content.split("\n")

    temp_df = pd.DataFrame(
        [line.split(",") for line in lines[:25]]
    )

    header_line_idx = find_header_row(temp_df, "Zona de Reserva", "Hora")
    logging.info(f"Header line index: {header_line_idx}")

    if header_line_idx == -1:
        notify_error(f"[Cantidades Asignadas SC] No se encontro la linea de encabezado en el archivo: '{filename}'")
        return []

    data_lines = lines[header_line_idx:]
    temp_csv_content = "\n".join(data_lines)

    try:
        df = pd.read_csv(StringIO(temp_csv_content), encoding="utf-8")
    except Exception as e:
        notify_error(f"[Cantidades Asignadas SC] Error al parsear CSV '{filename}': {e}")
        return []

    df.columns = clean_column_names(df.columns)
    df = rename_columns_to_target_structure(df)

    result = []
    for _, row in df.iterrows():
        if pd.isna(row.get("HoraOperacion")) or row.get("HoraOperacion") == "":
            continue

        try:
            record = {
                "FechaOperacion": fecha_operacion,
                "Sistema": sistema,
                "ZonaReserva": str(row.get("ZonaReserva", "")).strip().replace('"', ""),
                "HoraOperacion": int(float(str(row.get("HoraOperacion", 0)))),
                "RTReservaRegulacion_MW": float(
                    str(row.get("RTReservaRegulacion_MW", 0)).replace(",", "")
                ),
                "RTReservaRodante_10_MW": float(
                    str(row.get("RTReservaRodante_10_MW", 0)).replace(",", "")
                ),
                "RTReserva_10_MW": float(
                    str(row.get("RTReserva_10_MW", 0)).replace(",", "")
                ),
                "RTReservaSuplementaria_MW": float(
                    str(row.get("RTReservaSuplementaria_MW", 0)).replace(",", "")
                ),
            }
            result.append(record)
        except (ValueError, TypeError) as e:
            logging.warning(f"Error processing row in {filename}: {e}")
            continue

    # Remove duplicates - keep first occurrence of each (HoraOperacion, ZonaReserva) combination
    initial_count = len(result)
    seen_keys = {}

    for record in result:
        key = (record["HoraOperacion"], record["ZonaReserva"])
        if key not in seen_keys:
            seen_keys[key] = record

    deduplicated_result = sorted(
        seen_keys.values(),
        key=lambda x: (x["ZonaReserva"], x["HoraOperacion"]),
    )

    duplicates_removed = initial_count - len(deduplicated_result)
    if duplicates_removed > 0:
        logging.warning(
            f"Removed {duplicates_removed} duplicate hour entries from {filename}"
        )

    logging.info(f"Processed {len(deduplicated_result)} unique records from {filename}")
    return deduplicated_result


def process_and_send_csv_file(file_path: str, endpoint_url: str) -> bool:
    """
    Processes a single CSV file, sends data to API in chunks, and deletes file if successful.
    Returns True if successful, False otherwise.
    """
    filename = os.path.basename(file_path)
    logging.info(f"Processing file: {filename}")

    processed_data = process_csv_file(file_path)
    logging.info(f"Processed {len(processed_data)} records from {filename}")

    if not processed_data:
        notify_error(f"[Cantidades Asignadas SC] No se extrajeron datos del archivo: '{filename}'")
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
        notify_error(f"[Cantidades Asignadas SC] Fallo al enviar datos del archivo '{filename}' a la API")
        return False


def process_all_csv_files_with_api(
    download_folder: str,
    endpoint_url: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Dict:
    """
    Processes all CSV files in the download folder and sends them to the API.

    Validation:
    - If start_date and end_date are NONE (Cron mode): Validates exactly 3 files (SIN, BCS, BCA).
    - If dates ARE provided (Bulk mode): Skips strict count validation.

    Returns a summary of processed vs failed files.
    """
    csv_files = [f for f in os.listdir(download_folder) if f.endswith(".csv")]

    if start_date is None and end_date is None:
        if len(csv_files) != 3:
            if len(csv_files) == 0:
                error_msg = (
                    "[Cantidades Asignadas SC] No se encontraron archivos CSV en la carpeta de descarga. "
                    f"Carpeta: {download_folder}. "
                    "Se esperaban 3 archivos (uno por sistema: SIN, BCS, BCA)."
                )
            else:
                error_msg = (
                    f"[Cantidades Asignadas SC] Se esperaban exactamente 3 archivos CSV (SIN, BCS, BCA) pero se encontraron {len(csv_files)}. "
                    f"Archivos: {csv_files}. "
                    f"Carpeta: {download_folder}."
                )
            notify_error(error_msg)
            for csv_file in csv_files:
                file_path = os.path.join(download_folder, csv_file)
                try:
                    os.remove(file_path)
                    logging.info(f"Removed file after validation error: {file_path}")
                except OSError as e:
                    logging.warning(f"Failed to remove file {file_path}: {e}")
            return {
                "processed": 0,
                "failed": 0,
                "total": len(csv_files),
                "error": error_msg,
            }

        logging.info(
            f"Found {len(csv_files)} CSV files to process (validation passed)"
        )

        found_systems = set()
        for csv_file in csv_files:
            sistema = extract_sistema_from_filename(csv_file)
            if sistema:
                found_systems.add(sistema)

        expected_systems = {"SIN", "BCS", "BCA"}
        if found_systems != expected_systems:
            missing_systems = expected_systems - found_systems
            extra_systems = found_systems - expected_systems
            error_msg = (
                f"[Cantidades Asignadas SC] Validacion de sistemas fallida. "
                f"Faltantes: {missing_systems}. "
                f"Extra: {extra_systems}. "
                f"Archivos encontrados: {csv_files}."
            )
            notify_error(error_msg)
            for csv_file in csv_files:
                file_path = os.path.join(download_folder, csv_file)
                try:
                    os.remove(file_path)
                    logging.info(f"Removed file after validation error: {file_path}")
                except OSError as e:
                    logging.warning(f"Failed to remove file {file_path}: {e}")
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

    logging.info("Processing Summary:")
    logging.info(f"Successfully processed: {processed_count}")
    logging.info(f"Failed: {failed_count}")
    logging.info(f"Remaining CSV files: {len(remaining_files)}")

    if len(remaining_files) == 0:
        logging.info("Download folder is now empty.")
    else:
        logging.warning(f"{len(remaining_files)} files remain in download folder")
        for file in remaining_files:
            logging.info(f"   - {file}")

    return {
        "processed": processed_count,
        "failed": failed_count,
        "total": len(csv_files),
        "remaining": len(remaining_files),
    }
