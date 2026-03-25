import logging
import os
from datetime import datetime
from io import StringIO
from typing import Dict, List

import pandas as pd

from global_utils.clean_column_names import clean_column_names
from global_utils.extract_fecha_operacion_from_row import (
    extract_fecha_operacion_from_row,
)
from global_utils.extract_sistema_from_file import extract_sistema_from_filename
from global_utils.find_header_row import find_header_row
from global_utils.notify_error import notify_error
from global_utils.send_data_in_chunks import send_data_in_chunks


def rename_columns_to_target_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns to match the target structure for Capacidad de Transferencia data.
    Uses lowercase keys for case-insensitive matching after clean_column_names is applied.
    """
    name_map = {
        "horario": "Horario",
        "enlace": "Enlace",
        "capacidad de transferencia disponible para importacion comercial (mwh)": "CapTransDisImpComMwh",
        "capacidad reservada para importacion de energia inadvertida (mwh)": "CapResImpEneInadMwh",
        "capacidad reservada para importacion por confiabilidad (mwh)": "CapResImpConfMWh",
        "capacidad absoluta de transferencia disponible para importacion (mwh)": "CapAbsTransDisImpMWh",
        "capacidad de transferencia disponible para exportacion comercial (mwh)": "CapTransDisExpComMwh",
        "capacidad reservada para exportacion de energia inadvertida (mwh)": "CapResExpEneInaMwh",
        "capacidad reservada para exportacion por confiabilidad (mwh)": "CapResExpConfMwh",
        "capacidad absoluta de transferencia disponible para exportacion (mwh)": "CapAbsTransDisExpMwh",
        # Fallbacks for older formats if applicable
        "capacidad de importacion (mw)": "CapAbsTransDisImpMWh",
        "capacidad de exportacion (mw)": "CapAbsTransDisExpMwh",
    }

    rename_dict = {}
    for col in df.columns:
        clean_col = str(col).strip().replace('"', "").strip().lower()
        if clean_col in name_map:
            rename_dict[col] = name_map[clean_col]

    return df.rename(columns=rename_dict)


def safe_float_or_null(value) -> float | None:
    if pd.isna(value):
        return None
    str_value = str(value).strip()
    if str_value == "" or str_value.isspace():
        return None
    try:
        return float(str_value.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def parse_fecha(fecha_str: str) -> str | None:
    try:
        return datetime.strptime(fecha_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return None


def process_csv_file(file_path: str) -> List[Dict]:
    filename = os.path.basename(file_path)
    logging.info(f"Processing file: {filename}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="latin-1") as f:
                content = f.read()
        except Exception as e:
            notify_error(f"[Capacidad de Transferencia] Error al leer el archivo '{filename}': {e}")
            return []

    lines = content.split("\n")
    temp_df = pd.DataFrame([line.split(",") for line in lines[:25]])
    header_line_idx = find_header_row(temp_df, "Sistema", "Enlace")

    if header_line_idx == -1:
        notify_error(f"[Capacidad de Transferencia] No se encontro la linea de encabezado en el archivo: '{filename}'")
        return []

    cleaned_lines = [
        line.strip().strip(",") for line in lines[header_line_idx:] if line.strip()
    ]
    temp_csv_content = "\n".join(cleaned_lines)

    try:
        df = pd.read_csv(
            StringIO(temp_csv_content), encoding="utf-8", skipinitialspace=True
        )
    except Exception as e:
        notify_error(f"[Capacidad de Transferencia] Error al parsear CSV '{filename}': {e}")
        return []

    df.columns = clean_column_names(df.columns)
    df = rename_columns_to_target_structure(df)

    is_new_format = "Fecha" in df.columns
    if is_new_format:
        fecha_operacion_old = None
    else:
        fecha_operacion_old = extract_fecha_operacion_from_row(
            content, "Dia de operacion:"
        )
        if not fecha_operacion_old:
            notify_error(f"[Capacidad de Transferencia] No se pudo extraer 'Dia de operacion' del metadata en '{filename}'")
            return []

    result = []
    for _, row in df.iterrows():
        hora_str = str(row.get("Horario", "")).strip()
        if not hora_str or hora_str == "nan" or hora_str == "0":
            continue

        if is_new_format:
            fecha_raw = str(row.get("Fecha", "")).strip()
            fecha_operacion = parse_fecha(fecha_raw)
            if not fecha_operacion:
                continue
        else:
            fecha_operacion = fecha_operacion_old

        sistema = str(row.get("Sistema", "")).strip().replace('"', "")
        if not sistema or sistema == "nan":
            continue

        try:
            record = {
                "FechaOperacion": fecha_operacion,
                "Sistema": sistema,
                "Horario": int(float(hora_str)),
                "Enlace": str(row.get("Enlace", "")).strip().replace('"', ""),
                "CapTransDisImpComMwh": safe_float_or_null(
                    row.get("CapTransDisImpComMwh")
                ),
                "CapResImpEneInadMwh": safe_float_or_null(
                    row.get("CapResImpEneInadMwh")
                ),
                "CapResImpConfMWh": safe_float_or_null(row.get("CapResImpConfMWh")),
                "CapAbsTransDisImpMWh": safe_float_or_null(
                    row.get("CapAbsTransDisImpMWh")
                ),
                "CapTransDisExpComMwh": safe_float_or_null(
                    row.get("CapTransDisExpComMwh")
                ),
                "CapResExpEneInaMwh": safe_float_or_null(row.get("CapResExpEneInaMwh")),
                "CapResExpConfMwh": safe_float_or_null(row.get("CapResExpConfMwh")),
                "CapAbsTransDisExpMwh": safe_float_or_null(
                    row.get("CapAbsTransDisExpMwh")
                ),
            }
            result.append(record)
        except (ValueError, TypeError):
            continue

    seen_keys = set()
    deduplicated_result = []

    for record in result:
        key = (
            record["FechaOperacion"],
            record["Sistema"],
            record["Enlace"],
            record["Horario"],
        )
        if key not in seen_keys:
            seen_keys.add(key)
            deduplicated_result.append(record)

    return deduplicated_result


def process_and_send_csv_file(file_path: str, endpoint_url: str) -> bool:
    filename = os.path.basename(file_path)
    processed_data = process_csv_file(file_path)

    if not processed_data:
        notify_error(f"[Capacidad de Transferencia] No se extrajeron datos del archivo '{filename}'")
        return False

    success = send_data_in_chunks(processed_data, endpoint_url, chunk_size=2000)
    if success:
        try:
            os.remove(file_path)
            logging.info(f"✅ File {filename} processed and deleted successfully")
            return True
        except OSError as e:
            logging.warning(
                f"Data sent successfully but failed to delete file {filename}: {e}"
            )
            return True
    else:
        logging.error(f"❌ Failed to process {filename} - file kept for retry")
        return False


def process_all_csv_files_with_api(
    download_folder: str,
    endpoint_url: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Dict[str, int | str]:
    csv_files = [f for f in os.listdir(download_folder) if f.endswith(".csv")]

    if start_date is None and end_date is None:
        if len(csv_files) != 2:
            if len(csv_files) == 0:
                error_msg = (
                    "[Capacidad de Transferencia] No se encontraron archivos CSV en la carpeta de descarga. "
                    f"Carpeta: {download_folder}. "
                    "Se esperaban 2 archivos (SIN, BCA)."
                )
            else:
                error_msg = (
                    f"[Capacidad de Transferencia] Se esperaban exactamente 2 archivos CSV (SIN, BCA) pero se encontraron {len(csv_files)}. "
                    f"Archivos: {csv_files}. "
                    f"Carpeta: {download_folder}."
                )
            notify_error(error_msg)
            return {
                "processed": 0,
                "failed": 0,
                "total": len(csv_files),
                "remaining": len(csv_files),
                "error": error_msg,
            }

        found_systems = set()
        for csv_file in csv_files:
            sistema = extract_sistema_from_filename(csv_file)
            if sistema:
                found_systems.add(sistema)

        expected_systems = {"SIN", "BCA"}
        if found_systems != expected_systems:
            missing_systems = expected_systems - found_systems
            extra_systems = found_systems - expected_systems
            error_msg = (
                f"[Capacidad de Transferencia] Validacion de sistemas fallida. "
                f"Faltantes: {missing_systems}. "
                f"Extra: {extra_systems}. "
                f"Archivos encontrados: {csv_files}."
            )
            notify_error(error_msg)
            return {
                "processed": 0,
                "failed": 0,
                "total": len(csv_files),
                "remaining": len(csv_files),
                "error": error_msg,
            }
        logging.info("System validation passed for SIN and BCA files.")

    processed_count = 0
    failed_count = 0

    for csv_file in csv_files:
        file_path = os.path.join(download_folder, csv_file)
        success = process_and_send_csv_file(file_path, endpoint_url)

        if success:
            processed_count += 1
        else:
            failed_count += 1

    remaining_work = len(csv_files) - processed_count

    return {
        "processed": processed_count,
        "failed": failed_count,
        "total": len(csv_files),
        "remaining": remaining_work,
    }
