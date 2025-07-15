import requests
import os
import re
from bs4 import BeautifulSoup
import urllib.parse
import logging
import zipfile
import glob
import pandas as pd
from dotenv import load_dotenv

from .utils import (
    extract_field_value,
    are_files_different,
    check_date_exists,
    delete_csv_files,
    extract_date,
    preprocess_csv,
    send_dataframe_to_api,
    send_telegram_message,
    parse_spanish_date,
)

from .constants import SISTEMAS, URLMDA, HEADERS

API_TARGET_SOURCE_PNDMDA = "pnd_mda"


def get_pnd_mda():
    load_dotenv()  # Load environment variables from .env file
    bot_token = os.getenv("TELEGRAM_BOT_MERCADOS_LUX_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    API_URL = os.getenv("API_URL")
    if not bot_token or not chat_id or not API_URL:
        logging.error(
            "Error: Missing environment variables. Please check your .env file."
        )
        raise ValueError(
            "Missing environment variables. Please check your .env file."
        )

    session = requests.session()
    response = session.get(URLMDA, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, "html.parser")

    view_state = extract_field_value(soup, "__VIEWSTATE", "input")
    period = extract_field_value(soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input")
    date = extract_field_value(
        soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
    )

    for sistema in SISTEMAS:
        data = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlSistema",
            "ctl00$ContentPlaceHolder1$ddlReporte": "360,323",
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": "23/03/2016",
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlSistema",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state,
            "__VIEWSTATEGENERATOR": "35C9E14B",
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        response = session.post(URLMDA, headers=HEADERS, data=data)
        soup = BeautifulSoup(response.text, "html.parser")

        # Regular expression to capture the VIEWSTATE value
        match = re.search(r"\|hiddenField\|__VIEWSTATE\|([^|]+)", response.text)

        view_state_value = ""

        if match:
            view_state_value = match.group(1)
        else:
            logging.info("VIEWSTATE not found.")

        body = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": "360,323",
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": "27/01/2016",
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state_value,
            "__VIEWSTATEGENERATOR": "35C9E14B",
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        response = session.post(URLMDA, headers=HEADERS, data=body)
        soup = BeautifulSoup(response.text, "html.parser")
        # Regular expression to capture the VIEWSTATE value
        match = re.search(r"\|hiddenField\|__VIEWSTATE\|([^|]+)", response.text)

        new_view_state_value = ""
        if match:
            # new_view_state_value = match.group(1)
            new_view_state_value = urllib.parse.quote_plus(match.group(1), safe="")
        else:
            logging.info("VIEWSTATE not found.")

        period_encoded = urllib.parse.quote_plus(str(period), safe="")
        date_encoded = urllib.parse.quote_plus(str(date), safe="")

        node_data = f"ctl00%24ContentPlaceHolder1%24ddlReporte=360%2C323&ctl00%24ContentPlaceHolder1%24ddlPeriodicidad=D&ctl00%24ContentPlaceHolder1%24ddlSistema={sistema}&ctl00%24ContentPlaceHolder1%24txtPeriodo={period_encoded}&ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect=27%2F01%2F2016&ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={date_encoded}&ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={new_view_state_value}&__VIEWSTATEGENERATOR=35C9E14B&__VIEWSTATEENCRYPTED="

        response = session.post(URLMDA, headers=HEADERS, data=node_data)
        if response.status_code == 200:
            # Verifica el encabezado Content-Disposition
            content_disposition = response.headers.get("Content-Disposition", "")

            if "attachment" in content_disposition and ".zip" in content_disposition:
                # Extrae el nombre real del archivo ZIP si está presente en la cabecera
                # current_directory = os.path.dirname(os.path.abspath(__file__))
                filename = "resultado.zip"
                # file_path = os.path.join(current_directory, filename)
                if "filename=" in content_disposition:
                    filename_match = re.search(
                        r'filename=(?:"([^"]+)"|([^;]+))', content_disposition
                    )
                    if filename_match:
                        filename = filename_match.group(1) or filename_match.group(2)

                # Guarda el archivo ZIP descargado
                with open(filename, "wb") as f:
                    f.write(response.content)
                logging.info(f"Archivo ZIP '{filename}' descargado exitosamente")
                # Extrae el contenido del ZIP
                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall()
                # Elimina el ZIP después de extraerlo
                os.remove(filename)
                logging.info(f"Archivo ZIP '{filename}' eliminado exitosamente")
                # Busca archivos con un nombre similar pero diferentes extensiones
                base_name = os.path.splitext(filename)[0]
                csv_file = glob.glob(f"{base_name}.*")
                # Renombra el archivo CSV con el nombre del sistema
                new_filename = f"PND_MDA_{sistema}.csv"
                os.rename(csv_file[0], new_filename)
                logging.info(f"Archivo CSV renombrado a '{new_filename}'")
            else:
                logging.warning(
                    "Advertencia: La respuesta no parece ser un archivo ZIP"
                )
                logging.info(f"Content-Disposition: {content_disposition}")
                # Podrías querer inspeccionar los primeros bytes para confirmar que es un ZIP
                logging.info(f"Primeros bytes: {response.content[:20]}")
        else:
            logging.error(
                f"La solicitud falló con el código de estado: {response.status_code}"
            )

    try:
        # Verifica si todos los archivos son diferentes
        csv_files = [f for f in os.listdir() if f.endswith(".csv")]

        all_different = True
        for i in range(len(csv_files)):
            for j in range(i + 1, len(csv_files)):
                if not are_files_different(csv_files[i], csv_files[j]):
                    # Para windows
                    if os.name == "nt":
                        os.system("cls")
                    # Para mac and linux(here, os.name is 'posix')
                    else:
                        os.system("clear")
                    logging.warning(
                        f"Files {csv_files[i]} and {csv_files[j]} are identical. Skipping merge."
                    )
                    all_different = False
                    break
            if not all_different:
                break

        if all_different:
            logging.info("All files are different. Proceeding to merge.")

            date_sin = extract_date("PND_MDA_SIN.csv")
            date_bca = extract_date("PND_MDA_BCA.csv")
            date_bcs = extract_date("PND_MDA_BCS.csv")

            # Basic check if dates were extracted and match
            if not date_sin or not date_bca or not date_bcs:
                logging.error(
                    "Could not extract date from one or more CSV files. Aborting."
                )
                delete_csv_files(".")
                return

            if date_sin == date_bca == date_bcs:
                logging.info(f"Dates match: {date_sin}. Proceeding to merge.")

                dt, target_date_str_iso = parse_spanish_date(date_sin)
                if not dt or not target_date_str_iso:
                    delete_csv_files(".")
                    return 
                

                # ===> ADD THE DATE CHECK HERE <===
                data_type_to_check = (
                    API_TARGET_SOURCE_PNDMDA  # Use the constant defined for this script
                )
                exists = check_date_exists(
                    API_URL, data_type_to_check, target_date_str_iso
                )

                if exists is None:
                    logging.error(
                        f"Failed to check API for existing data for {target_date_str_iso}. Aborting processing for this date."
                    )
                    return  # Stop processing this date

                if exists is True:
                    logging.warning(
                        f"Data for date {target_date_str_iso} and type '{data_type_to_check}' already exists in the database (according to API check). Skipping batch sending."
                    )
                    # Delete the downloaded CSVs as they are not needed now
                    delete_csv_files(".")
                    return  # Stop processing this date

                df_sin = preprocess_csv("PND_MDA_SIN.csv", "SIN")
                df_bca = preprocess_csv("PND_MDA_BCA.csv", "BCA")
                df_bcs = preprocess_csv("PND_MDA_BCS.csv", "BCS")

                df_sin_actualizado = pd.concat(
                    [df_sin, df_bca, df_bcs], ignore_index=True
                )

                df_sin_actualizado["Fecha"] = dt
                df_sin_actualizado = df_sin_actualizado.rename(
                    columns={
                        "Zona de Carga": "Clave",
                        "Precio Zonal": "PML",
                        "Componente energia": "Energia",
                        "Componente perdidas": "Perdidas",
                        "Componente Congestion": "Congestion",
                    }
                )

                # --- Instead of sending all at once, send in batches ---
                BATCH_SIZE = (
                    800  # <<< Choose a batch size (e.g., 100, 250, 500) - Tune this!
                )
                num_records = len(df_sin_actualizado)
                num_batches = (
                    num_records + BATCH_SIZE - 1
                ) // BATCH_SIZE  # Calculate needed batches

                overall_success = True  # Track if all batches succeeded
                records_successfully_sent_count = 0
                logging.info("--- Starting API Upload ---")
                logging.info(f"Total records to send: {num_records}")
                logging.info(f"Batch size: {BATCH_SIZE}")
                logging.info(f"Number of batches: {num_batches}")
                for i in range(num_batches):
                    start_index = i * BATCH_SIZE
                    # end_index calculation ensures we don't go past the end of the DataFrame
                    end_index = min(start_index + BATCH_SIZE, num_records)

                    # Select the batch from the DataFrame using row indices
                    df_batch = df_sin_actualizado.iloc[start_index:end_index]

                    logging.info(
                        f"--- Sending Batch {i + 1}/{num_batches} (Records {start_index + 1}-{end_index}) ---"
                    )

                    if df_batch.empty:
                        logging.warning(f"Batch {i + 1} is empty, skipping.")
                        continue

                    # Call the existing function with the smaller batch DataFrame
                    batch_success = send_dataframe_to_api(
                        df_batch, API_URL, API_TARGET_SOURCE_PNDMDA
                    )
                    if batch_success:
                        logging.info(f"Batch {i + 1}/{num_batches} sent successfully.")
                        records_successfully_sent_count += len(df_batch)
                    else:
                        logging.error(
                            f"Failed to send Batch {i + 1}/{num_batches}. Check logs above."
                        )
                        overall_success = False
                logging.info("--- API Upload Summary ---")
                logging.info(f"Total records from processed files: {num_records}")
                logging.info(
                    f"Total records successfully sent in batches: {records_successfully_sent_count}"
                )
                if overall_success and records_successfully_sent_count == num_records:
                    logging.info("All batches appear to have been sent successfully.")
                    # Optional: Save the combined CSV locally only if everything was sent
                    # Send Telegram Success Message
                    message = (
                        f"PML_MTR Script Success: Successfully uploaded {records_successfully_sent_count} "
                        f"records for date {target_date_str_iso}."
                    )
                    send_telegram_message(bot_token, chat_id, message)
                    delete_csv_files(".")  # Replace "." with the target directory path

                    return True  # Indicate overall success
                else:
                    logging.error(
                        "One or more batches failed to send, or record counts mismatch."
                    )
                    # Send Telegram Alert
                    message = (
                        f"PML_MTR Script Error: Date mismatch between files "
                        f"SIN({date_sin}), BCA({date_bca}), BCS({date_bcs}). Aborting upload."
                    )
                    send_telegram_message(bot_token, chat_id, message)
                    delete_csv_files(".")  # Replace "." with the target directory path
                    return False  # Indicate failure
        else:
            logging.error("Files are not the same")
            delete_csv_files(".")  # Replace "." with the target directory path
    except FileNotFoundError as e:
        logging.error(
            f"Error: {e.filename} no encontrado. Asegúrate de que todos los CSV requeridos estén descargados."
        )

    except Exception as e:
        logging.exception(f"Error inesperado: {e}")

# get_pnd_mda()