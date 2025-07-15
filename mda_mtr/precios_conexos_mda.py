import requests
import os
import re
from bs4 import BeautifulSoup, Tag
import urllib.parse
import logging
import zipfile
import glob
import pandas as pd

from utils import (
    extract_field_value,
    are_files_different,
    check_date_exists,
    delete_csv_files,
    extract_date,
    preprocess_csv,
    send_dataframe_to_api,
    send_telegram_message,
)

from constants import SISTEMAS, URLMDA, API_BASE_URL, HEADERS

# Logging Setup
logging.basicConfig(
    # filename="/var/www/html/mercados/logs/pnd_script.log",
    filename="./logs/precios_conexos_mda.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


def get_precios_conexos_mda():
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
            "ctl00$ContentPlaceHolder1$ddlReporte": "361,324",
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": "29/03/2016",
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlReporte",
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
            print("VIEWSTATE not found.")

        body = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": "361,324",
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
            print("VIEWSTATE not found.")

        period_encoded = urllib.parse.quote_plus(str(period), safe="")
        date_encoded = urllib.parse.quote_plus(str(date), safe="")

        conexo_data = f"ctl00%24ContentPlaceHolder1%24ddlReporte=361%2C324&ctl00%24ContentPlaceHolder1%24ddlPeriodicidad=D&ctl00%24ContentPlaceHolder1%24ddlSistema={sistema}&ctl00%24ContentPlaceHolder1%24txtPeriodo={period_encoded}&ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect=29%2F01%2F2016&ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={date_encoded}&ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={new_view_state_value}&__VIEWSTATEGENERATOR=35C9E14B&__VIEWSTATEENCRYPTED="

        response = session.post(URLMDA, headers=HEADERS, data=conexo_data)
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
                new_filename = f"PRECIOS_CONEXOS_MDA_{sistema}.csv"
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
            # Une todos los archivos CSV en uno solo
            df_sin = pd.read_csv(
                "PRECIOS_CONEXOS_MDA_SIN.csv", delimiter=",", skiprows=7
            )
            df_bca = pd.read_csv(
                "PRECIOS_CONEXOS_MDA_BCA.csv", delimiter=",", skiprows=7
            )
            df_bcs = pd.read_csv(
                "PRECIOS_CONEXOS_MDA_BCS.csv", delimiter=",", skiprows=7
            )

            df_sin_actualizado = pd.concat([df_sin, df_bca, df_bcs], ignore_index=True)
            df_sin_actualizado.to_csv(
                "PRECIOS_CONEXOS_MDA_actualizado.csv", index=False
            )
            logging.info(
                "Archivo CSV combinado guardado como PRECIOS_CONEXOS_MDA_actualizado.csv"
            )
    except FileNotFoundError as e:
        logging.error(
            f"Error: {e.filename} no encontrado. Asegúrate de que todos los CSV requeridos estén descargados."
        )

    except Exception as e:
        logging.exception(f"Error inesperado: {e}")
