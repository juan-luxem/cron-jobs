import logging
import os
import requests
from bs4 import BeautifulSoup
from global_utils.extract_field_value import extract_field_value
from global_utils.extract_viewstate import extract_viewstate
from global_utils.send_telegram_message import send_telegram_message
from global_utils.download_zip import download_zip
from pml.constants import HEADERS, SISTEMAS, URL, ID_REPORT, MIN_DATE, VIEW_GENERATOR
import urllib.parse

# --- Logger Setup ---
# This sets up a simple logger to print info and error messages to the console.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_pml_generic(market_type: str):
    # --- Setup Download Folder ---
    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    try:
        session = requests.session()
        response = session.get(
            URL[market_type], headers={"User-Agent": "Mozilla/5.0"}, timeout=30
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except requests.Timeout:
        error_msg = (
            f"Timeout error while fetching initial page for market type: {market_type}"
        )
        logging.error(error_msg)
        send_telegram_message(message=error_msg)
        return
    except requests.RequestException as e:
        error_msg = f"Network error for market type '{market_type}': {e}"
        logging.error(error_msg)
        send_telegram_message(message=error_msg)
        return
    except Exception as e:
        error_msg = (
            f"Unexpected error while fetching initial page for '{market_type}': {e}"
        )
        logging.error(error_msg)
        send_telegram_message(message=error_msg)
        return

    view_state = extract_field_value(soup, "__VIEWSTATE", "input")
    period = extract_field_value(soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input")
    date = extract_field_value(
        soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
    )

    for sistema in SISTEMAS:
        data = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlReporte",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": MIN_DATE[market_type],
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlReporte",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state,
            "__VIEWSTATEGENERATOR": VIEW_GENERATOR[market_type],
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        try:
            response = session.post(
                URL[market_type], headers=HEADERS, data=data, timeout=30
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            error_msg = f"Error posting first form for system '{sistema}': {e}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            continue

        view_state_value = extract_viewstate(response.text)

        body = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": MIN_DATE[market_type],
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state_value,
            "__VIEWSTATEGENERATOR": VIEW_GENERATOR[market_type],
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        try:
            response = session.post(
                URL[market_type], headers=HEADERS, data=body, timeout=30
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            error_msg = f"Error posting second form for system '{sistema}': {e}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            continue

        new_view_state_value = extract_viewstate(response.text, url_encode=True)

        period_encoded = urllib.parse.quote_plus(str(period), safe="")
        date_encoded = urllib.parse.quote_plus(str(date), safe="")
        id_report_encoded = urllib.parse.quote_plus(
            str(ID_REPORT[market_type]), safe=""
        )
        min_date_encoded = urllib.parse.quote_plus(str(MIN_DATE[market_type]), safe="")

        nodos_data = f"ctl00%24ContentPlaceHolder1%24ddlReporte={id_report_encoded}&ctl00%24ContentPlaceHolder1%24ddlPeriodicidad=D&ctl00%24ContentPlaceHolder1%24ddlSistema={sistema}&ctl00%24ContentPlaceHolder1%24txtPeriodo={period_encoded}&ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect={min_date_encoded}&ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={date_encoded}&ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={new_view_state_value}&__VIEWSTATEGENERATOR={VIEW_GENERATOR[market_type]}&__VIEWSTATEENCRYPTED="

        try:
            response = session.post(
                URL[market_type], headers=HEADERS, data=nodos_data, timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as e:
            error_msg = f"Error downloading ZIP for system '{sistema}': {e}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            continue

        if response.status_code == 200:
            # Verifica el encabezado Content-Disposition
            content_disposition = response.headers.get("Content-Disposition", "")
            download_zip(content_disposition, download_folder, response)
        else:
            error_msg = (
                f"La solicitud falló con el código de estado: {response.status_code}"
            )
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
