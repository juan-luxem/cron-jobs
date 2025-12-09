import logging
import os
import requests
import urllib.parse
from bs4 import BeautifulSoup
from global_utils.extract_field_value import extract_field_value
from global_utils.extract_viewstate import extract_viewstate
from global_utils.send_telegram_message import send_telegram_message
from global_utils.download_zip import download_zip
from constants import (
    HEADERS,
    SISTEMAS,
)

from generacion_gi_ofertada.constants import (
    URL,
    ID_REPORT,
    MIN_DATE,
    VIEW_GENERATOR,
)

# --- Logger Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_generacion_gi_ofertada_generic(market_type: str):
    """
    Generic function to download Generacion GI Ofertada data from CENACE
    using requests and BeautifulSoup.
    It iterates through each system starting a fresh session to ensure clean ViewStates.
    """
    # --- Setup Download Folder ---
    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    # Iterate through each system (SIN, BCA, BCS)
    for sistema in SISTEMAS:
        logging.info(f"🔄 Processing System: {sistema}")

        try:
            # 1. Start a FRESH session for each system to avoid ViewState conflicts
            session = requests.session()
            response = session.get(
                URL[market_type], headers={"User-Agent": "Mozilla/5.0"}, timeout=30
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract initial values
            view_state = extract_field_value(soup, "__VIEWSTATE", "input")
            period = extract_field_value(
                soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input"
            )
            date = extract_field_value(
                soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
            )

            # ---------------------------------------------------------
            # STEP 1: Select Report Type (Async Postback)
            # ---------------------------------------------------------
            data_report = {
                "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlReporte",
                "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
                "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
                "ctl00$ContentPlaceHolder1$ddlSistema": sistema,  # Try setting system early
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

            response = session.post(
                URL[market_type], headers=HEADERS, data=data_report, timeout=30
            )
            response.raise_for_status()

            # Extract updated ViewState from Async Response
            view_state = extract_viewstate(response.text)

            # ---------------------------------------------------------
            # STEP 2: Select System (Async Postback)
            # Necessary to switch to BCA/BCS or confirm SIN
            # ---------------------------------------------------------
            data_system = {
                "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlSistema",
                "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
                "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
                "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
                "ctl00$ContentPlaceHolder1$txtPeriodo": period,
                "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
                "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
                "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": MIN_DATE[market_type],
                "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
                "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlSistema",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": VIEW_GENERATOR[market_type],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
                "": "",
            }

            response = session.post(
                URL[market_type], headers=HEADERS, data=data_system, timeout=30
            )
            response.raise_for_status()
            view_state = extract_viewstate(response.text)

            # ---------------------------------------------------------
            # STEP 3: Update Period (Async Postback)
            # Ensures the server calculates the correct date range for the file
            # ---------------------------------------------------------
            data_period = {
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
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": VIEW_GENERATOR[market_type],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
                "": "",
            }

            response = session.post(
                URL[market_type], headers=HEADERS, data=data_period, timeout=30
            )
            response.raise_for_status()
            view_state = extract_viewstate(response.text)  # No URL encode here yet

            # ---------------------------------------------------------
            # STEP 4: Download ZIP (Full Postback)
            # ---------------------------------------------------------
            # We need to URL Encode the ViewState for the final string construction
            # (Or verify if extract_viewstate does it; standard is usually raw, but for string formatting we need encoded)
            # We will use extract_viewstate(..., url_encode=True) if your util supports it, or quote it manually.
            # Assuming your util can do it based on your snippet:
            new_view_state_value = extract_viewstate(response.text, url_encode=True)

            period_encoded = urllib.parse.quote_plus(str(period), safe="")
            date_encoded = urllib.parse.quote_plus(str(date), safe="")
            id_report_encoded = urllib.parse.quote_plus(
                str(ID_REPORT[market_type]), safe=""
            )
            min_date_encoded = urllib.parse.quote_plus(
                str(MIN_DATE[market_type]), safe=""
            )

            nodos_data = f"ctl00%24ContentPlaceHolder1%24ddlReporte={id_report_encoded}&ctl00%24ContentPlaceHolder1%24ddlPeriodicidad=D&ctl00%24ContentPlaceHolder1%24ddlSistema={sistema}&ctl00%24ContentPlaceHolder1%24txtPeriodo={period_encoded}&ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect={min_date_encoded}&ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={date_encoded}&ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={new_view_state_value}&__VIEWSTATEGENERATOR={VIEW_GENERATOR[market_type]}&__VIEWSTATEENCRYPTED="

            response = session.post(
                URL[market_type], headers=HEADERS, data=nodos_data, timeout=30
            )
            response.raise_for_status()

            if response.status_code == 200:
                content_disposition = response.headers.get("Content-Disposition", "")
                download_zip(content_disposition, download_folder, response)
            else:
                error_msg = f"La solicitud falló con el código de estado: {response.status_code}"
                logging.error(error_msg)
                send_telegram_message(message=error_msg)

        except requests.Timeout:
            error_msg = f"Timeout error while fetching data for system {sistema}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            continue  # Move to next system
        except Exception as e:
            error_msg = f"Unexpected error processing system {sistema}: {e}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            continue
