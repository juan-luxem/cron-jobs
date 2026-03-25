import logging
from global_utils.notify_error import notify_error
import urllib.parse

import requests
from bs4 import BeautifulSoup

from capacidad_transferencia.constants import (
    ID_REPORT,
    MIN_DATE,
    SISTEMAS,
    URL,
    VIEW_GENERATOR,
)
from global_constants import HEADERS
from global_utils.chunk_date_range import chunk_date_range
from global_utils.download_zip import download_zip
from global_utils.extract_field_value import extract_field_value
from global_utils.extract_viewstate import extract_viewstate
from global_utils.get_download_folder import get_download_folder


def download_capacidad_transferencia_files(
    start_date: str | None = None,
    end_date: str | None = None,
    sistema: str | None = None,
):
    """
    Downloads Capacidad de Transferencia en Enlaces Internacionales MDA files.

    Args:
        start_date (str, optional): Start date for date range downloads (YYYY-MM-DD)
        end_date (str, optional): End date for date range downloads (YYYY-MM-DD)
        sistema (str, optional): Specific system to download ('SIN' or 'BCA').
                                 If not provided, downloads all systems.
    """
    download_folder = get_download_folder(start_date=start_date, end_date=end_date)

    # Validate sistema if provided
    if sistema and sistema not in SISTEMAS:
        notify_error(f"[Capacidad de Transferencia] Sistema invalido: '{sistema}'. Debe ser uno de {SISTEMAS}")
        return

    # 1. Determine ranges to process
    ranges_to_process = []

    if start_date and end_date:
        try:
            # chunk_date_range handles format conversion (YYYY-MM-DD -> DD/MM/YYYY)
            ranges_to_process = list(chunk_date_range(start_date, end_date))
        except ValueError as e:
            notify_error(f"[Capacidad de Transferencia] Error al parsear fechas: {e}")
            return
    else:
        # No dates provided: use website defaults (Cron mode)
        ranges_to_process = [(None, None)]

    # 2. Process each range (chunk)
    for fmt_start, fmt_end in ranges_to_process:
        _process_single_range(download_folder, fmt_start, fmt_end, sistema)


def _process_single_range(download_folder, fmt_start, fmt_end, sistema=None):
    """Helper to run the A->B->C flow for a specific date range."""
    # Use a fresh session for each chunk to prevent ViewState corruption
    try:
        session = requests.session()
        response = session.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        notify_error(f"[Capacidad de Transferencia] Error al inicializar sesion: {e}")
        return

    # Extract initial values from the page
    try:
        view_state = extract_field_value(soup, "__VIEWSTATE", "input")
        default_period = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input"
        )
        default_date = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
        )
        max_date_val = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect", "input"
        )
    except Exception as e:
        notify_error(f"[Capacidad de Transferencia] Error al extraer valores iniciales del formulario: {e}")
        return

    # Determine values to use
    if fmt_start and fmt_end:
        period_str = f"{fmt_start} - {fmt_end}"
        sel_start = fmt_start
        sel_end = fmt_end
        sistema_msg = f" for {sistema}" if sistema else ""
        logging.info(
            f"Downloading Capacidad de Transferencia range: {period_str}{sistema_msg}"
        )
    else:
        period_str = default_period
        sel_start = default_date
        sel_end = default_date
        sistema_msg = f" for {sistema}" if sistema else ""
        logging.info(
            f"Downloading Capacidad de Transferencia default/current period{sistema_msg}."
        )

    current_view_state = view_state

    # Loop through systems or just the specified one
    sistemas_to_process = [sistema] if sistema else SISTEMAS

    for current_sistema in sistemas_to_process:
        # --- A. Change System ---
        data_sys = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlSistema",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT,
            "ctl00$ContentPlaceHolder1$ddlSistema": current_sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period_str,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": sel_start,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": sel_end,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": MIN_DATE,
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlSistema",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": current_view_state,
            "__VIEWSTATEGENERATOR": VIEW_GENERATOR,
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        try:
            r_sys = session.post(URL, headers=HEADERS, data=data_sys, timeout=30)
            r_sys.raise_for_status()
            current_view_state = extract_viewstate(r_sys.text)
        except Exception as e:
            notify_error(f"[Capacidad de Transferencia] Error al cambiar sistema a '{current_sistema}': {e}")
            continue

        # --- B. Set Period ---
        data_period = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT,
            "ctl00$ContentPlaceHolder1$ddlSistema": current_sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period_str,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": sel_start,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": sel_end,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": MIN_DATE,
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": current_view_state,
            "__VIEWSTATEGENERATOR": VIEW_GENERATOR,
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        try:
            r_per = session.post(URL, headers=HEADERS, data=data_period, timeout=30)
            r_per.raise_for_status()
            current_view_state = extract_viewstate(r_per.text)
            vs_encoded = extract_viewstate(r_per.text, url_encode=True)
        except Exception as e:
            notify_error(f"[Capacidad de Transferencia] Error al fijar periodo para '{current_sistema}': {e}")
            continue

        # --- C. Download ---
        p_enc = urllib.parse.quote_plus(period_str, safe="")
        s_enc = urllib.parse.quote_plus(sel_start, safe="")
        e_enc = urllib.parse.quote_plus(sel_end, safe="")
        id_enc = urllib.parse.quote_plus(ID_REPORT, safe="")
        min_enc = urllib.parse.quote_plus(MIN_DATE, safe="")
        max_enc = urllib.parse.quote_plus(max_date_val, safe="")

        payload = (
            f"ctl00%24ContentPlaceHolder1%24ddlReporte={id_enc}&"
            f"ctl00%24ContentPlaceHolder1%24ddlSistema={current_sistema}&"
            f"ctl00%24ContentPlaceHolder1%24txtPeriodo={p_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={s_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={e_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect={min_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={max_enc}&"
            f"ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&"
            f"__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&"
            f"__VIEWSTATE={vs_encoded}&"
            f"__VIEWSTATEGENERATOR={VIEW_GENERATOR}&"
            f"__VIEWSTATEENCRYPTED="
        )

        try:
            r_down = session.post(URL, headers=HEADERS, data=payload, timeout=60)
            if r_down.status_code == 200:
                cd = r_down.headers.get("Content-Disposition", "")
                download_zip(cd, download_folder, r_down)
            else:
                notify_error(f"[Capacidad de Transferencia] Descarga fallida para '{current_sistema}': HTTP {r_down.status_code}")
        except Exception as e:
            notify_error(f"[Capacidad de Transferencia] Error al descargar '{current_sistema}': {e}")
