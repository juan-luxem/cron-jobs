import logging
import urllib.parse

import requests
from bs4 import BeautifulSoup

from asignacion_por_participante_mercado.constants import ID_REPORT, URL, VIEW_GENERATOR
from global_constants import HEADERS, SISTEMAS
from global_utils import get_download_folder
from global_utils.chunk_date_range import chunk_date_range
from global_utils.download_zip import download_zip
from global_utils.extract_field_value import extract_field_value
from global_utils.extract_viewstate import extract_viewstate
from global_utils.notify_error import notify_error


def download_asignacion_por_participante_mercado_files(
    market_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
    sistema: str | None = None,
):
    """
    Downloads Asignación por Participante del Mercado files for MDA.
    Flow: Select Report -> Refresh VS -> System -> Refresh VS -> Set Dates -> Download.
    """
    if sistema and sistema not in SISTEMAS:
        notify_error(f"Invalid sistema: {sistema}. Must be one of {SISTEMAS}")
        return

    download_folder = get_download_folder(start_date=start_date, end_date=end_date)

    if market_type not in URL:
        notify_error(
            f"Invalid market_type: {market_type}. Supported types are {list(URL.keys())}"
        )
        return

    ranges_to_process = []
    if start_date and end_date:
        try:
            ranges_to_process = list(chunk_date_range(start_date, end_date))
        except ValueError as e:
            notify_error(f"Date parsing error: {e}")
            return
    else:
        ranges_to_process = [(None, None)]

    for fmt_start, fmt_end in ranges_to_process:
        _process_single_range(market_type, download_folder, fmt_start, fmt_end, sistema)


def _process_single_range(
    market_type, download_folder, fmt_start, fmt_end, sistema=None
):
    """
    Executes the download workflow for a specific date range and market type.
    Flow: Select Report -> Refresh VS -> Loop Systems (Select -> Refresh -> Set Period -> Download).
    """
    sistemas_to_process = [sistema] if sistema else SISTEMAS
    try:
        session = requests.session()
        response = session.get(
            URL[market_type], headers={"User-Agent": "Mozilla/5.0"}, timeout=30
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        notify_error(f"Error initializing session for {market_type}: {e}")
        return

    # Extract initial values from the page
    view_state = extract_field_value(soup, "__VIEWSTATE", "input")
    view_generator = VIEW_GENERATOR[market_type]
    default_period = extract_field_value(
        soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input"
    )
    default_date = extract_field_value(
        soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
    )
    min_date_val = extract_field_value(
        soup, "ctl00$ContentPlaceHolder1$hdfMinDateToSelect", "input"
    )
    max_date_val = extract_field_value(
        soup, "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect", "input"
    )

    # Determine target dates
    if fmt_start and fmt_end:
        target_period_str = f"{fmt_start} - {fmt_end}"
        target_start = fmt_start
        target_end = fmt_end
        logging.info(
            f"Downloading Asignación por Participante del Mercado {market_type} range: {target_period_str}"
        )
    else:
        target_period_str = default_period
        target_start = default_date
        target_end = default_date
        logging.info(
            f"Downloading Asignación por Participante del Mercado {market_type} default/current period."
        )

    current_view_state = view_state

    # --- Step 0: Select Report (348 - Asignación por Participante del Mercado) ---
    logging.info(f"Selecting report: {ID_REPORT[market_type]}")
    data_report = {
        "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlReporte",
        "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
        "ctl00$ContentPlaceHolder1$ddlSistema": "SIN",
        "ctl00$ContentPlaceHolder1$txtPeriodo": default_period,
        "ctl00$ContentPlaceHolder1$hdfStartDateSelected": default_date,
        "ctl00$ContentPlaceHolder1$hdfEndDateSelected": default_date,
        "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": min_date_val,
        "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlReporte",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": current_view_state,
        "__VIEWSTATEGENERATOR": view_generator,
        "__VIEWSTATEENCRYPTED": "",
        "__ASYNCPOST": "true",
        "": "",
    }

    try:
        r_report = session.post(
            URL[market_type], headers=HEADERS, data=data_report, timeout=30
        )
        r_report.raise_for_status()
        current_view_state = extract_viewstate(r_report.text)
    except Exception as e:
        notify_error(f"Error selecting report: {e}")
        return

    # --- Step 0.5: Refresh ViewState after selecting report ---
    data_refresh_report = {
        "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
        "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
        "ctl00$ContentPlaceHolder1$ddlSistema": "SIN",
        "ctl00$ContentPlaceHolder1$txtPeriodo": default_period,
        "ctl00$ContentPlaceHolder1$hdfStartDateSelected": default_date,
        "ctl00$ContentPlaceHolder1$hdfEndDateSelected": default_date,
        "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": min_date_val,
        "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": current_view_state,
        "__VIEWSTATEGENERATOR": view_generator,
        "__VIEWSTATEENCRYPTED": "",
        "__ASYNCPOST": "true",
        "": "",
    }

    try:
        r_ref_report = session.post(
            URL[market_type], headers=HEADERS, data=data_refresh_report, timeout=30
        )
        r_ref_report.raise_for_status()
        current_view_state = extract_viewstate(r_ref_report.text)
    except Exception as e:
        notify_error(f"Error refreshing viewstate after report selection: {e}")
        return

    # Loop through each system
    for sistema in sistemas_to_process:
        logging.info(f"Processing system: {sistema}")

        # --- Step 1: Switch System ---
        data_sys = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlSistema",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": default_period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": default_date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": default_date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": min_date_val,
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlSistema",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": current_view_state,
            "__VIEWSTATEGENERATOR": view_generator,
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        try:
            r_sys = session.post(
                URL[market_type], headers=HEADERS, data=data_sys, timeout=30
            )
            r_sys.raise_for_status()
            current_view_state = extract_viewstate(r_sys.text)
        except Exception as e:
            notify_error(f"Error switching system to {sistema}: {e}")
            continue

        # --- Step 2: Refresh ViewState (Target txtPeriodo with OLD dates) ---
        data_refresh = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": default_period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": default_date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": default_date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": min_date_val,
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": current_view_state,
            "__VIEWSTATEGENERATOR": view_generator,
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        try:
            r_ref = session.post(
                URL[market_type], headers=HEADERS, data=data_refresh, timeout=30
            )
            r_ref.raise_for_status()
            current_view_state = extract_viewstate(r_ref.text)
        except Exception as e:
            notify_error(f"Error refreshing ViewState for {sistema}: {e}")
            continue

        # --- Step 3: Set Period (Target txtPeriodo with NEW dates) ---
        data_period = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": target_period_str,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": target_start,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": target_end,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": min_date_val,
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": current_view_state,
            "__VIEWSTATEGENERATOR": view_generator,
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        try:
            r_per = session.post(
                URL[market_type], headers=HEADERS, data=data_period, timeout=30
            )
            r_per.raise_for_status()
            current_view_state = extract_viewstate(r_per.text)
        except Exception as e:
            notify_error(f"Error setting period for {sistema}: {e}")
            continue

        # --- Step 4: Download ZIP ---
        p_enc = urllib.parse.quote_plus(target_period_str, safe="")
        s_enc = urllib.parse.quote_plus(target_start, safe="")
        e_enc = urllib.parse.quote_plus(target_end, safe="")
        id_enc = urllib.parse.quote_plus(ID_REPORT[market_type], safe="")
        min_enc = urllib.parse.quote_plus(min_date_val, safe="")
        max_enc = urllib.parse.quote_plus(max_date_val, safe="")
        vs_encoded = urllib.parse.quote_plus(current_view_state, safe="")

        payload = (
            f"ctl00%24ContentPlaceHolder1%24ddlReporte={id_enc}&"
            f"ctl00%24ContentPlaceHolder1%24ddlSistema={sistema}&"
            f"ctl00%24ContentPlaceHolder1%24txtPeriodo={p_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={s_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={e_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect={min_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={max_enc}&"
            f"ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&"
            f"__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&"
            f"__VIEWSTATE={vs_encoded}&"
            f"__VIEWSTATEGENERATOR={view_generator}&"
            f"__VIEWSTATEENCRYPTED="
        )

        try:
            r_down = session.post(
                URL[market_type], headers=HEADERS, data=payload, timeout=60
            )
            if r_down.status_code == 200:
                cd = r_down.headers.get("Content-Disposition", "")
                download_zip(cd, download_folder, r_down)
            else:
                notify_error(f"Download failed for {sistema}: {r_down.status_code}")
        except Exception as e:
            notify_error(f"Error downloading {sistema}: {e}")
