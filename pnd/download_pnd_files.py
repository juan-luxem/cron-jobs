import logging
import urllib.parse

import requests
from bs4 import BeautifulSoup

from constants import HEADERS, SISTEMAS
from global_utils import get_download_folder
from global_utils.chunk_date_range import chunk_date_range
from global_utils.download_zip import download_zip
from global_utils.extract_field_value import extract_field_value
from global_utils.extract_viewstate import extract_viewstate
from global_utils.notify_error import notify_error
from pnd.constants import ID_REPORT, MIN_DATE, URL, VIEW_GENERATOR


def download_pnd_files(
    market_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
    sistema: str | None = None,
):
    download_folder = get_download_folder(start_date=start_date, end_date=end_date)

    # Validate sistema if provided
    if sistema and sistema not in SISTEMAS:
        notify_error(f"Invalid sistema: {sistema}. Must be one of {SISTEMAS}")
        return

    # 1. Determine ranges to process
    ranges_to_process = []

    if start_date and end_date:
        try:
            # We rely on run.py for length validation.
            # chunk_date_range handles the format conversion (YYYY-MM-DD -> DD/MM/YYYY)
            ranges_to_process = list(chunk_date_range(start_date, end_date))
        except ValueError as e:
            notify_error(f"Date parsing error in PND {market_type}: {e}")
            return
    else:
        # No dates provided: Use None to indicate "use website defaults" (Cron mode)
        ranges_to_process = [(None, None)]

    # 2. Process each range (Chunk)
    for fmt_start, fmt_end in ranges_to_process:
        _process_single_range(market_type, download_folder, fmt_start, fmt_end, sistema)


def _process_single_range(
    market_type, download_folder, fmt_start, fmt_end, sistema=None
):
    """Helper to run the flow for a specific date range."""
    # We use a fresh session for each chunk to prevent ViewState corruption
    try:
        session = requests.session()
        response = session.get(
            URL[market_type], headers={"User-Agent": "Mozilla/5.0"}, timeout=30
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        notify_error(f"Error initializing session for PND {market_type}: {e}")
        return

    # Extract initial values
    try:
        view_state = extract_field_value(soup, "__VIEWSTATE", "input")
        default_period = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input"
        )
        default_date = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
        )
    except Exception as e:
        notify_error(f"Error extracting initial values for PND {market_type}: {e}")
        return

    # Determine values to use
    if fmt_start and fmt_end:
        period_str = f"{fmt_start} - {fmt_end}"
        sel_start = fmt_start
        sel_end = fmt_end
        sistema_msg = f" for {sistema}" if sistema else ""
        logging.info(f"Downloading PND range: {period_str}{sistema_msg}")
    else:
        period_str = default_period
        sel_start = default_date
        sel_end = default_date
        sistema_msg = f" for {sistema}" if sistema else ""
        logging.info(f"Downloading PND default/current period{sistema_msg}.")

    try:
        min_date_val = MIN_DATE[market_type]
        max_date_val = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect", "input"
        )
    except Exception as e:
        notify_error(f"Error extracting dates for PND {market_type}: {e}")
        return

    current_view_state = view_state

    # Loop through systems (SIN, BCA, BCS) or just the specified one
    sistemas_to_process = [sistema] if sistema else SISTEMAS

    for current_sistema in sistemas_to_process:
        # --- A. Change System ---
        data_sys = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlSistema",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": current_sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period_str,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": sel_start,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": sel_end,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": min_date_val,
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlSistema",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": current_view_state,
            "__VIEWSTATEGENERATOR": VIEW_GENERATOR[market_type],
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
            notify_error(
                f"Error switching system to {current_sistema} in PND {market_type}: {e}"
            )
            continue

        # --- B. Set Period ---
        data_period = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": ID_REPORT[market_type],
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": current_sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period_str,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": sel_start,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": sel_end,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": min_date_val,
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": max_date_val,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": current_view_state,
            "__VIEWSTATEGENERATOR": VIEW_GENERATOR[market_type],
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
            vs_encoded = extract_viewstate(r_per.text, url_encode=True)
        except Exception as e:
            notify_error(
                f"Error setting period for {current_sistema} in PND {market_type}: {e}"
            )
            continue

        # --- C. Download ---
        # URL encode params
        p_enc = urllib.parse.quote_plus(period_str, safe="")
        s_enc = urllib.parse.quote_plus(sel_start, safe="")
        e_enc = urllib.parse.quote_plus(sel_end, safe="")
        id_enc = urllib.parse.quote_plus(str(ID_REPORT[market_type]), safe="")
        min_enc = urllib.parse.quote_plus(min_date_val, safe="")
        max_enc = urllib.parse.quote_plus(max_date_val, safe="")

        payload = (
            f"ctl00%24ContentPlaceHolder1%24ddlReporte={id_enc}&"
            f"ctl00%24ContentPlaceHolder1%24ddlPeriodicidad=D&"
            f"ctl00%24ContentPlaceHolder1%24ddlSistema={current_sistema}&"
            f"ctl00%24ContentPlaceHolder1%24txtPeriodo={p_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={s_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={e_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect={min_enc}&"
            f"ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={max_enc}&"
            f"ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&"
            f"__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&"
            f"__VIEWSTATE={vs_encoded}&"
            f"__VIEWSTATEGENERATOR={VIEW_GENERATOR[market_type]}&"
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
                notify_error(
                    f"Download failed for {current_sistema} in PND {market_type}: HTTP {r_down.status_code}"
                )
        except Exception as e:
            notify_error(
                f"Error downloading {current_sistema} in PND {market_type}: {e}"
            )
