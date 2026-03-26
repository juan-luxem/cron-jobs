import logging
import urllib.parse

import requests
from bs4 import BeautifulSoup

from cantidades_asignadas_servicios_conexos.constants import URL
from global_constants import HEADERS, SISTEMAS
from global_utils import get_download_folder
from global_utils.chunk_date_range import chunk_date_range
from global_utils.download_zip import download_zip
from global_utils.extract_field_value import extract_field_value
from global_utils.extract_viewstate import extract_viewstate
from global_utils.notify_error import notify_error


def download_cantidades_asignadas_servicios_conexos_files(
    market_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
    sistema: str | None = None,
):
    """
    Downloads Cantidades Asignadas de Servicios Conexos files.
    Uses dynamic extraction for IDs and a robust System->Period->Download flow.

    Args:
        market_type (str): 'MDA' or 'MTR'
        start_date (str, optional): Start date for date range downloads (YYYY-MM-DD)
        end_date (str, optional): End date for date range downloads (YYYY-MM-DD)
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS').
                                 If not provided, downloads all systems.
    """
    download_folder = get_download_folder(start_date=start_date, end_date=end_date)

    if sistema and sistema not in SISTEMAS:
        notify_error(
            f"[Cantidades Asignadas SC] Sistema invalido: '{sistema}'. Debe ser uno de {SISTEMAS}"
        )
        return

    ranges_to_process = []

    if start_date and end_date:
        try:
            ranges_to_process = list(chunk_date_range(start_date, end_date))
        except ValueError as e:
            notify_error(f"[Cantidades Asignadas SC] Error al parsear fechas: {e}")
            return
    else:
        ranges_to_process = [(None, None)]

    for fmt_start, fmt_end in ranges_to_process:
        _process_single_range(market_type, download_folder, fmt_start, fmt_end, sistema)


def _process_single_range(
    market_type, download_folder, fmt_start, fmt_end, sistema=None
):
    """
    Executes the System->Period->Download workflow for a specific date range.
    """
    try:
        session = requests.session()
        response = session.get(
            URL[market_type], headers={"User-Agent": "Mozilla/5.0"}, timeout=30
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        notify_error(
            f"[Cantidades Asignadas SC] Error al inicializar sesion para {market_type}: {e}"
        )
        return

    try:
        id_report = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$ddlReporte", "select"
        )
        if not id_report or id_report == "None":
            selected_option = soup.select_one(
                "select[name='ctl00$ContentPlaceHolder1$ddlReporte'] option[selected]"
            )
            if selected_option:
                id_report = selected_option.get("value")

        view_generator = extract_field_value(soup, "__VIEWSTATEGENERATOR", "input")
        min_date_val = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$hdfMinDateToSelect", "input"
        )
        max_date_val = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect", "input"
        )
        view_state = extract_field_value(soup, "__VIEWSTATE", "input")
        default_period = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input"
        )
        default_date = extract_field_value(
            soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
        )
    except Exception as e:
        notify_error(
            f"[Cantidades Asignadas SC] Error al extraer valores dinamicos de la pagina para {market_type}: {e}"
        )
        return

    if fmt_start and fmt_end:
        target_period_str = f"{fmt_start} - {fmt_end}"
        target_start = fmt_start
        target_end = fmt_end
        logging.info(
            f"Downloading Cantidades Asignadas SC {market_type} range: {target_period_str}"
        )
    else:
        target_period_str = default_period
        target_start = default_date
        target_end = default_date
        logging.info(
            f"Downloading Cantidades Asignadas SC {market_type} default/current period."
        )

    current_view_state = view_state

    sistemas_to_process = [sistema] if sistema else SISTEMAS

    for current_sistema in sistemas_to_process:
        # --- Step A: Switch System ---
        data_sys = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlSistema",
            "ctl00$ContentPlaceHolder1$ddlReporte": id_report,
            "ctl00$ContentPlaceHolder1$ddlSistema": current_sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": target_period_str
            if fmt_start
            else default_period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": target_start
            if fmt_start
            else default_date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": target_end
            if fmt_start
            else default_date,
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
            notify_error(
                f"[Cantidades Asignadas SC] Error al cambiar sistema a '{current_sistema}' ({market_type}): {e}"
            )
            continue

        # --- Step B: Set Period ---
        data_period = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": id_report,
            "ctl00$ContentPlaceHolder1$ddlSistema": current_sistema,
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
            vs_encoded = extract_viewstate(r_per.text, url_encode=True)
        except Exception as e:
            notify_error(
                f"[Cantidades Asignadas SC] Error al fijar periodo para '{current_sistema}' ({market_type}): {e}"
            )
            continue

        # --- Step C: Download ---
        p_enc = urllib.parse.quote_plus(target_period_str, safe="")
        s_enc = urllib.parse.quote_plus(target_start, safe="")
        e_enc = urllib.parse.quote_plus(target_end, safe="")
        id_enc = urllib.parse.quote_plus(id_report, safe="")  # type: ignore
        min_enc = urllib.parse.quote_plus(min_date_val, safe="")
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
                notify_error(
                    f"[Cantidades Asignadas SC] Descarga fallida para '{current_sistema}' ({market_type}): HTTP {r_down.status_code}"
                )
        except Exception as e:
            notify_error(
                f"[Cantidades Asignadas SC] Error al descargar '{current_sistema}' ({market_type}): {e}"
            )
