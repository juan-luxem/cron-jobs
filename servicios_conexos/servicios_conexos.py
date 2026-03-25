import logging

from global_utils.delete_csv_files_after_process import delete_csv_files_after_process
from global_utils.notify_error import notify_error
from servicios_conexos.download_servicios_conexos_files import (
    download_servicios_conexos_files,
)
from servicios_conexos.process_servicios_conexos import process_servicios_conexos_data


def get_servicios_conexos_mda(**kwargs):
    """
    Trigger Servicios Conexos MDA process. Accepts optional start_date, end_date, and sistema.

    Args:
        start_date (str, optional): Start date for date range downloads
        end_date (str, optional): End date for date range downloads
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    if bool(start_date) != bool(end_date):
        notify_error(
            f"[Servicios Conexos MDA] Error de validacion: start_date y end_date deben proporcionarse juntos. "
            f"Recibido: start_date={start_date!r}, end_date={end_date!r}"
        )
        return

    try:
        download_servicios_conexos_files(
            "MDA", start_date=start_date, end_date=end_date, sistema=sistema
        )
        logging.info("Downloaded Servicios Conexos MDA files")

        process_servicios_conexos_data("MDA", start_date=start_date, end_date=end_date)

        if not start_date and not end_date:
            delete_csv_files_after_process()
        logging.info("Servicios Conexos MDA process finished.")
    except Exception as e:
        notify_error(f"[Servicios Conexos MDA] Error inesperado en la ejecucion: {e}")


def get_servicios_conexos_mtr(**kwargs):
    """
    Trigger Servicios Conexos MTR process. Accepts optional start_date, end_date, and sistema.

    Args:
        start_date (str, optional): Start date for date range downloads
        end_date (str, optional): End date for date range downloads
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    if bool(start_date) != bool(end_date):
        notify_error(
            f"[Servicios Conexos MTR] Error de validacion: start_date y end_date deben proporcionarse juntos. "
            f"Recibido: start_date={start_date!r}, end_date={end_date!r}"
        )
        return

    try:
        download_servicios_conexos_files(
            "MTR", start_date=start_date, end_date=end_date, sistema=sistema
        )
        logging.info("Downloaded Servicios Conexos MTR files")

        process_servicios_conexos_data("MTR", start_date=start_date, end_date=end_date)

        if not start_date and not end_date:
            delete_csv_files_after_process()
        logging.info("Servicios Conexos MTR process finished.")
    except Exception as e:
        notify_error(f"[Servicios Conexos MTR] Error inesperado en la ejecucion: {e}")
