import logging

from global_utils.delete_csv_files_after_process import delete_csv_files_after_process
from global_utils.notify_error import notify_error
from pml.download_pml_files import download_pml_files
from pml.process_pml import process_pml_data


def get_pml_mda(**kwargs):
    """
    Trigger PML MDA process. Accepts optional start_date, end_date, and sistema.

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
            f"[PML MDA] Error de validacion: start_date y end_date deben proporcionarse juntos. "
            f"Recibido: start_date={start_date!r}, end_date={end_date!r}"
        )
        return

    try:
        download_pml_files("MDA", start_date=start_date, end_date=end_date, sistema=sistema)
        logging.info("Downloaded PML MDA files")

        process_pml_data("MDA", start_date=start_date, end_date=end_date)

        if not start_date and not end_date:
            delete_csv_files_after_process()
        logging.info("PML MDA process finished.")
    except Exception as e:
        notify_error(f"[PML MDA] Error inesperado en la ejecucion: {e}")


def get_pml_mtr(**kwargs):
    """
    Trigger PML MTR process. Accepts optional start_date, end_date, and sistema.

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
            f"[PML MTR] Error de validacion: start_date y end_date deben proporcionarse juntos. "
            f"Recibido: start_date={start_date!r}, end_date={end_date!r}"
        )
        return

    try:
        download_pml_files("MTR", start_date=start_date, end_date=end_date, sistema=sistema)
        logging.info("Downloaded PML MTR files")

        process_pml_data("MTR", start_date=start_date, end_date=end_date)

        if not start_date and not end_date:
            delete_csv_files_after_process()
        logging.info("PML MTR process finished.")
    except Exception as e:
        notify_error(f"[PML MTR] Error inesperado en la ejecucion: {e}")
