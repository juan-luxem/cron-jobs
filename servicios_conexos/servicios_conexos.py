from global_utils.delete_csv_files_after_process import delete_csv_files_after_process
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

    download_servicios_conexos_files(
        "MDA", start_date=start_date, end_date=end_date, sistema=sistema
    )
    print("Downloaded Servicios Conexos MDA files")

    try:
        process_servicios_conexos_data("MDA", start_date=start_date, end_date=end_date)
    except TypeError:
        process_servicios_conexos_data("MDA")

    if not start_date and not end_date:
        delete_csv_files_after_process()
    print("Servicios Conexos MDA process finished.")


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

    download_servicios_conexos_files(
        "MTR", start_date=start_date, end_date=end_date, sistema=sistema
    )
    print("Downloaded Servicios Conexos MTR files")

    try:
        process_servicios_conexos_data("MTR", start_date=start_date, end_date=end_date)
    except TypeError:
        process_servicios_conexos_data("MTR")

    if not start_date and not end_date:
        delete_csv_files_after_process()
    print("Servicios Conexos MTR process finished.")
