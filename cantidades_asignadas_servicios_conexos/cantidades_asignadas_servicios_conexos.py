import logging

from cantidades_asignadas_servicios_conexos.download_cantidades_asignadas_servicios_conexos_files import (
    download_cantidades_asignadas_servicios_conexos_files,
)
from cantidades_asignadas_servicios_conexos.process_cantidades_asignadas_servicios_conexos import (
    process_cantidades_asignadas_servicios_conexos,
)
from global_utils.delete_csv_files_after_process import delete_csv_files_after_process
from global_utils.notify_error import notify_error


def get_cantidades_asignadas_servicios_conexos_mda(**kwargs):
    """
    Trigger Cantidades Asignadas de Servicios Conexos MDA process.

    Args:
        start_date (str, optional): Start date for date range downloads (YYYY-MM-DD)
        end_date (str, optional): End date for date range downloads (YYYY-MM-DD)
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    if bool(start_date) != bool(end_date):
        notify_error(
            "[Cantidades Asignadas SC MDA] Error de validacion: se debe proporcionar "
            "start_date y end_date juntos, o ninguno de los dos."
        )
        return

    try:
        download_cantidades_asignadas_servicios_conexos_files(
            "MDA", start_date=start_date, end_date=end_date, sistema=sistema
        )
        logging.info("Downloaded Cantidades Asignadas SC MDA files")
        process_cantidades_asignadas_servicios_conexos(
            "MDA", start_date=start_date, end_date=end_date
        )
        if not start_date and not end_date:
            delete_csv_files_after_process()
        logging.info("Cantidades Asignadas SC MDA process finished.")
    except Exception as e:
        notify_error(f"[Cantidades Asignadas SC MDA] Error inesperado en la ejecucion: {e}")


def get_cantidades_asignadas_servicios_conexos_mtr(**kwargs):
    """
    Trigger Cantidades Asignadas de Servicios Conexos MTR process.

    Args:
        start_date (str, optional): Start date for date range downloads (YYYY-MM-DD)
        end_date (str, optional): End date for date range downloads (YYYY-MM-DD)
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    if bool(start_date) != bool(end_date):
        notify_error(
            "[Cantidades Asignadas SC MTR] Error de validacion: se debe proporcionar "
            "start_date y end_date juntos, o ninguno de los dos."
        )
        return

    try:
        download_cantidades_asignadas_servicios_conexos_files(
            "MTR", start_date=start_date, end_date=end_date, sistema=sistema
        )
        logging.info("Downloaded Cantidades Asignadas SC MTR files")
        process_cantidades_asignadas_servicios_conexos(
            "MTR", start_date=start_date, end_date=end_date
        )
        if not start_date and not end_date:
            delete_csv_files_after_process()
        logging.info("Cantidades Asignadas SC MTR process finished.")
    except Exception as e:
        notify_error(f"[Cantidades Asignadas SC MTR] Error inesperado en la ejecucion: {e}")
