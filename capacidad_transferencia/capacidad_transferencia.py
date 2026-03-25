import logging

from capacidad_transferencia.download_capacidad_transferencia_files import (
    download_capacidad_transferencia_files,
)
from capacidad_transferencia.process_capacidad_transferencia import (
    process_capacidad_transferencia_data,
)
from global_utils.delete_csv_files_after_process import delete_csv_files_after_process
from global_utils.notify_error import notify_error


def get_capacidad_transferencia(**kwargs) -> None:
    """
    Main trigger function for Capacidad de Transferencia MDA.
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    if bool(start_date) != bool(end_date):
        notify_error(
            f"[Capacidad de Transferencia] Error de validacion: start_date y end_date deben proporcionarse juntos. "
            f"Recibido: start_date={start_date!r}, end_date={end_date!r}"
        )
        return

    try:
        logging.info("Starting Capacidad de Transferencia process...")

        # 1. Download files
        download_capacidad_transferencia_files(
            start_date=start_date, end_date=end_date, sistema=sistema
        )

        # 2. Process and send to API
        process_capacidad_transferencia_data(start_date=start_date, end_date=end_date)

        # 3. Clean up only if running in Cron mode (no dates provided)
        if start_date is None and end_date is None:
            delete_csv_files_after_process()

        logging.info("Capacidad de Transferencia process completed.")

    except Exception as e:
        notify_error(f"[Capacidad de Transferencia] Error inesperado en la ejecucion: {e}")
