import logging

from global_utils import delete_csv_files_after_process

from .download_servicios_conexos_por_zona_reserva_files import (
    download_servicios_conexos_por_zona_reserva_files,
)
from .process_servicios_conexos_por_zona_reserva import (
    process_servicios_conexos_por_zona_reserva,
)


def get_servicios_conexos_por_zona_reserva_mda(**kwargs):
    """
    Trigger Servicios Conexos por Zona de Reserva MDA process.

    Args:
        start_date (str, optional): Start date for date range downloads
        end_date (str, optional): End date for date range downloads
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """

    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    download_servicios_conexos_por_zona_reserva_files(
        market_type="MDA",
        start_date=start_date,
        end_date=end_date,
        sistema=sistema,
    )

    process_servicios_conexos_por_zona_reserva(
        "MDA",
        start_date=start_date,
        end_date=end_date,
    )

    if not start_date and not end_date:
        delete_csv_files_after_process()
    logging.info("Servicios Conexos por Zona de Reserva MDA process finished.")
