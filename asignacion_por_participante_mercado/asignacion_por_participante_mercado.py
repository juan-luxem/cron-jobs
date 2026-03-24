import logging

from global_utils import delete_csv_files_after_process

from .download_asignacion_por_participante_mercado_files import (
    download_asignacion_por_participante_mercado_files,
)
from .process_asignacion_por_participante_mercado import (
    process_asignacion_por_participante_mercado,
)


def get_asignacion_por_participante_mercado_mda(**kwargs):
    """
    Trigger Asignación por Participante del Mercado MDA process.
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    download_asignacion_por_participante_mercado_files(
        market_type="MDA",
        start_date=start_date,
        end_date=end_date,
        sistema=sistema,
    )

    process_asignacion_por_participante_mercado(
        market_type="MDA",
        start_date=start_date,
        end_date=end_date,
    )

    if not start_date and not end_date:
        delete_csv_files_after_process()

    logging.info("Asignacion por Participante Mercado process finished.")
