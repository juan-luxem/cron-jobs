from .get_servicios_conexos_por_zona_reserva import (
    get_servicios_conexos_por_zona_reserva,
)
from .process_servicios_conexos_por_zona_reserva import (
    process_servicios_conexos_por_zona_reserva,
)
from global_utils import delete_csv_files_after_process

def run_servicios_conexos_por_zona_reserva():
    get_servicios_conexos_por_zona_reserva()
    process_servicios_conexos_por_zona_reserva()
    delete_csv_files_after_process()
