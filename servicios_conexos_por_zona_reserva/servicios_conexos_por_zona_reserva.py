from .get_servicios_conexos_por_zona_reserva import (
    get_servicios_conexos_por_zona_reserva,
)
from .process_servicios_conexos_por_zona_reserva import (
    process_servicios_conexos_por_zona_reserva,
)


def run_servicios_conexos_por_zona_reserva():
    get_servicios_conexos_por_zona_reserva()
    process_servicios_conexos_por_zona_reserva()
