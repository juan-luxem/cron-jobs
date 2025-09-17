from .get_generic_cantidades_asignadas_servicios_conexos import get_cantidades_asignadas_servicios_conexos_generic
from .process_cantidades_asignadas_servicios_conexos import process_cantidades_asignadas_servicios_conexos

def cantidades_asignadas_servicios_conexos_mda():
    get_cantidades_asignadas_servicios_conexos_generic('MDA')
    process_cantidades_asignadas_servicios_conexos('MDA')

def cantidades_asignadas_servicios_conexos_mtr():
    get_cantidades_asignadas_servicios_conexos_generic('MTR')
    process_cantidades_asignadas_servicios_conexos('MTR')
