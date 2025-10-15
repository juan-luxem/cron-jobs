from .get_generic_cantidades_asignadas_servicios_conexos import get_cantidades_asignadas_servicios_conexos_generic
from .process_cantidades_asignadas_servicios_conexos import process_cantidades_asignadas_servicios_conexos
from global_utils import delete_csv_files_after_process

def cantidades_asignadas_servicios_conexos_mda():
    get_cantidades_asignadas_servicios_conexos_generic('MDA')
    process_cantidades_asignadas_servicios_conexos('MDA')
    delete_csv_files_after_process()


def cantidades_asignadas_servicios_conexos_mtr():
    get_cantidades_asignadas_servicios_conexos_generic('MTR')
    process_cantidades_asignadas_servicios_conexos('MTR')
    delete_csv_files_after_process()
