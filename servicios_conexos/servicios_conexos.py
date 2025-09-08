from servicios_conexos.get_generic_servicios_conexos import get_servicios_conexos_generic
from servicios_conexos.process_servicios_conexos import process_servicios_conexos_data


def get_servicios_mda():
    get_servicios_conexos_generic('MDA')
    process_servicios_conexos_data('MDA')


def get_servicios_mtr():
    get_servicios_conexos_generic('MTR')
    process_servicios_conexos_data('MTR')
