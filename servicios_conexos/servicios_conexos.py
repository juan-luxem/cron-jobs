from servicios_conexos.get_servicios_conexos import get_servicios_conexos
from servicios_conexos.process_servicios_conexos import process_servicios_conexos_data
from global_utils import delete_csv_files_after_process


def get_servicios_mda():
    get_servicios_conexos("MDA")
    # process_servicios_conexos_data("MDA")
    # delete_csv_files_after_process()


def get_servicios_mtr():
    get_servicios_conexos("MTR")
    # process_servicios_conexos_data("MTR")
    # delete_csv_files_after_process()
