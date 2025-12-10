from .get_generacion_ofertada import get_generacion_ofertada
from .process_generacion_ofertada import process_generacion_ofertada
from global_utils import delete_csv_files_after_process


def get_generacion_ofertada_mda():
    get_generacion_ofertada("MDA")
    process_generacion_ofertada("MDA")
    delete_csv_files_after_process()


def get_generacion_ofertada_mtr():
    get_generacion_ofertada("MTR")
    process_generacion_ofertada("MTR")
    delete_csv_files_after_process()
