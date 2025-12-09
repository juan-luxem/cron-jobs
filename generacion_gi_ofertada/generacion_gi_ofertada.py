from .get_generacion_gi_ofertada import get_generacion_gi_ofertada_generic
from .process_generacion_gi_ofertada import process_generacion_gi_ofertada
from global_utils import delete_csv_files_after_process


def get_generacion_gi_ofertada_mda():
    get_generacion_gi_ofertada_generic("MDA")
    process_generacion_gi_ofertada("MDA")
    delete_csv_files_after_process()


def get_generacion_gi_ofertada_mtr():
    get_generacion_gi_ofertada_generic("MTR")
    process_generacion_gi_ofertada("MTR")
    delete_csv_files_after_process()
