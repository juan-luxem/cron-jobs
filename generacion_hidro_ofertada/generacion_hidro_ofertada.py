from .get_generacion_hidro_ofertada import get_generacion_hidro_ofertada
from .process_generacion_hidro_ofertada import process_generacion_hidro_ofertada
from global_utils import delete_csv_files_after_process


def get_generacion_hidro_ofertada_mda():
    get_generacion_hidro_ofertada("MDA")
    process_generacion_hidro_ofertada("MDA")
    delete_csv_files_after_process()


def get_generacion_hidro_ofertada_mtr():
    get_generacion_hidro_ofertada("MTR")
    process_generacion_hidro_ofertada("MTR")
    delete_csv_files_after_process()
