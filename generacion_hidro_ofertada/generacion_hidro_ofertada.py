from .get_generic_generacion_hidro_ofertada import get_generacion_hidro_ofertada_generic
from .process_generacion_hidro_ofertada import process_generacion_hidro_ofertada


def get_generacion_hidro_ofertada_mda():
    get_generacion_hidro_ofertada_generic("MDA")
    process_generacion_hidro_ofertada("MDA")


def get_generacion_hidro_ofertada_mtr():
    get_generacion_hidro_ofertada_generic("MTR")
    process_generacion_hidro_ofertada("MTR")
