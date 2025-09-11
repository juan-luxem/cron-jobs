from .get_generic_generacion_ndso_ofertada import get_generacion_ndso_ofertada_generic
from .process_generacion_ndso_ofertada import process_generacion_ndso_ofertada

def get_generacion_ndso_ofertada_mda():
    get_generacion_ndso_ofertada_generic('MDA')
    process_generacion_ndso_ofertada('MDA')

def get_generacion_ndso_ofertada_mtr():
    get_generacion_ndso_ofertada_generic('MTR')
    process_generacion_ndso_ofertada('MTR')
