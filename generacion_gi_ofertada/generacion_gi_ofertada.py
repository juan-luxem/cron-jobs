from .get_generic_generacion_gi_ofertada import get_generacion_gi_ofertada_generic
from .process_generacion_gi_ofertada import process_generacion_gi_ofertada

def get_generacion_gi_ofertada_mda():
    get_generacion_gi_ofertada_generic('MDA')
    process_generacion_gi_ofertada('MDA')


def get_generacion_gi_ofertada_mtr():
    get_generacion_gi_ofertada_generic('MTR')
    process_generacion_gi_ofertada('MTR')