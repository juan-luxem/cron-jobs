from pnd.get_generic_pnd import get_pnd_generic
from pnd.process_pnd import process_pnd_data


def get_pml_mda():
    get_pnd_generic("MDA")
    # process_pnd_data("MDA")


def get_pml_mtr():
    get_pnd_generic("MTR")
    # process_pnd_data("MTR")
