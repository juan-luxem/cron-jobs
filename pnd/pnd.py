from pnd.get_generic_pnd import get_pnd_generic
from pnd.process_pnd import process_pnd_data
from global_utils import delete_csv_files_after_process

def get_pml_mda():
    get_pnd_generic("MDA")
    process_pnd_data("MDA")
    delete_csv_files_after_process()


def get_pml_mtr():
    get_pnd_generic("MTR")
    process_pnd_data("MTR")
    delete_csv_files_after_process()
