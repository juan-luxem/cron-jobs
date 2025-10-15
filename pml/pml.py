from pml.get_generic_pml import get_pml_generic
from pml.process_pml import process_pml_data
from global_utils import delete_csv_files_after_process

def get_pml_mda():
    get_pml_generic('MDA')
    process_pml_data('MDA')
    delete_csv_files_after_process()


def get_pml_mtr():
    get_pml_generic('MTR')
    process_pml_data('MTR')
    delete_csv_files_after_process()