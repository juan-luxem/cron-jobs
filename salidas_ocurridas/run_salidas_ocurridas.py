from salidas_ocurridas.get_salidas_ocurridas import get_salidas_ocurridas
from salidas_ocurridas.process_salidas_ocurridas import process_salidas_ocurridas
from global_utils import delete_csv_files_after_process

def run_salidas_ocurridas():
    get_salidas_ocurridas()
    process_salidas_ocurridas()
    delete_csv_files_after_process()