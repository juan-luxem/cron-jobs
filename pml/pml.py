from global_utils.delete_csv_files_after_process import delete_csv_files_after_process
from pml.download_pml_files import download_pml_files
from pml.process_pml import process_pml_data


def get_pml_mda(**kwargs):
    """
    Trigger PML MDA process. Accepts optional start_date, end_date, and sistema.

    Args:
        start_date (str, optional): Start date for date range downloads
        end_date (str, optional): End date for date range downloads
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")
    download_pml_files("MDA", start_date=start_date, end_date=end_date, sistema=sistema)
    print("downloaded PML MDA files")

    # Intentamos pasar los argumentos de fecha por si process_pml_data ya los soporta,
    # si no (como en la versión actual), llamamos sin ellos temporalmente hasta que lo migremos.
    try:
        process_pml_data("MDA", start_date=start_date, end_date=end_date)
    except TypeError:
        process_pml_data("MDA")

    if not start_date and not end_date:
        delete_csv_files_after_process()
    print("PML MDA process started.")


def get_pml_mtr(**kwargs):
    """
    Trigger PML MTR process. Accepts optional start_date, end_date, and sistema.

    Args:
        start_date (str, optional): Start date for date range downloads
        end_date (str, optional): End date for date range downloads
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")
    download_pml_files("MTR", start_date=start_date, end_date=end_date, sistema=sistema)

    try:
        process_pml_data("MTR", start_date=start_date, end_date=end_date)
    except TypeError:
        process_pml_data("MTR")

    if not start_date and not end_date:
        delete_csv_files_after_process()
    print("PML MTR process started.")
