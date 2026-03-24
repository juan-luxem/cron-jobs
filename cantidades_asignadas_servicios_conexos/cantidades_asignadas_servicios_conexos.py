from cantidades_asignadas_servicios_conexos.download_cantidades_asignadas_servicios_conexos_files import (
    download_cantidades_asignadas_servicios_conexos_files,
)
from cantidades_asignadas_servicios_conexos.process_cantidades_asignadas_servicios_conexos_data import (
    process_cantidades_asignadas_servicios_conexos_data,
)

from global_utils.delete_csv_files_after_process import delete_csv_files_after_process


def get_cantidades_asignadas_servicios_conexos_mda():
    download_cantidades_asignadas_servicios_conexos_files("MDA")
    process_cantidades_asignadas_servicios_conexos_data("MDA")
    delete_csv_files_after_process()


def get_cantidades_asignadas_servicios_conexos_mtr():
    download_cantidades_asignadas_servicios_conexos_files("MTR")
    process_cantidades_asignadas_servicios_conexos_data("MTR")
    delete_csv_files_after_process()


# def get_req_servicios_conexos_mda(**kwargs):
#     """
#     Trigger Servicios Conexos MDA process. Accepts optional start_date, end_date, and sistema.

#     Args:
#         start_date (str, optional): Start date for date range downloads
#         end_date (str, optional): End date for date range downloads
#         sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
#     """
#     start_date = kwargs.get("start_date")
#     end_date = kwargs.get("end_date")
#     sistema = kwargs.get("sistema")
#     download_req_servicios_conexos_files(
#         "MDA", start_date=start_date, end_date=end_date, sistema=sistema
#     )
#     process_req_servicios_conexos_data("MDA", start_date=start_date, end_date=end_date)
#     if not start_date and not end_date:
#         delete_csv_files_after_process()
#     print("Requerimientos de Servicios Conexos MDA process started.")


# def get_req_servicios_conexos_mtr(**kwargs):
#     """
#     Trigger Requerimientos de Servicios Conexos MTR process. Accepts optional start_date, end_date, and sistema.

#     Args:
#         start_date (str, optional): Start date for date range downloads
#         end_date (str, optional): End date for date range downloads
#         sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
#     """
#     start_date = kwargs.get("start_date")
#     end_date = kwargs.get("end_date")
#     sistema = kwargs.get("sistema")
#     download_req_servicios_conexos_files(
#         "MTR", start_date=start_date, end_date=end_date, sistema=sistema
#     )
#     process_req_servicios_conexos_data("MTR", start_date=start_date, end_date=end_date)
#     if not start_date and not end_date:
#         delete_csv_files_after_process()
#     print("Requerimientos de Servicios Conexos MTR process started.")
