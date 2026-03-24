from global_utils.delete_csv_files_after_process import delete_csv_files_after_process
from pnd.download_pnd_files import download_pnd_files
from pnd.process_pnd import process_pnd_data


def get_pnd_mda(**kwargs):
    """
    Trigger PND MDA process. Accepts optional start_date, end_date, and sistema.

    Args:
        start_date (str, optional): Start date for date range downloads
        end_date (str, optional): End date for date range downloads
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    download_pnd_files("MDA", start_date=start_date, end_date=end_date, sistema=sistema)
    print("Downloaded PND MDA files")

    # Try passing dates in case process_pnd_data supports them,
    # fallback to original call signature if it hasn't been migrated yet.
    try:
        process_pnd_data("MDA", start_date=start_date, end_date=end_date)
    except TypeError:
        process_pnd_data("MDA")

    if not start_date and not end_date:
        delete_csv_files_after_process()
    print("PND MDA process finished.")


def get_pnd_mtr(**kwargs):
    """
    Trigger PND MTR process. Accepts optional start_date, end_date, and sistema.

    Args:
        start_date (str, optional): Start date for date range downloads
        end_date (str, optional): End date for date range downloads
        sistema (str, optional): Specific system to download ('SIN', 'BCA', or 'BCS')
    """
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    sistema = kwargs.get("sistema")

    download_pnd_files("MTR", start_date=start_date, end_date=end_date, sistema=sistema)
    print("Downloaded PND MTR files")

    try:
        process_pnd_data("MTR", start_date=start_date, end_date=end_date)
    except TypeError:
        process_pnd_data("MTR")

    if not start_date and not end_date:
        delete_csv_files_after_process()
    print("PND MTR process finished.")
