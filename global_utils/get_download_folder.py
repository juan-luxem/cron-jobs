from config import ENV


def get_download_folder(start_date: str | None = None, end_date: str | None = None):
    if start_date and end_date:
        return ENV.DOWNLOAD_FOLDER_RANGE
    else:
        return ENV.DOWNLOAD_FOLDER
