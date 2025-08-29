from selenium.webdriver.chrome.options import Options

def get_selenium_options(headless: bool, download_folder: str) -> Options:
    """
    Get Chrome WebDriver options.

    headless: Whether to run Chrome in headless mode.
    download_folder: The folder to use for downloading files.
    """
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")

    # Always set window size unless headless
    if not headless:
        chrome_options.add_argument("--window-size=1280,720")

    # Headless options
    if headless:
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")

    # Download folder options
    if download_folder:
        prefs = {
            "download.default_directory": download_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)

    return chrome_options