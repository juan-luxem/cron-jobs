import sys

sys.path.append("/var/www/html/mercados/python-packages")

import time
import os  # Added to potentially check/create directory

# *** SET WDM CACHE PATH VARIABLES (TRYING BOTH) ***
wdm_cache_path = "/var/www/html/mercados/wdm-cache"
print(f"Attempting to use WDM cache path: {wdm_cache_path}")

try:
    # Ensure directory exists
    os.makedirs(wdm_cache_path, exist_ok=True)
    # Try to write a test file to confirm permissions
    test_file_path = os.path.join(wdm_cache_path, "permission_test.tmp")
    with open(test_file_path, "w") as f:
        f.write("test")
    os.remove(test_file_path)  # Clean up test file
    print(f"Successfully confirmed write access to: {wdm_cache_path}")

    # Set environment variables ONLY if write access is confirmed
    print(f"Setting WDM_LOCAL environment variable to: {wdm_cache_path}")
    print(f"Setting WDM_CACHE_DIR environment variable to: {wdm_cache_path}")
    os.environ["WDM_LOCAL"] = wdm_cache_path
    os.environ["WDM_CACHE_DIR"] = wdm_cache_path
    # *** ENABLE WDM DEBUG LOGGING ***
    # Different versions might use different variables, try WDM_LOG_LEVEL=0 first
    # If no extra output, try WDM_LOG=1 or LOG_LEVEL=DEBUG instead.
    log_var = "WDM_LOG_LEVEL"  # Or 'WDM_LOG', or 'LOG_LEVEL'
    log_val = "0"  # For DEBUG (most verbose)
    print(f"Setting WDM Logging environment variable: {log_var}={log_val}")
    os.environ[log_var] = log_val
except Exception as e:
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(
        f"ERROR: Failed to create or write to cache directory '{wdm_cache_path}': {e}"
    )
    print(
        f"Please ensure the parent directory '/var/www/html/mercados/' exists and the user running the script ('desarrollo'?) has write permissions there."
    )
    print(f"webdriver-manager will likely fail now.")
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    # You might want to exit here if the cache isn't writable
    # sys.exit(1)

# Check what the script sees as HOME (useful debug info)
print(
    f"HOME environment variable is set to: {os.getenv('HOME')}"
)  # Should ideally be None or something other than /home/desarrollo if it doesn't exist

import pathlib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Better import style
from selenium.webdriver.chrome.service import Service  # Added
from webdriver_manager.chrome import ChromeDriverManager  # Added
# path = pathlib.Path(__file__).parent.resolve()

# --- Configuration ---
download_path = "/var/www/html/mercados/mercados-scripts"
profile_path = "/var/www/html/mercados/mercados-scripts/chrome-profile-for-script"
print(f"Using download path: {download_path}")
print(f"Using profile path: {profile_path}")
print("Configuring Chrome options...")
chrome_options = Options()

chrome_options.add_argument(f"--user-data-dir={profile_path}")
# Switches and Arguments for VPS/Headless
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_argument("--headless=new")  # Use =new for modern Chrome
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")  # Often helpful in headless
chrome_options.add_argument("--window-size=1920,1080")  # Define virtual window size

# Download Preferences
prefs = {
    "download.default_directory": download_path,
    "download.prompt_for_download": False,  # Crucial for automation
    "download.directory_upgrade": True,  # Recommended
    "safebrowsing.enabled": True,  # Corrected key name
}
chrome_options.add_experimental_option("prefs", prefs)
print("Chrome options configured.")

# --- Initialize Driver ---
driver = None  # Initialize for the finally block
try:
    print("Setting up Chrome WebDriver using webdriver-manager...")
    # Use webdriver-manager to handle driver download and path
    driver = webdriver.Chrome(
        service=Service(
            ChromeDriverManager().install()
        ),  # Automatically handles driver
        options=chrome_options,
    )
    print("WebDriver setup complete.")
    # allow downloads in headless Chrome
    # driver.execute_cdp_cmd("Page.setDownloadBehavior",
    #                    {"behavior": "allow", "downloadPath": download_path})
    # --- Automation Logic ---
    target_url = (
        "https://www.cenace.gob.mx/Paginas/SIM/Reportes/ImportacionExportacion.aspx"
    )
    print(f"Navigating to {target_url}...")
    driver.get(target_url)
    # Consider adding a wait here for a specific element that indicates page load, instead of sleep
    # e.g., WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "some_stable_element_id")))
    print("Page navigation complete. Waiting briefly...")
    time.sleep(5)  # Give initial time for elements to render after get()

    # --- Download 1 ---
    button_xpath_1 = (
        '//*[@id="ctl00_ContentPlaceHolder1_GridRadIntercambiosSIN_ctl00_ctl04_gbcCsv"]'
    )
    print(f"Waiting for first CSV button: {button_xpath_1}")
    csv_button_1 = WebDriverWait(driver, 20).until(  # Increased wait time slightly
        EC.element_to_be_clickable((By.XPATH, button_xpath_1))
    )
    print("Clicking first CSV button...")
    csv_button_1.click()
    print("Waiting for first download to start (15 seconds)...")
    # WARNING: time.sleep() is NOT a reliable way to wait for downloads.
    # The file might take longer or shorter. Increase if needed.
    time.sleep(15)

    # --- Download 2 ---
    button_xpath_2 = (
        '//*[@id="ctl00_ContentPlaceHolder1_GridRadIntercambiosBCA_ctl00_ctl04_gbcCsv"]'
    )
    print(f"Waiting for second CSV button: {button_xpath_2}")
    csv_button_2 = WebDriverWait(driver, 20).until(  # Increased wait time slightly
        EC.element_to_be_clickable((By.XPATH, button_xpath_2))
    )
    print("Clicking second CSV button...")
    csv_button_2.click()
    print("Waiting for second download to start (15 seconds)...")
    # WARNING: time.sleep() is NOT a reliable way to wait for downloads.
    time.sleep(15)

    print("Script finished initiating downloads.")
    print(f"Check for downloaded files in: {download_path}")

except Exception as e:
    print(f"ERROR during automation: {e}")
    # Optional: Screenshot on error helps debug headless issues
    try:
        if driver:
            error_screenshot_path = os.path.join(download_path, "error_screenshot.png")
            driver.save_screenshot(error_screenshot_path)
            print(f"Error screenshot saved to {error_screenshot_path}")
    except Exception as screenshot_e:
        print(f"Could not save error screenshot: {screenshot_e}")

finally:
    # --- Cleanup ---
    if driver:
        print("Closing WebDriver session...")
        driver.quit()  # Use quit() to close browser AND terminate driver process
        print("WebDriver closed.")