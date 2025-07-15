import time
import os  # Added to potentially check/create directory
import pathlib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Better import style
from selenium.webdriver.chrome.service import Service  # Added
from webdriver_manager.chrome import ChromeDriverManager  # Added

path = pathlib.Path(__file__).parent.resolve()

# --- Configuration ---
# IMPORTANT: Make sure this path exists on your VPS and the 'gomen' user has write permissions.
# If it doesn't exist, you might need to create it first using 'mkdir -p /path/on/vps' in your terminal.
download_path = str(path)  # Adjust this path as needed
# download_path = 'C:\\Users\\becario.desarrollo\\Documents\\luxem\\mercados-scripts\\impor_exp\\downloads'
print(f"Using download path: {download_path}")

# Optional: Check if directory exists and try to create it if not
if not os.path.isdir(download_path):
   print(f"Directory {download_path} does not exist. Attempting to create.")
   try:
      os.makedirs(download_path, exist_ok=True)
      print("Directory created successfully.")
   except OSError as e:
      print(f"ERROR: Could not create directory {download_path}: {e}")
      print("Please ensure the directory exists and is writable.")
      # Exit or use a fallback path like /tmp if creation fails
      exit()


# --- Chrome Options ---
print("Configuring Chrome options...")
chrome_options = Options()
# Switches and Arguments for VPS/Headless
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
#chrome_options.add_argument("--headless=new")  # Use =new for modern Chrome
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")  # Often helpful in headless
chrome_options.add_argument("--window-size=1920,1080")  # Define virtual window size

# Download Preferences
prefs = {
    "download.default_directory": download_path,
    "download.prompt_for_download": False,  # Crucial for automation
    "download.directory_upgrade": True,  # Recommended
    "safeBrowse.enabled": True,  # Or False if it interferes
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
