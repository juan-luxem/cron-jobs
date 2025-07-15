from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager  # Added
from selenium.webdriver.chrome.service import Service  # Added
import logging
import time

def get_selenium_options(headless: bool, download_folder: str) -> Options:
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

def get_csv_file(file_cer, file_key, file_credentials_password, username, password, download_folder, salidas):
    driver = None
    # Set up Chrome options
    chrome_options = get_selenium_options(headless=False, download_folder=download_folder)
    try:
        # Initialize the WebDriver
        driver = webdriver.Chrome(
            service=Service(
                ChromeDriverManager().install()
            ),  # Automatically handles driver
            options=chrome_options,
        )

        # 1. Navigate to the login page
        login_url = "https://memsim.cenace.gob.mx/Produccion/Participantes/LOGIN/" # Replace with the actual URL

        driver.get(login_url)
        e_key_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "uploadCer"))  # Wait for the body to load
        ) 
        cert_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "uploadKey")) # Or By.NAME, By.XPATH, etc.
        )
        password_key_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "txtPrivateKey")) # Or By.NAME, By.XPATH, etc.
        )

        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "btnEnviar"))
        )

        e_key_input_element.send_keys(file_cer)
        cert_input_element.send_keys(file_key)
        password_key_input_element.send_keys(file_credentials_password)

        login_button.click()
        # 2. Wait for the login to complete and the next page to load
        # This might involve waiting for a specific element on the next page or a URL change
        username_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "txtUsuario"))
        )
        password_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "txtPassword"))
        )

        send_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "Button1"))
        )

        username_input_element.send_keys(username)
        password_input_element.send_keys(password)

        send_button.click()

        time.sleep(2)  # Wait for the page to load
        # Print the current URL to verify the login
        area_menu_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "RadMenuAS"))
        )
        area_menu_element.click()
        time.sleep(2)  # Wait for the menu to load
        sen_nav_element= WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'DropdownB1300'))
        )
        sen_nav_element.click()
        time.sleep(2)  # Wait for the menu to load

        eventos_sen_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'A146'))
        )
        eventos_sen_element.click()
        time.sleep(2)  # Wait for the menu to load
        salidas_ocurridas_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, salidas))
        )
        salidas_ocurridas_element.click()
        iframe = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'areaTrabajoModelosUtilizadosMem'))
        )
        driver.switch_to.frame(iframe)
        csv_input_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 'RadGridPublicacion_ctl00_ctl04_gbccolumn'))
        )
        print(f"CSV input element found: {csv_input_element}")
        csv_input_element.click()
        time.sleep(2)  # Wait for the download to start

    except Exception as e:
        logging.error(f"Error during Selenium operations: {e}")
        return None

    finally:
        if driver:
            driver.quit()
