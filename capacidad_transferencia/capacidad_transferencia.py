import os
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager  # Added
from selenium.webdriver.chrome.options import Options  # Better import style
from selenium.webdriver.chrome.service import Service  # Added
import time
from dotenv import load_dotenv
from capacidad_transferencia.get_capacidad_transferencia_data import get_capacidad_transferencia_data


def get_capacidad_transferencia():
    load_dotenv()
    url = "https://www.cenace.gob.mx/Paginas/SIM/Reportes/CapacidadTransferMDA.aspx"

    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    if not os.path.exists(download_folder):
        logging.error("Files not found in credentials path")
        return

    chrome_options = Options()
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_argument("--headless=new")  # Use =new for modern Chrome
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")  # Often helpful in headless
    # chrome_options.add_argument("--window-size=1280,720")  # Define virtual window size
    # Download folder
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = None
    try:
        # Initialize the Chrome driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get(url)  # Replace with the actual URL

        # Wait for the element to be present
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_gvReportes_imgBtnCSV_0"))  # Replace with actual ID
        ).click()
        logging.info("CSV file downloaded successfully.")

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_ddlSistema"))
        ).click()
        
        time.sleep(2)  # Wait for the dropdown to be populated
        try:

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_ddlSistema"))
            ).click()

            # Select the BCA option from the dropdown
            bca_option = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//option[@value='BCA']"))
            )
            bca_option.click()
            # WebDriverWait(driver, 10).until(
            #     EC.presence_of_element_located((By., "//option[@value='BCA']"))
            # ).click()

            time.sleep(5)  # Wait for the page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_gvReportes_imgBtnCSV_0"))
            ).click()
            time.sleep(5)  # Wait for the download to complete
            get_capacidad_transferencia_data(download_folder)
            
        except Exception as e:
            logging.error(f"Error clicking BCA option: {e}")
            return

        logging.info("CSV files downloaded successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        time.sleep(10)  # Wait for the download to complete
        # Check if the download folder is empty, remove files if it not
        if os.path.exists(download_folder):
            files = os.listdir(download_folder)
            if len(files) > 0:
                for file in files:
                    file_path = os.path.join(download_folder, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        logging.info(f"Removed file: {file_path}")
            else:
                logging.info("No files to remove.")
        
        if driver:
            # Wait for the download to complete
            driver.quit()
            logging.info("Driver closed successfully.")