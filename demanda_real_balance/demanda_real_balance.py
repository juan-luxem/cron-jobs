import os
import logging
# import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager  # Added
from selenium.webdriver.chrome.service import Service  # Added
import time
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys
from .extract_data_from_file import extract_data_from_file
import requests
from config import ENV
from global_utils.get_selenium_options import get_selenium_options

def get_csv_file_id_from_last_release_date_row(driver, base_date_str, day_to_subtract, release_date) -> str | None:
    """
    Sets the date in the date picker, waits for the table to load,
    and returns the ID of the CSV download input from the last row of the table.
    """
    try:
        # Re-locate the date input element each time
        date_input_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_RadDatePickerVisualizarPorBalance_dateInput"))
        )

        # Calculate the target date
        base_date_dt = datetime.strptime(base_date_str, '%d/%m/%Y')
        target_date_dt = base_date_dt - timedelta(days=day_to_subtract)
        target_date_str_to_send = target_date_dt.strftime("%d/%m/%Y")
        
        logging.info(f"Setting date to: {target_date_str_to_send}")

        # Clear the field and send new date
        date_input_element.send_keys(Keys.CONTROL, 'a')
        date_input_element.send_keys(Keys.DELETE)
        date_input_element.send_keys(target_date_str_to_send)
        date_input_element.send_keys(Keys.ENTER) # Submit the date

        time.sleep(3)  # Wait for the table to load

        xpath = (
            f"//table[@id='ctl00_ContentPlaceHolder1_GridRadPorBalance_ctl00']/tbody/tr"
            f"[td[@class='Letras' and contains(text(), '{release_date}')]]"
            f"[last()]/td//input[@type='image' and contains(@src, 'imgCsv.png')]"
        )

        csv_input = driver.find_element(By.XPATH, xpath)
        logging.info(f"Found CSV input ID in last row: {csv_input.get_attribute('id')} for date {target_date_str_to_send}")
        return csv_input.get_attribute('id')
    except Exception as e:
        logging.error(f"Error in get_csv_file_id_from_last_row for date {target_date_str_to_send if 'target_date_str_to_send' in locals() else 'unknown'}: {e}")
        return None



def get_demanda_real_balance():
    API_URL = ENV.API_URL

    url = "https://www.cenace.gob.mx/Paginas/SIM/Reportes/EstimacionDemandaReal.aspx"
    days_to_download = [0, 42, 98, 203]

    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    if not os.path.exists(download_folder):
        logging.error("Files not found in download path")
        return

    chrome_options = get_selenium_options(headless=False, download_folder=download_folder)
    driver = None

    try:
        # Initialize the Chrome driver
        # service = Service(ChromeDriverManager().install())  # Use ChromeDriverManager to install the driver
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)

        input_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_RadDatePickerVisualizarPorBalance_dateInput"))
        )
        
        date = datetime.now().strftime("%d/%m/%Y")  
        input_value = input_element.get_attribute("value")
        for day in days_to_download:
            print(f"Downloading file for {day} days ago")
            id = get_csv_file_id_from_last_release_date_row(driver=driver, base_date_str=input_value, day_to_subtract=day, release_date=date)
            if id is None:
                logging.error(f"Failed to get CSV file ID for {day} days ago")
                continue
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, id))
            ).click()
            time.sleep(3)  # Wait for the page to load
        
        data_to_send = extract_data_from_file(download_folder)
        # # create a json file to insert and debugging purposes and create file if not exists
        # if not os.path.exists('data_to_send.json'):
        #     with open('data_to_send.json', 'w') as f:
        #         json.dump(data_to_send, f, indent=4)


        if data_to_send is None:
            logging.error("No data extracted from the file.")
            return

        response = requests.post(
            f"{API_URL}api/v1/demanda-real-balance",
            json=data_to_send,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200 and response.status_code != 201:
            logging.error(f"Failed to upload data: {response.status_code} - {response.text}")
            return

        logging.info(f"Data uploaded successfully: {response.json()}")

        logging.info(f"Data extracted: {data_to_send}")
        time.sleep(3)  # Wait for the download to start

    except Exception as e:
        logging.error(f"Error while downloading file: {e}")
        return
    finally:
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
            driver.quit()