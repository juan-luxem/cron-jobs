import os
from config import ENV
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from global_utils.get_selenium_options import get_selenium_options
from global_utils.send_telegram_message import send_telegram_message

# --- Logger Setup ---
# This sets up a simple logger to print info and error messages to the console.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_salidas_ocurridas():
    """
    Download Salidas Adelanto data from CENACE.
    """
    url = "https://memsim.cenace.gob.mx/Produccion/Participantes/LOGIN/"

    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)
    credentials_path = os.path.join(cwd, "credenciales")
    files = os.listdir(credentials_path)

    options = get_selenium_options(headless=False, download_folder=download_folder)

    logging.info(f"Files will be saved to: {download_folder}")

    # --- Selenium WebDriver Options ---
    driver = webdriver.Chrome(options=options)
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID
    mau_credentials_password = ENV.MAU_CREDENTIALS_PASSWORD.get_secret_value()
    mau_username = ENV.MAU_USERNAME
    mau_password = ENV.MAU_PASSWORD.get_secret_value()

    files_dict = {file: os.path.join(credentials_path, file) for file in files}

    mau_cer = files_dict.get("mau.cer")
    if not os.path.exists(str(mau_cer)):
        # print("Files not found in credentials path")
        logging.error("Files not found in credentials path")
        return

    mau_key = files_dict.get("Claveprivada_Mau.key")
    if not os.path.exists(str(mau_key)):
        logging.error("Key files not found in credentials path")
        return

    try:
        driver.get(url)

        e_key_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.ID, "uploadCer")
            )  # Wait for the body to load
        )
        cert_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.ID, "uploadKey")
            )  # Or By.NAME, By.XPATH, etc.
        )
        password_key_input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.ID, "txtPrivateKey")
            )  # Or By.NAME, By.XPATH, etc.
        )

        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "btnEnviar"))
        )

        e_key_input_element.send_keys(str(mau_cer))
        cert_input_element.send_keys(str(mau_key))
        password_key_input_element.send_keys(mau_credentials_password)

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

        username_input_element.send_keys(mau_username)
        password_input_element.send_keys(mau_password)

        send_button.click()

        time.sleep(6)  # Wait for the page to load
        # Print the current URL to verify the login
        current_url = driver.current_url
        logging.info(f"Current URL after login: {current_url}")
        area_menu_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "RadMenuAS"))
        )
        area_menu_element.click()
        time.sleep(2)  # Wait for the menu to load
        op_nav_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "DropdownB1300"))
        )
        op_nav_element.click()
        time.sleep(2)  # Wait for the menu to load
        sal_nav_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "DropdownB18000"))
        )
        sal_nav_element.click()
        time.sleep(2)  # Wait for the menu to load
        sal_adelanto_nav_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "S22"))
        )
        sal_adelanto_nav_element.click()
        time.sleep(6)  # Wait for the menu to load
        iframe_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "areaTrabajoModelosUtilizadosMem"))
        )
        driver.switch_to.frame(iframe_element)
        time.sleep(4)  # Wait for the menu to load

        csv_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.ID, "RadGridPublicacion_ctl00_ctl04_gbccolumn")
            )
        )
        csv_button.click()

        logging.info(f"Clicked CSV download button for salidas adelanto")

        time.sleep(4)

    except TimeoutException as e:
        logging.error(
            f"❌ A page element did not load in time. Could not complete the process for {url}"
        )
        send_telegram_message(
            bot_token, chat_id, f"Timeout en get_salidas_ocurridas: {e}"
        )
    except Exception as e:
        logging.error(
            f"❌ An unexpected error occurred during the {mau_password} process: {e}"
        )
        send_telegram_message(
            bot_token, chat_id, f"Error inesperado en get_salidas_ocurridas: {e}"
        )
    finally:
        logging.info(f"🏁 Download process for {mau_key} finished.")
        if driver:
            driver.quit()
