import logging
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from global_utils.get_selenium_options import get_selenium_options
from global_utils.send_telegram_message import send_telegram_message
from config import ENV


def get_generacion_ndso_ofertada_generic(
    market_type: str, systems: list = ["SIN", "BCS", "BCA"]
):
    """
    Generic function to download Ofertas de Venta – No Despachable data from CENACE for any market type.

    Args:
        market_type (str): 'MDA' or 'MTR' to determine which URL to use.
        systems (list): A list of electrical systems to process.
    """
    # --- Configuration ---
    urls = {
        "MDA": "https://www.cenace.gob.mx/Paginas/SIM/Reportes/OfertasMDA.aspx",
        "MTR": "https://www.cenace.gob.mx/Paginas/SIM/Reportes/OfertasMTR.aspx",
    }
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID

    if market_type not in urls:
        logging.error(f"Invalid market type: {market_type}. Use 'MDA' or 'MTR'.")
        return

    url = urls[market_type]

    # --- Setup Download Folder ---
    cwd = os.getcwd()
    download_folder = os.path.join(cwd, "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    options = get_selenium_options(headless=True, download_folder=download_folder)

    logging.info(f"Files will be saved to: {download_folder}")

    # --- Selenium WebDriver Options ---
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(url)

        # First, select "Ofertas de Venta – No Despachable" from the first dropdown
        try:
            report_select_element = wait.until(
                EC.presence_of_element_located(
                    (By.ID, "ContentPlaceHolder1_ddlReporte")
                )
            )
            report_select = Select(report_select_element)
            if market_type == "MDA":
                report_select.select_by_value("370")  # Ofertas No Despachables MDA
            else:
                report_select.select_by_value(
                    "380"
                )  # Ofertas de Venta – No Despachable
            logging.info("Selected 'Ofertas de Venta – No Despachable' option")

            # Wait for postback to complete and page to load
            time.sleep(5)

        except Exception as e:
            logging.error(f"Error selecting report type: {e}")
            send_telegram_message(
                bot_token,
                chat_id,
                f"Error en get_generacion_ndso_ofertada_generic ({market_type}): {e}",
            )
            return None

        # --- Iterate Through Each System ---
        for _, system in enumerate(systems):
            try:
                # Find and select the system in the second dropdown
                system_select_element = wait.until(
                    EC.presence_of_element_located(
                        (By.ID, "ContentPlaceHolder1_ddlSistema")
                    )
                )
                # ContentPlaceHolder1_ddlSistema
                system_select = Select(system_select_element)
                system_select.select_by_value(system)
                logging.info(f"Selected {system} option")

                # Wait for postback to complete
                time.sleep(3)

                # Click on the CSV download button
                csv_button = wait.until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//input[@src='../imagenes/csv.svg']")
                    )
                )
                time.sleep(2)

                csv_button.click()
                logging.info(f"Clicked CSV download button for {system}")

                # Wait for download to complete
                time.sleep(4)

            except Exception as e:
                logging.error(f"Error processing system {system}: {e}")
                send_telegram_message(
                    bot_token,
                    chat_id,
                    f"Error en get_generacion_ndso_ofertada_generic ({market_type}) procesando {system}: {e}",
                )
                continue

    except TimeoutException as e:
        logging.error(
            f"❌ A page element did not load in time. Could not complete the process for {url}"
        )
        send_telegram_message(
            bot_token,
            chat_id,
            f"Timeout en get_generacion_ndso_ofertada_generic ({market_type}): {e}",
        )
    except Exception as e:
        logging.error(
            f"❌ An unexpected error occurred during the {market_type} process: {e}"
        )
        send_telegram_message(
            bot_token,
            chat_id,
            f"Error inesperado en get_generacion_ndso_ofertada_generic ({market_type}): {e}",
        )
    finally:
        logging.info(f"🏁 Download process for {market_type} finished.")
        if os.path.exists(download_folder):
            for file in os.listdir(download_folder):
                file_path = os.path.join(download_folder, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    logging.info(f"Removed file: {file_path}")
        if driver:
            driver.quit()
