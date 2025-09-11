import logging
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from global_utils.get_selenium_options import get_selenium_options


def get_generacion_idr_ofertada_generic(
    market_type: str, systems: list = ["SIN", "BCS", "BCA"]
):
    """
    Generic function to download Ofertas de Venta – Recursos Interm Despachables data from CENACE for any market type.

    Args:
        market_type (str): 'MDA' or 'MTR' to determine which URL to use.
        systems (list): A list of electrical systems to process.
    """
    # --- Configuration ---
    urls = {
        "MDA": "https://www.cenace.gob.mx/Paginas/SIM/Reportes/OfertasMDA.aspx",
        "MTR": "https://www.cenace.gob.mx/Paginas/SIM/Reportes/OfertasMTR.aspx",
    }

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

        # First, select "Ofertas de Venta – Recursos Interm Despachables" from the first dropdown
        try:
            report_select_element = wait.until(
                EC.presence_of_element_located(
                    (By.ID, "ContentPlaceHolder1_ddlReporte")
                )
            )
            report_select = Select(report_select_element)
            if market_type == "MDA":
                report_select.select_by_value("369")  # Ofertas de Venta – Recursos Interm Despachables
            else:
                report_select.select_by_value(
                    "381"
                )  # Ofertas de Venta – Recursos Interm Despachables
            logging.info("Selected 'Ofertas de Venta – Recursos Interm Despachables' option")

            # Wait for postback to complete and page to load
            time.sleep(5)

        except Exception as e:
            logging.error(f"Error selecting report type: {e}")
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
                continue

    except TimeoutException:
        logging.error(
            f"❌ A page element did not load in time. Could not complete the process for {url}"
        )
    except Exception as e:
        logging.error(
            f"❌ An unexpected error occurred during the {market_type} process: {e}"
        )
    finally:
        logging.info(f"🏁 Download process for {market_type} finished.")
        driver.quit()
