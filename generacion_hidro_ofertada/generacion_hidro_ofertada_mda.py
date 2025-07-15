import time
import os
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import logging
from extract_data_from_csv import process_all_csv_files, send_data_to_endpoint

def obtener_generacion_hidro_ofertada_mda():
    """
    Fetches the requirements for generacion_hidro_ofertada_mda.
    """
    try:
        # Load environment variables
        load_dotenv()
        API_URL = os.getenv("API_URL")
        if not API_URL:
            logging.error("API_URL not found in environment variables.")
            return None

        URL = "https://www.cenace.gob.mx/Paginas/SIM/Reportes/OfertasMDA.aspx"

        cwd = os.getcwd()
        download_folder = os.path.join(cwd, "download_folder")
        os.makedirs(download_folder, exist_ok=True)

        # Configure Chrome options for downloading
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        # Initialize WebDriver
        driver = webdriver.Chrome(options=chrome_options)
        wait = WebDriverWait(driver, 15)
        
        try:
            # Navigate to the URL
            driver.get(URL)
            logging.info("Navigated to the URL")
            
            # Wait for page to load
            time.sleep(3)
            
            # First, select "Ofertas Hidro MDA" from the first dropdown
            try:
                report_select_element = wait.until(EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_ddlReporte")))
                report_select = Select(report_select_element)
                report_select.select_by_value("367,368")  # Ofertas Hidro MDA
                logging.info("Selected 'Ofertas Hidro MDA' option")
                
                # Wait for postback to complete and page to load
                time.sleep(5)
                
            except Exception as e:
                logging.error(f"Error selecting report type: {e}")
                return None
            
            # For Ofertas Hidro MDA, only SIN system is available
            logging.info("Processing SIN system for Ofertas Hidro MDA")
            
            try:
                # Click on the CSV download button (no need to select system as only SIN is available)
                csv_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@src='../imagenes/csv.svg']")))
                csv_button.click()
                logging.info("Clicked CSV download button for SIN system")
                
                # Wait for download to complete
                time.sleep(4)
                
            except Exception as e:
                logging.error(f"Error downloading CSV for SIN system: {e}")
                return None
            
            logging.info("All CSV downloads completed successfully")

            extracted_data = process_all_csv_files(download_folder)
            
            if not extracted_data or len(extracted_data) == 0:
                logging.error("No data was extracted from CSV files")
                return
            
            # Validate data structure for Ofertas Hidro MDA
            required_fields = ['DiaOperacion', 'Sistema', 'Codigo', 'CostoOportunidad_MWh', 'GrupoUnidadesEmbalse']

            
            valid_records = []
            for record in extracted_data:
                if all(field in record for field in required_fields):
                    valid_records.append(record)
                else:
                    logging.warning(f"Invalid record found: {record}")
            
            logging.info(f"Valid records after validation: {len(valid_records)}")
            logging.info(f"Valid records after validation: {len(valid_records)}")
            
            # TODO: Replace with your actual endpoint URL
            endpoint_url = f"{API_URL}/api/v1/generacion_hidro_ofertada?market=mda"

            # Send data to endpoint
            success = send_data_to_endpoint(valid_records, endpoint_url)
            
            if success:
                logging.info("Data processing and sending completed successfully")
            else:
                logging.error("Failed to send data to endpoint")
                
            # import json
            # from datetime import datetime
            # backup_file = os.path.join(cwd, f"extracted_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            # with open(backup_file, 'w', encoding='utf-8') as f:
            #     json.dump(valid_records, f, indent=2, ensure_ascii=False)
            # logging.info(f"Data backed up to: {backup_file}")

        finally:
            # Close the browser
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
                logging.info("Browser closed")

    except Exception as e:
        logging.error(f"Error in selenium automation: {e}")
        if 'driver' in locals():
            driver.quit()
        return


if __name__ == "__main__":
    """
    Main entry point for the script.
    """
    obtener_generacion_hidro_ofertada_mda()
