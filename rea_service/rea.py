# Example in Python
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager  # Added
from selenium.webdriver.chrome.options import Options  # Better import style
from selenium.webdriver.chrome.service import Service  # Added
import time
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from .utils import send_telegram_message, send_telegram_image  # Assuming these are defined in utils.py
import logging

# --- Constants ---
MAX_RETRIES = 2 # Total attempts = MAX_RETRIES + 1
# RETRY_DELAY_SECONDS = 15 # Wait 15 seconds between retries

THRESHOLD_PERCENTAGE = 50.0

# --- WebDriver Setup (ensure chromedriver or geckodriver is in your PATH or specify its location) ---
# Example for Chrome:
chrome_options = Options()
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_argument("--headless=new")  # Use =new for modern Chrome
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")  # Often helpful in headless
# chrome_options.add_argument("--window-size=1280,720")  # Define virtual window size

def parse_percentage(percentage_str):
    """Helper function to parse 'XX.XX%' strings to float."""
    if isinstance(percentage_str, str) and percentage_str.endswith('%'):
        try:
            # Remove '$', ',', '%' and convert to float
            cleaned_str = percentage_str.replace('$', '').replace(',', '').replace('%', '').strip()
            return float(cleaned_str)
        except ValueError:
            return None # Handle cases where conversion fails
    return None

def get_reas_value():
    load_dotenv()
    bot_token = os.getenv("TELEGRAM_BOT_GAS_NOTIFIER_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    mau_credentials_password = os.getenv("MAU_CREDENTIALS_PASSWORD")
    mau_username = os.getenv("MAU_USERNAME")
    mau_password = os.getenv("MAU_PASSWORD")
    if not all([mau_credentials_password, mau_username, mau_password]):
        logging.error("Missing environment variables")
        return

    cwd = os.getcwd()
    credentials_path = os.path.join(cwd, "credenciales")
    files = os.listdir(credentials_path)

    files_dict = {file: os.path.join(credentials_path, file) for file in files}

    mau_cer = files_dict.get("mau.cer")
    if not os.path.exists(mau_cer):
        # print("Files not found in credentials path")
        logging.error("Files not found in credentials path")
        return

    mau_key = files_dict.get("Claveprivada_Mau.key")
    if not os.path.exists(mau_key):
        logging.error("Key files not found in credentials path")
        return

# --- Define Entities to Process ---
    entities = [
        {
            "name": "Luxem",
            "cer": mau_cer,
            "key": mau_key,
            "cred_pw": mau_credentials_password,
            "user": mau_username,
            "pw": mau_password,
        },
    ]

    for entity in entities:
        entity_name = entity["name"]
        result = get_rea_table(
            entity["cer"],
            entity["key"],
            entity["cred_pw"],
            entity["user"],
            entity["pw"]
        )

        if isinstance(result, list) and len(result) > 0:
            rea_mgp = result[0].get("REAvsMGP")
            if not isinstance(rea_mgp, str):
                send_telegram_message(
                    bot_token,
                    chat_id,
                    f"Error: REA value for {entity_name} is not a string."
                )
                continue
            rea_mgp = parse_percentage(rea_mgp)
            if rea_mgp is None:
                send_telegram_message(
                    bot_token,
                    chat_id,
                    f"Error: No es posible parsear el valor de la REAvsMGP para {entity_name}."
                    # f"Error: Unable to parse REA value for {entity_name}."
                )
                continue
            output_string = ""

            for item in result:
                if isinstance(item, dict):
                    for key, value in item.items():
                        # Special handling for 'CLAVE PART' to match your desired output 'PART'
                        if key == 'CLAVE PART':
                            output_string += f"PART: {value}\n"
                        else:
                            output_string += f"{key}: {value}\n"

            if rea_mgp > THRESHOLD_PERCENTAGE:
                send_telegram_message(
                    bot_token,
                    chat_id,
                    f"Peligro: Valor de REA para {entity_name} es mayor {THRESHOLD_PERCENTAGE}%: {rea_mgp}%, \n{output_string}"
                )

        if isinstance(result, list) and len(result) == 0:
            send_telegram_message(
                bot_token,
                chat_id,
                f"Error: No se encontraron datos para {entity_name}."
            )
            continue

        if isinstance(result, str):
            logging.error(f"Ha ocurrido un error {entity_name}: {result}")
            if os.path.exists(result):
                try:
                    send_telegram_image(
                        bot_token,
                        chat_id,
                        result,
                        caption=f"Error: {entity_name} - {result}"
                    )
                    os.remove(result)
                    logging.info(f"Screenshot at {result} has been removed.")
                except Exception as e:
                    logging.error(f"Failed to remove screenshot: {e}")
            else:
                logging.error(f"Screenshot at {result} does not exist.")
        
def get_rea_table(file_cer, file_key, file_credentials_password, username, password):
    driver = None
    try:
        # Initialize the WebDriver
        driver = webdriver.Chrome(
            service=Service(
                ChromeDriverManager().install()
            ),  # Automatically handles driver
            options=chrome_options,
        )
        original_window = driver.current_window_handle # Get the handle of the first tab

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

        time.sleep(6)  # Wait for the page to load
        # Print the current URL to verify the login
        current_url = driver.current_url
        logging.info(f"Current URL after login: {current_url}")
        area_menu_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "RadMenuAC"))
        )
        area_menu_element.click()
        time.sleep(2)  # Wait for the menu to load
        liq_nav_elment= WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'DropdownB9'))
        )
        liq_nav_elment.click()
        time.sleep(2)  # Wait for the menu to load

        try:
            warranty_nav_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'DropdownB190')))
        except:
            warranty_nav_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'DropdownC9')))

        warranty_nav_element.click()
        time.sleep(2)  # Wait for the menu to load
        rea_nav_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'S12')))
        rea_nav_element.click()
         # ---- HANDLE THE NEW TAB ----
        WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2)) # Wait until 2 windows are open

        all_windows = driver.window_handles
        new_window = [window for window in all_windows if window != original_window][0]
        driver.switch_to.window(new_window)
        # Now the driver is focused on the new tab
        # print url of the new tab

        time.sleep(5)  # Wait for the page to load
        current_url = driver.current_url
        logging.info(f"Current URL after switching to new tab: {current_url}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "tablaResultado")) # Wait for the table you want to scrape
        )
        logging.info("Successfully logged in!")

        # 5. Extract HTML content (table by ID)
        table_id = "tablaResultado"  # Replace with the actual ID of the table
        table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, table_id))
        )

        table_html = table.get_attribute("outerHTML")

        if table_html:
            soup = BeautifulSoup(table_html, 'html.parser')

            headers = []
            # Find the table header row
            header_row = soup.find('thead').find('tr')
            if header_row:
                for th in header_row.find_all('th'):
                    headers.append(th.get_text(strip=True)) # strip=True removes extra whitespace

            table_data = []
            # Find all data rows in the table body
            body_rows = soup.find('tbody').find_all('tr')
            for row in body_rows:
                row_dict = {}
                cells = row.find_all('td')
                for i, cell in enumerate(cells):
                    if i < len(headers): # Ensure we don't go out of bounds for headers
                        cell_text = cell.get_text(strip=True)

                        if headers[i] == "Estatus": # Special handling for 'Estatus' if needed
                            status_div = cell.find('div', class_=["semaforoVerde", "semaforoRojo", "semaforoAmarillo"]) # Add other possible classes
                            if status_div:
                                if "semaforoVerde" in status_div.get('class', []):
                                    cell_text = "Verde" # Or "OK", "Good", etc.
                                elif "semaforoRojo" in status_div.get('class', []):
                                    cell_text = "Rojo"
                                elif "semaforoAmarillo" in status_div.get('class', []):
                                    cell_text = "Amarillo"
                                # If cell_text is still empty from get_text and no specific div found, it remains empty
                            elif not cell_text: # If get_text was empty and no specific div was found
                                cell_text = "N/A" # Or some default

                        row_dict[headers[i]] = cell_text
                if row_dict: # Ensure the row_dict is not empty (e.g. if a row had no <td> for some reason)
                    table_data.append(row_dict)

            return table_data
        else:
            logging.error("Table HTML was empty, skipping parsing.")
            return []

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        screenshot_path = None
        if driver:
            # Create a screenshots directory if it doesn't exist
            screenshots_dir = "screenshots"
            os.makedirs(screenshots_dir, exist_ok=True)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            screenshot_path = os.path.join(screenshots_dir, f"error_screenshot_{timestamp}.png")
            try:
                driver.save_screenshot(screenshot_path)
                logging.info(f"Screenshot saved to {screenshot_path}")

            except Exception as se:
                logging.error(f"Failed to save screenshot: {se}")
        logging.error(f"An error occurred: {e}")
        return screenshot_path
    finally:
        time.sleep(10)  # Wait for a few seconds to see the result
        if driver:
            driver.quit()
