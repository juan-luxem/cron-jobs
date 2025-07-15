import requests
import os
from bs4 import BeautifulSoup, Tag
import logging
import glob
import pandas as pd
import json
from datetime import datetime

def get_sistema_from_filename(filename):
    if "BCS" in filename:
        return "BCS"
    elif "BCA" in filename:
        return "BCA"
    elif "SIN" in filename:
        return "SIN"
    else:
        return None

def send_telegram_message(bot_token, chat_id, message):
    # Send a message to a Telegram chat.
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        logging.info("Telegram message sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Faild to send Telegram message: {e}")


def delete_csv_files(directory):
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    for file in csv_files:
        os.remove(file)
        logging.info(f"Deleted: {file}")


def send_dataframe_to_api(df_to_send, api_url, target_source):
    """
    Transforms a DataFrame and sends its data as JSON to the specified API endpoint.

    Args:
        df_to_send (pd.DataFrame): The DataFrame containing the data.
                                   Expected columns: Hora (int), Clave (str), PML (float),
                                   Energia (float), Perdidas (float), Congestion (float),
                                   Sistema (str), Fecha (datetime or parseable string).
        api_url (str): The base URL of the Flask API (e.g., "API_URL/api/v1/mercado/mda_mtr/<string:data_type>").
        target_source (str): The target source key for the API URL path
                             (e.g., "pnd_mda", "pml_mda").

    Returns:
        bool: True if the API call resulted in a 2xx status code, False otherwise.
    """
    if not isinstance(df_to_send, pd.DataFrame):
        logging.error("Invalid input: df_to_send must be a Pandas DataFrame.")
        return False

    if df_to_send.empty:
        logging.warning("DataFrame to send is empty. Skipping API call.")
        # Depending on your workflow, an empty DataFrame might be considered a success
        # or failure. Returning False here assumes it's not the intended success state.
        return False

    logging.info(f"Preparing data for API endpoint '{target_source}'...")

    # --- 1. Create a copy to avoid modifying the original DataFrame ---
    df_api = df_to_send.copy()

    # --- 2. Verify required columns exist ---
    required_cols = [
        "Sistema",
        "Fecha",
        "Hora",
        "Clave",
        "PML",
        "Energia",
        "Congestion",
        "Perdidas",
    ]
    missing_cols = [col for col in required_cols if col not in df_api.columns]
    if missing_cols:
        logging.error(
            f"DataFrame is missing required columns for the API: {missing_cols}"
        )
        return False

    # --- 3. Transform 'Fecha' column ---
    # Convert to datetime objects first (handles various string formats), then format to 'YYYY-MM-DD' string.
    try:
        # pd.to_datetime is robust in parsing various date formats
        df_api["Fecha"] = pd.to_datetime(df_api["Fecha"]).dt.strftime("%Y-%m-%d")
        logging.debug("'Fecha' column formatted to YYYY-MM-DD.")
    except Exception as e:
        logging.error(
            f"Failed to parse or format the 'Fecha' column. Ensure it contains valid dates. Error: {e}"
        )
        return False

    # --- 4. Transform 'Hora' column ---
    try:
        # Ensure 'Hora' is numeric, coerce errors, fill NaNs (e.g., with 0), convert to int
        df_api["Hora"] = (
            pd.to_numeric(df_api["Hora"], errors="coerce").fillna(0).astype(int)  # type: ignore
        )

        # INFO: uncomment this if you want to handle the 24-hour case
        # # Map the integer value 24 to 0
        # # Hours 1-23 will remain unchanged.
        # # Any values that were originally non-numeric/NaN (and became 0 above) will also remain 0.
        # df_api["Hora"] = df_api["Hora"].replace(24, 0)
        #
        # logging.debug(
        #     "'Hora' column processed as integer (with hour 24 mapped to 0)."  # Updated log message
        # )
    except Exception as e:
        logging.error(
            f"Failed to process the 'Hora' column. Ensure it contains numeric hour values (1-24). Error: {e}"
        )
        return False

    # --- 5. Select only the columns required by the API ---
    # Ensures no extra columns are sent and sets a specific order (though order doesn't matter in JSON objects)
    try:
        df_api = df_api[required_cols]
    except KeyError:
        # This check should be redundant due to the check in step 2, but added for extra safety
        logging.error(
            "One of the required columns was lost during processing. This should not happen."
        )
        return False

    # --- 6. Convert DataFrame to JSON list of dictionaries ---
    # This is the format the Flask endpoint expects (request.get_json())
    try:
        payload = df_api.to_dict("records")  # type: ignore
        logging.info(
            f"Successfully converted DataFrame to JSON payload ({len(payload)} records)."
        )
        # Optional: print first record to verify format
        # if payload:
        #    logging.debug(f"Sample payload record: {json.dumps(payload[0], indent=2)}")
    except Exception as e:
        logging.error(f"Failed to convert DataFrame to dictionary list: {e}")
        return False

    # --- 7. Construct the full API URL ---
    if not api_url.endswith("/"):
        api_url += "/"
    # Ensure target_source doesn't start with /
    target_source_cleaned = target_source.lstrip("/")

    full_api_url = f"{api_url}api/v1/mda_mtr/{target_source_cleaned}"
    logging.info(f"Target API URL: {full_api_url}")

    # --- 8. Make the POST request ---
    api_success = False
    try:
        logging.info(f"Sending {len(payload)} records to API...")
        response = requests.post(
            full_api_url,
            json=payload,  # requests handles JSON serialization and headers
            timeout=180,  # Set a timeout (in seconds) for the request
        )

        # Check if the request was successful (status code 2xx)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

        logging.info(f"API call successful! Status Code: {response.status_code}")
        try:
            # Log the response from the API (if JSON)
            api_result = response.json()
            logging.info("API Response:")
            logging.info(json.dumps(api_result, indent=2))
        except json.JSONDecodeError:
            logging.info("API Response (non-JSON):")
            logging.info(response.text)

        api_success = True  # Mark as success

    except requests.exceptions.ConnectionError as e:
        logging.error(
            f"API Connection Error: Could not connect to {api_url}. Is the server running? Details: {e}"
        )
    except requests.exceptions.Timeout:
        logging.error(f"API Error: The request to {full_api_url} timed out.")
    except requests.exceptions.HTTPError as e:
        # Error raised by response.raise_for_status() for 4xx/5xx responses
        logging.error(f"API HTTP Error: Status Code {e.response.status_code}")
        logging.error(f"Reason: {e.response.reason}")
        logging.error(
            f"Response Body: {e.response.text}"
        )  # Show error details from API
    except requests.exceptions.RequestException as e:
        # Catch other potential request errors (e.g., URL issues)
        logging.error(
            f"API Request Error: An error occurred during the request. Details: {e}"
        )
    except Exception as e:
        # Catch any other unexpected errors (e.g., during payload creation if missed earlier)
        logging.error(f"An unexpected error occurred: {e}")

    return api_success


def are_files_different(file1, file2):
    """Compara el contenido de dos archivos y devuelve True si son diferentes."""
    with open(file1, "r") as f1, open(file2, "r") as f2:
        return f1.read() != f2.read()


# Extract dates from the files
def extract_date(file_path):
    with open(file_path, "r") as file:
        for line in file:
            if "Fecha:" in line:  # Check if "Fecha:" is in the line
                date = (
                    line.split("Fecha:")[1].strip().strip('"')
                )  # Remove quotes if present
                return date
    return None


def find_hour_row(file_path):
    with open(file_path, "r") as file:
        for i, line in enumerate(file):
            if "Hora" in line:
                return i
    return -1  # Return -1 if "Hora" is not found


def preprocess_csv(file_path, system_name):
    row_index = find_hour_row(file_path)
    skiprows = row_index if row_index != -1 else 7
    df = pd.read_csv(file_path, delimiter=",", skiprows=skiprows)
    df["Sistema"] = system_name
    df.columns = [col.replace("($/MWh)", "").strip() for col in df.columns]
    return df


def check_date_exists(
    api_url: str, data_type: str, check_date_str: str
) -> bool | None:
    """
    Calls the API endpoint to check if data exists for a given date.

    Args:
        api_url: The base URL of the Flask API.
        data_type: The data type identifier (e.g., 'pml_mda').
        check_date_str: The date string in 'YYYY-MM-DD' format.

    Returns:
        True if data exists, False if it doesn't, None if the check failed.
    """
    if not api_url.endswith("/"):
        api_url += "/"
    check_url = (
        f"{api_url}api/v1/mda_mtr/{data_type.lower()}/{check_date_str}"
    )
    logging.info(f"Checking API for existing data: {check_url}")

    try:
        response = requests.get(check_url, timeout=30)  # Add a timeout
        logging.info(f"Response: {response}")
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        result = response.json()
        if "exists" in result and isinstance(result["exists"], bool):
            logging.info(
                f"API check result for {check_date_str}: {'Exists' if result['exists'] else 'Does not exist'}"
            )
            return result["exists"]
        else:
            logging.error(f"API check returned unexpected JSON format: {result}")
            return None  # Indicate check failure

    except requests.exceptions.Timeout:
        logging.error(f"API Check Error: The request to {check_url} timed out.")
        return None
    except requests.exceptions.ConnectionError as e:
        logging.error(f"API Check Connection Error: Could not connect. Details: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"API Check HTTP Error: Status Code {e.response.status_code}")
        logging.error(f"Reason: {e.response.reason}")
        logging.error(f"Response Body: {e.response.text}")
        return (
            None  # Indicate check failure (e.g., 404 if URL wrong, 500 if server error)
        )
    except requests.exceptions.RequestException as e:
        logging.error(f"API Check Request Error: {e}")
        return None
    except json.JSONDecodeError:
        logging.error(
            f"API Check Error: Failed to decode JSON response from {check_url}"
        )
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during API check: {e}")
        return None

def parse_spanish_date(date_str):
    """
    Parses a date string in the format 'd/m/y' with Spanish month abbreviations
    and returns a datetime object and its ISO 8601 string representation.

    Args:
        date_str (str): The date string to parse (e.g., '15/abr/2025').

    Returns:
        tuple: A tuple containing the datetime object and the ISO 8601 string (YYYY-MM-DD).
               Returns (None, None) if parsing fails.

    Raises:
        ValueError: If the date string is invalid or cannot be parsed.
    """
    spanish_months_map_lc = {
        "ene": 1,
        "feb": 2,
        "mar": 3,
        "abr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dic": 12,
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12,
    }

    try:
        if not date_str:
            raise ValueError("Input date string is empty or None")

        parts = date_str.split("/")
        print(f"Parts after split: {parts}")  # Debugging line
        if len(parts) != 3:
            raise ValueError(f"Date string '{date_str}' not in expected d/m/y format")

        day_str, month_abbr, year_str = parts

        # Normalize the abbreviation: lowercase and remove any trailing period
        month_abbr_clean = month_abbr.lower().rstrip(".")
        if month_abbr_clean not in spanish_months_map_lc:
            raise ValueError(f"Unrecognized Spanish month abbreviation: '{month_abbr}'")

        # Get the month number from the map
        month_num = spanish_months_map_lc[month_abbr_clean]

        # Convert parts to integers
        day_int = int(day_str)
        year_int = int(year_str)

        # Construct the datetime object
        dt = datetime(year_int, month_num, day_int)
        target_date_str_iso = dt.strftime("%Y-%m-%d")  # Format as YYYY-MM-DD

        return dt, target_date_str_iso

    except ValueError as ve:
        logging.error(f"Data validation or parsing error for date '{date_str}': {ve}")
        return None, None

    except Exception as e:
        logging.error(f"Unexpected error processing date '{date_str}': {type(e).__name__} - {e}")
        logging.exception("Full traceback:")
        return None, None

def extract_field_value(soup: BeautifulSoup, field_name: str, html_element: str) -> str:
    """
    Extrae el valor de un campo específico dentro de un formulario HTML.

    Parámetros:
    - soup: Objeto BeautifulSoup con la página analizada.
    - field_name: Nombre del campo HTML del que se extraerá el valor.
    - html_element: Tipo de etiqueta HTML donde se encuentra el campo.

    Retorna:
    - Valor del campo codificado en URL para su uso en una solicitud POST.
    """
    element_tag = soup.find(html_element, {"name": field_name})
    element = element_tag.get("value", None) if isinstance(element_tag, Tag) else None
    return str(element)
