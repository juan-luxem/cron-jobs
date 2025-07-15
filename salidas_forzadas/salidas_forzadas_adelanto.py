from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager  # Added
from selenium.webdriver.chrome.service import Service  # Added
import os
from dotenv import load_dotenv
from utils import get_csv_file  # Assuming this is defined in utils.py
import logging

def get_salidas_forzadas_adelanto():
    """
    Fetches the forced exits that have occurred.
    """
    try:
        # Load environment variables
        load_dotenv()
        mau_credentials_password = os.getenv("MAU_CREDENTIALS_PASSWORD")
        mau_username = os.getenv("MAU_USERNAME")
        mau_password = os.getenv("MAU_PASSWORD")

        URL = "https://memsim.cenace.gob.mx/Produccion/Participantes/LOGIN/"

        cwd = os.getcwd()
        download_folder = os.path.join(cwd, "download_folder")
        os.makedirs(download_folder, exist_ok=True)

        credentials_path = os.path.join(cwd, "credenciales")
        files = os.listdir(credentials_path)

        if not os.path.exists(download_folder) or not os.path.exists(credentials_path):
            logging.error("Files not found in download path or credentials path")
            return

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
        entities = {
            "name": "Luxem",
            "cer": mau_cer,
            "key": mau_key,
            "cred_pw": mau_credentials_password,
            "user": mau_username,
            "pw": mau_password,
        }
        # --- Get CSV File ---
        get_csv_file(
            file_cer=entities["cer"],
            file_key=entities["key"],
            file_credentials_password=entities["cred_pw"],
            username=entities["user"],
            password=entities["pw"],
            download_folder=download_folder,
            salidas="ctl07"
        )

    except Exception as e:
        logging.error(f"Error loading environment variables or setting up paths: {e}")
        return


get_salidas_forzadas_adelanto()