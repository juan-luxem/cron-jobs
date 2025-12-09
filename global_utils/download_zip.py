import os
import zipfile
from requests import Response
from global_utils.send_telegram_message import send_telegram_message
import logging

# --- Logger Setup ---
# This sets up a simple logger to print info and error messages to the console.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def download_zip(content_disposition: str, download_folder: str, response: Response):
    if "attachment" in content_disposition and ".zip" in content_disposition:
        print(content_disposition)
        download_file = os.path.join(download_folder, content_disposition)

        try:
            # Guarda el archivo ZIP descargado
            with open(download_file, "wb") as f:
                f.write(response.content)
            logging.info(f"Archivo ZIP '{content_disposition}' descargado exitosamente")

            # Extrae el contenido del ZIP
            with zipfile.ZipFile(download_file, "r") as zip_ref:
                zip_ref.extractall(download_folder)
            logging.info(f"Contenido extraído exitosamente de '{content_disposition}'")

        except (IOError, OSError) as e:
            error_msg = f"Error al guardar el archivo ZIP '{content_disposition}': {e}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            if os.path.exists(download_file):
                try:
                    os.remove(download_file)
                except OSError:
                    pass
            return
        except zipfile.BadZipFile as e:
            error_msg = f"Archivo ZIP corrupto '{content_disposition}': {e}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            if os.path.exists(download_file):
                try:
                    os.remove(download_file)
                except OSError:
                    pass
            return
        except Exception as e:
            error_msg = f"Error inesperado al procesar ZIP '{content_disposition}': {e}"
            logging.error(error_msg)
            send_telegram_message(message=error_msg)
            if os.path.exists(download_file):
                try:
                    os.remove(download_file)
                except OSError:
                    pass
            return
        finally:
            # Elimina el ZIP después de extraerlo
            if os.path.exists(download_file):
                try:
                    os.remove(download_file)
                    logging.info(
                        f"Archivo ZIP '{content_disposition}' eliminado exitosamente"
                    )
                except OSError as e:
                    logging.warning(f"No se pudo eliminar '{download_file}': {e}")
    else:
        error_msg = f"Advertencia: La respuesta no parece ser un archivo ZIP. Content-Disposition: {content_disposition}"
        logging.warning(error_msg)
        send_telegram_message(message=error_msg)
        logging.info(f"Content-Disposition: {content_disposition}")
        # Podrías querer inspeccionar los primeros bytes para confirmar que es un ZIP
        logging.info(f"Primeros bytes: {response.content[:20]}")
