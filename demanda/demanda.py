import logging
from typing import TypedDict, Optional
from datetime import datetime
import requests
import json
from dotenv import load_dotenv
import os

SIN = [
    "Central",
    "Noreste",
    "Noroeste",
    "Norte",
    "Occidental",
    "Oriental",
    "Peninsular",
]
BCS = ["Baja California Sur"]
BCA = ["Baja California"]

# Diccionario de números de gerencia
MAPEO_GERENCIAS = {
    1: "Baja California",
    2: "Baja California Sur",
    3: "Central",
    4: "Noreste",
    5: "Noroeste",
    6: "Norte",
    7: "Occidental",
    8: "Oriental",
    9: "Peninsular",
}

URL = "https://www.cenace.gob.mx/GraficaDemanda.aspx/obtieneValoresTotal"

CODE_STATUS = [200, 201]


# Establece el tipo de datos para la gerencia
class Gerencia(TypedDict):
    hora: str
    valorDemanda: str
    valorGeneracion: str
    valorEnlace: Optional[None]
    valorPronostico: str


# Establece el tipo de datos para la respuesta de la API
class DataSchema(TypedDict):
    FechaOperacion: str
    HoraOperacion: int
    Demanda: int
    Generacion: int
    Enlace: Optional[None]
    Pronostico: int
    Gerencia: str
    Sistema: str
    FechaCreacion: str
    FechaModificacion: str

    # Pronostico = a, Demanda = b


def send_telegram_message(bot_token, chat_id, message):
    # Send a message to a Telegram chat.
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        logging.info("Telegram message sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Faild to send Telegram message: {e}")


def diferencia_en_porcentaje(pronostico: int, demanda: int) -> tuple[float, str]:
    # Added check for pronostico == 0 to avoid ZeroDivisionError
    if pronostico == 0:
        logging.warning("Pronostico is zero, cannot calculate percentage difference.")
        return (0.0, "división por cero")
    diferencia = ((demanda - pronostico) / pronostico) * 100
    direccion = (
        "más alta" if diferencia > 0 else "más baja" if diferencia < 0 else "igual"
    )
    return (abs(round(diferencia, 2)), direccion)


def obtener_sistema(gerencia_name: str) -> str:
    if gerencia_name in BCA:
        return "BCA"
    elif gerencia_name in BCS:
        return "BCS"
    elif gerencia_name in SIN:
        return "SIN"
    logging.warning("Could not determine system for gerencia: %s", gerencia_name)
    return ""


def enviar_peticion(
    gerencia: Gerencia, hora: int, gerencia_name: str, API_URL: str
) -> Optional[requests.Response]:
    fecha_operacion = datetime.today().strftime("%Y-%m-%d")

    data: DataSchema = {
        "FechaOperacion": fecha_operacion,
        "HoraOperacion": hora,
        "Demanda": int(gerencia["valorDemanda"]),
        "Generacion": int(gerencia["valorGeneracion"]),
        "Enlace": gerencia["valorEnlace"] if gerencia["valorEnlace"] else None,
        "Pronostico": int(gerencia["valorPronostico"]),
        "Gerencia": gerencia_name,
        "Sistema": obtener_sistema(gerencia_name),
        "FechaCreacion": datetime.now().isoformat(),  # Using ISO format for JSON compatibility
        "FechaModificacion": datetime.now().isoformat(),  # Using ISO format for JSON compatibility
    }

    try:
        response = requests.post(
            f"{API_URL}/api/v1/mercado/demanda", json=data, timeout=20
        )  # Added timeout
        return response
    except requests.exceptions.RequestException as e:
        logging.error(
            "Error sending request to API URL/api/v1/mercado/demanda %s: %s", API_URL, e
        )
        return None


def obtener_demanda():
    logging.info("Starting demand data retrieval process...")
    load_dotenv()  # Load environment variables from .env file
    bot_token = os.getenv("TELEGRAM_BOT_MERCADOS_LUX_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    API_URL = os.getenv("API_URL")
    if not API_URL:
        logging.error("API_URL environment variable is not set.")
        raise ValueError("API_URL environment variable is not set.")

    if not bot_token or not chat_id:
        logging.error("Telegram bot token or chat ID is not set in environment variables.")
        raise ValueError("Telegram bot token or chat ID is not set in environment variables.")

    for gerencia_id, gerencia_name in MAPEO_GERENCIAS.items():
        logging.info("Processing gerencia: %s (%s)", gerencia_name, gerencia_id)
        # Datos que se enviarán en la solicitud (formato JSON)
        data = {"gerencia": f"{gerencia_id}"}

        # Encabezados de la solicitud
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
        }

        try:
            # Realizar la solicitud POST con los datos y encabezados proporcionados
            response = requests.post(
                URL, json=data, headers=headers, timeout=20
            )  # Added timeout
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

        except requests.exceptions.RequestException as e:
            logging.error(
                "Error during request to %s for gerencia %s: %s", URL, gerencia_name, e
            )
            continue  # Skip to the next gerencia

        # Obtener la respuesta en formato JSON
        try:
            json_data = response.json()
            # Cargar la cadena JSON asociada a la clave "d" en un formato JSON válido
            d_json = json.loads(json_data["d"])
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(
                "Error parsing JSON response for gerencia %s: %s. Response text: %s",
                gerencia_name,
                e,
                response.text[:500],  # Log first 500 chars
            )
            continue  # Skip to the next gerencia

        # Elimina la hora 24 al final de la lista ya que nunca se actualiza en el día actual
        if d_json and d_json[-1].get("hora") == "24":
            d_json_actualizado: list[Gerencia] = d_json[:-1]
        else:
            d_json_actualizado: list[Gerencia] = d_json

        # Checamos si existe la hora 24 al inicio de la lista ya que esto
        # significa que tenemos datos de la hora 00
        if d_json_actualizado and d_json_actualizado[0].get("hora") == "24":
            logging.info("Hora 00 data found for gerencia %s.", gerencia_name)
            hora_00 = d_json_actualizado[0]
            logging.info("Hora 00 data: %s", hora_00)

            # Check for potentially empty data which might cause int() errors
            if (
                hora_00.get("valorDemanda", "").strip()
                and hora_00.get("valorGeneracion", "").strip()
                and hora_00.get("valorPronostico", "").strip()
            ):
                api_response = enviar_peticion(hora_00, 0, gerencia_name, API_URL=API_URL)

                if api_response is not None:
                    # Check the response status
                    if api_response.status_code in CODE_STATUS:
                        logging.info(
                            "API request for Hora 00 (%s) successful (Status: %s).",
                            gerencia_name,
                            api_response.status_code,
                        )
                        # Perform success actions (like percentage check)
                        try:
                            porcentaje, direccion = diferencia_en_porcentaje(
                                int(hora_00["valorPronostico"]),
                                int(hora_00["valorDemanda"]),
                            )
                        except (ValueError, KeyError) as e:
                            logging.error(
                                "Error calculating percentage diff for Hora 00 (%s): %s. Data: %s",
                                gerencia_name,
                                e,
                                hora_00,
                            )

                    elif api_response.status_code == 409:
                        logging.warning(
                            "API request for Hora 00 (%s): Data already exists (Status 409).",
                            gerencia_name,
                        )
                    else:
                        logging.error(
                            "Error sending API request for Hora 00 (%s). Status: %s. Response: %s",
                            gerencia_name,
                            api_response.status_code,
                            api_response.text[:500],  # Log first 500 chars
                        )
                else:
                    logging.error(
                        "Did not receive a response from API for Hora 00 (%s).",
                        gerencia_name,
                    )
            else:
                logging.warning(
                    "Skipping Hora 00 for gerencia %s due to missing values in data: %s",
                    gerencia_name,
                    hora_00,
                )

        # Obtenemos la hora actual para obtener solo la hora que corresponde de la lista
        ahora = datetime.now()
        hora_actual_str = (
            ahora.strftime("%H").lstrip("0") or "0"
        )  # Handle midnight correctly
        hora_actual_int = int(hora_actual_str)
        logging.info(
            "Current hour check: %s for gerencia %s", hora_actual_str, gerencia_name
        )

        # Obtener el valor de la hora actual
        valor_hora_actual = next(
            (
                elemento
                for elemento in d_json_actualizado
                if elemento.get("hora") == hora_actual_str
            ),
            None,
        )

        # Si no se encuentra la hora actual, o la demanda está vacía, se salta esta gerencia
        if (
            valor_hora_actual is None
            or not valor_hora_actual.get("valorDemanda", "").strip()
        ):
            logging.warning(
                "No data available for current hour %s in gerencia %s. Skipping.",
                hora_actual_str,
                gerencia_name,
            )
            continue

        logging.info(
            "Data for current hour %s (%s): %s",
            hora_actual_str,
            gerencia_name,
            valor_hora_actual,
        )

        # Check for potentially empty data which might cause int() errors
        if (
            valor_hora_actual.get("valorDemanda", "").strip()
            and valor_hora_actual.get("valorGeneracion", "").strip()
            and valor_hora_actual.get("valorPronostico", "").strip()
        ):
            api_response = enviar_peticion(
                valor_hora_actual, hora_actual_int, gerencia_name, API_URL=API_URL
            )

            if api_response is not None:
                # Check the response status
                if api_response.status_code in CODE_STATUS:
                    logging.info(
                        "API request for Hora %s (%s) successful (Status: %s).",
                        hora_actual_str,
                        gerencia_name,
                        api_response.status_code,
                    )
                    # Perform success actions (like percentage check)
                    try:
                        porcentaje, direccion = diferencia_en_porcentaje(
                            int(valor_hora_actual["valorPronostico"]),
                            int(valor_hora_actual["valorDemanda"]),
                        )
                        if porcentaje >= 20 and direccion != "división por cero":
                            message = (
                                f"ALERTA: Gerencia {gerencia_name} - "
                                f"Demanda {porcentaje}% {direccion} que el pronóstico."
                            )
                            send_telegram_message(bot_token, chat_id, message)
                            logging.warning(
                                "--> ALERT Hora %s (%s): Demand is %s%% %s than forecast.",
                                hora_actual_str,
                                gerencia_name,
                                porcentaje,
                                direccion,
                            )
                    except (ValueError, KeyError) as e:
                        logging.error(
                            "Error calculating percentage diff for Hora %s (%s): %s. Data: %s",
                            hora_actual_str,
                            gerencia_name,
                            e,
                            valor_hora_actual,
                        )

                elif api_response.status_code == 409:
                    logging.warning(
                        "API request for Hora %s (%s): Data already exists (Status 409).",
                        hora_actual_str,
                        gerencia_name,
                    )
                else:
                    logging.error(
                        "Error sending API request for Hora %s (%s). Status: %s. Response: %s",
                        hora_actual_str,
                        gerencia_name,
                        api_response.status_code,
                        api_response.text[:500],  # Log first 500 chars
                    )
            else:
                logging.error(
                    "Did not receive a response from API for Hora %s (%s).",
                    hora_actual_str,
                    gerencia_name,
                )
        else:
            logging.warning(
                "Skipping current hour %s for gerencia %s due to missing values in data: %s",
                hora_actual_str,
                gerencia_name,
                valor_hora_actual,
            )
    logging.info("Demand data retrieval process finished.")

