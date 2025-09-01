from typing import TypedDict, Optional
from datetime import datetime
import logging
import requests
import json
from .get_system import get_system

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CODE_STATUS = [200, 201]

class Gerencia(TypedDict):
    hora: str
    valorDemanda: str
    valorGeneracion: str
    valorEnlace: Optional[str]
    valorPronostico: str

class DataSchema(TypedDict):
    FechaOperacion: str
    HoraOperacion: int
    Demanda: Optional[float]
    Generacion: Optional[float]
    Enlace: Optional[float]
    Pronostico: float 
    Gerencia: str
    Sistema: str

def safe_float_conversion(value: str) -> Optional[float]:
    """Safely convert string to float, return None for empty strings"""
    if not value or value.strip() == "":
        return None
    return float(value)

def get_data(gerenciaId: str, gerencia: str) -> list[DataSchema]:
    demanda_url = "https://www.cenace.gob.mx/GraficaDemanda.aspx/obtieneValoresTotal"
    logging.info(f"🌐 Consultando API del CENACE para gerencia: {gerencia} (ID: {gerenciaId})")
    
    # Datos que se enviarán en la solicitud (formato JSON)
    data = {"gerencia": f"{gerenciaId}"}

    # Encabezados de la solicitud
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
    }

    response = requests.post(demanda_url, json=data, headers=headers)
    print(gerenciaId, gerencia)
    print("response", response)
    
    if response.status_code not in CODE_STATUS:
        logging.error(f"❌ Error al consultar la API del CENACE para gerencia: {gerencia} (ID: {gerenciaId})")
        raise Exception(f"Error en la solicitud: {response.status_code}")

    # Get current date in ISO Format (YYYY-MM-DD)
    current_date = datetime.now().date().isoformat()
    
    # Obtener la respuesta en formato JSON
    json_data = response.json()

    # Cargar la cadena JSON asociada a la clave "d" en un formato JSON válido
    d_json: list[Gerencia] = json.loads(json_data["d"])
    system = get_system(gerencia)
    
    if system == "":
        raise ValueError(f"Gerencia no reconocida: {gerencia}")

    logging.info(f"📋 Procesando {len(d_json)} registros para sistema: {system}")

    formatted_data: list[DataSchema] = []
    for i, item in enumerate(d_json):
        formatted_data.append(DataSchema(
            FechaOperacion=current_date,
            HoraOperacion=0 if item["hora"] == "24" and i == 0 else int(item["hora"]),
            # ✅ FIXED: Using correct key names from API response
            Demanda=safe_float_conversion(item["valorDemanda"]),
            Generacion=safe_float_conversion(item["valorGeneracion"]),
            Pronostico=float(item["valorPronostico"]),
            Enlace=float(item["valorEnlace"]) if item["valorEnlace"] else None,
            Gerencia=gerencia,
            Sistema=system,
        ))
    
    logging.info(f"✅ Datos procesados exitosamente para {gerencia}")
    return formatted_data