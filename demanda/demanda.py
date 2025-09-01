from config import ENV
import logging
from .get_data import get_data
import requests

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

def get_demanda():
    for gerencia in MAPEO_GERENCIAS:
        try:
            logging.info(f"📊 Procesando gerencia: {MAPEO_GERENCIAS[gerencia]} (ID: {gerencia})")
            data = get_data(str(gerencia), MAPEO_GERENCIAS[gerencia])
            response = requests.post(
                f"{ENV.API_URL}api/v1/demanda/bulk-upsert",
                json={"data": data},
                headers={"Content-Type": "application/json"}
            )

            if not response.ok:
                raise ValueError(f"Failed to write data to the database: {response.status_code}")

            logging.info(f"✅ Datos enviados exitosamente a la API para gerencia {MAPEO_GERENCIAS[gerencia]}")
        except Exception as e:
            logging.error(f"❌ Error al procesar la gerencia {MAPEO_GERENCIAS[gerencia]}: {e}")