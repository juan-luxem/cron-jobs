from typing import TypedDict, Optional
import csv
from datetime import datetime
import time

import requests
import json

URL = "https://www.cenace.gob.mx/GraficaDemanda.aspx/obtieneValoresTotal"

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
    FechaCreacion: datetime
    FechaModificacion: datetime


def obtener_sistema(gerencia_name: str) -> str:
    if gerencia_name in BCA:
        return "BCA"
    elif gerencia_name in BCS:
        return "BCS"
    elif gerencia_name in SIN:
        return "SIN"
    return ""


def transform_data(old_list, gerencia_name: str):
    new_list = []
    data_list = [item for item in old_list if item["valorDemanda"] != " "]
    for item in data_list:
        new_dict = {
            "FechaOperacion": time.strftime("%Y-%m-%d"),
            "HoraOperacion": int(item["hora"]),
            "Demanda": int(item["valorDemanda"]),
            "Generacion": int(item["valorGeneracion"]),
            "Enlace": item["valorEnlace"] if item["valorEnlace"] else None,
            "Pronostico": int(item["valorPronostico"]),
            "Gerencia": gerencia_name,
            "Sistema": obtener_sistema(gerencia_name),
            "FechaCreacion": datetime.now().isoformat(),
            "FechaModificacion": datetime.now().isoformat(),
        }
        new_list.append(new_dict)
    return new_list


def main():
    for gerencia in MAPEO_GERENCIAS:
        # Datos que se enviarán en la solicitud (formato JSON)
        data = {"gerencia": f"{gerencia}"}

        # Encabezados de la solicitud
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
        }

        # Realizar la solicitud POST con los datos y encabezados proporcionados
        response = requests.post(URL, json=data, headers=headers)
        print(
            f"Realizando la solicitud a la API para la gerencia {MAPEO_GERENCIAS[gerencia]}"
        )
        print(f"Respuesta: {response.status_code}")
        if response.status_code != 200:
            print(
                f"Error al realizar la solicitud. Código de estado: {response.status_code}"
            )
            return

        # Obtener la respuesta en formato JSON
        json_data = response.json()

        # Cargar la cadena JSON asociada a la clave "d" en un formato JSON válido
        d_json = json.loads(json_data["d"])
        # json_actualizado = d_json[: len(d_json) - 1]
        # Escribir archivo JSON y anidar la respuesta
        new_list = transform_data(d_json, MAPEO_GERENCIAS[gerencia])
        with open("demanda.json", "a") as file:
            json.dump(new_list, file, indent=4)


main()
# gerencia = 1  # Cambia este valor según la gerencia que desees consultar

# data = {"gerencia": f"{gerencia}"}

# # Encabezados de la solicitud
# headers = {
#     "Content-Type": "application/json; charset=UTF-8",
# }

# # Realizar la solicitud POST con los datos y encabezados proporcionados
# response = requests.post(URL, json=data, headers=headers)

# # Obtener la respuesta en formato JSON
# json_data = response.json()

# # Cargar la cadena JSON asociada a la clave "d" en un formato JSON válido
# d_json = json.loads(json_data["d"])
# print(d_json)
# # Escribir archivo JSON y anidar la respuesta
# with open("demanda3.json", "a") as file:
#     json.dump(d_json, file, indent=4)
# # with open("demanda.csv", "w", newline="") as file: