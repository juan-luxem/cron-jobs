import logging
import os
import threading
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request

from asignacion_por_participante_mercado import asignacion_por_participante_mercado
from cantidades_asignadas_servicios_conexos import (
    cantidades_asignadas_servicios_conexos,
)
from capacidad_transferencia import capacidad_transferencia

# from demanda import demanda
from demanda_real_balance import demanda_real_balance
from generacion_gi_ofertada import generacion_gi_ofertada
from generacion_hidro_ofertada import generacion_hidro_ofertada
from generacion_idr_ofertada import generacion_idr_ofertada
from generacion_ndso_ofertada import generacion_ndso_ofertada
from generacion_ofertada import generacion_ofertada
from ngi_api import ngi
from pml import pml
from pnd import pnd
from rea_service import rea
from salidas_adelanto import run_salidas_adelanto
from salidas_ocurridas import run_salidas_ocurridas
from servicios_conexos import servicios_conexos
from servicios_conexos_por_zona_reserva import servicios_conexos_por_zona_reserva

# Ensure the logs directory exists
os.makedirs("./logs", exist_ok=True)

app = Flask(__name__)

# --- MAP for API Triggers ---
MODULE_MAP = {
    "asignacion_por_participante_mercado": asignacion_por_participante_mercado,
    "cantidades_asignadas_servicios_conexos": cantidades_asignadas_servicios_conexos,
    "capacidad_transferencia": capacidad_transferencia,
    # "demanda": demanda,
    "demanda_real_balance": demanda_real_balance,
    "generacion_gi_ofertada": generacion_gi_ofertada,
    "generacion_hidro_ofertada": generacion_hidro_ofertada,
    "generacion_idr_ofertada": generacion_idr_ofertada,
    "generacion_ndso_ofertada": generacion_ndso_ofertada,
    "generacion_ofertada": generacion_ofertada,
    "ngi": ngi,
    "pml": pml,
    "pnd": pnd,
    "rea": rea,
    "run_salidas_adelanto": run_salidas_adelanto,
    "run_salidas_ocurridas": run_salidas_ocurridas,
    "servicios_conexos": servicios_conexos,
    "servicios_conexos_por_zona_reserva": servicios_conexos_por_zona_reserva,
}

# --- Scheduler Setup ---
scheduler = BackgroundScheduler()


def start_scheduler():
    """Defines and starts all Cron Jobs."""

    # Run REA service every day from 8:00 AM to 11:55 PM every day every 30 minutes
    scheduler.add_job(
        rea.get_reas_value, "cron", day_of_week="*", hour="8-23", minute="*/30"
    )

    # NGI API
    # Run this script from monday to friday at 8:02 AM
    scheduler.add_job(ngi.get_ngi_data, "cron", day_of_week="mon-fri", hour=8, minute=2)

    # Demanda tiempo real
    # Run this script every day from 8:00 AM to 11:55 PM
    # scheduler.add_job(
    #     demanda.get_demanda,
    #     "cron",
    #     day_of_week="*",
    #     hour="9-23",
    #     minute="0,5,15,44,55",
    # )

    # Run salidas adelanto every day at 5:30 AM
    scheduler.add_job(
        run_salidas_adelanto.run_salidas_adelanto,
        "cron",
        day_of_week="*",
        hour="5",
        minute="30",
    )

    # Run salidas ocurridas every day at 5:40 AM
    scheduler.add_job(
        run_salidas_ocurridas.run_salidas_ocurridas,
        "cron",
        day_of_week="*",
        hour="5",
        minute="40",
    )

    # PML MDA - 05:50 AM
    scheduler.add_job(
        pml.get_pml_mda,
        "cron",
        day_of_week="*",
        hour="11",
        minute="47",
    )

    # PML MTR - 05:55 AM
    scheduler.add_job(
        pml.get_pml_mtr,
        "cron",
        day_of_week="*",
        hour="5",
        minute="55",
    )

    # Run PND MDA - 6:00 AM
    scheduler.add_job(
        pnd.get_pnd_mda,
        "cron",
        day_of_week="*",
        hour="6",
        minute="0",
    )

    # Run PND MTR - 6:05 AM
    scheduler.add_job(
        pnd.get_pnd_mtr,
        "cron",
        day_of_week="*",
        hour="6",
        minute="5",
    )

    # Servicios Conexos MDA - 06:10 AM
    scheduler.add_job(
        servicios_conexos.get_servicios_mda,
        "cron",
        day_of_week="*",
        hour="6",
        minute="10",
    )

    # Servicios Conexos MTR - 06:15 AM
    scheduler.add_job(
        servicios_conexos.get_servicios_mtr,
        "cron",
        day_of_week="*",
        hour="6",
        minute="15",
    )

    # Ofertas del GI - Programa de Generación MDA - 06:20 AM
    scheduler.add_job(
        generacion_gi_ofertada.get_generacion_gi_ofertada_mda,
        "cron",
        day_of_week="*",
        hour="6",
        minute="20",
    )
    # Ofertas del GI - Programa de Generación MTR - 06:22 AM
    scheduler.add_job(
        generacion_gi_ofertada.get_generacion_gi_ofertada_mtr,
        "cron",
        day_of_week="*",
        hour="6",
        minute="22",
    )

    # Ofertas de Venta – No Despachable MDA - 06:24 AM
    scheduler.add_job(
        generacion_ndso_ofertada.get_generacion_ndso_ofertada_mda,
        "cron",
        day_of_week="*",
        hour="6",
        minute="24",
    )

    # Ofertas de Venta – Despachable MTR - 06:26 AM
    scheduler.add_job(
        generacion_ndso_ofertada.get_generacion_ndso_ofertada_mtr,
        "cron",
        day_of_week="*",
        hour="6",
        minute="26",
    )

    # Ofertas de Venta – Recursos Interm Despachables MDA - 06:28 AM
    scheduler.add_job(
        generacion_idr_ofertada.get_generacion_idr_ofertada_mda,
        "cron",
        day_of_week="*",
        hour="6",
        minute="28",
    )

    # Ofertas de Venta – Recursos Interm Despachables MTR - 06:30 AM
    scheduler.add_job(
        generacion_idr_ofertada.get_generacion_idr_ofertada_mtr,
        "cron",
        day_of_week="*",
        hour="6",
        minute="30",
    )

    # Ofertas de Venta – Hidroeléctricas MDA - 06:32 AM
    scheduler.add_job(
        generacion_hidro_ofertada.get_generacion_hidro_ofertada_mda,
        "cron",
        day_of_week="*",
        hour="6",
        minute="32",
    )

    # Ofertas de Venta – Hidroeléctricas MTR - 06:34 AM
    scheduler.add_job(
        generacion_hidro_ofertada.get_generacion_hidro_ofertada_mtr,
        "cron",
        day_of_week="*",
        hour="6",
        minute="34",
    )

    # Ofertas de Venta – Térmicas MDA - 06:36 AM
    scheduler.add_job(
        generacion_ofertada.get_generacion_ofertada_mda,
        "cron",
        day_of_week="*",
        hour="6",
        minute="36",
    )

    # Ofertas de Venta – Térmicas MTR - 06:38 AM
    scheduler.add_job(
        generacion_ofertada.get_generacion_ofertada_mtr,
        "cron",
        day_of_week="*",
        hour="6",
        minute="38",
    )

    # Capacidad de Transferencia MDA - 9:50 AM
    scheduler.add_job(
        capacidad_transferencia.get_capacidad_transferencia,
        "cron",
        day_of_week="*",
        hour="9",
        minute="50",
    )

    # Demanda real balance - 04:25 PM
    scheduler.add_job(
        demanda_real_balance.get_demanda_real_balance,
        "cron",
        day_of_week="*",
        hour="16",
        minute="25",
    )

    # Cantidades Asignadas Servicios Conexos MDA - 8:04 PM
    scheduler.add_job(
        cantidades_asignadas_servicios_conexos.cantidades_asignadas_servicios_conexos_mda,
        "cron",
        day_of_week="*",
        hour="20",
        minute="4",
    )

    # Cantidades Asignadas Servicios Conexos MTR - 8:07 PM
    scheduler.add_job(
        cantidades_asignadas_servicios_conexos.cantidades_asignadas_servicios_conexos_mtr,
        "cron",
        day_of_week="*",
        hour="20",
        minute="7",
    )

    # Asignación por Participante de Mercado - 10:05 PM
    scheduler.add_job(
        asignacion_por_participante_mercado.get_asignacion_por_participante_mercado,
        "cron",
        day_of_week="*",
        hour="22",
        minute="5",
    )

    # Servicios Conexos por Zona de Reserva - 10:08 PM
    scheduler.add_job(
        servicios_conexos_por_zona_reserva.run_servicios_conexos_por_zona_reserva,
        "cron",
        day_of_week="*",
        hour="22",
        minute="8",
    )

    scheduler.start()


# --- Helper for API Background Tasks ---
def run_in_background(func, *args, **kwargs):
    """Executes a function in a separate thread."""
    thread = threading.Thread(target=func, args=args, kwargs=kwargs)
    thread.start()


def parse_date(date_str):
    """Fallback date parser in case global_utils is not available."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Incorrect data format, should be YYYY-MM-DD")


# --- API Endpoints ---
@app.route("/health", methods=["GET"])
def health_check():
    scheduler_status = "running" if scheduler.running else "stopped"
    return jsonify(
        {
            "status": "ok",
            "service": "mercados-scripts-worker",
            "scheduler": scheduler_status,
        }
    ), 200


@app.route("/trigger/<module_name>", methods=["POST"])
def trigger_catalog(module_name):
    """
    Triggers a catalog module (no MDA/MTR variant).
    """
    module = MODULE_MAP.get(module_name)

    if not module:
        return jsonify({"error": f"Module '{module_name}' not found"}), 404

    # The function name is typically get_<module_name> or run_<module_name>
    func_name = f"get_{module_name}"
    if not hasattr(module, func_name):
        func_name = f"run_{module_name}"  # fallback for some scripts
        if not hasattr(module, func_name):
            # Final fallback, check if we can call the function directly with module_name
            if hasattr(module, module_name):
                func_name = module_name
            else:
                return jsonify(
                    {
                        "error": f"Function to run '{module_name}' not found in module '{module_name}'"
                    }
                ), 500

    target_func = getattr(module, func_name)

    # Get optional parameters from JSON body
    data = request.get_json(silent=True) or {}
    start_date = data.get("start_date")
    end_date = data.get("end_date")

    try:
        kwargs = {}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        run_in_background(target_func, **kwargs)

        date_msg = (
            f" (dates: {start_date} to {end_date})" if start_date and end_date else ""
        )
        return jsonify(
            {
                "status": "accepted",
                "message": f"Job {module_name}{date_msg} started successfully.",
            }
        ), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/trigger/<module_name>/<market_type>", methods=["POST"])
def trigger_job(module_name, market_type):
    """
    Triggers a specific module manually with MDA/MTR.
    """
    market_type = market_type.upper()
    module = MODULE_MAP.get(module_name)

    if not module:
        return jsonify({"error": f"Module '{module_name}' not found"}), 404

    if market_type not in ["MDA", "MTR"]:
        return jsonify({"error": "Invalid market type. Use MDA or MTR"}), 400

    data = request.get_json(silent=True) or {}
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    sistema = data.get("sistema")

    # --- VALIDATION BLOCK START ---
    if sistema and sistema not in ["SIN", "BCA", "BCS"]:
        return jsonify(
            {
                "error": "Invalid sistema value.",
                "detail": f"Sistema must be one of: SIN, BCA, BCS. Got: {sistema}",
            }
        ), 400

    if start_date and end_date:
        try:
            s_obj = parse_date(start_date)
            e_obj = parse_date(end_date)

            delta = (e_obj - s_obj).days

            if delta > 61:
                return jsonify(
                    {
                        "error": "Date range exceeds maximum allowed length.",
                        "detail": f"You requested {delta} days. Max allowed is 60 days (2 months).",
                    }
                ), 400

        except ValueError as e:
            return jsonify({"error": f"Invalid date format: {str(e)}"}), 400
    # --- VALIDATION BLOCK END ---

    func_name = f"get_{module_name}_{market_type.lower()}"
    if not hasattr(module, func_name):
        # Specific overrides based on module names structure
        if "cantidades_asignadas_servicios_conexos" in module_name:
            func_name = f"cantidades_asignadas_servicios_conexos_{market_type.lower()}"

        if not hasattr(module, func_name):
            return jsonify(
                {"error": f"Function '{func_name}' not found in module '{module_name}'"}
            ), 500

    target_func = getattr(module, func_name)

    try:
        kwargs = {}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        if sistema:
            kwargs["sistema"] = sistema

        run_in_background(target_func, **kwargs)

        sistema_msg = f" (sistema: {sistema})" if sistema else ""
        return jsonify(
            {
                "status": "accepted",
                "message": f"Job {module_name} ({market_type}){sistema_msg} started successfully.",
            }
        ), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- Entry Point ---
if __name__ == "__main__":
    # Logging Setup
    logging.basicConfig(
        filename="./logs/cron.log",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    # 1. Start the Scheduler (Background)
    start_scheduler()

    # 2. Start the Flask API
    app.run(host="0.0.0.0", port=5000)
