import time
import signal
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os
from ngi_api import ngi

# from demanda import demanda
from demanda_real_balance import demanda_real_balance
from pml import pml
from pnd import pnd
from servicios_conexos import servicios_conexos
from generacion_gi_ofertada import generacion_gi_ofertada
from generacion_ndso_ofertada import generacion_ndso_ofertada
from generacion_idr_ofertada import generacion_idr_ofertada
from generacion_hidro_ofertada import generacion_hidro_ofertada
from generacion_ofertada import generacion_ofertada
from cantidades_asignadas_servicios_conexos import (
    cantidades_asignadas_servicios_conexos,
)
from capacidad_transferencia import capacidad_transferencia
from asignacion_por_participante_mercado import asignacion_por_participante_mercado
from servicios_conexos_por_zona_reserva import servicios_conexos_por_zona_reserva
from rea_service import rea

# from demanda import demanda
from salidas_adelanto import run_salidas_adelanto
from salidas_ocurridas import run_salidas_ocurridas

# Ensure the logs directory exists
os.makedirs("./logs", exist_ok=True)
scheduler = BackgroundScheduler()


def shutdown(signum, frame):
    logging.info("Shutting down scheduler...")
    scheduler.shutdown(wait=False)  # Use wait=True if you want running jobs to finish
    exit(0)


if __name__ == "__main__":
    # Logging Setup
    logging.basicConfig(
        filename="./logs/cron.log",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    # Run REA service every day from 8:00 AM to 11:55 PM every day every 30 minutes
    # Checked ✅
    scheduler.add_job(
        rea.get_reas_value, "cron", day_of_week="*", hour="8-23", minute="*/30"
    )

    # NGI API
    # Run this script from monday to friday at 8:00 AM
    scheduler.add_job(ngi.get_ngi_data, "cron", day_of_week="mon-fri", hour=8, minute=0)

    # Demanda tiempo real
    # Run this script every day from 8:00 AM to 11:55 PM
    # scheduler.add_job(
    #     demanda.get_demanda,
    #     "cron",
    #     day_of_week="*",  # every day"
    #     hour="9-23",
    #     minute="15,44,55",  #
    # )

    # Run salidas adelanto every day at 5:30 AM
    # Checked ✅
    scheduler.add_job(
        run_salidas_adelanto.run_salidas_adelanto,
        "cron",
        day_of_week="*",
        hour="5",
        minute="30",
    )

    # Run salidas ocurridas every day at 5:40 AM
    # Checked ✅
    scheduler.add_job(
        run_salidas_ocurridas.run_salidas_ocurridas,
        "cron",
        day_of_week="*",
        hour="5",
        minute="40",
    )

    # PML MDA
    # Run this script every day at 05:50 AM
    # Checked ✅
    scheduler.add_job(
        pml.get_pml_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="5",
        minute="50",  # 05:50
    )

    # PML MTR
    # Run this script every day at 05:55 AM
    # Checked ✅
    scheduler.add_job(
        pml.get_pml_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="5",
        minute="55",  # 05:55
    )

    # Run PND MDA
    # Run this script every day at 6:00 AM
    scheduler.add_job(
        pnd.get_pml_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="0",  # 06:00
    )

    # Run PND MTR
    # Run this script every day at 6:05 AM
    scheduler.add_job(
        pnd.get_pml_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="5",  # 06:05
    )

    # Servicios Conexos MDA
    # Run this script every day at 06:10 AM
    # Checked ✅
    scheduler.add_job(
        servicios_conexos.get_servicios_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="10",  # 06:10
    )

    # Servicios Conexos MTR
    # Run this script every day at 06:15 AM
    # Checked ✅
    scheduler.add_job(
        servicios_conexos.get_servicios_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="15",  # 06:15
    )

    # Ofertas del GI - Programa de Generación MDA
    # Run this script every day at 06:20 AM
    # Checked ✅
    scheduler.add_job(
        generacion_gi_ofertada.get_generacion_gi_ofertada_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="20",  # 06:20
    )
    # Ofertas del GI - Programa de Generación MTR
    # Run this script every day at 06:22 AM
    # Checked ✅
    scheduler.add_job(
        generacion_gi_ofertada.get_generacion_gi_ofertada_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="22",  # 06:22
    )

    # Ofertas de Venta – No Despachable MDA
    # Run this script every day at 06:24 AM
    # Checked ✅
    scheduler.add_job(
        generacion_ndso_ofertada.get_generacion_ndso_ofertada_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="24",  # 06:34
    )

    # Ofertas de Venta – Despachable MTR
    # Run this script every day at 06:26 AM
    # Checked ✅
    scheduler.add_job(
        generacion_ndso_ofertada.get_generacion_ndso_ofertada_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="26",  # 06:26
    )

    # Ofertas de Venta – Recursos Interm Despachables MDA
    # Run this script every day at 06:28 AM
    # Checked ✅
    scheduler.add_job(
        generacion_idr_ofertada.get_generacion_idr_ofertada_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="6",
        minute="28",  # 06:28
    )

    # Capacidad de Transferencia MDA
    # Run this script every day at 9:50 AM
    # Checked ✅
    scheduler.add_job(
        capacidad_transferencia.get_capacidad_transferencia,
        "cron",
        day_of_week="*",  # every day"
        hour="9",
        minute="50",  # 09:50
    )

    # Ofertas de Venta – Recursos Interm Despachables MTR
    # Run this script every day at 06:30 AM
    # Checked ✅
    scheduler.add_job(
        generacion_idr_ofertada.get_generacion_idr_ofertada_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="15",
        minute="30",  # 06:30
    )
    # Ofertas de Venta – Hidroeléctricas MDA
    # Run this script every day at 06:32 AM
    # Checked ✅
    scheduler.add_job(
        generacion_hidro_ofertada.get_generacion_hidro_ofertada_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="15",
        minute="32",  # 06:32
    )

    # Ofertas de Venta – Hidroeléctricas MTR
    # Run this script every day at 06:34 AM
    # Checked ✅
    scheduler.add_job(
        generacion_hidro_ofertada.get_generacion_hidro_ofertada_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="15",
        minute="34",  # 06:34
    )

    # Ofertas de Venta – Térmicas MDA
    # Run this script every day at 06:36 AM
    # Checked ✅
    scheduler.add_job(
        generacion_ofertada.get_generacion_ofertada_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="15",
        minute="36",  # 06:36
    )

    # Ofertas de Venta – Térmicas MTR
    # Run this script every day at 06:38 AM
    # Checked ✅
    scheduler.add_job(
        generacion_ofertada.get_generacion_ofertada_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="15",
        minute="38",  # 06:38
    )

    # Demanda real balance
    # Run this script every day at 04:25 PM
    # Checked ✅
    scheduler.add_job(
        demanda_real_balance.get_demanda_real_balance,
        "cron",
        day_of_week="*",  # every day"
        hour="16",
        minute="25",  # 16:25
    )

    # Cantidades Asignadas Servicios Conexos MDA
    # Run this script every day at 8:00 PM
    # Checked ✅
    scheduler.add_job(
        cantidades_asignadas_servicios_conexos.cantidades_asignadas_servicios_conexos_mda,
        "cron",
        day_of_week="*",  # every day"
        hour="23",
        minute="39",  # 20:00
    )

    # Cantidades Asignadas Servicios Conexos MTR
    # Run this script every day at 8:05 PM
    # Checked ✅
    scheduler.add_job(
        cantidades_asignadas_servicios_conexos.cantidades_asignadas_servicios_conexos_mtr,
        "cron",
        day_of_week="*",  # every day"
        hour="23",
        minute="41",  # 20:05
    )

    # Asignación por Participante de Mercado
    # Run this script every day at 10:00 PM
    # Checked ✅
    scheduler.add_job(
        asignacion_por_participante_mercado.get_asignacion_por_participante_mercado,
        "cron",
        day_of_week="*",  # every day"
        hour="23",
        minute="43",  # 22:00
    )

    # Servicios Conexos por Zona de Reserva
    # Run this script every day at 10:05 PM
    # Checked ✅
    scheduler.add_job(
        servicios_conexos_por_zona_reserva.run_servicios_conexos_por_zona_reserva,
        "cron",
        day_of_week="*",  # every day"
        hour="23",
        minute="45",  # 22:05
    )

    # logging.info("process started")
    scheduler.start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)  # Ctrl+C

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown(None, None)
