import time
import signal
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os
from ngi_api import ngi
# from demanda import demanda
from demanda_real_balance import demanda_real_balance
from pml import pml
from servicios_conexos import servicios_conexos
from generacion_gi_ofertada import generacion_gi_ofertada

# Ensure the logs directory exists
os.makedirs("./logs", exist_ok=True)
scheduler = BackgroundScheduler()

def shutdown(signum, frame):
    logging.info("Shutting down scheduler...")
    scheduler.shutdown(wait=False) # Use wait=True if you want running jobs to finish
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

    # NGI API
    # Run this script from monday to friday at 8:00 AM
    scheduler.add_job(ngi.get_ngi_data, "cron", day_of_week="mon-fri", hour=8, minute=0)

    # Demanda tiempo real
    # Run this script every day from 8:00 AM to 11:55 PM
    # scheduler.add_job(
    #     demanda.get_demanda,
    #     "cron",
    #     day_of_week="*", # every day"
    #     hour="9-23",
    #     minute="15,35,55" #
    #    )

    # Demanda real balance
    # Run this script every day at 03:25 PM
    scheduler.add_job(
        demanda_real_balance.get_demanda_real_balance,
        "cron",
        day_of_week="*", # every day"
        hour="15",
        minute="25" # 15:25
       )

    # PML MDA
    # Run this script every day at 06:00 AM
    scheduler.add_job(
        pml.get_pml_mda,
        "cron",
        day_of_week="*", # every day"
        hour="6",
        minute="0" # 06:00
       )

    # PML MTR
    # Run this script every day at 06:05 AM
    scheduler.add_job(
        pml.get_pml_mtr,
        "cron",
        day_of_week="*", # every day"
        hour="06",
        minute="5" # 06:05
       )

    # Servicios Conexos MDA
    # Run this script every day at 06:10 AM
    scheduler.add_job(
        servicios_conexos.get_servicios_mda,
        "cron",
        day_of_week="*", # every day"
        hour="6",
        minute="10" # 06:10
       )

    # Servicios Conexos MTR
    # Run this script every day at 06:15 AM
    scheduler.add_job(
        servicios_conexos.get_servicios_mtr,
        "cron",
        day_of_week="*", # every day"
        hour="6",
        minute="15" # 06:15
       )
    
    # Ofertas del GI - Programa de Generación MDA
    # Run this script every day at 06:20 AM
    scheduler.add_job(
        generacion_gi_ofertada.get_generacion_gi_ofertada_mda,
        "cron",
        day_of_week="*", # every day"
        hour="6",
        minute="20" # 06:20
    )
    # Ofertas del GI - Programa de Generación MTR
    # Run this script every day at 06:25 AM
    scheduler.add_job(
        generacion_gi_ofertada.get_generacion_gi_ofertada_mtr,
        "cron",
        day_of_week="*", # every day"
        hour="10",
        minute="5" # 06:25
    )

    # logging.info("process started")
    scheduler.start() 

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown) # Ctrl+C

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown(None, None)