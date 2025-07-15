import time
import signal
from apscheduler.schedulers.background import BackgroundScheduler
from ngi_api import ngi
from mda_mtr import pml_mda, pml_mtr, pnd_mda, pnd_mtr
from demanda import demanda
import logging
import os
from dotenv import load_dotenv
from rea_service import rea
from capacidad_transferencia import capacidad_transferencia
from demanda_real_balance import demanda_real_balance

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

    load_dotenv()


    # scheduler.add_job(
    #     demanda.obtener_demanda,
    #     "cron",
    #     day_of_week="*", # every day"
    #     hour="1-23",
    #     minute="15,35,55" # 10:05, 10:30, 10:55
    #    )

    # scheduler.add_job(ngi.get_ngi_data, "cron", day_of_week="mon-fri", hour=8, minute=30)

    # scheduler.add_job( rea.get_reas_value, "cron",
    #     day_of_week="*", # every day
    #     hour="*", # every hour
    #     minute="58" # every 30 minutes
    # )
    
    scheduler.add_job(
        capacidad_transferencia.get_capacidad_transferencia,
        "cron",
        day_of_week="*",
        # hour="9",
        # minute="0"
        hour="8",
        minute="19"
    )   

    scheduler.add_job(
        demanda_real_balance.obtener_demanda_real_balance,   
        "cron",
        day_of_week="*", # every day
        hour="15", # Every day at 3 PM
        minute="15" # Every day at 3:15 PM
    )   
    
    scheduler.add_job(
        pml_mda.get_pml_mda,
        "cron",
        day_of_week="*",
        hour="8",
        minute="29"
       )

    scheduler.add_job(
        pml_mtr.get_pml_mtr,
        "cron",
        day_of_week="*",
        hour="8",
        minute="5"
       )

    scheduler.add_job(
        pnd_mda.get_pnd_mda,
        "cron",
        day_of_week="*",
        hour="8",
        minute="32"
       )

    # scheduler.add_job(
    #     pnd_mtr.get_pnd_mtr,
    #     "cron",
    #     day_of_week="*",
    #     hour="8",
    #     minute="11",
    # )

    logging.info("process started")
    scheduler.start() 

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown) # Ctrl+C

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown(None, None)