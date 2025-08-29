import time
import signal
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os
from ngi_api import ngi

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
    # scheduler.add_job(
    #     demanda.obtener_demanda,
    #     "cron",
    #     day_of_week="*", # every day"
    #     hour="1-23",
    #     minute="15,35,55" # 10:05, 10:30, 10:55
    #    )

    logging.info("process started")
    scheduler.start() 

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown) # Ctrl+C

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown(None, None)