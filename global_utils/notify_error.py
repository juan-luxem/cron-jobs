import logging

from global_utils import send_telegram_message


def notify_error(message: str):
    """Helper to send telegram alerts on error."""
    logging.error(message)
    try:
        send_telegram_message(message)
    except Exception as e:
        logging.error(f"Failed to send Telegram alert: {e}")
