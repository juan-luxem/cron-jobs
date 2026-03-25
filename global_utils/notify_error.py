import logging

from config import ENV
from global_utils import send_telegram_message


def notify_error(message: str):
    """Helper to send telegram alerts on error."""
    logging.error(message)
    bot_token = ENV.TELEGRAM_BOT_GAS_NOTIFIER_TOKEN.get_secret_value()
    chat_id = ENV.TELEGRAM_GROUP_CHAT_ID
    try:
        send_telegram_message(bot_token, chat_id, message)
    except Exception as e:
        logging.error(f"Failed to send Telegram alert: {e}")
