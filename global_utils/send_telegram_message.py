import requests
import logging

def send_telegram_message(bot_token, chat_id, message):
    """Send a message to a Telegram chat."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Telegram message sent successfully")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Telegram message: {e}")
