import requests
import logging

def send_telegram_message(bot_token, chat_id, message):
    # Send a message to a Telegram chat.
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        logging.info("Telegram message sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Faild to send Telegram message: {e}")

def send_telegram_image(bot_token, chat_id, image_path, caption=None):
    """
    Send an image to a Telegram chat.
    
    :param bot_token: The Telegram bot token.
    :param chat_id: The chat ID to send the image to.
    :param image_path: The local path to the image file.
    :param caption: Optional caption for the image.
    """
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
        with open(image_path, 'rb') as image_file:
            data = {
                "chat_id": chat_id,
                "caption": caption
            }
            files = {
                "photo": image_file
            }
            response = requests.post(url, data=data, files=files)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            logging.info("Telegram image sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Telegram image: {e}")