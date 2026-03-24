import os

from dotenv import load_dotenv
from pydantic import EmailStr, Field, HttpUrl, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class Settings(BaseSettings):
    """
    Defines and validates application environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignores extra variables in the .env file
    )
    DOWNLOAD_FOLDER: str = "download_folder"
    DOWNLOAD_FOLDER_RANGE: str = "download_folder_range"

    # NGI Credentials
    NGI_EMAIL: EmailStr
    NGI_PASSWORD: SecretStr = Field(..., min_length=3)
    NGI_ENDPOINT: HttpUrl
    API_URL: HttpUrl

    # Telegram Bot Tokens and IDs
    TELEGRAM_BOT_GAS_NOTIFIER_TOKEN: SecretStr = Field(..., min_length=11)
    TELEGRAM_BOT_MERCADOS_LUX_TOKEN: SecretStr = Field(..., min_length=11)

    TELEGRAM_GROUP_CHAT_ID: str = Field(..., min_length=5)
    TELEGRAM_CHAT_ID: str = Field(..., min_length=5)

    MAU_CREDENTIALS_PASSWORD: SecretStr = Field(..., min_length=4)
    MAU_USERNAME: str = Field(..., min_length=4)
    MAU_PASSWORD: SecretStr = Field(..., min_length=4)


try:
    ENV = Settings()
    os.makedirs(ENV.DOWNLOAD_FOLDER, exist_ok=True)
    os.makedirs(ENV.DOWNLOAD_FOLDER_RANGE, exist_ok=True)
except ValidationError as e:
    print("🔥 Error: Environment variable validation failed!")
    print(e)
