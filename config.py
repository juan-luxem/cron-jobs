from pydantic import EmailStr, SecretStr, HttpUrl, Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()
class Settings(BaseSettings):
    """
    Defines and validates application environment variables.
    """
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore' # Ignores extra variables in the .env file
    )

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
except ValidationError as e:
    print("🔥 Error: Environment variable validation failed!")
    print(e)
