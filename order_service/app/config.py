from pathlib import Path

from pydantic import computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).parent.parent / ".env"


class Settings(BaseSettings):
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "orders_db"
    db_user: str = "orders_user"
    db_pass: str = "orders_password"
    db_echo: bool = False

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    pending_order_timeout_seconds: int = 900
    internal_api_token: str = "logpose-internal-secret"

    @computed_field
    @property
    def database_url(self) -> str:
        url = MultiHostUrl.build(
            scheme="postgresql+asyncpg",
            username=self.db_user,
            password=self.db_pass,
            host=self.db_host,
            port=self.db_port,
            path=self.db_name,
        )
        return str(url)

    model_config = SettingsConfigDict(
        env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore"
    )


settings = Settings()
