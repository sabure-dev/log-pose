from pathlib import Path

from pydantic import computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).parent.parent / ".env"


class Settings(BaseSettings):
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "orders_db"
    db_user: str
    db_pass: str

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

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")


settings = Settings()
