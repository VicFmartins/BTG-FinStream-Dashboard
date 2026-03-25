from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "BTG FinStream Dashboard API"
    app_env: str = "local"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    postgres_dsn: str | None = None
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_db: str = "btg_finstream"
    postgres_user: str = "btg"
    postgres_password: str = "btg_secret"
    cors_allowed_origins: str = "http://localhost:5173,http://localhost:8080"
    kafka_brokers: str = "localhost:19092"
    event_topic: str = "transactions.events"
    kafka_consumer_group: str = "btg-finstream-backend"
    enable_event_consumer: bool = False

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def cors_allowed_origins_list(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.cors_allowed_origins.split(",")
            if origin.strip()
        ]

    @property
    def database_url(self) -> str:
        if self.postgres_dsn:
            return self.postgres_dsn

        return (
            "postgresql+psycopg://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
