"""Central configuration. Every setting is read from environment variables or `.env`."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# src/flight_platform/config.py -> project root is 2 levels above the package
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- OpenSky ---
    opensky_base_url: str = "https://opensky-network.org/api"
    opensky_token_url: str = (
        "https://auth.opensky-network.org/auth/realms/opensky-network"
        "/protocol/openid-connect/token"
    )
    opensky_client_id: str | None = None
    opensky_client_secret: str | None = None
    opensky_poll_interval_seconds: int = 900
    opensky_timeout_seconds: int = 30
    opensky_max_retries: int = 3
    opensky_min_interval_seconds: float = 10.0  # client-side pause between API calls
    opensky_bbox: str | None = None  # "lamin,lomin,lamax,lomax"; empty = whole world

    # --- Data Lake ---
    data_dir: Path = Field(default=PROJECT_ROOT / "data")

    # --- SQL Server ---
    sqlserver_host: str = "localhost"
    sqlserver_port: int = 1433
    sqlserver_database: str = "flight_dw"
    sqlserver_user: str = "sa"
    sqlserver_password: str = ""

    # --- Spark ---
    spark_master: str = "local[*]"

    # --- Logging ---
    log_level: str = "INFO"

    # Derived paths
    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
