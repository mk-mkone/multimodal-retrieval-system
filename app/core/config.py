from functools import lru_cache
from typing import Literal

from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuration for the Multimodal Retrieval System.
    All fields can be overridden via environment variables (MRS_*).
    """

    # --- App ---
    APP_NAME: str = Field(
        default="Multimodal Retrieval System",
        description="Name of the application.",
        validation_alias="MRS_APP_NAME",
    )
    ENV: Literal["local", "dev", "staging", "prod"] = Field(
        default="local",
        description="Runtime environment.",
        validation_alias="MRS_ENV",
    )
    DEBUG: bool = Field(
        default=True,
        description="Enable/disable debug mode.",
        validation_alias="MRS_DEBUG",
    )

    # --- API ---
    API_PREFIX: str = Field(
        default="/api",
        description="Base URL prefix for all API routes.",
        validation_alias="MRS_API_PREFIX",
    )
    HOST: str = Field(
        default="0.0.0.0",
        description="API host binding.",
        validation_alias="MRS_HOST",
    )
    PORT: int = Field(
        default=8000,
        description="API port.",
        validation_alias="MRS_PORT",
    )

    # --- Postgre ---
    POSTGRES_URI: AnyUrl = Field(
        default="postgresql://appuser:apppassword@postgres:5432/materials_db",
        description="Postgres connection URI.",
        validation_alias="MRS_POSTGRES_URI",
    )

    # --- S3 / Minio ---
    S3_ENDPOINT_URL: str = Field(
        default="http://minio:9000",
        description="S3 or Minio endpoint URL.",
        validation_alias="MRS_S3_ENDPOINT_URL",
    )
    S3_BUCKET: str = Field(
        default="mrs",
        description="S3 or Minio bucket name.",
        validation_alias="MRS_S3_BUCKET",
    )
    S3_ACCESS_KEY: str = Field(
        default="minio_user",
        description="S3 or Minio access key.",
        validation_alias="MRS_S3_ACCESS_KEY",
    )
    S3_SECRET_KEY: str = Field(
        default="minio_password",
        description="S3 or Minio secret key.",
        validation_alias="MRS_S3_SECRET_KEY",
    )
    S3_REGION: str = Field(
        default="eu-west-3",
        description="S3 or Minio region.",
        validation_alias="MRS_S3_REGION",
    )

    # --- Logging ---
    LOG_FORMAT: str = Field(
        default="json",
        description='Log output format: "json" or "text".',
        validation_alias="MRS_LOG_FORMAT",
    )
    LOG_LEVEL: str = Field(
        default="INFO",
        description='Logging level (e.g., "DEBUG", "INFO").',
        validation_alias="MRS_LOG_LEVEL",
    )
    LOG_FILE_ENABLED: bool = Field(
        default=False,
        description="If true, also write logs to a file.",
        validation_alias="MRS_LOG_FILE_ENABLED",
    )
    LOG_FILE: str = Field(
        default="logs/app.log",
        description="Path of the log file (when enabled).",
        validation_alias="MRS_LOG_FILE",
    )

    # --- Api URL ---
    EUROPEPMC_API_URL: str = Field(
        default="https://www.ebi.ac.uk/europepmc/webservices/rest/search",
        description="Europepmc API endpoint.",
        validation_alias="MRS_EUROPEPMC_API_URL",
    )

    MP_API_URL: str = Field(
        default="https://api.materialsproject.org/materials/summary",
        description="Materials Project API endpoint.",
        validation_alias="MRS_MP_API_URL",
    )

    MATERIALS_PROJECT_API_KEY: str = Field(
        default="",
        description="Materials API key",
        validation_alias="MRS_MATERIALS_PROJECT_API_KEY",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
