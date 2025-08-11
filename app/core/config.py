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

    # --- Mongo ---
    MONGO_URI: AnyUrl = Field(
        default="mongodb://mongo:27017",
        description="MongoDB connection URI.",
        validation_alias="MRS_MONGO_URI",
    )
    MONGO_DB: str = Field(
        default="materials_db",
        description="MongoDB database name.",
        validation_alias="MRS_MONGO_DB",
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
