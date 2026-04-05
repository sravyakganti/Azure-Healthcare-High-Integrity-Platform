"""
ingestion/config.py
-------------------
Centralised configuration for the Healthcare Data Ingestion pipeline.
Uses pydantic-settings so every field can be overridden by an environment
variable or a .env file at the project root.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ---------------------------------------------------------------------------
# Dataset file-name registry
# ---------------------------------------------------------------------------
DATA_FILES: dict[str, str] = {
    "patients": "patients.csv",
    "encounters": "encounters.csv",
    "lab_tests": "lab_tests.csv",
    "claims": "claims_and_billing.csv",
}

# Absolute path to the repository root (two levels up from this file)
_REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Settings model
# ---------------------------------------------------------------------------
class Settings(BaseSettings):
    """Application-wide settings resolved from environment variables / .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Azure connection details
    # ------------------------------------------------------------------
    AZURE_STORAGE_ACCOUNT_NAME: Optional[str] = Field(
        default=None,
        description="Name of the ADLS Gen2 storage account.",
    )
    AZURE_STORAGE_ACCOUNT_KEY: Optional[str] = Field(
        default=None,
        description="Primary access key for the storage account.",
    )
    AZURE_TENANT_ID: Optional[str] = Field(
        default=None,
        description="Azure AD tenant ID.",
    )
    AZURE_CLIENT_ID: Optional[str] = Field(
        default=None,
        description="Service-principal client (application) ID.",
    )
    AZURE_CLIENT_SECRET: Optional[str] = Field(
        default=None,
        description="Service-principal client secret.",
    )

    # ------------------------------------------------------------------
    # Container / layer names
    # ------------------------------------------------------------------
    RAW_CONTAINER: str = Field(default="raw", description="Raw landing container name.")
    BRONZE_CONTAINER: str = Field(default="bronze", description="Bronze layer container name.")
    SILVER_CONTAINER: str = Field(default="silver", description="Silver layer container name.")
    GOLD_CONTAINER: str = Field(default="gold", description="Gold layer container name.")

    # ------------------------------------------------------------------
    # Local paths
    # ------------------------------------------------------------------
    LOCAL_DATA_PATH: Path = Field(
        default=_REPO_ROOT / "data" / "raw",
        description="Filesystem path to the raw CSV data files.",
    )
    LOCAL_BRONZE_PATH: Path = Field(
        default=_REPO_ROOT / "data" / "bronze",
        description="Filesystem path for locally written bronze parquet files.",
    )
    LOCAL_SILVER_PATH: Path = Field(
        default=_REPO_ROOT / "data" / "silver",
        description="Filesystem path for locally written silver parquet files.",
    )
    LOCAL_GOLD_PATH: Path = Field(
        default=_REPO_ROOT / "data" / "gold",
        description="Filesystem path for locally written gold parquet files.",
    )

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Loguru log level (TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )

    # ------------------------------------------------------------------
    # Pipeline behaviour
    # ------------------------------------------------------------------
    BATCH_SIZE: int = Field(
        default=10_000,
        description="Row batch size used when streaming data to ADLS.",
    )
    MAX_VALIDATION_ERRORS: int = Field(
        default=100,
        description="Maximum number of schema validation errors to capture before short-circuiting.",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton Settings instance."""
    return Settings()
