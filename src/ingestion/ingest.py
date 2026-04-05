"""
ingestion/ingest.py
-------------------
Main data ingestion pipeline: reads raw CSVs, validates them, and writes
Parquet files to the Bronze layer (locally or to Azure Data Lake Gen2).
"""

from __future__ import annotations

import io
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from loguru import logger
from pandera import DataFrameSchema

from config import DATA_FILES, Settings, get_settings
from validators import (
    ClaimsSchema,
    EncounterSchema,
    LabTestSchema,
    PatientSchema,
    ValidationResult,
    validate_dataframe,
)


# ---------------------------------------------------------------------------
# Schema registry — maps dataset name → pandera schema
# ---------------------------------------------------------------------------
SCHEMA_REGISTRY: Dict[str, DataFrameSchema] = {
    "patients": PatientSchema,
    "encounters": EncounterSchema,
    "lab_tests": LabTestSchema,
    "claims": ClaimsSchema,
}


# ---------------------------------------------------------------------------
# Pipeline class
# ---------------------------------------------------------------------------
class DataIngestionPipeline:
    """
    Orchestrates reading, validating, and writing each healthcare dataset
    to the Bronze layer.
    """

    def __init__(self, settings: Settings, local_mode: bool = True) -> None:
        self.settings = settings
        self.local_mode = local_mode

        # Configure loguru
        logger.remove()
        logger.add(
            sys.stderr,
            level=settings.LOG_LEVEL,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "{message}"
            ),
        )
        logger.add(
            Path(__file__).resolve().parents[2] / "logs" / "ingestion.log",
            level="DEBUG",
            rotation="10 MB",
            retention="7 days",
            compression="zip",
        )

        logger.info(
            f"DataIngestionPipeline initialised | local_mode={local_mode} | "
            f"data_path={settings.LOCAL_DATA_PATH}"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_blob_client(self, container: str, path: str):  # type: ignore[return]
        """Return an Azure BlobClient for the given container + path."""
        from azure.storage.blob import BlobServiceClient  # lazy import

        account_name = self.settings.AZURE_STORAGE_ACCOUNT_NAME
        account_key = self.settings.AZURE_STORAGE_ACCOUNT_KEY

        if not account_name:
            raise ValueError("AZURE_STORAGE_ACCOUNT_NAME is not configured.")

        if account_key:
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={account_name};"
                f"AccountKey={account_key};"
                f"EndpointSuffix=core.windows.net"
            )
            service = BlobServiceClient.from_connection_string(connection_string)
        else:
            # Fall back to DefaultAzureCredential (managed identity / SP env vars)
            from azure.identity import DefaultAzureCredential  # lazy import

            account_url = f"https://{account_name}.blob.core.windows.net"
            service = BlobServiceClient(
                account_url=account_url,
                credential=DefaultAzureCredential(),
            )

        return service.get_blob_client(container=container, blob=path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read_csv(self, filepath: Path) -> pd.DataFrame:
        """
        Read a CSV file into a DataFrame.

        Parameters
        ----------
        filepath:
            Absolute path to the CSV file.

        Returns
        -------
        pd.DataFrame
            Loaded DataFrame (all columns as object dtype initially).
        """
        logger.info(f"Reading CSV: {filepath}")
        if not filepath.exists():
            raise FileNotFoundError(f"CSV file not found: {filepath}")

        try:
            df = pd.read_csv(
                filepath,
                dtype=str,        # read everything as string first — let validators coerce
                keep_default_na=True,
                low_memory=False,
            )
            logger.success(f"Read {len(df):,} rows × {len(df.columns)} columns from {filepath.name}")
            return df
        except Exception as exc:
            logger.error(f"Failed to read {filepath}: {exc}")
            raise

    def validate_data(
        self,
        df: pd.DataFrame,
        schema: DataFrameSchema,
        schema_name: str,
    ) -> ValidationResult:
        """
        Run pandera validation and log the outcome.

        Parameters
        ----------
        df:
            DataFrame to validate.
        schema:
            Pandera DataFrameSchema to apply.
        schema_name:
            Human-readable label used in log messages.

        Returns
        -------
        ValidationResult
        """
        result = validate_dataframe(df, schema, schema_name)

        if result.is_valid:
            logger.success(
                f"[{schema_name}] Validation PASSED — "
                f"{result.valid_row_count:,}/{result.row_count:,} rows valid."
            )
        else:
            logger.warning(
                f"[{schema_name}] Validation PARTIAL — "
                f"{result.valid_row_count:,}/{result.row_count:,} rows valid | "
                f"{len(result.errors)} error(s) detected."
            )
            for err in result.errors[:10]:
                logger.debug(f"  -> {err}")

        return result

    def save_to_bronze_local(self, df: pd.DataFrame, dataset_name: str) -> Path:
        """
        Persist a DataFrame as Parquet under
        ``data/bronze/{dataset_name}/ingestion_date={today}/{dataset_name}.parquet``.

        Parameters
        ----------
        df:
            DataFrame to persist.
        dataset_name:
            Logical name of the dataset (e.g. "patients").

        Returns
        -------
        Path
            Full path of the written Parquet file.
        """
        today_str = date.today().isoformat()
        partition_dir = (
            self.settings.LOCAL_BRONZE_PATH
            / dataset_name
            / f"ingestion_date={today_str}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_path = partition_dir / f"{dataset_name}.parquet"
        df.to_parquet(output_path, index=False, engine="pyarrow")

        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.success(
            f"[Bronze] Saved {len(df):,} rows to {output_path} ({size_mb:.2f} MB)"
        )
        return output_path

    def save_to_adls(
        self,
        df: pd.DataFrame,
        container: str,
        path: str,
    ) -> None:
        """
        Serialise *df* as Parquet bytes and upload to Azure Blob / ADLS Gen2.

        Parameters
        ----------
        df:
            DataFrame to upload.
        container:
            Target container name (e.g. "bronze").
        path:
            Blob path within the container
            (e.g. "patients/ingestion_date=2025-01-01/patients.parquet").
        """
        logger.info(f"[ADLS] Uploading to {container}/{path} …")
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            blob_client = self._get_blob_client(container, path)
            blob_client.upload_blob(buffer, overwrite=True)

            logger.success(
                f"[ADLS] Uploaded {len(df):,} rows to "
                f"abfss://{container}@.../{path}"
            )
        except Exception as exc:
            logger.error(f"[ADLS] Upload failed for {container}/{path}: {exc}")
            raise

    def run_dataset(
        self,
        dataset_name: str,
        filename: str,
        schema: DataFrameSchema,
    ) -> Dict[str, Any]:
        """
        Execute the full ingestion pipeline for a single dataset:
        read → validate → write to bronze.

        Parameters
        ----------
        dataset_name:
            Logical name (key in DATA_FILES).
        filename:
            CSV filename inside LOCAL_DATA_PATH.
        schema:
            Pandera schema for validation.

        Returns
        -------
        dict
            Summary containing row counts and validation results.
        """
        logger.info(f"===== Starting ingestion for dataset: {dataset_name} =====")
        filepath = self.settings.LOCAL_DATA_PATH / filename

        # 1. Read
        df = self.read_csv(filepath)

        # 2. Validate
        validation_result = self.validate_data(df, schema, dataset_name)

        # 3. Persist
        if self.local_mode:
            output_path = self.save_to_bronze_local(df, dataset_name)
            destination = str(output_path)
        else:
            today_str = date.today().isoformat()
            blob_path = (
                f"{dataset_name}/ingestion_date={today_str}/{dataset_name}.parquet"
            )
            self.save_to_adls(df, self.settings.BRONZE_CONTAINER, blob_path)
            destination = blob_path

        return {
            "dataset_name": dataset_name,
            "source_file": filename,
            "destination": destination,
            "row_count": validation_result.row_count,
            "valid_row_count": validation_result.valid_row_count,
            "is_valid": validation_result.is_valid,
            "validation_errors": validation_result.errors[:20],
            "error_rate": validation_result.error_rate,
        }

    def run(self) -> Dict[str, Any]:
        """
        Ingest all four datasets and return an aggregate summary.

        Returns
        -------
        dict
            Keys: "datasets" (list of per-dataset summaries), "total_rows",
            "total_valid_rows", "pipeline_status".
        """
        logger.info("==============================")
        logger.info(" Healthcare Data Ingestion Run ")
        logger.info("==============================")

        summaries = []
        for dataset_name, filename in DATA_FILES.items():
            schema = SCHEMA_REGISTRY[dataset_name]
            try:
                summary = self.run_dataset(dataset_name, filename, schema)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Dataset '{dataset_name}' failed: {exc}")
                summary = {
                    "dataset_name": dataset_name,
                    "source_file": filename,
                    "destination": None,
                    "row_count": 0,
                    "valid_row_count": 0,
                    "is_valid": False,
                    "validation_errors": [str(exc)],
                    "error_rate": 1.0,
                }
            summaries.append(summary)

        total_rows = sum(s["row_count"] for s in summaries)
        total_valid = sum(s["valid_row_count"] for s in summaries)
        all_valid = all(s["is_valid"] for s in summaries)

        report: Dict[str, Any] = {
            "datasets": summaries,
            "total_rows": total_rows,
            "total_valid_rows": total_valid,
            "pipeline_status": "SUCCESS" if all_valid else "PARTIAL_SUCCESS",
        }

        logger.info(
            f"Ingestion complete | total_rows={total_rows:,} | "
            f"valid_rows={total_valid:,} | status={report['pipeline_status']}"
        )
        return report


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    settings = get_settings()

    # Ensure log directory exists
    (Path(__file__).resolve().parents[2] / "logs").mkdir(parents=True, exist_ok=True)

    pipeline = DataIngestionPipeline(settings=settings, local_mode=True)
    report = pipeline.run()

    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)
    print(json.dumps(report, indent=2, default=str))
