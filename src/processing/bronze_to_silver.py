"""
processing/bronze_to_silver.py
-------------------------------
Silver layer transformations: clean, standardise, and enrich each bronze
dataset before writing Parquet to the Silver layer.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger


def _sha256(value: Optional[str]) -> str:
    """Return the SHA-256 hex digest of *value*, or an empty string for nulls."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return hashlib.sha256(str(value).strip().encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{line}</cyan> | {message}"
    ),
)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _parse_date(series: pd.Series, fmt: str = "%d-%m-%Y") -> pd.Series:
    """Parse a string Series to datetime using *fmt*; coerce errors to NaT."""
    return pd.to_datetime(series, format=fmt, errors="coerce")


def _parse_datetime(series: pd.Series, fmt: str = "%d-%m-%Y %H:%M") -> pd.Series:
    """Parse a string Series to datetime using *fmt*; coerce errors to NaT."""
    return pd.to_datetime(series, format=fmt, errors="coerce")


def _find_latest_parquet(base_path: Path, dataset_name: str) -> Path:
    """
    Under ``base_path/dataset_name/ingestion_date=*/``, return the path of the
    most-recently written Parquet partition directory (or the file itself).
    """
    pattern = list(base_path.glob(f"{dataset_name}/ingestion_date=*/{dataset_name}.parquet"))
    if not pattern:
        # Fallback: look for any .parquet inside the dataset directory
        pattern = list(base_path.glob(f"{dataset_name}/**/*.parquet"))
    if not pattern:
        raise FileNotFoundError(
            f"No parquet files found for dataset '{dataset_name}' under {base_path}"
        )
    # Sort by modification time — most recent first
    pattern.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return pattern[0]


# ---------------------------------------------------------------------------
# Silver transformer
# ---------------------------------------------------------------------------

class SilverTransformer:
    """Applies Silver-layer cleaning and enrichment to each bronze dataset."""

    # ------------------------------------------------------------------
    # Patients
    # ------------------------------------------------------------------

    def transform_patients(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and enrich the patients dataset.

        Steps
        -----
        * dob: parse DD-MM-YYYY string → Python date (parquet date32 = YYYY-MM-DD).
          Equivalent of Spark: from_unixtime(col('dob')/1000).cast('date').
        * SHA-256 hash all PII: first_name, last_name, full_name, address, email, phone.
        * DROP raw PII + registration_date entirely — Silver must be PII-free.
          Equivalent of Spark: .drop('first_name','last_name','full_name',
                                     'address','email','phone','registration_date').
        * to_parquet() always overwrites the destination file (equivalent of .mode('overwrite')).
        """
        df = df.copy()
        logger.info(f"[patients] Transforming {len(df):,} rows …")

        # --- dob: parse to Python date → stored as parquet date32 (YYYY-MM-DD) ---
        # Handles both "DD-MM-YYYY" strings and epoch-ms integers defensively.
        dob_parsed = pd.to_datetime(df["dob"], format="%d-%m-%Y", errors="coerce")
        if dob_parsed.isna().all():
            # Fallback: treat as epoch milliseconds (matches Spark from_unixtime logic)
            dob_parsed = pd.to_datetime(
                pd.to_numeric(df["dob"], errors="coerce"), unit="ms", errors="coerce"
            )
        df["dob"] = dob_parsed.dt.date  # date32 in parquet — no more epoch integers

        # --- Gender standardisation ---
        gender_map = {
            "female": "Female", "male": "Male", "other": "Other",
            "unknown": "Unknown", "f": "Female", "m": "Male",
        }
        df["gender"] = (
            df["gender"].str.strip().str.lower().map(gender_map).fillna("Unknown")
        )

        # --- Derive full_name BEFORE hashing ---
        df["full_name"] = (
            df["first_name"].fillna("").str.strip()
            + " "
            + df["last_name"].fillna("").str.strip()
        ).str.strip()

        # --- SHA-256 hash every PII field ---
        df["first_name_hashed"] = df["first_name"].apply(_sha256)
        df["last_name_hashed"] = df["last_name"].apply(_sha256)
        df["full_name_hashed"] = df["full_name"].apply(_sha256)
        df["email_hashed"] = df["email"].apply(_sha256)
        df["phone_hashed"] = df["phone"].apply(_sha256)
        if "address" in df.columns:
            df["address_hashed"] = df["address"].apply(_sha256)

        # --- DROP all raw PII + registration_date (strict rule) ---
        drop_cols = [
            "first_name", "last_name", "full_name",
            "address", "email", "phone", "registration_date",
        ]
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])
        logger.info(f"[patients] Dropped PII/raw columns: {drop_cols}")

        # --- age_group ---
        age = pd.to_numeric(df["age"], errors="coerce")

        def _age_group(a: Optional[float]) -> str:
            if pd.isna(a):
                return "Unknown"
            if a <= 17:
                return "Pediatric"
            if a <= 64:
                return "Adult"
            return "Senior"

        df["age_group"] = age.apply(_age_group)

        # --- Silver metadata ---
        df["silver_processed_at"] = pd.Timestamp.utcnow()

        logger.success(f"[patients] Transformation complete — {len(df):,} rows.")
        return df

    # ------------------------------------------------------------------
    # Encounters
    # ------------------------------------------------------------------

    def transform_encounters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and enrich the encounters dataset.

        Steps
        -----
        * Parse visit_date and discharge_date from DD-MM-YYYY.
        * Standardise visit_type ("Inpatients" → "Inpatient").
        * Fill missing length_of_stay for ambulatory visit types.
        * Derive is_readmitted boolean.
        """
        df = df.copy()
        logger.info(f"[encounters] Transforming {len(df):,} rows …")

        # --- Date parsing ---
        df["visit_date"] = _parse_date(df["visit_date"])
        df["discharge_date"] = _parse_date(df["discharge_date"])

        # --- Visit type standardisation ---
        visit_type_map = {
            "Inpatients": "Inpatient",
            "inpatients": "Inpatient",
            "inpatient": "Inpatient",
            "outpatient": "Outpatient",
            "emergency": "Emergency",
            "telehealth": "Telehealth",
        }
        df["visit_type"] = df["visit_type"].str.strip()
        df["visit_type"] = df["visit_type"].replace(visit_type_map)

        # --- Fill missing length_of_stay ---
        ambulatory_types = {"Outpatient", "Telehealth", "Emergency"}
        missing_los = df["length_of_stay"].isna()
        is_ambulatory = df["visit_type"].isin(ambulatory_types)
        df.loc[missing_los & is_ambulatory, "length_of_stay"] = 0

        df["length_of_stay"] = pd.to_numeric(df["length_of_stay"], errors="coerce")

        # --- is_readmitted ---
        df["is_readmitted"] = df["readmitted_flag"].str.strip().str.lower() == "yes"

        # --- Silver metadata ---
        df["silver_processed_at"] = pd.Timestamp.utcnow()

        logger.success(f"[encounters] Transformation complete — {len(df):,} rows.")
        return df

    # ------------------------------------------------------------------
    # Lab Tests
    # ------------------------------------------------------------------

    def transform_lab_tests(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and enrich the lab_tests dataset.

        Steps
        -----
        * Parse test_date from DD-MM-YYYY.
        * Standardise status to Title Case.
        * Derive is_abnormal boolean.
        """
        df = df.copy()
        logger.info(f"[lab_tests] Transforming {len(df):,} rows …")

        # --- De-duplication (Phase 2: chaos simulator inserts exact-row duplicates) ---
        before_dedup = len(df)
        df = df.drop_duplicates()
        dupes_removed = before_dedup - len(df)
        if dupes_removed:
            logger.info(f"[lab_tests] Removed {dupes_removed} exact duplicate rows")

        # --- Date parsing ---
        df["test_date"] = _parse_date(df["test_date"])

        # --- Status standardisation ---
        df["status"] = df["status"].str.strip().str.title()

        # --- is_abnormal ---
        df["is_abnormal"] = (
            df["test_result"].str.strip().str.lower() == "abnormal"
        )

        # --- Silver metadata ---
        df["silver_processed_at"] = pd.Timestamp.utcnow()

        logger.success(f"[lab_tests] Transformation complete — {len(df):,} rows.")
        return df

    # ------------------------------------------------------------------
    # Claims & Billing
    # ------------------------------------------------------------------

    def transform_claims(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and enrich the claims_and_billing dataset.

        Steps
        -----
        * Parse claim_billing_date from DD-MM-YYYY HH:MM.
        * Compute payment_rate (paid / billed), guarding against division by zero.
        * Derive is_denied boolean.
        * Fill missing denial_reason.
        """
        df = df.copy()
        logger.info(f"[claims] Transforming {len(df):,} rows …")

        # --- Datetime parsing ---
        df["claim_billing_date"] = _parse_datetime(df["claim_billing_date"])

        # --- Numeric coercion ---
        df["billed_amount"] = pd.to_numeric(df["billed_amount"], errors="coerce")
        df["paid_amount"] = pd.to_numeric(df["paid_amount"], errors="coerce")

        # --- payment_rate: avoid ZeroDivisionError ---
        df["payment_rate"] = df.apply(
            lambda row: (
                row["paid_amount"] / row["billed_amount"]
                if pd.notna(row["billed_amount"]) and row["billed_amount"] > 0
                else 0.0
            ),
            axis=1,
        )

        # --- is_denied ---
        df["is_denied"] = (
            df["claim_status"].str.strip().str.lower() == "denied"
        )

        # --- Fill missing denial_reason ---
        df["denial_reason"] = df["denial_reason"].fillna("N/A").replace("", "N/A")

        # --- Silver metadata ---
        df["silver_processed_at"] = pd.Timestamp.utcnow()

        logger.success(f"[claims] Transformation complete — {len(df):,} rows.")
        return df

    # ------------------------------------------------------------------
    # Pipeline runner
    # ------------------------------------------------------------------

    def run(self, input_path: Path, output_path: Path) -> dict:
        """
        Load all four bronze datasets, transform each one, and write to Silver.

        Parameters
        ----------
        input_path:
            Root bronze directory (e.g. ``data/bronze``).
        output_path:
            Root silver directory (e.g. ``data/silver``).

        Returns
        -------
        dict
            Summary with row counts per dataset.
        """
        logger.info("===== Bronze → Silver transformation started =====")
        output_path.mkdir(parents=True, exist_ok=True)

        transformations = {
            "patients": self.transform_patients,
            "encounters": self.transform_encounters,
            "lab_tests": self.transform_lab_tests,
            "claims": self.transform_claims,
        }

        summary = {}
        for dataset_name, transform_fn in transformations.items():
            try:
                parquet_file = _find_latest_parquet(input_path, dataset_name)
                logger.info(f"Loading bronze parquet: {parquet_file}")
                df_bronze = pd.read_parquet(parquet_file, engine="pyarrow")

                df_silver = transform_fn(df_bronze)

                # Write Silver parquet (flat, no date partition — partitioned by dataset)
                silver_dir = output_path / dataset_name
                silver_dir.mkdir(parents=True, exist_ok=True)
                silver_file = silver_dir / f"{dataset_name}.parquet"
                # to_parquet always overwrites the destination (equiv. .mode('overwrite'))
                df_silver.to_parquet(silver_file, index=False, engine="pyarrow")

                size_mb = silver_file.stat().st_size / (1024 * 1024)
                logger.success(
                    f"[{dataset_name}] Written to {silver_file} "
                    f"({len(df_silver):,} rows, {size_mb:.2f} MB)"
                )
                summary[dataset_name] = {
                    "status": "success",
                    "input_rows": len(df_bronze),
                    "output_rows": len(df_silver),
                    "output_file": str(silver_file),
                }
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[{dataset_name}] Transformation failed: {exc}")
                summary[dataset_name] = {"status": "error", "error": str(exc)}

        logger.info("===== Bronze → Silver transformation complete =====")
        return summary


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    _repo_root = Path(__file__).resolve().parents[2]
    bronze_path = _repo_root / "data" / "bronze"
    silver_path = _repo_root / "data" / "silver"

    transformer = SilverTransformer()
    report = transformer.run(bronze_path, silver_path)

    print("\n" + "=" * 60)
    print("BRONZE → SILVER SUMMARY")
    print("=" * 60)
    print(json.dumps(report, indent=2, default=str))
