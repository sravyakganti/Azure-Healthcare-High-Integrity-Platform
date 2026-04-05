"""
processing/data_quality.py
---------------------------
Data quality checks applied to the Silver layer datasets.
Produces a structured JSON report covering completeness, referential integrity,
duplicates, and date validity.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from loguru import logger

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
# Helper
# ---------------------------------------------------------------------------

def _load_silver(silver_path: Path, dataset_name: str) -> pd.DataFrame:
    parquet_file = silver_path / dataset_name / f"{dataset_name}.parquet"
    if not parquet_file.exists():
        raise FileNotFoundError(f"Silver parquet not found: {parquet_file}")
    df = pd.read_parquet(parquet_file, engine="pyarrow")
    logger.info(f"[{dataset_name}] Loaded {len(df):,} silver rows for DQ checks.")
    return df


# ---------------------------------------------------------------------------
# Data quality checker
# ---------------------------------------------------------------------------

class DataQualityChecker:
    """Runs a suite of data-quality checks against Silver-layer DataFrames."""

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def check_completeness(
        self,
        df: pd.DataFrame,
        required_cols: List[str],
    ) -> Dict[str, Any]:
        """
        Compute the percentage of non-null values for each required column.

        Returns
        -------
        dict
            ``{column_name: {"non_null_count": int, "null_count": int, "completeness_pct": float}}``
        """
        result: Dict[str, Any] = {}
        total_rows = len(df)

        for col in required_cols:
            if col not in df.columns:
                result[col] = {
                    "non_null_count": 0,
                    "null_count": total_rows,
                    "completeness_pct": 0.0,
                    "note": "column_missing",
                }
                continue

            non_null = int(df[col].notna().sum())
            null_count = total_rows - non_null
            pct = round(non_null / total_rows * 100, 2) if total_rows > 0 else 0.0
            result[col] = {
                "non_null_count": non_null,
                "null_count": null_count,
                "completeness_pct": pct,
            }

        return result

    def check_referential_integrity(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        key1: str,
        key2: str,
    ) -> Dict[str, Any]:
        """
        Check what percentage of ``df1[key1]`` values exist in ``df2[key2]``.

        Returns
        -------
        dict
            ``{"total_keys": int, "matched_keys": int, "unmatched_keys": int,
               "integrity_pct": float}``
        """
        ref_set = set(df2[key2].dropna().unique())
        total = len(df1)
        if total == 0:
            return {
                "total_keys": 0,
                "matched_keys": 0,
                "unmatched_keys": 0,
                "integrity_pct": 100.0,
            }

        matched = int(df1[key1].isin(ref_set).sum())
        unmatched = total - matched
        pct = round(matched / total * 100, 2)

        return {
            "total_keys": total,
            "matched_keys": matched,
            "unmatched_keys": unmatched,
            "integrity_pct": pct,
        }

    def check_duplicates(
        self,
        df: pd.DataFrame,
        key_col: str,
    ) -> Dict[str, Any]:
        """
        Count duplicate values in *key_col*.

        Returns
        -------
        dict
            ``{"total_rows": int, "duplicate_rows": int, "duplicate_pct": float,
               "unique_keys": int}``
        """
        total = len(df)
        if key_col not in df.columns:
            return {
                "total_rows": total,
                "duplicate_rows": 0,
                "duplicate_pct": 0.0,
                "unique_keys": 0,
                "note": "column_missing",
            }

        unique_keys = int(df[key_col].nunique())
        duplicate_rows = int(df[key_col].duplicated(keep=False).sum())
        pct = round(duplicate_rows / total * 100, 2) if total > 0 else 0.0

        return {
            "total_rows": total,
            "duplicate_rows": duplicate_rows,
            "duplicate_pct": pct,
            "unique_keys": unique_keys,
        }

    def check_date_validity(
        self,
        df: pd.DataFrame,
        date_col: str,
    ) -> Dict[str, Any]:
        """
        Compute the percentage of non-NaT values in a datetime column.

        Returns
        -------
        dict
            ``{"total_rows": int, "valid_dates": int, "invalid_dates": int,
               "validity_pct": float}``
        """
        total = len(df)
        if date_col not in df.columns:
            return {
                "total_rows": total,
                "valid_dates": 0,
                "invalid_dates": total,
                "validity_pct": 0.0,
                "note": "column_missing",
            }

        col = pd.to_datetime(df[date_col], errors="coerce")
        valid = int(col.notna().sum())
        invalid = total - valid
        pct = round(valid / total * 100, 2) if total > 0 else 0.0

        return {
            "total_rows": total,
            "valid_dates": valid,
            "invalid_dates": invalid,
            "validity_pct": pct,
        }

    # ------------------------------------------------------------------
    # Full report
    # ------------------------------------------------------------------

    def generate_report(self, silver_path: Path) -> Dict[str, Any]:
        """
        Run all DQ checks on all four silver datasets and return a structured
        report dictionary.

        Parameters
        ----------
        silver_path:
            Root silver directory containing per-dataset subdirectories.

        Returns
        -------
        dict
            Nested dict keyed by dataset name, then check name.
        """
        logger.info("===== Data Quality Report generation started =====")

        # Load silver datasets
        patients_df = _load_silver(silver_path, "patients")
        encounters_df = _load_silver(silver_path, "encounters")
        lab_df = _load_silver(silver_path, "lab_tests")
        claims_df = _load_silver(silver_path, "claims")

        report: Dict[str, Any] = {
            "generated_at": pd.Timestamp.utcnow().isoformat(),
            "silver_path": str(silver_path),
            "datasets": {},
        }

        # ------------------------------------------------------------------
        # Patients
        # ------------------------------------------------------------------
        logger.info("Running DQ checks on: patients")
        report["datasets"]["patients"] = {
            "row_count": len(patients_df),
            "completeness": self.check_completeness(
                patients_df,
                ["patient_id", "first_name", "last_name", "dob", "age", "gender", "registration_date"],
            ),
            "duplicates": self.check_duplicates(patients_df, "patient_id"),
            "date_validity": {
                "dob": self.check_date_validity(patients_df, "dob"),
                "registration_date": self.check_date_validity(patients_df, "registration_date"),
            },
        }

        # ------------------------------------------------------------------
        # Encounters
        # ------------------------------------------------------------------
        logger.info("Running DQ checks on: encounters")
        report["datasets"]["encounters"] = {
            "row_count": len(encounters_df),
            "completeness": self.check_completeness(
                encounters_df,
                ["encounter_id", "patient_id", "visit_date", "visit_type", "department", "status"],
            ),
            "duplicates": self.check_duplicates(encounters_df, "encounter_id"),
            "date_validity": {
                "visit_date": self.check_date_validity(encounters_df, "visit_date"),
                "discharge_date": self.check_date_validity(encounters_df, "discharge_date"),
            },
            "referential_integrity": {
                "patient_id_in_patients": self.check_referential_integrity(
                    encounters_df, patients_df, "patient_id", "patient_id"
                ),
            },
        }

        # ------------------------------------------------------------------
        # Lab Tests
        # ------------------------------------------------------------------
        logger.info("Running DQ checks on: lab_tests")
        report["datasets"]["lab_tests"] = {
            "row_count": len(lab_df),
            "completeness": self.check_completeness(
                lab_df,
                ["lab_id", "encounter_id", "test_name", "test_date", "status"],
            ),
            "duplicates": self.check_duplicates(lab_df, "lab_id"),
            "date_validity": {
                "test_date": self.check_date_validity(lab_df, "test_date"),
            },
            "referential_integrity": {
                "encounter_id_in_encounters": self.check_referential_integrity(
                    lab_df, encounters_df, "encounter_id", "encounter_id"
                ),
            },
        }

        # ------------------------------------------------------------------
        # Claims
        # ------------------------------------------------------------------
        logger.info("Running DQ checks on: claims")
        report["datasets"]["claims"] = {
            "row_count": len(claims_df),
            "completeness": self.check_completeness(
                claims_df,
                ["billing_id", "patient_id", "encounter_id", "billed_amount", "paid_amount", "claim_status"],
            ),
            "duplicates": self.check_duplicates(claims_df, "billing_id"),
            "date_validity": {
                "claim_billing_date": self.check_date_validity(claims_df, "claim_billing_date"),
            },
            "referential_integrity": {
                "patient_id_in_patients": self.check_referential_integrity(
                    claims_df, patients_df, "patient_id", "patient_id"
                ),
                "encounter_id_in_encounters": self.check_referential_integrity(
                    claims_df, encounters_df, "encounter_id", "encounter_id"
                ),
            },
        }

        # ------------------------------------------------------------------
        # Overall health score (simple average of first-level completeness)
        # ------------------------------------------------------------------
        all_completeness_pcts: List[float] = []
        for ds_data in report["datasets"].values():
            for col_stats in ds_data.get("completeness", {}).values():
                if "completeness_pct" in col_stats:
                    all_completeness_pcts.append(col_stats["completeness_pct"])

        report["overall_completeness_pct"] = (
            round(sum(all_completeness_pcts) / len(all_completeness_pcts), 2)
            if all_completeness_pcts
            else 0.0
        )

        logger.success(
            f"DQ Report complete | "
            f"overall_completeness={report['overall_completeness_pct']}%"
        )
        return report

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_report(self, report: Dict[str, Any], output_path: Path) -> Path:
        """
        Save the DQ report as a JSON file.

        Parameters
        ----------
        report:
            Report dict returned by :meth:`generate_report`.
        output_path:
            Directory where the JSON file will be written.

        Returns
        -------
        Path
            Full path of the written JSON file.
        """
        output_path.mkdir(parents=True, exist_ok=True)
        timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
        report_file = output_path / f"dq_report_{timestamp}.json"

        with open(report_file, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)

        logger.success(f"DQ report saved to {report_file}")
        return report_file


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    _repo_root = Path(__file__).resolve().parents[2]
    silver_path = _repo_root / "data" / "silver"
    reports_path = _repo_root / "pipeline" / "reports"

    checker = DataQualityChecker()
    report = checker.generate_report(silver_path)
    report_file = checker.save_report(report, reports_path)

    print("\n" + "=" * 60)
    print("DATA QUALITY REPORT")
    print("=" * 60)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nReport saved to: {report_file}")
