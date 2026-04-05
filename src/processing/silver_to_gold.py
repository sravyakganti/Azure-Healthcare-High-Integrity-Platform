"""
processing/silver_to_gold.py
-----------------------------
Gold layer: builds analytics-ready, aggregated tables from the Silver datasets.
"""

from __future__ import annotations

import sys
from pathlib import Path

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

def _payment_rate_or_null(billed: float, paid: float):
    """
    COALESCE equivalent for payment rate.
    Returns NULL (None) when:
      - billed is 0 / NaN  → rate is undefined
      - paid is 0 but billed > 0 → claim is Pending; 0 would be misleading
    Returns the ratio only when both values are positive.
    """
    if pd.isna(billed) or billed <= 0:
        return None
    if pd.isna(paid) or paid == 0:
        return None  # Pending — show as NULL, not 0
    return paid / billed


def _load_silver(silver_path: Path, dataset_name: str) -> pd.DataFrame:
    """Load a silver parquet file for the given dataset."""
    parquet_file = silver_path / dataset_name / f"{dataset_name}.parquet"
    if not parquet_file.exists():
        raise FileNotFoundError(f"Silver parquet not found: {parquet_file}")
    df = pd.read_parquet(parquet_file, engine="pyarrow")
    logger.info(f"[{dataset_name}] Loaded {len(df):,} silver rows.")
    return df


# ---------------------------------------------------------------------------
# Gold transformer
# ---------------------------------------------------------------------------

class GoldTransformer:
    """Builds Gold-layer aggregated tables from Silver DataFrames."""

    # ------------------------------------------------------------------
    # Patient 360
    # ------------------------------------------------------------------

    def build_patient_360(
        self,
        patients_df: pd.DataFrame,
        encounters_df: pd.DataFrame,
        claims_df: pd.DataFrame,
        lab_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Build a single row per patient with visit, financial, and lab metrics.

        Metrics included
        ----------------
        * visit_count, last_visit_date, departments_visited
        * total_billed, total_paid, overall_payment_rate, claim_count, denied_count
        * lab_count, abnormal_lab_count
        """
        logger.info("[patient_360] Building Patient 360 table …")

        # --- Encounter aggregation ---
        enc_agg = (
            encounters_df
            .groupby("patient_id", as_index=False)
            .agg(
                visit_count=("encounter_id", "count"),
                last_visit_date=("visit_date", "max"),
                departments_visited=("department", lambda s: ", ".join(sorted(s.dropna().unique()))),
                readmission_count=("is_readmitted", "sum"),
            )
        )

        # --- Claims aggregation ---
        claims_numeric = claims_df.copy()
        claims_numeric["billed_amount"] = pd.to_numeric(
            claims_numeric["billed_amount"], errors="coerce"
        )
        claims_numeric["paid_amount"] = pd.to_numeric(
            claims_numeric["paid_amount"], errors="coerce"
        )

        claims_agg = (
            claims_numeric
            .groupby("patient_id", as_index=False)
            .agg(
                claim_count=("billing_id", "count"),
                total_billed=("billed_amount", "sum"),
                total_paid=("paid_amount", "sum"),
                denied_count=("is_denied", "sum"),
            )
        )
        # COALESCE: NULL when total_paid == 0 (Pending), not a misleading 0.0
        claims_agg["overall_payment_rate"] = claims_agg.apply(
            lambda r: _payment_rate_or_null(r["total_billed"], r["total_paid"]),
            axis=1,
        )

        # --- Lab aggregation (via encounters) ---
        # Merge lab tests with encounters to get patient_id
        lab_with_patient = lab_df.merge(
            encounters_df[["encounter_id", "patient_id"]],
            on="encounter_id",
            how="left",
        )
        lab_agg = (
            lab_with_patient
            .groupby("patient_id", as_index=False)
            .agg(
                lab_count=("lab_id", "count"),
                abnormal_lab_count=("is_abnormal", "sum"),
            )
        )

        # --- Join everything onto patients ---
        gold = (
            patients_df
            .merge(enc_agg, on="patient_id", how="left")
            .merge(claims_agg, on="patient_id", how="left")
            .merge(lab_agg, on="patient_id", how="left")
        )

        # Fill numeric NaNs from patients with no encounters/claims/labs
        for col in [
            "visit_count", "readmission_count",
            "claim_count", "denied_count",
            "lab_count", "abnormal_lab_count",
        ]:
            gold[col] = gold[col].fillna(0).astype(int)

        # total_billed/paid default to 0 for patients with no claims;
        # overall_payment_rate stays NULL (not filled) — NULL = Pending in Synapse
        for col in ["total_billed", "total_paid"]:
            gold[col] = gold[col].fillna(0.0)

        # is_anomaly: billed > 0 but nothing paid — flag for investigation
        gold["is_anomaly"] = (gold["total_billed"] > 0) & (gold["total_paid"] == 0)

        gold["gold_processed_at"] = pd.Timestamp.utcnow()
        logger.success(f"[patient_360] Built {len(gold):,} rows.")
        return gold

    # ------------------------------------------------------------------
    # Encounter summary
    # ------------------------------------------------------------------

    def build_encounter_summary(
        self,
        encounters_df: pd.DataFrame,
        claims_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Encounter-level summary with financial data attached.

        Returns one row per encounter_id enriched with claims fields.
        """
        logger.info("[encounter_summary] Building Encounter Summary table …")

        claims_numeric = claims_df.copy()
        claims_numeric["billed_amount"] = pd.to_numeric(
            claims_numeric["billed_amount"], errors="coerce"
        )
        claims_numeric["paid_amount"] = pd.to_numeric(
            claims_numeric["paid_amount"], errors="coerce"
        )

        # Aggregate claims per encounter (there may be multiple claims per encounter)
        claims_enc_agg = (
            claims_numeric
            .groupby("encounter_id", as_index=False)
            .agg(
                total_billed=("billed_amount", "sum"),
                total_paid=("paid_amount", "sum"),
                claim_count=("billing_id", "count"),
                primary_claim_status=("claim_status", lambda s: s.mode().iloc[0] if len(s) > 0 else "Unknown"),
                denial_count=("is_denied", "sum"),
            )
        )
        # COALESCE: NULL when total_paid == 0 (Pending), not a misleading 0.0
        claims_enc_agg["payment_rate"] = claims_enc_agg.apply(
            lambda r: _payment_rate_or_null(r["total_billed"], r["total_paid"]),
            axis=1,
        )

        summary = encounters_df.merge(claims_enc_agg, on="encounter_id", how="left")

        # total_billed/paid default to 0 for encounters with no claims;
        # payment_rate stays NULL — NULL = Pending in Synapse
        for col in ["total_billed", "total_paid"]:
            summary[col] = summary[col].fillna(0.0)
        for col in ["claim_count", "denial_count"]:
            summary[col] = summary[col].fillna(0).astype(int)

        # is_anomaly: billed > 0 but nothing paid
        summary["is_anomaly"] = (summary["total_billed"] > 0) & (summary["total_paid"] == 0)

        summary["gold_processed_at"] = pd.Timestamp.utcnow()
        logger.success(f"[encounter_summary] Built {len(summary):,} rows.")
        return summary

    # ------------------------------------------------------------------
    # Department metrics
    # ------------------------------------------------------------------

    def build_department_metrics(
        self,
        encounters_df: pd.DataFrame,
        claims_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Department-level aggregation:
        visit_count, avg_length_of_stay, total_billed, total_paid,
        readmission_rate.
        """
        logger.info("[department_metrics] Building Department Metrics table …")

        # Get financial totals per encounter first
        claims_numeric = claims_df.copy()
        claims_numeric["billed_amount"] = pd.to_numeric(
            claims_numeric["billed_amount"], errors="coerce"
        )
        claims_numeric["paid_amount"] = pd.to_numeric(
            claims_numeric["paid_amount"], errors="coerce"
        )

        enc_claims = (
            claims_numeric
            .groupby("encounter_id", as_index=False)
            .agg(total_billed=("billed_amount", "sum"), total_paid=("paid_amount", "sum"))
        )

        encounters_with_fin = encounters_df.merge(enc_claims, on="encounter_id", how="left")
        encounters_with_fin["total_billed"] = encounters_with_fin["total_billed"].fillna(0.0)
        encounters_with_fin["total_paid"] = encounters_with_fin["total_paid"].fillna(0.0)
        encounters_with_fin["length_of_stay"] = pd.to_numeric(
            encounters_with_fin["length_of_stay"], errors="coerce"
        )

        dept_metrics = (
            encounters_with_fin
            .groupby("department", as_index=False)
            .agg(
                visit_count=("encounter_id", "count"),
                avg_length_of_stay=("length_of_stay", "mean"),
                total_billed=("total_billed", "sum"),
                total_paid=("total_paid", "sum"),
                readmission_count=("is_readmitted", "sum"),
            )
        )

        dept_metrics["readmission_rate"] = (
            dept_metrics["readmission_count"] / dept_metrics["visit_count"]
        ).fillna(0.0)

        dept_metrics["avg_length_of_stay"] = dept_metrics["avg_length_of_stay"].fillna(0.0)
        dept_metrics["gold_processed_at"] = pd.Timestamp.utcnow()

        logger.success(f"[department_metrics] Built {len(dept_metrics):,} department rows.")
        return dept_metrics

    # ------------------------------------------------------------------
    # Claims analytics
    # ------------------------------------------------------------------

    def build_claims_analytics(self, claims_df: pd.DataFrame) -> pd.DataFrame:
        """
        Insurance / claims analytics:
        group by insurance_provider × claim_status with counts and financials,
        plus a denial_rate per provider.
        """
        logger.info("[claims_analytics] Building Claims Analytics table …")

        df = claims_df.copy()
        df["billed_amount"] = pd.to_numeric(df["billed_amount"], errors="coerce")
        df["paid_amount"] = pd.to_numeric(df["paid_amount"], errors="coerce")
        df["payment_rate"] = pd.to_numeric(df["payment_rate"], errors="coerce")
        df["is_denied"] = df["is_denied"].astype(bool)

        # Provider × status breakdown
        provider_status = (
            df
            .groupby(["insurance_provider", "claim_status"], as_index=False)
            .agg(
                claim_count=("billing_id", "count"),
                total_billed=("billed_amount", "sum"),
                total_paid=("paid_amount", "sum"),
                avg_payment_rate=("payment_rate", "mean"),
            )
        )
        # COALESCE: if the group paid nothing, avg_payment_rate → NULL (Pending)
        provider_status["avg_payment_rate"] = provider_status.apply(
            lambda r: _payment_rate_or_null(r["total_billed"], r["total_paid"]),
            axis=1,
        )

        # Provider-level denial rate
        provider_totals = (
            df
            .groupby("insurance_provider", as_index=False)
            .agg(
                total_claims=("billing_id", "count"),
                denied_claims=("is_denied", "sum"),
            )
        )
        provider_totals["denial_rate"] = (
            provider_totals["denied_claims"] / provider_totals["total_claims"]
        ).fillna(0.0)

        analytics = provider_status.merge(
            provider_totals[["insurance_provider", "denial_rate"]],
            on="insurance_provider",
            how="left",
        )
        analytics["gold_processed_at"] = pd.Timestamp.utcnow()

        logger.success(f"[claims_analytics] Built {len(analytics):,} rows.")
        return analytics

    # ------------------------------------------------------------------
    # Pipeline runner
    # ------------------------------------------------------------------

    def run(self, silver_path: Path, gold_path: Path) -> dict:
        """
        Load all four silver datasets, build all gold tables, and persist them.

        Parameters
        ----------
        silver_path:
            Root silver directory (e.g. ``data/silver``).
        gold_path:
            Root gold directory (e.g. ``data/gold``).

        Returns
        -------
        dict
            Summary with row counts per gold table.
        """
        logger.info("===== Silver → Gold transformation started =====")
        gold_path.mkdir(parents=True, exist_ok=True)

        # Load silver datasets
        patients_df = _load_silver(silver_path, "patients")
        encounters_df = _load_silver(silver_path, "encounters")
        claims_df = _load_silver(silver_path, "claims")
        lab_df = _load_silver(silver_path, "lab_tests")

        gold_tables = {
            "patient_360": self.build_patient_360(
                patients_df, encounters_df, claims_df, lab_df
            ),
            "encounter_summary": self.build_encounter_summary(
                encounters_df, claims_df
            ),
            "department_metrics": self.build_department_metrics(
                encounters_df, claims_df
            ),
            "claims_analytics": self.build_claims_analytics(claims_df),
        }

        summary = {}
        for table_name, df_gold in gold_tables.items():
            try:
                table_dir = gold_path / table_name
                table_dir.mkdir(parents=True, exist_ok=True)
                output_file = table_dir / f"{table_name}.parquet"
                df_gold.to_parquet(output_file, index=False, engine="pyarrow")

                size_mb = output_file.stat().st_size / (1024 * 1024)
                logger.success(
                    f"[{table_name}] Written to {output_file} "
                    f"({len(df_gold):,} rows, {size_mb:.2f} MB)"
                )
                summary[table_name] = {
                    "status": "success",
                    "row_count": len(df_gold),
                    "output_file": str(output_file),
                }
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[{table_name}] Failed: {exc}")
                summary[table_name] = {"status": "error", "error": str(exc)}

        logger.info("===== Silver → Gold transformation complete =====")
        return summary


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    _repo_root = Path(__file__).resolve().parents[2]
    silver_path = _repo_root / "data" / "silver"
    gold_path = _repo_root / "data" / "gold"

    transformer = GoldTransformer()
    report = transformer.run(silver_path, gold_path)

    print("\n" + "=" * 60)
    print("SILVER → GOLD SUMMARY")
    print("=" * 60)
    print(json.dumps(report, indent=2, default=str))
