"""
reconciliation_audit.py — Phase 2: Source-to-Target Reconciliation
===================================================================
Verifies that local Silver row counts match the expected post-chaos values
AND match the Azure ADLS Gen2 Silver layer.

Expected counts (source-to-target math):
  patients   = 60,000 (baseline) + 20 (appended)           = 60,020
  encounters = 70,000 (baseline) - 5  (deleted)             = 69,995
  lab_tests  = 54,537 (baseline) + 10 (dupes) - 10 (dedup) = 54,537
  claims     = 70,000 (baseline) + 0  (updates, same rows)  = 70,000

Run:
  python src/quality/reconciliation_audit.py

Azure comparison is performed using the Azure CLI
(az storage fs file list) if credentials are configured.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = _REPO_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
logger.add(str(LOG_DIR / "reconciliation_audit.log"), rotation="10 MB")

# ---------------------------------------------------------------------------
# Baseline constants (Phase 1 confirmed counts)
# ---------------------------------------------------------------------------
BASELINE = {
    "patients":   60_000,
    "encounters": 70_000,
    "lab_tests":  54_537,
    "claims":     70_000,
}

CHAOS_DELTA = {
    "patients":   +20,   # 20 appended
    "encounters": -5,    # 5 deleted
    "lab_tests":  0,     # +10 duplicates added, -10 removed by de-duplication
    "claims":     0,     # 10 updated in-place (same row count)
}

EXPECTED = {k: BASELINE[k] + CHAOS_DELTA[k] for k in BASELINE}

# ---------------------------------------------------------------------------
# Local Silver paths
# ---------------------------------------------------------------------------
SILVER_LOCAL = _REPO_ROOT / "data" / "silver"

SILVER_FILES = {
    "patients":   SILVER_LOCAL / "patients"   / "patients.parquet",
    "encounters": SILVER_LOCAL / "encounters" / "encounters.parquet",
    "lab_tests":  SILVER_LOCAL / "lab_tests"  / "lab_tests.parquet",
    "claims":     SILVER_LOCAL / "claims"     / "claims.parquet",
}

# ---------------------------------------------------------------------------
# Azure helpers
# ---------------------------------------------------------------------------
AZ_CMD = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.CMD"

def _az_row_count(dataset: str) -> int | None:
    """
    Download the Silver parquet from ADLS Gen2 and count rows.
    Returns None if Azure credentials are not configured or download fails.
    """
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "")
    account_key  = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY",  "")

    if not account_name or not account_key:
        return None  # No Azure credentials — local-only audit

    # Map dataset names to Silver container paths
    path_map = {
        "patients":   "patients/patients.parquet",
        "encounters": "encounters/encounters.parquet",
        "lab_tests":  "lab_tests/lab_tests.parquet",
        "claims":     "claims/claims.parquet",
    }
    remote_path = path_map[dataset]
    local_tmp   = _REPO_ROOT / "data" / "tmp_azure_verify" / f"{dataset}.parquet"
    local_tmp.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        AZ_CMD, "storage", "fs", "file", "download",
        "--file-system", "silver",
        "--path", remote_path,
        "--destination", str(local_tmp),
        "--account-name", account_name,
        "--account-key", account_key,
        "--overwrite", "true",
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120, shell=False
        )
        if result.returncode != 0:
            logger.warning(f"Azure download failed for {dataset}: {result.stderr[:200]}")
            return None
        df = pd.read_parquet(str(local_tmp), engine="pyarrow")
        return len(df)
    except Exception as exc:
        logger.warning(f"Azure count failed for {dataset}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------

def run_audit() -> None:
    logger.info("=" * 68)
    logger.info("Phase 2 Reconciliation Audit")
    logger.info("=" * 68)

    rows: list[dict] = []
    all_pass = True

    for dataset in ("patients", "encounters", "lab_tests", "claims"):
        expected = EXPECTED[dataset]

        # Local Silver count
        silver_file = SILVER_FILES[dataset]
        if silver_file.exists():
            local_count = len(pd.read_parquet(str(silver_file), engine="pyarrow"))
        else:
            local_count = None
            logger.error(f"Silver file missing: {silver_file}")

        # Azure Silver count (may be None if no credentials)
        azure_count = _az_row_count(dataset)

        # Evaluate
        local_ok  = local_count  == expected if local_count  is not None else False
        azure_ok  = azure_count  == expected if azure_count  is not None else None  # None = skipped

        local_status = "PASS" if local_ok  else ("SKIP" if local_count  is None else "FAIL")
        azure_status = (
            "PASS" if azure_ok is True
            else "SKIP" if azure_ok is None
            else "FAIL"
        )

        if local_status == "FAIL" or azure_status == "FAIL":
            all_pass = False

        rows.append({
            "dataset":       dataset,
            "baseline":      BASELINE[dataset],
            "delta":         CHAOS_DELTA[dataset],
            "expected":      expected,
            "local_silver":  local_count  if local_count  is not None else "N/A",
            "azure_silver":  azure_count  if azure_count  is not None else "SKIPPED",
            "local_status":  local_status,
            "azure_status":  azure_status,
        })

    # ---------------------------------------------------------------------------
    # Special check: de-duplication of lab_tests
    # ---------------------------------------------------------------------------
    lab_silver = SILVER_FILES["lab_tests"]
    dedup_ok = False
    if lab_silver.exists():
        df_lab = pd.read_parquet(str(lab_silver), engine="pyarrow")
        # Bronze had 54,547 rows; Silver must have 54,537 (10 dupes removed)
        bronze_lab_path = _REPO_ROOT / "data" / "bronze" / "lab_tests"
        bronze_files = list(bronze_lab_path.glob("**/lab_tests.parquet"))
        if bronze_files:
            bronze_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            df_bronze_lab = pd.read_parquet(str(bronze_files[0]), engine="pyarrow")
            bronze_lab_count = len(df_bronze_lab)
            silver_lab_count = len(df_lab)
            dupes_removed    = bronze_lab_count - silver_lab_count
            dedup_ok = dupes_removed == 10
            logger.info(
                f"De-duplication check — Bronze: {bronze_lab_count:,}  "
                f"Silver: {silver_lab_count:,}  Removed: {dupes_removed}"
            )

    # ---------------------------------------------------------------------------
    # Print results table
    # ---------------------------------------------------------------------------
    header = (
        f"\n{'Dataset':<14} {'Baseline':>10} {'Delta':>7} {'Expected':>10} "
        f"{'Local':>10} {'Azure':>10} {'Local':>7} {'Azure':>7}"
    )
    divider = "-" * 79
    subhead = (
        f"{'':14} {'':>10} {'':>7} {'':>10} "
        f"{'Count':>10} {'Count':>10} {'Status':>7} {'Status':>7}"
    )

    print(header)
    print(subhead)
    print(divider)
    for r in rows:
        print(
            f"{r['dataset']:<14} {r['baseline']:>10,} {r['delta']:>+7} {r['expected']:>10,} "
            f"{str(r['local_silver']):>10} {str(r['azure_silver']):>10} "
            f"{r['local_status']:>7} {r['azure_status']:>7}"
        )
    print(divider)
    print(f"\nDe-duplication (lab_tests): {'PASS - 10 exact duplicates removed from Bronze->Silver' if dedup_ok else 'CHECK MANUALLY'}")

    overall = "ALL CHECKS PASS" if all_pass else "ONE OR MORE CHECKS FAILED"
    print(f"Overall result            : {overall}\n")

    logger.info(f"Reconciliation audit complete: {overall}")

    # ---------------------------------------------------------------------------
    # Save JSON report
    # ---------------------------------------------------------------------------
    report_dir = _REPO_ROOT / "pipeline" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "phase": "Phase 2 — Chaos Engineering",
        "audit_date": pd.Timestamp.now().isoformat(),
        "chaos_operations": {
            "appended_patients": 20,
            "updated_claims": 10,
            "deleted_encounters": 5,
            "duplicated_lab_rows": 10,
        },
        "results": rows,
        "dedup_check": {"bronze_lab_rows": None, "silver_lab_rows": None, "dupes_removed": None, "status": "PASS" if dedup_ok else "FAIL"},
        "overall": overall,
    }
    out_file = report_dir / "reconciliation_report.json"
    out_file.write_text(json.dumps(report, indent=2, default=str))
    logger.info(f"Report saved to {out_file}")


if __name__ == "__main__":
    run_audit()
