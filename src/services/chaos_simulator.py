"""
chaos_simulator.py — Phase 2: Chaos Engineering
================================================
Programmatically modifies data/raw/ CSVs to simulate Day-2 production changes:

  APPEND  : 20 new realistic patient records  → patients.csv
  UPDATE  : 10 denied claims → paid, clear denial_reason → claims_and_billing.csv
  DELETE  : 5 encounter records               → encounters.csv
  DUPLICATE: 10 exact row copies              → lab_tests.csv

Run:  python src/services/chaos_simulator.py
The script is idempotent — re-running it detects already-applied changes and
skips them rather than double-appending or double-deleting.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "src" / "ingestion"))

import pandas as pd
from loguru import logger

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RAW = _REPO_ROOT / "data" / "raw"
PATIENTS_CSV   = RAW / "patients.csv"
ENCOUNTERS_CSV = RAW / "encounters.csv"
LAB_TESTS_CSV  = RAW / "lab_tests.csv"
CLAIMS_CSV     = RAW / "claims_and_billing.csv"

# Log file
LOG_DIR = _REPO_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
logger.add(str(LOG_DIR / "chaos_simulator.log"), rotation="10 MB", retention="7 days")

# ---------------------------------------------------------------------------
# Constants — deterministic IDs so the script is idempotent
# ---------------------------------------------------------------------------
NEW_PATIENT_START_ID = 60001   # PAT060001 … PAT060020

CLAIMS_TO_UPDATE = [
    "BILL000001", "BILL000010", "BILL000024", "BILL000035",
    "BILL000052", "BILL000086", "BILL000116", "BILL000121",
    "BILL000123", "BILL000149",
]  # 10 denied claims identified during baseline audit

ENCOUNTERS_TO_DELETE = [
    "ENC017535", "ENC030367", "ENC038533", "ENC056041", "ENC046849",
]  # 5 encounters from head of baseline file

LAB_ROWS_TO_DUPLICATE = list(range(0, 10))  # first 10 rows → exact copies


# ---------------------------------------------------------------------------
# 1. APPEND — 20 new patients
# ---------------------------------------------------------------------------
def _append_patients() -> None:
    df = pd.read_csv(PATIENTS_CSV)

    existing_ids = set(df["patient_id"].tolist())
    new_ids = [f"PAT{NEW_PATIENT_START_ID + i:06d}" for i in range(20)]

    already_done = all(pid in existing_ids for pid in new_ids)
    if already_done:
        logger.info("APPEND patients: already applied — skipping")
        return

    new_rows = [
        # (patient_id, first_name, last_name, dob,          age, gender,   ethnicity, insurance,  marital,      address,                    city,         state, zip,   phone,          email,                             reg_date)
        (f"PAT{NEW_PATIENT_START_ID+0:06d}",  "James",     "Rivera",   "12-06-1985", 40, "Male",   "Hispanic",  "Cigna",    "Married",                    "14 Oak Lane",              "Austin",       "TX",  "78701", "512-555-0101", "james.rivera@email.com",          "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+1:06d}",  "Sophia",    "Chen",     "03-11-1992", 33, "Female", "Asian",     "Aetna",    "Single",                     "88 Maple Ave",             "San Jose",     "CA",  "95101", "408-555-0102", "sophia.chen@email.com",           "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+2:06d}",  "Marcus",    "Johnson",  "29-02-1980", 46, "Male",   "White",     "BCBS",     "Married",                    "3 Birch Rd",               "Charlotte",    "NC",  "28201", "704-555-0103", "marcus.johnson@email.com",        "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+3:06d}",  "Aisha",     "Williams", "17-07-1975", 50, "Female", "Asian",     "Medicare", "Widowed/Divorced/Separated", "501 Elm St",               "Detroit",      "MI",  "48201", "313-555-0104", "aisha.williams@email.com",        "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+4:06d}",  "Carlos",    "Martinez", "08-01-2000", 26, "Male",   "Hispanic",  "Medicaid", "Single",                     "22 Cedar Blvd",            "El Paso",      "TX",  "79901", "915-555-0105", "carlos.martinez@email.com",       "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+5:06d}",  "Emily",     "Davis",    "14-09-1968", 57, "Female", "White",     "UHC",      "Married",                    "7 Willow Ct",              "Nashville",    "TN",  "37201", "615-555-0106", "emily.davis@email.com",           "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+6:06d}",  "David",     "Lee",      "25-12-1995", 30, "Male",   "Asian",     "Humana",   "Single",                     "19 Spruce Dr",             "Louisville",   "KY",  "40201", "502-555-0107", "david.lee@email.com",             "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+7:06d}",  "Fatima",    "Hassan",   "30-04-1988", 37, "Female", "Hispanic",  "Cigna",    "Married",                    "66 Pine Way",              "Memphis",      "TN",  "38101", "901-555-0108", "fatima.hassan@email.com",         "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+8:06d}",  "Tyler",     "Brown",    "11-03-2005", 21, "Male",   "White",     "Aetna",    "Single",                     "100 Aspen Ln",             "Baltimore",    "MD",  "21201", "410-555-0109", "tyler.brown@email.com",           "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+9:06d}",  "Natalie",   "Garcia",   "07-08-1955", 70, "Female", "Hispanic",  "Medicare", "Widowed/Divorced/Separated", "45 Sequoia St",            "Albuquerque",  "NM",  "87101", "505-555-0110", "natalie.garcia@email.com",        "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+10:06d}", "Kevin",     "Thompson", "19-05-1970", 55, "Male",   "White",     "BCBS",     "Married",                    "82 Redwood Ave",           "Tucson",       "AZ",  "85701", "520-555-0111", "kevin.thompson@email.com",        "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+11:06d}", "Priya",     "Patel",    "22-10-1983", 42, "Female", "Asian",     "UHC",      "Married",                    "9 Magnolia Rd",            "Fresno",       "CA",  "93701", "559-555-0112", "priya.patel@email.com",           "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+12:06d}", "Omar",      "Gonzalez", "16-02-1998", 28, "Male",   "Hispanic",  "Medicaid", "Single",                     "33 Cypress Ct",            "Sacramento",   "CA",  "95814", "916-555-0113", "omar.gonzalez@email.com",         "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+13:06d}", "Hannah",    "Wilson",   "04-06-1945", 80, "Female", "White",     "Medicare", "Widowed/Divorced/Separated", "71 Poplar Blvd",           "Long Beach",   "CA",  "90801", "562-555-0114", "hannah.wilson@email.com",         "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+14:06d}", "Brandon",   "Moore",    "28-09-1990", 35, "Male",   "White",     "Humana",   "Married",                    "5 Hickory Dr",             "Kansas City",  "MO",  "64101", "816-555-0115", "brandon.moore@email.com",         "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+15:06d}", "Lin",       "Wang",     "13-01-2008", 18, "Female", "Asian",     "Aetna",    "Single",                     "27 Sycamore Way",          "Virginia Beach","VA", "23451", "757-555-0116", "lin.wang@email.com",              "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+16:06d}", "Derek",     "Clark",    "01-11-1962", 63, "Male",   "White",     "Medicare", "Married",                    "88 Dogwood St",            "Atlanta",      "GA",  "30301", "404-555-0117", "derek.clark@email.com",           "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+17:06d}", "Isabella",  "Robinson", "20-03-1977", 49, "Female", "Hispanic",  "Cigna",    "Married",                    "44 Walnut Ave",            "Colorado Springs","CO","80901","719-555-0118", "isabella.robinson@email.com",     "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+18:06d}", "Samuel",    "Lewis",    "09-07-2003", 22, "Male",   "White",     "BCBS",     "Single",                     "16 Chestnut Ln",           "Raleigh",      "NC",  "27601", "984-555-0119", "samuel.lewis@email.com",          "05-04-2026"),
        (f"PAT{NEW_PATIENT_START_ID+19:06d}", "Grace",     "Turner",   "26-12-1935", 90, "Female", "Asian",     "Medicare", "Widowed/Divorced/Separated", "3 Ironwood Blvd",          "Minneapolis",  "MN",  "55401", "612-555-0120", "grace.turner@email.com",          "05-04-2026"),
    ]

    columns = [
        "patient_id", "first_name", "last_name", "dob", "age", "gender",
        "ethnicity", "insurance_type", "marital_status", "address",
        "city", "state", "zip", "phone", "email", "registration_date",
    ]
    new_df = pd.DataFrame(new_rows, columns=columns)

    # Filter out any rows whose patient_id already exists (idempotency)
    new_df = new_df[~new_df["patient_id"].isin(existing_ids)]

    result = pd.concat([df, new_df], ignore_index=True)
    result.to_csv(PATIENTS_CSV, index=False)
    logger.info(f"APPEND patients: added {len(new_df)} rows → total {len(result):,}")


# ---------------------------------------------------------------------------
# 2. UPDATE — 10 denied claims → paid
# ---------------------------------------------------------------------------
_CLAIM_PAYMENTS = {
    "BILL000001": (1577.22, "Paid"),
    "BILL000010": (730.38,  "Paid"),
    "BILL000024": (1662.82, "Paid"),
    "BILL000035": (1794.26, "Paid"),
    "BILL000052": (925.97,  "Paid"),
    "BILL000086": (449.30,  "Paid"),
    "BILL000116": (1165.32, "Paid"),
    "BILL000121": (1208.49, "Paid"),
    "BILL000123": (394.79,  "Paid"),
    "BILL000149": (199.77,  "Paid"),
}


def _update_claims() -> None:
    df = pd.read_csv(CLAIMS_CSV)

    # Idempotency: check if any target claim is already 'Paid'
    target_mask = df["billing_id"].isin(CLAIMS_TO_UPDATE)
    if (df.loc[target_mask, "claim_status"] == "Paid").all():
        logger.info("UPDATE claims: already applied — skipping")
        return

    for billing_id, (paid_amt, new_status) in _CLAIM_PAYMENTS.items():
        idx = df.index[df["billing_id"] == billing_id]
        if len(idx) == 0:
            logger.warning(f"UPDATE claims: {billing_id} not found — skipping")
            continue
        df.loc[idx, "paid_amount"]   = paid_amt
        df.loc[idx, "claim_status"]  = new_status
        df.loc[idx, "denial_reason"] = ""

    df.to_csv(CLAIMS_CSV, index=False)
    logger.info(f"UPDATE claims: resolved {len(_CLAIM_PAYMENTS)} denied → paid")


# ---------------------------------------------------------------------------
# 3. DELETE — 5 encounter records
# ---------------------------------------------------------------------------
def _delete_encounters() -> None:
    df = pd.read_csv(ENCOUNTERS_CSV)

    still_present = df["encounter_id"].isin(ENCOUNTERS_TO_DELETE).sum()
    if still_present == 0:
        logger.info("DELETE encounters: already applied — skipping")
        return

    before = len(df)
    df = df[~df["encounter_id"].isin(ENCOUNTERS_TO_DELETE)]
    removed = before - len(df)
    df.to_csv(ENCOUNTERS_CSV, index=False)
    logger.info(f"DELETE encounters: removed {removed} rows → remaining {len(df):,}")
    logger.info(f"  Deleted IDs: {ENCOUNTERS_TO_DELETE}")


# ---------------------------------------------------------------------------
# 4. DUPLICATE — 10 exact row copies into lab_tests
# ---------------------------------------------------------------------------
def _duplicate_lab_rows() -> None:
    df = pd.read_csv(LAB_TESTS_CSV)
    baseline_count = 54_537  # Phase 1 Bronze count

    if len(df) > baseline_count:
        logger.info(f"DUPLICATE lab_tests: already applied ({len(df):,} rows) — skipping")
        return

    rows_to_copy = df.iloc[LAB_ROWS_TO_DUPLICATE]
    result = pd.concat([df, rows_to_copy], ignore_index=True)
    result.to_csv(LAB_TESTS_CSV, index=False)
    logger.info(f"DUPLICATE lab_tests: inserted {len(rows_to_copy)} copies → total {len(result):,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("=" * 60)
    logger.info("Phase 2 Chaos Simulator — START")
    logger.info("=" * 60)

    _append_patients()
    _update_claims()
    _delete_encounters()
    _duplicate_lab_rows()

    # Summary
    patients_count   = len(pd.read_csv(PATIENTS_CSV))
    encounters_count = len(pd.read_csv(ENCOUNTERS_CSV))
    lab_count        = len(pd.read_csv(LAB_TESTS_CSV))
    claims_count     = len(pd.read_csv(CLAIMS_CSV))

    logger.info("=" * 60)
    logger.info("Post-chaos row counts (data/raw/):")
    logger.info(f"  patients.csv          : {patients_count:>7,}  (baseline 60,000 + 20 new)")
    logger.info(f"  encounters.csv        : {encounters_count:>7,}  (baseline 70,000 - 5 deleted)")
    logger.info(f"  lab_tests.csv         : {lab_count:>7,}  (baseline 54,537 + 10 duplicates)")
    logger.info(f"  claims_and_billing.csv: {claims_count:>7,}  (baseline 70,000, 10 updated)")
    logger.info("Phase 2 Chaos Simulator — DONE")


if __name__ == "__main__":
    main()
