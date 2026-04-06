# Final Reconciliation Audit — Phase 2: Chaos Engineering

**Audit Date:** 2026-04-05  
**Pipeline Version:** Phase 2 — Chaos Engineering & Automated Reconciliation  
**Azure Storage Account:** `hcpdevuvxb03` (ADLS Gen2)  
**Auditor:** Automated reconciliation via `src/quality/reconciliation_audit.py`

---

## Mathematical Proof of System Integrity

The following table demonstrates source-to-target record-count integrity across all four chaos operations applied to `data/raw/` and propagated through the full Bronze → Silver → Gold pipeline.

| Dataset | Phase 1 Baseline | Chaos Operation | Expected Silver | Azure Silver | Local Silver | Status |
|---------|----------------:|-----------------|----------------:|-------------:|-------------:|:------:|
| `patients` | 60,000 | +20 appended (PAT060001–PAT060020) | **60,020** | 60,020 | 60,020 | ✅ PASS |
| `encounters` | 70,000 | −5 deleted (ENC017535, ENC030367, ENC038533, ENC056041, ENC046849) | **69,995** | 69,995 | 69,995 | ✅ PASS |
| `lab_tests` | 54,537 | +10 duplicates inserted → −10 de-duplicated in Silver | **54,537** | 54,537 | 54,537 | ✅ PASS |
| `claims` | 70,000 | 10 records updated in-place (Denied→Paid) | **70,000** | 70,000 | 70,000 | ✅ PASS |

**Overall result: ALL 4 CHECKS PASS — Local Silver = Azure Silver on every dataset.**

---

## Chaos Operation Detail

### 1. APPEND — 20 New Patients

20 new patient records (IDs `PAT060001` through `PAT060020`) were appended to `patients.csv`, simulating a batch intake of new registrations. All 20 passed Pandera schema validation, SHA-256 PII masking was applied, and the rows propagated into both the Silver and Gold `patient_360` layers.

| Metric | Value |
|--------|------:|
| Rows before chaos | 60,000 |
| Rows appended | +20 |
| Bronze rows ingested | 60,020 |
| Silver rows (post-PII masking) | 60,020 |
| Gold `patient_360` rows | 60,020 |

---

### 2. UPDATE — 10 Denied Claims Resolved (Self-Healing)

10 claims previously in `claim_status = 'Denied'` with `paid_amount = 0.00` were updated to `claim_status = 'Paid'` with real payment amounts. This simulates an insurer reversing a denial after appeal — a common Day-2 production event.

| billing_id | Original paid_amount | Original status | Updated paid_amount | Updated status |
|------------|---------------------:|-----------------|--------------------:|----------------|
| BILL000001 | $0.00 | Denied | **$1,577.22** | Paid |
| BILL000010 | $0.00 | Denied | **$730.38** | Paid |
| BILL000024 | $0.00 | Denied | **$1,662.82** | Paid |
| BILL000035 | $0.00 | Denied | **$1,794.26** | Paid |
| BILL000052 | $0.00 | Denied | **$925.97** | Paid |
| BILL000086 | $0.00 | Denied | **$449.30** | Paid |
| BILL000116 | $0.00 | Denied | **$1,165.32** | Paid |
| BILL000121 | $0.00 | Denied | **$1,208.49** | Paid |
| BILL000123 | $0.00 | Denied | **$394.79** | Paid |
| BILL000149 | $0.00 | Denied | **$199.77** | Paid |

**Self-Healing effect in Gold `patient_360`:** For each of these patients, `is_anomaly` flipped from `TRUE` → `FALSE` and `overall_payment_rate` changed from `NULL` → a real percentage, because `total_paid` is now > 0. No code change was required — the Medallion pipeline replayed naturally.

---

### 3. DELETE — 5 Encounter Records

5 encounter rows were removed from `encounters.csv` to simulate record drift (e.g., a source-system retraction). The pipeline ingested 69,995 Bronze rows (validated), produced 69,995 Silver rows, and the referential integrity check in the DQ report flagged 5 orphaned FK references in `lab_tests` and `claims` — exactly as expected.

**Deleted IDs:** `ENC017535`, `ENC030367`, `ENC038533`, `ENC056041`, `ENC046849`

---

### 4. DUPLICATE → DE-DUPLICATION — 10 Lab Test Rows

10 exact-row copies were inserted into `lab_tests.csv` (Bronze count: 54,547). The `transform_lab_tests()` function in `bronze_to_silver.py` applies `df.drop_duplicates()` before writing Silver, silently absorbing the duplicates. Silver count returns to 54,537 — identical to the Phase 1 baseline.

| Stage | Row Count | Note |
|-------|----------:|------|
| Raw CSV (post-chaos) | 54,547 | +10 exact duplicates |
| Bronze parquet | 54,547 | Raw copy — duplicates preserved intentionally |
| Silver parquet | 54,537 | `drop_duplicates()` removes 10 copies |
| Delta | −10 | 100% of injected duplicates removed |

---

## PII Masking Verification (Zero-Trust)

All 7 PII fields are SHA-256 hashed before Silver write. Raw values are confirmed absent from Silver, Gold, and Azure Synapse.

| PII Field | Present in Bronze | Present in Silver | Present in Gold |
|-----------|:-----------------:|:-----------------:|:---------------:|
| `first_name` | YES (raw) | NO — hash only | NO |
| `last_name` | YES (raw) | NO — hash only | NO |
| `full_name` | YES (raw) | NO — hash only | NO |
| `email` | YES (raw) | NO — hash only | NO |
| `phone` | YES (raw) | NO — hash only | NO |
| `address` | YES (raw) | NO — hash only | NO |
| `registration_date` | YES (raw) | DROPPED entirely | NO |
| `dob` | string `DD-MM-YYYY` | ISO `date32` (YYYY-MM-DD) | ISO `date32` |

---

## Azure Sync Confirmation

All layers uploaded to `hcpdevuvxb03` (ADLS Gen2) and row counts verified by downloading each parquet and counting via PyArrow:

| Container | Path | Azure Rows | Expected | Status |
|-----------|------|-----------:|---------:|:------:|
| `silver` | `patients/patients.parquet` | 60,020 | 60,020 | ✅ PASS |
| `silver` | `encounters/encounters.parquet` | 69,995 | 69,995 | ✅ PASS |
| `silver` | `lab_tests/lab_tests.parquet` | 54,537 | 54,537 | ✅ PASS |
| `silver` | `claims/claims.parquet` | 70,000 | 70,000 | ✅ PASS |
| `gold` | `patient_360/patient_360.parquet` | 60,020 | 60,020 | ✅ PASS |
| `gold` | `encounter_summary/encounter_summary.parquet` | 69,995 | 69,995 | ✅ PASS |
