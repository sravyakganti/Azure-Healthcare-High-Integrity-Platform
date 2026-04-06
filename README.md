# Azure Healthcare Data Platform

A **production-grade, cloud-native healthcare data pipeline** built on **Microsoft Azure** using a **Medallion Architecture** (Bronze → Silver → Gold). The platform ingests 254,537+ records, enforces HIPAA-compliant Zero-Trust PII masking, and validates resilience through a full Chaos Engineering test suite — all provisioned via Terraform with no manual portal clicks.

---

## Architecture Overview

```
data/raw/ (CSV)
    │
    ▼  [Stage 1: Ingest — Pandera schema validation]
 BRONZE  — Raw Parquet, partitioned by ingestion_date, immutable
    │
    ▼  [Stage 2: Bronze → Silver — SHA-256 PII masking, type casting]
 SILVER  — Cleaned, date-typed (date32), Zero-Trust PII-hashed Parquet
    │
    ▼  [Stage 3: Silver → Gold — aggregated analytics]
  GOLD   — patient_360, encounter_summary, department_metrics, claims_analytics
    │
    ▼  [Stage 4: Quality]
  REPORT — JSON data-quality report (completeness, referential integrity,
            duplicate detection, date validity)
```

All layers are persisted to **Azure Data Lake Storage Gen2** (ADLS Gen2) with Hierarchical Namespace and are queryable via **Azure Synapse Analytics**.

---

## Repository Structure

```
.
├── infrastructure/
│   └── terraform/               # Azure IaC — all 19 resources in one apply
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       ├── providers.tf
│       └── terraform.tfvars.example
│
├── src/
│   ├── ingestion/               # Stage 1 — CSV → Bronze
│   │   ├── config.py            # Pydantic-Settings (env-var driven)
│   │   ├── validators.py        # Pandera schemas for all 4 datasets
│   │   └── ingest.py            # DataIngestionPipeline class
│   │
│   ├── processing/              # Stages 2–4 — Bronze → Silver → Gold
│   │   ├── bronze_to_silver.py  # SilverTransformer (SHA-256 PII hashing + de-duplication)
│   │   ├── silver_to_gold.py    # GoldTransformer (aggregated analytics tables)
│   │   └── data_quality.py      # DataQualityChecker (JSON DQ report)
│   │
│   ├── services/
│   │   └── chaos_simulator.py   # Phase 2 — controlled data mutation (APPEND/UPDATE/DELETE/DUPLICATE)
│   │
│   ├── quality/
│   │   └── reconciliation_audit.py  # Phase 2 — source-to-target math audit (PASS/FAIL table)
│   │
│   └── pipeline/
│       └── orchestrator.py      # HealthcarePipeline — runs all stages end-to-end
│
├── data/
│   ├── samples/                 # Data contract inputs (10-row CSV snapshots, raw PII visible)
│   │   ├── input_raw_patients.csv
│   │   └── input_raw_claims.csv
│   │
│   └── outputs/                 # Engineering outputs (10-row CSV snapshots, post-transformation)
│       ├── secure_patients_silver.csv   # Proves PII removal — SHA-256 hashes only
│       └── reconciled_billing_gold.csv  # Proves self-healing — $0 denials corrected
│
├── tests/
│   ├── test_validators.py
│   └── test_transformations.py
│
├── docs/
│   ├── images/
│   │   ├── Azure_lake.png          # ADLS Gen2 medallion containers (Azure Portal)
│   │   ├── pii_masking.png         # Bronze vs Silver PII comparison
│   │   └── Synpase_compute.png     # Synapse Spark Pool
│   │
│   └── evidence/
│       ├── final_reconciliation_audit.md   # Phase 2 chaos math proof
│       ├── quality_report.md               # Phase 1 baseline DQ audit
│       ├── terraform_resource_map.md       # 19-resource Terraform inventory
│       └── data_previews/
│           ├── bronze_patients_sample.csv  # Raw PII visible
│           └── silver_patients_sample.csv  # SHA-256 hashes only
│
├── .env.example
├── requirements.txt
└── README.md
```

---

## Phase 1 — Baseline Load

### Infrastructure as Code (Terraform)

All 19 Azure resources provisioned with a single `terraform apply -auto-approve`. Zero manual portal clicks.

| Resource | Azure Name | Purpose |
|---|---|---|
| Resource Group | `healthcare-platform-dev-rg` | Logical blast-radius boundary |
| ADLS Gen2 | `hcpdevuvxb03` | Data lake — bronze / silver / gold / raw / configs |
| Azure Data Factory | `healthcare-platform-dev-adf-uvxb03` | ETL orchestration (Phase 2+) |
| Azure Synapse | `healthcare-platform-dev-syn-uvxb03` | Analytics & SQL serverless queries |
| Synapse Spark Pool | `sparkpooluvxb03` | Distributed compute (MemoryOptimized, 3–5 nodes) |
| Key Vault | `hcp-dev-kv-uvxb03` | Secrets — no credentials in source control |
| Log Analytics | `healthcare-platform-dev-law-uvxb03` | Centralised log sink (30-day retention) |
| Application Insights | `healthcare-platform-dev-ai-uvxb03` | Pipeline telemetry |

Full resource inventory: [`docs/evidence/terraform_resource_map.md`](docs/evidence/terraform_resource_map.md)

### Medallion Architecture

**Bronze** — Raw, immutable Parquet copy of each source CSV. Validated with Pandera schemas. Partitioned by `ingestion_date`. 254,537 records across 4 datasets.

**Silver** — PII-scrubbed, typed, and enriched:
- `dob` cast from raw string to `date32` (ISO `YYYY-MM-DD`) — no Unix epoch integers in Synapse
- All 6 PII columns removed: `first_name`, `last_name`, `full_name`, `address`, `email`, `phone`, `registration_date`
- SHA-256 deterministic hashes retained for each PII field (enables cross-dataset record linkage)
- Derived columns: `age_group`, `is_readmitted`, `is_abnormal`, `payment_rate`
- `df.drop_duplicates()` applied to `lab_tests` to absorb exact-row injections

**Gold** — Four analytics-ready tables:

| Table | Rows (Phase 2) | Description |
|---|---:|---|
| `patient_360` | 60,020 | One row per patient with visit, lab, and financial KPIs |
| `encounter_summary` | 69,995 | Encounter-level financials; `is_anomaly` flag |
| `department_metrics` | 21 | Per-department billing aggregations |
| `claims_analytics` | 14 | Provider × claim-status breakdown with denial rates |

### Data Quality Results (Phase 1 Baseline)

| Metric | Result |
|--------|--------|
| Overall Silver completeness | **100%** (22 columns × 4 datasets) |
| Duplicate primary keys | **0** (patients, encounters, claims) |
| Referential integrity | **100%** across all 4 FK relationships |
| Anomalies (`billed > 0`, `paid = 0`) | **4,403 patients (7.3%)** |
| Denied claims identified | **5,998 (8.6%)** |
| Negative billing amounts | **0** |
| Raw PII columns in Silver / Gold | **0** |

---

## Phase 2 — Chaos Engineering & Automated Reconciliation

Phase 2 validates **pipeline resilience** by injecting controlled data faults and proving the system self-heals without manual intervention.

### Chaos Operations (`src/services/chaos_simulator.py`)

| # | Operation | Target File | Detail | Effect |
|---|---|---|---|---|
| 1 | **APPEND** | `patients.csv` | 20 new patients (PAT060001–PAT060020) | Tests schema validation + PII masking on new records |
| 2 | **UPDATE** | `claims_and_billing.csv` | 10 denied claims (`paid=0`) → resolved (`paid=real amount`) | Tests self-healing: `is_anomaly` flips FALSE in Gold |
| 3 | **DELETE** | `encounters.csv` | 5 encounter rows removed | Tests referential integrity detection |
| 4 | **DUPLICATE** | `lab_tests.csv` | 10 exact-row copies inserted | Tests de-duplication in Silver transformation |

The simulator is **idempotent** — re-running it detects already-applied mutations and skips them.

### Reconciliation Audit Results

Source-to-target math confirmed across local Silver and Azure ADLS Gen2:

| Dataset | Baseline | Chaos Delta | Expected | Local Silver | Azure Silver | Status |
|---------|--------:|------------|--------:|------------:|-----------:|:------:|
| `patients` | 60,000 | +20 appended | **60,020** | 60,020 | 60,020 | ✅ PASS |
| `encounters` | 70,000 | −5 deleted | **69,995** | 69,995 | 69,995 | ✅ PASS |
| `lab_tests` | 54,537 | +10 dupes → −10 dedup | **54,537** | 54,537 | 54,537 | ✅ PASS |
| `claims` | 70,000 | 10 updated in-place | **70,000** | 70,000 | 70,000 | ✅ PASS |

Full audit: [`docs/evidence/final_reconciliation_audit.md`](docs/evidence/final_reconciliation_audit.md)

---

## Design Patterns

### Zero-Trust PII Masking (SHA-256)

All personally identifiable fields are SHA-256 hashed before the Silver write. Raw values never leave Bronze.

```python
# src/processing/bronze_to_silver.py
df["first_name_hashed"] = df["first_name"].apply(_sha256)
df["last_name_hashed"]  = df["last_name"].apply(_sha256)
df["email_hashed"]      = df["email"].apply(_sha256)
df["phone_hashed"]      = df["phone"].apply(_sha256)
df["address_hashed"]    = df["address"].apply(_sha256)

# Raw PII columns dropped — Silver is PII-free
df = df.drop(columns=["first_name", "last_name", "full_name",
                       "address", "email", "phone", "registration_date"])
```

The `_sha256` helper produces a deterministic 64-character hex digest, enabling cross-dataset record linkage without exposing raw PII. This satisfies HIPAA Safe Harbor and meets Salesforce-grade data privacy standards.

### Idempotency

Every pipeline stage is designed to be safe to re-run:

- **Ingest**: `to_parquet(..., overwrite=True)` — re-running the ingestion stage always produces the same Bronze output for the same input CSV
- **Bronze→Silver**: `to_parquet()` overwrites the Silver file atomically — no partial writes, no stale rows
- **Chaos Simulator**: each mutation function checks for existing state before applying — APPEND checks for existing patient IDs, DELETE checks whether rows still exist, DUPLICATE checks the row count against baseline
- **Reconciliation Audit**: reads current state, computes expected counts, compares — safe to run at any time

This means any stage can be re-triggered on failure without producing duplicates or corrupt state — a Salesforce-grade reliability requirement.

### NULL COALESCE for Payment Rates

```python
def _payment_rate_or_null(billed: float, paid: float):
    if pd.isna(billed) or billed <= 0:
        return None
    if pd.isna(paid) or paid == 0:
        return None  # Pending/Denied — display as NULL, not 0%
    return paid / billed
```

`payment_rate = 0%` (computed) is semantically different from `payment_rate = NULL` (pending/denied). The COALESCE pattern prevents misleading 0% rates in Synapse dashboards.

---

## System Verification & Azure Migration

Live evidence that the full pipeline ran end-to-end against a real Azure environment. All supporting artefacts are in [`docs/evidence/`](docs/evidence/).

### 1. Azure Data Lake Storage Gen2 — Medallion Containers

![ADLS Gen2 Medallion Containers](docs/images/Azure_lake.png)

> **Fig 1.** Azure Portal view of storage account `hcpdevuvxb03` showing all five Medallion layer containers (`raw`, `bronze`, `silver`, `gold`, `configs`) provisioned by Terraform. Hierarchical Namespace (HNS) is enabled, confirming true Data Lake Gen2 semantics rather than standard blob storage.

---

### 2. SHA-256 PII Masking — Bronze vs Silver

![PII Masking Bronze vs Silver](docs/images/pii_masking.png)

> **Fig 2.** Side-by-side comparison of the same patient record at the Bronze layer (raw PII: name, address, email visible) and the Silver layer (all six PII columns replaced with deterministic SHA-256 hashes; `registration_date` dropped entirely). The `dob` field is cast from raw string to ISO `date32` (`YYYY-MM-DD`). Raw PII never reaches Silver, Gold, or Synapse.

---

### 3. Azure Synapse Analytics — Spark Pool Compute

![Synapse Spark Pool](docs/images/Synpase_compute.png)

> **Fig 3.** Azure Synapse Studio showing the `sparkpooluvxb03` Spark pool (Memory Optimized, 3–5 nodes, auto-pause 15 min) attached to workspace `healthcare-platform-dev-syn-uvxb03`. This is the distributed compute layer for large-scale transformations against the Silver and Gold ADLS containers.

---

### Engineering Output Samples

| File | Description |
|------|-------------|
| [`data/samples/input_raw_patients.csv`](data/samples/input_raw_patients.csv) | 10-row data contract — real names, emails, DOBs visible (Bronze input) |
| [`data/samples/input_raw_claims.csv`](data/samples/input_raw_claims.csv) | 10-row data contract — $0.00 denied claims visible (pre-heal) |
| [`data/outputs/secure_patients_silver.csv`](data/outputs/secure_patients_silver.csv) | 10-row Silver output — SHA-256 hashes only, ISO date32 DOB |
| [`data/outputs/reconciled_billing_gold.csv`](data/outputs/reconciled_billing_gold.csv) | 10-row Gold output — corrected payment ($500.00), `is_anomaly=False` |

---

## Quick Start

### Prerequisites
- Python 3.11+
- Terraform 1.5+
- Azure CLI (`az login` authenticated)

### 1. Clone and install

```bash
git clone https://github.com/sravyakganti/Azure-Healthcare-High-Integrity-Platform.git
cd Azure-Healthcare-High-Integrity-Platform
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Fill in AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY
```

### 3. Provision Azure infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform plan      # Review 19 resources
terraform apply -auto-approve
```

### 4. Run the pipeline

```bash
# Full end-to-end
python src/pipeline/orchestrator.py --stage all

# Stage by stage
python src/pipeline/orchestrator.py --stage ingest
python src/pipeline/orchestrator.py --stage bronze_silver
python src/pipeline/orchestrator.py --stage silver_gold
python src/pipeline/orchestrator.py --stage quality
```

### 5. Run Phase 2 chaos + reconciliation

```bash
# Apply controlled data mutations
python src/services/chaos_simulator.py

# Re-run pipeline to process mutations
python src/pipeline/orchestrator.py --stage all

# Verify source-to-target integrity
python src/quality/reconciliation_audit.py
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud | Microsoft Azure |
| IaC | Terraform (azurerm 3.x, azuread 2.x) |
| Storage | ADLS Gen2 (Hierarchical Namespace enabled) |
| Analytics | Azure Synapse Analytics + Spark Pool |
| Orchestration | Azure Data Factory |
| Language | Python 3.11 |
| Data | Pandas 2.x, PyArrow 22.x |
| Validation | Pandera |
| Config | Pydantic-Settings |
| Logging | Loguru |
| Security | SHA-256 HIPAA PII masking, TLS 1.2, Managed Identity RBAC |
