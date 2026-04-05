# Phase 1 Prompt History — Baseline Load

This document records the key prompts used during Phase 1 of the Azure Healthcare Data Platform build. It serves as a decision log and a reproducibility guide.

---

## Session 1 — Infrastructure Verification & Terraform Apply

**Prompt:**
> Since the password and configuration are already set, please move directly to the deployment. Run `terraform apply -auto-approve`. I will wait for the deployment to finish. Once it says 'Apply complete!', show me the Outputs so we can verify the Azure Resources are live.

**Outcome:**
- `terraform apply` completed with 7 resources updated (tag refresh on existing infra).
- Confirmed 19 Azure resources live, including ADLS Gen2 (`hcpdevuvxb03`), Synapse workspace, ADF, Key Vault, Log Analytics, and Application Insights.
- All Terraform outputs captured (storage DFS endpoint, Synapse connectivity endpoints, etc.).

---

## Session 2 — Initial Baseline Load (Medallion Pipeline)

**Prompt:**
> The Azure infrastructure is successfully provisioned. Now, I want to execute the Initial Baseline Load to establish our 'Source of Truth' in the cloud using the 4 source files you identified in `data/raw/`. Run `python pipeline/orchestrator.py --stage ingest`. This should move patients.csv, lab_tests.csv, encounters.csv, and claims_and_billing.csv into the Azure 'bronze' container. Please confirm the upload success via Azure CLI. Run `--stage bronze_silver`. This must apply the SHA-256 hashing to the PII fields (Name/Address) and move the data to the 'silver' container. Run `--stage silver_gold` to populate the final analytical 'gold' layer. Run `--stage quality`. Once finished, show me a table of the record counts for each layer (Bronze, Silver, and Gold) to prove the end-to-end flow is working perfectly.

**Outcome:**
- All 4 stages executed successfully.
- 254,537 records ingested across patients (60k), encounters (70k), lab_tests (54.5k), claims (70k).
- Bronze, Silver, and Gold layers written locally and uploaded to Azure ADLS Gen2 via `az storage fs file upload`.
- Data Quality report: 100% completeness, 100% referential integrity.
- Note: `lab_tests.status` had 18,085 rows with value `"Pending"` outside the validator allow-list — carried through to Bronze as-is (schema issue flagged for Phase 2).

---

## Session 3 — Synapse Access & Firewall Fix

**Prompt:**
> I am getting a 403 Forbidden error in Synapse Studio and cannot find the 'Synapse Administrator' or 'Contributor' roles in the Azure Portal UI. Please use the Azure CLI to assign the 'Owner' role to my current logged-in user at the Resource Group level. Use `az ad signed-in-user show --query id -o tsv` to get my ID. Then run `az role assignment create`. Once that is done, whitelist my current public IP address in the Synapse Workspace firewall using the `az synapse workspace firewall-rule create` command.

**Outcome:**
- Retrieved user object ID: `dcbe2ed1-c7a9-44be-b2b2-54422dedb43b`.
- Owner role assigned via `az rest` (ARM REST API) due to a CLI subscription context issue.
  - Assignment ID: `6c82d108-760a-49a5-b3e9-150f94e76d4e`
- Synapse firewall rule `AllowMyIP` created for public IP `71.172.153.79`.
- Advisory: Synapse Administrator role (workspace-level, inside Synapse Studio) is separate from Azure RBAC Owner — must be set in Synapse Studio → Manage → Access control.

---

## Session 4 — Silver Layer Security Remediation (Production Issues)

**Prompt:**
> I have audited the Silver layer in Synapse and found two 'Production-Level' issues that I want to remediate: (1) The Silver layer currently contains both the raw PII columns and the hashed columns. Please update `processing/bronze_to_silver.py` to DROP the original first_name, last_name, full_name, address, email, and phone columns after the hashes are generated. Only the hashed versions should exist in Silver. (2) The `dob` column is currently showing raw Unix Epoch timestamps. Please update the transformation to cast `dob` into a standard Date type (YYYY-MM-DD) so it is human-readable. After updating the code, re-run `--stage bronze_silver` and verify that the raw names are gone and the dates look correct. Update `silver_to_gold.py` to ensure it consumes these cleaned Silver records.

**Outcome:**
- `transform_patients()` updated in `bronze_to_silver.py`:
  - `dob` cast via `.dt.date` → stored as `date32` in Parquet (YYYY-MM-DD in Synapse).
  - All 6 raw PII columns dropped post-hashing: `first_name`, `last_name`, `full_name`, `address`, `email`, `phone`.
- `silver_to_gold.py` confirmed clean — no references to dropped columns.
- Re-ran both stages; Gold `patient_360` table verified: no raw names, `dob` shows `1940-05-14` format.

---

## Session 5 — Hard Reset & Strict PII Rules

**Prompt:**
> The Silver layer is still showing old, insecure data. We need to perform a Hard Reset to ensure the security fix is applied. Use `az storage fs directory delete` to DELETE everything inside the silver and gold containers. Update `bronze_to_silver.py` with these strict rules: Use `df.withColumn('dob', from_unixtime(col('dob')/1000).cast('date'))` for the date fix. Create the hashed columns, then use `.drop(...)` to completely remove the PII. CRITICAL: Ensure the final write command uses `.mode('overwrite')`.

**Key decision logged:**
> The pipeline is Pandas-based, not PySpark. `df.withColumn`, `from_unixtime`, `col()`, and `.mode('overwrite')` are Spark APIs that would crash here. The pandas equivalents were implemented:
> - `dob` fix: `pd.to_datetime(df["dob"], format="%d-%m-%Y").dt.date` (with epoch-ms fallback)
> - Drop: `df.drop(columns=[...])` — identical semantics
> - Overwrite: `to_parquet()` always overwrites (equivalent to `.mode('overwrite')`)

**Outcome:**
- `registration_date` also added to the drop list (new requirement in this session).
- Hard reset executed via `az storage fs directory delete` on silver and gold containers.
- New parquet files uploaded; Azure CLI download confirmed: `RAW PII COLUMNS PRESENT: NONE — PASS`.

---

## Session 6 — Gold Layer: NULL Payment Rates & Anomaly Detection

**Prompt:**
> I noticed that the Gold layer shows 0 for `total_paid` in some rows, which is making the `avg_payment_rate` 0. Update `silver_to_gold.py` to use a CASE statement or COALESCE. If `total_paid` is 0, the `avg_payment_rate` should show as NULL instead of 0 (to indicate 'Pending' status). Add a new column called `is_anomaly`. Set it to TRUE if `total_billed > 0` but `total_paid == 0`.

**Outcome:**
- Added `_payment_rate_or_null()` helper — COALESCE equivalent:
  - Returns `None` (SQL NULL) when `total_paid == 0` or `total_billed == 0`.
  - Returns the ratio only when both are positive.
- `is_anomaly` column added to `patient_360` and `encounter_summary`.
- `avg_payment_rate` in `claims_analytics` also patched.
- **4,403 anomalies detected** (7.3% of patients) — billed > 0 but paid = 0.
- Confirmed: `overall_payment_rate == 0` count = **0** across 60,000 rows.

---

## Session 7 — GitHub Preparation (This Session)

**Prompt:**
> I want to prepare this project for GitHub as a Phase 1: Baseline Load. Ensure the directory is professional: `/infrastructure/terraform`: Cloud resources. `/src/pipeline`: Ingestion and Transformation scripts. `/data/samples`: Create a folder and save the first 100 rows of each of our 4 healthcare files. Security Gate: Create a `.gitignore` file. It must exclude: `*.tfstate`, `*.tfvars`, `.terraform/`. `data/raw/`, `data/bronze/`. `.env` files. Documentation: Generate a `README.md` that describes Phase 1: Infrastructure as Code, Medallion Architecture, and SHA-256 HIPAA masking. Prompt History: Create `docs/prompts_baseline.md` and document the prompts we've used so far.

**Outcome (this file):**
- `ingestion/`, `processing/`, `pipeline/` → moved to `src/ingestion/`, `src/processing/`, `src/pipeline/`.
- All `_REPO_ROOT` path computations updated from `.parent.parent` to `.parents[2]`.
- `data/samples/` created with 100-row CSVs for each source file (~10–16 KB each, safe for Git).
- `.gitignore` created, excluding `*.tfstate`, `*.tfvars`, `.terraform/`, `data/raw/`, `data/bronze/`, `.env`.
- `README.md` generated covering IaC, Medallion Architecture, and SHA-256 HIPAA masking.
- This prompt history document created at `docs/prompts_baseline.md`.
