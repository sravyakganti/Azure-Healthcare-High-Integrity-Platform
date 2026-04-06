# Terraform Resource Map — Azure Healthcare Platform (Phase 1)

**Subscription:** `4f55df38-a321-4fb8-ac08-693e58722814`  
**Resource Group:** `healthcare-platform-dev-rg`  
**Region:** East US  
**Terraform providers:** azurerm 3.117.1 · azuread 2.53.1 · random 3.8.1  
**Deployment:** `terraform apply -auto-approve` — completed 2026-04-05

---

## Resource Inventory (19 resources)

| # | Terraform Resource | Azure Name | Type | Purpose |
|---|-------------------|------------|------|---------|
| 1 | `random_string.suffix` | `uvxb03` | Random | Unique 6-char suffix appended to all globally scoped resource names to avoid naming conflicts |
| 2 | `azurerm_resource_group.main` | `healthcare-platform-dev-rg` | Resource Group | Logical container for all Phase 1 Azure resources; single blast-radius boundary for teardown |
| 3 | `azurerm_storage_account.adls` | `hcpdevuvxb03` | Storage Account (ADLS Gen2) | Primary data lake with Hierarchical Namespace enabled; hosts all Medallion layer containers |
| 4 | `azurerm_storage_data_lake_gen2_filesystem.raw` | `raw` | ADLS Container | Landing zone for original source CSV files |
| 5 | `azurerm_storage_data_lake_gen2_filesystem.bronze` | `bronze` | ADLS Container | Immutable raw Parquet — schema-validated, partitioned by `ingestion_date` |
| 6 | `azurerm_storage_data_lake_gen2_filesystem.silver` | `silver` | ADLS Container | PII-masked, typed, enriched Parquet; also used as Synapse primary storage |
| 7 | `azurerm_storage_data_lake_gen2_filesystem.gold` | `gold` | ADLS Container | Analytics-ready aggregated tables (patient_360, encounter_summary, etc.) |
| 8 | `azurerm_storage_data_lake_gen2_filesystem.configs` | `configs` | ADLS Container | Reserved for pipeline configuration files, schema definitions, and lookup tables |
| 9 | `azurerm_key_vault.main` | `hcp-dev-kv-uvxb03` | Key Vault (Standard SKU) | Centralised secret store for connection strings, credentials, and API keys; soft-delete enabled |
| 10 | `azurerm_key_vault_access_policy.adf` | *(inline policy)* | Key Vault Access Policy | Grants ADF managed identity `Get` + `List` on secrets so pipelines can retrieve credentials without hardcoding |
| 11 | `azurerm_data_factory.main` | `healthcare-platform-dev-adf-uvxb03` | Azure Data Factory | ETL orchestration service with SystemAssigned managed identity; configured for Phase 2 pipeline triggers |
| 12 | `azurerm_role_assignment.adf_storage` | *(role assignment)* | RBAC Role Assignment | Grants ADF managed identity **Storage Blob Data Contributor** on ADLS; enables ADF to read/write all containers |
| 13 | `azurerm_log_analytics_workspace.main` | `healthcare-platform-dev-law-uvxb03` | Log Analytics Workspace | Centralised log sink for all Azure resource diagnostics; 30-day retention (PerGB2018 SKU) |
| 14 | `azurerm_application_insights.main` | `healthcare-platform-dev-ai-uvxb03` | Application Insights | Pipeline telemetry and custom metrics; workspace-linked to Log Analytics for unified querying |
| 15 | `azurerm_synapse_workspace.main` | `healthcare-platform-dev-syn-uvxb03` | Synapse Analytics Workspace | Unified analytics platform for SQL serverless and Spark queries against ADLS Gold layer |
| 16 | `azurerm_role_assignment.synapse_storage` | *(role assignment)* | RBAC Role Assignment | Grants Synapse managed identity **Storage Blob Data Contributor** on ADLS; enables Synapse to query Parquet directly |
| 17 | `azurerm_synapse_firewall_rule.allow_azure_services` | `AllowAllWindowsAzureIps` | Synapse Firewall Rule | Permits Azure-internal service-to-service traffic (ADF → Synapse, etc.) |
| 18 | `azurerm_synapse_spark_pool.main` | `sparkpooluvxb03` | Synapse Spark Pool (MemoryOptimized) | Distributed Spark compute for large-scale transformations; auto-scale 3–5 nodes, auto-pause 15 min |
| 19 | `azurerm_monitor_diagnostic_setting.adf` | `healthcare-platform-dev-adf-uvxb03-diag` | Diagnostic Setting | Routes ADF ActivityRuns, PipelineRuns, TriggerRuns, and AllMetrics to Log Analytics workspace |

---

## Architecture Diagram (text)

```
  Azure Subscription: 4f55df38-...
  └── Resource Group: healthcare-platform-dev-rg  [East US]
      │
      ├── ADLS Gen2: hcpdevuvxb03
      │   ├── Container: raw       ← source CSVs
      │   ├── Container: bronze    ← validated Parquet (partitioned)
      │   ├── Container: silver    ← PII-masked Parquet  ◄── Synapse primary
      │   ├── Container: gold      ← analytics tables
      │   └── Container: configs   ← lookup / config files
      │
      ├── Key Vault: hcp-dev-kv-uvxb03
      │   └── Access Policy → ADF managed identity (Get, List secrets)
      │
      ├── Data Factory: healthcare-platform-dev-adf-uvxb03
      │   ├── Identity: SystemAssigned
      │   ├── Role: Storage Blob Data Contributor → ADLS
      │   └── Diagnostics → Log Analytics
      │
      ├── Synapse Workspace: healthcare-platform-dev-syn-uvxb03
      │   ├── Primary storage: silver container
      │   ├── Identity: SystemAssigned
      │   ├── Role: Storage Blob Data Contributor → ADLS
      │   ├── Firewall: AllowAllWindowsAzureIps
      │   └── Spark Pool: sparkpooluvxb03 (MemoryOptimized, 3-5 nodes)
      │
      ├── Log Analytics: healthcare-platform-dev-law-uvxb03
      └── Application Insights: healthcare-platform-dev-ai-uvxb03
              └── Workspace-linked to Log Analytics
```

---

## Security Configuration

| Control | Setting | Notes |
|---------|---------|-------|
| TLS version | TLS 1.2 minimum | Enforced on ADLS storage account |
| Public blob access | Disabled | `allow_nested_items_to_be_public = false` |
| Key Vault soft-delete | Enabled (7 days) | Protection against accidental deletion |
| Key Vault purge protection | Disabled | Dev environment — enable for production |
| Managed Identity | SystemAssigned on ADF + Synapse | No credential hardcoding for service auth |
| Role assignments | Least-privilege (Blob Data Contributor) | ADF and Synapse scoped to storage account only |
| Synapse SQL admin | Credentials in Key Vault | Never committed to source control |

---

## Connectivity Endpoints

| Service | Endpoint |
|---------|----------|
| ADLS DFS | `https://hcpdevuvxb03.dfs.core.windows.net/` |
| Key Vault | `https://hcp-dev-kv-uvxb03.vault.azure.net/` |
| Synapse Dev | `https://healthcare-platform-dev-syn-uvxb03.dev.azuresynapse.net` |
| Synapse SQL | `healthcare-platform-dev-syn-uvxb03.sql.azuresynapse.net` |
| Synapse SQL Serverless | `healthcare-platform-dev-syn-uvxb03-ondemand.sql.azuresynapse.net` |
| Synapse Studio | `https://web.azuresynapse.net?workspace=.../healthcare-platform-dev-syn-uvxb03` |
