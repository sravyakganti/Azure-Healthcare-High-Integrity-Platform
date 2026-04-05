# ---------------------------------------------------------------------------
# Random suffix for globally unique resource names
# ---------------------------------------------------------------------------
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# ---------------------------------------------------------------------------
# Local computed values
# ---------------------------------------------------------------------------
locals {
  # Use override if provided, otherwise generate from project + environment
  resource_group_name = var.resource_group_name != "" ? var.resource_group_name : "${var.project_name}-${var.environment}-rg"

  # Storage account names must be 3-24 chars, lowercase alphanumeric only
  # Using short prefix "hcp" (healthcare-platform) to stay within the 24-char limit
  storage_account_name = "hcp${var.environment}${random_string.suffix.result}"

  # Key Vault names: 3-24 chars, alphanumeric and hyphens
  # Using short prefix "hcp" to stay within the 24-char limit
  key_vault_name = "hcp-${var.environment}-kv-${random_string.suffix.result}"

  # Data Factory name
  data_factory_name = "${var.project_name}-${var.environment}-adf-${random_string.suffix.result}"

  # Log Analytics workspace name
  log_analytics_name = "${var.project_name}-${var.environment}-law-${random_string.suffix.result}"

  # Application Insights name
  app_insights_name = "${var.project_name}-${var.environment}-ai-${random_string.suffix.result}"

  # Synapse workspace name
  synapse_workspace_name = "${var.project_name}-${var.environment}-syn-${random_string.suffix.result}"

  # Synapse Spark pool name (max 15 chars, alphanumeric only)
  spark_pool_name = "sparkpool${random_string.suffix.result}"

  # Merge default tags with environment-specific values
  common_tags = merge(var.tags, {
    Environment = var.environment
    Project     = var.project_name
    DeployedAt  = timestamp()
  })
}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "main" {
  name     = local.resource_group_name
  location = var.location
  tags     = local.common_tags
}

# ---------------------------------------------------------------------------
# Azure Data Lake Storage Gen2 (ADLS)
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "adls" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = var.adls_account_tier
  account_replication_type = var.adls_replication
  account_kind             = "StorageV2"

  # Enable Hierarchical Namespace for Data Lake Gen2
  is_hns_enabled = true

  # Security hardening
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# ADLS Gen2 Filesystem Containers (raw / bronze / silver / gold / configs)
# ---------------------------------------------------------------------------
resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "configs" {
  name               = "configs"
  storage_account_id = azurerm_storage_account.adls.id
}

# ---------------------------------------------------------------------------
# Azure Key Vault
# ---------------------------------------------------------------------------
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = local.key_vault_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = var.key_vault_sku

  # Disable purge protection for dev (enable for prod)
  purge_protection_enabled   = false
  soft_delete_retention_days = 7

  # Allow ARM template deployments to access secrets
  enabled_for_deployment          = false
  enabled_for_disk_encryption     = false
  enabled_for_template_deployment = true

  network_acls {
    bypass         = "AzureServices"
    default_action = "Allow"
  }

  # Grant the deploying principal full access
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get", "List", "Create", "Delete", "Update", "Import",
      "Backup", "Restore", "Recover", "Purge"
    ]

    secret_permissions = [
      "Get", "List", "Set", "Delete", "Backup", "Restore", "Recover", "Purge"
    ]

    certificate_permissions = [
      "Get", "List", "Create", "Delete", "Update", "Import",
      "Backup", "Restore", "Recover", "Purge"
    ]
  }

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# Azure Data Factory
# ---------------------------------------------------------------------------
resource "azurerm_data_factory" "main" {
  name                = local.data_factory_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  # Enable git integration scaffold (disabled — configure post-deploy)
  # vsts_configuration or github_configuration block can be added here

  tags = local.common_tags
}

# Grant ADF managed identity access to Key Vault secrets
resource "azurerm_key_vault_access_policy" "adf" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = azurerm_data_factory.main.identity[0].tenant_id
  object_id    = azurerm_data_factory.main.identity[0].principal_id

  secret_permissions = ["Get", "List"]
}

# Grant ADF managed identity Storage Blob Data Contributor on ADLS
resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# ---------------------------------------------------------------------------
# Log Analytics Workspace
# ---------------------------------------------------------------------------
resource "azurerm_log_analytics_workspace" "main" {
  name                = local.log_analytics_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_analytics_retention_days

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# Application Insights (linked to Log Analytics)
# ---------------------------------------------------------------------------
resource "azurerm_application_insights" "main" {
  name                = local.app_insights_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "other"

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# Synapse Workspace
# ---------------------------------------------------------------------------
resource "azurerm_synapse_workspace" "main" {
  name                                 = local.synapse_workspace_name
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = var.synapse_location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.silver.id
  sql_administrator_login              = var.synapse_sql_admin_login
  sql_administrator_login_password     = var.synapse_sql_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      # Ignore timestamp tag churn
      tags["DeployedAt"],
    ]
  }
}

# Grant Synapse managed identity Storage Blob Data Contributor
resource "azurerm_role_assignment" "synapse_storage" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.main.identity[0].principal_id
}

# Synapse firewall rule — allow Azure services
resource "azurerm_synapse_firewall_rule" "allow_azure_services" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

# ---------------------------------------------------------------------------
# Synapse Spark Pool
# ---------------------------------------------------------------------------
resource "azurerm_synapse_spark_pool" "main" {
  name                 = local.spark_pool_name
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  node_size_family     = "MemoryOptimized"
  node_size            = var.spark_pool_node_size

  auto_scale {
    max_node_count = var.spark_pool_max_nodes
    min_node_count = var.spark_pool_min_nodes
  }

  auto_pause {
    delay_in_minutes = var.spark_auto_pause_delay_minutes
  }

  spark_version = "3.3"

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      library_requirement,
    ]
  }
}

# ---------------------------------------------------------------------------
# Diagnostic Settings — send ADF logs to Log Analytics
# ---------------------------------------------------------------------------
resource "azurerm_monitor_diagnostic_setting" "adf" {
  name                       = "${local.data_factory_name}-diag"
  target_resource_id         = azurerm_data_factory.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "ActivityRuns"
  }

  enabled_log {
    category = "PipelineRuns"
  }

  enabled_log {
    category = "TriggerRuns"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}
