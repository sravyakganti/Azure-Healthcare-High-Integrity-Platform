output "resource_group_name" {
  description = "Name of the deployed Azure Resource Group"
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account"
  value       = azurerm_storage_account.adls.name
}

output "storage_account_primary_dfs_endpoint" {
  description = "Primary DFS (Data Lake) endpoint for the ADLS Gen2 storage account"
  value       = azurerm_storage_account.adls.primary_dfs_endpoint
}

output "storage_account_primary_connection_string" {
  description = "Primary connection string for the ADLS Gen2 storage account"
  value       = azurerm_storage_account.adls.primary_connection_string
  sensitive   = true
}

output "data_factory_name" {
  description = "Name of the Azure Data Factory instance"
  value       = azurerm_data_factory.main.name
}

output "data_factory_id" {
  description = "Resource ID of the Azure Data Factory instance"
  value       = azurerm_data_factory.main.id
}

output "data_factory_principal_id" {
  description = "Principal ID of the Data Factory system-assigned managed identity"
  value       = azurerm_data_factory.main.identity[0].principal_id
}

output "synapse_workspace_name" {
  description = "Name of the Azure Synapse Analytics workspace"
  value       = azurerm_synapse_workspace.main.name
}

output "synapse_workspace_id" {
  description = "Resource ID of the Azure Synapse Analytics workspace"
  value       = azurerm_synapse_workspace.main.id
}

output "synapse_connectivity_endpoints" {
  description = "Connectivity endpoints map for the Synapse workspace"
  value       = azurerm_synapse_workspace.main.connectivity_endpoints
}

output "synapse_spark_pool_name" {
  description = "Name of the Synapse Spark pool"
  value       = azurerm_synapse_spark_pool.main.name
}

output "key_vault_id" {
  description = "Resource ID of the Azure Key Vault"
  value       = azurerm_key_vault.main.id
}

output "key_vault_uri" {
  description = "URI of the Azure Key Vault (used to reference secrets)"
  value       = azurerm_key_vault.main.vault_uri
}

output "log_analytics_workspace_id" {
  description = "Resource ID of the Log Analytics workspace"
  value       = azurerm_log_analytics_workspace.main.id
}

output "log_analytics_workspace_key" {
  description = "Primary shared key for the Log Analytics workspace"
  value       = azurerm_log_analytics_workspace.main.primary_shared_key
  sensitive   = true
}

output "application_insights_instrumentation_key" {
  description = "Instrumentation key for Application Insights"
  value       = azurerm_application_insights.main.instrumentation_key
  sensitive   = true
}

output "application_insights_connection_string" {
  description = "Connection string for Application Insights"
  value       = azurerm_application_insights.main.connection_string
  sensitive   = true
}

output "application_insights_app_id" {
  description = "App ID of the Application Insights resource"
  value       = azurerm_application_insights.main.app_id
}

output "random_suffix" {
  description = "Random suffix appended to globally unique resource names"
  value       = random_string.suffix.result
}
