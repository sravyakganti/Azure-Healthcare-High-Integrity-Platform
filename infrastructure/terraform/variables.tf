variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  type        = string
  sensitive   = true
}

variable "project_name" {
  description = "Name of the project used as a prefix for all resources"
  type        = string
  default     = "healthcare-platform"

  validation {
    condition     = length(var.project_name) <= 20 && can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "project_name must be lowercase alphanumeric with hyphens, max 20 characters."
  }
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "East US"
}

variable "synapse_location" {
  description = "Azure region for Synapse workspace (may differ from main location if SQL provisioning is restricted)"
  type        = string
  default     = "West US 2"
}

variable "resource_group_name" {
  description = "Override for the resource group name. If empty, a name is generated from project_name and environment."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Map of tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "healthcare-platform"
    Environment = "dev"
    ManagedBy   = "Terraform"
    Owner       = "DataEngineering"
    CostCenter  = "Healthcare-IT"
  }
}

variable "adls_account_tier" {
  description = "Storage account performance tier (Standard or Premium)"
  type        = string
  default     = "Standard"

  validation {
    condition     = contains(["Standard", "Premium"], var.adls_account_tier)
    error_message = "adls_account_tier must be Standard or Premium."
  }
}

variable "adls_replication" {
  description = "Storage account replication type (LRS, GRS, RAGRS, ZRS)"
  type        = string
  default     = "LRS"

  validation {
    condition     = contains(["LRS", "GRS", "RAGRS", "ZRS", "GZRS", "RAGZRS"], var.adls_replication)
    error_message = "adls_replication must be one of: LRS, GRS, RAGRS, ZRS, GZRS, RAGZRS."
  }
}

variable "synapse_sql_admin_login" {
  description = "SQL administrator login for the Synapse workspace"
  type        = string
  default     = "sqladminuser"
}

variable "synapse_sql_admin_password" {
  description = "SQL administrator password for the Synapse workspace"
  type        = string
  sensitive   = true
}

variable "key_vault_sku" {
  description = "Key Vault SKU (standard or premium)"
  type        = string
  default     = "standard"

  validation {
    condition     = contains(["standard", "premium"], var.key_vault_sku)
    error_message = "key_vault_sku must be standard or premium."
  }
}

variable "log_analytics_retention_days" {
  description = "Number of days to retain logs in Log Analytics workspace"
  type        = number
  default     = 30

  validation {
    condition     = var.log_analytics_retention_days >= 30 && var.log_analytics_retention_days <= 730
    error_message = "log_analytics_retention_days must be between 30 and 730."
  }
}

variable "spark_pool_node_size" {
  description = "Node size for the Synapse Spark pool"
  type        = string
  default     = "Small"
}

variable "spark_pool_min_nodes" {
  description = "Minimum node count for the Synapse Spark auto-scale pool"
  type        = number
  default     = 1
}

variable "spark_pool_max_nodes" {
  description = "Maximum node count for the Synapse Spark auto-scale pool"
  type        = number
  default     = 3
}

variable "spark_auto_pause_delay_minutes" {
  description = "Number of idle minutes before the Spark pool auto-pauses"
  type        = number
  default     = 15
}
