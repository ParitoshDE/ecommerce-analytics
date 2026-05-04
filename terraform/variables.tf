variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for the data lake (must be globally unique)"
  type        = string
}

variable "redshift_namespace_name" {
  description = "Redshift Serverless namespace name"
  type        = string
  default     = "olist-namespace"
}

variable "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name"
  type        = string
  default     = "olist-workgroup"
}

variable "redshift_db_name" {
  description = "Initial database name in Redshift Serverless"
  type        = string
  default     = "olist"
}

variable "redshift_admin_username" {
  description = "Redshift admin username"
  type        = string
  default     = "adminuser"
}

variable "redshift_admin_password" {
  description = "Redshift admin password (8-64 chars, mixed case + digit required)"
  type        = string
  sensitive   = true
}

variable "raw_schema" {
  description = "Redshift schema for raw Spark output"
  type        = string
  default     = "olist_raw"
}

variable "prod_schema" {
  description = "Redshift schema for dbt production models"
  type        = string
  default     = "olist_prod"
}
