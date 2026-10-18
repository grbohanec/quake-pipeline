variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-2"
}

variable "bucket_name" {
  description = "S3 bucket holding the data lake (data/), Athena results and Terraform state."
  type        = string
  default     = "gabe-quake-pipeline"
}

variable "github_user_name" {
  description = "IAM user whose access key the GitHub Actions workflows use."
  type        = string
  default     = "quake-pipeline-github"
}

variable "athena_results_retention_days" {
  description = "Days to keep Athena query result files before S3 deletes them."
  type        = number
  default     = 7
}

variable "athena_max_scan_gb" {
  description = "Per-query scan limit for the Athena workgroup, in GB. A query that would scan more is cancelled."
  type        = number
  default     = 1
}
