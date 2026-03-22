# ============================================================
# envs/prod/variables.tf
# ============================================================

variable "aws_region" {
  default = "ap-southeast-2"
}

variable "aws_account_id" {
  type = string
}

variable "project" {
  default = "fraud-detection-kinesis-glue"
}

variable "github_org" {
  type = string
}

variable "github_repo" {
  type = string
}

variable "tf_state_bucket" {
  type = string
}

variable "redshift_admin_password" {
  type      = string
  sensitive = true
}

variable "fraud_alert_email" {
  type    = string
  default = ""
}

variable "ops_alert_email" {
  type    = string
  default = ""
}

variable "image_tag" {
  type    = string
  default = "latest"
}
