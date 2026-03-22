# ============================================================
# envs/prod/main.tf
#
# Backend: S3 (tf state)
# Provider: AWS ap-southeast-2
# ============================================================

terraform {
  required_version = ">= 1.7.5"

  backend "s3" {
    key    = "frauddetection-kinesis-glue-tf-state "
    region = "ap-southeast-2"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}
