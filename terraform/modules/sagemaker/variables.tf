variable "project"                  { type = string }
variable "aws_region"               { type = string }
variable "sagemaker_role_arn"       { type = string }
variable "model_artifacts_bucket"   { type = string }
variable "subnet_ids"               { type = list(string) }
variable "security_group_ids"       { type = list(string) }
