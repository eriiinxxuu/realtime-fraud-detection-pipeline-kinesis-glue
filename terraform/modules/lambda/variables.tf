variable "project"                 { type = string }
variable "lambda_role_arn"         { type = string }
variable "sagemaker_endpoint_name" { type = string }
variable "fraud_alerts_arn"        { type = string }
variable "raw_bucket"              { type = string }
variable "raw_bucket_arn"          { type = string }
variable "predictions_bucket"      { type = string }
variable "subnet_ids"              { type = list(string) }
variable "security_group_ids"      { type = list(string) }
variable "fraud_threshold" { 
    type = string
    default = "0.86" 
    }
