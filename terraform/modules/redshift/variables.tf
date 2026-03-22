variable "project"            { type = string }
variable "admin_password" { 
    type = string
    sensitive = true
    }
variable "subnet_ids"         { type = list(string) }
variable "security_group_ids" { type = list(string) }
variable "redshift_role_arn"  { type = string }
