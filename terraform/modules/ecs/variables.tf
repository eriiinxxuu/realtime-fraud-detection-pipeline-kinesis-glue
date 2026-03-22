variable "project"               { type = string }
variable "aws_region"            { type = string }
variable "ecr_url"               { type = string }
variable "image_tag" { 
    type = string
    default = "latest" 
    }
variable "kinesis_stream_name"   { type = string }
variable "private_subnet_ids"    { type = list(string) }
variable "ecs_sg_id"             { type = string }
variable "task_execution_role_arn" { type = string }
variable "task_role_arn"         { type = string }
