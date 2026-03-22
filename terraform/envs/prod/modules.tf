# ============================================================
# envs/prod/modules.tf
#
# Module wiring order (dependency chain):
#   networking → (no deps)
#   s3         → (no deps)
#   sns        → (no deps)
#   ecr        → (no deps)
#   kinesis    → (no deps)
#   iam        → s3, sns, kinesis
#   ecs        → networking, ecr, iam, kinesis
#   glue       → networking, iam, kinesis, s3
#   sagemaker  → networking, iam, s3
#   lambda     → networking, iam, s3, sns, sagemaker
#   redshift   → networking, iam, s3
#   cloudwatch → kinesis, glue, lambda, sagemaker, sns
# ============================================================

module "networking" {
  source  = "../../modules/networking"
  project = var.project
}

module "s3" {
  source  = "../../modules/s3"
  project = var.project
}

module "sns" {
  source            = "../../modules/sns"
  project           = var.project
  fraud_alert_email = var.fraud_alert_email
  ops_alert_email   = var.ops_alert_email
}

module "ecr" {
  source  = "../../modules/ecr"
  project = var.project
}

module "kinesis" {
  source  = "../../modules/kinesis"
  project = var.project
}

module "iam" {
  source         = "../../modules/iam"
  project        = var.project
  aws_region     = var.aws_region
  aws_account_id = var.aws_account_id
  github_org     = var.github_org
  github_repo    = var.github_repo
  tf_state_bucket = var.tf_state_bucket

  kinesis_stream_arn          = module.kinesis.stream_arn
  raw_bucket_arn              = module.s3.raw_bucket_arn
  predictions_bucket_arn      = module.s3.predictions_bucket_arn
  glue_assets_bucket_arn      = module.s3.glue_assets_bucket_arn
  model_artifacts_bucket_arn  = module.s3.model_artifacts_bucket_arn
  fraud_alerts_arn            = module.sns.fraud_alerts_arn
}

module "ecs" {
  source                  = "../../modules/ecs"
  project                 = var.project
  aws_region              = var.aws_region
  ecr_url                 = module.ecr.producer_url
  image_tag               = var.image_tag
  kinesis_stream_name     = module.kinesis.stream_name
  private_subnet_ids      = module.networking.private_subnet_ids
  ecs_sg_id               = module.networking.ecs_sg_id
  task_execution_role_arn = module.iam.ecs_task_execution_role_arn
  task_role_arn           = module.iam.ecs_task_role_arn
}

module "glue" {
  source              = "../../modules/glue"
  project             = var.project
  aws_region          = var.aws_region
  glue_role_arn       = module.iam.glue_role_arn
  kinesis_stream_name = module.kinesis.stream_name
  raw_bucket          = module.s3.raw_bucket_name
  glue_assets_bucket  = module.s3.glue_assets_bucket_name
}

module "sagemaker" {
  source                 = "../../modules/sagemaker"
  project                = var.project
  aws_region             = var.aws_region
  sagemaker_role_arn     = module.iam.sagemaker_role_arn
  model_artifacts_bucket = module.s3.model_artifacts_bucket_name
  subnet_ids             = module.networking.private_subnet_ids
  security_group_ids     = [module.networking.sagemaker_sg_id]
}

module "lambda" {
  source                  = "../../modules/lambda"
  project                 = var.project
  lambda_role_arn         = module.iam.lambda_role_arn
  sagemaker_endpoint_name = module.sagemaker.endpoint_name
  fraud_alerts_arn        = module.sns.fraud_alerts_arn
  raw_bucket              = module.s3.raw_bucket_name
  raw_bucket_arn          = module.s3.raw_bucket_arn
  predictions_bucket      = module.s3.predictions_bucket_name
  subnet_ids              = module.networking.private_subnet_ids
  security_group_ids      = [module.networking.lambda_sg_id]
}

module "redshift" {
  source             = "../../modules/redshift"
  project            = var.project
  admin_password     = var.redshift_admin_password
  subnet_ids         = module.networking.private_subnet_ids
  security_group_ids = [module.networking.redshift_sg_id]
  redshift_role_arn  = module.iam.redshift_role_arn
}

module "cloudwatch" {
  source                  = "../../modules/cloudwatch"
  project                 = var.project
  ops_alerts_arn          = module.sns.ops_alerts_arn
  kinesis_stream_name     = module.kinesis.stream_name
  glue_job_name           = module.glue.job_name
  lambda_function_name    = module.lambda.function_name
  sagemaker_endpoint_name = module.sagemaker.endpoint_name
}
