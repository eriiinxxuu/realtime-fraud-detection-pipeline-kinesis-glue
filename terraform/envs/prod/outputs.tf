# ============================================================
# envs/prod/outputs.tf
# ============================================================

output "kinesis_stream_name"      { value = module.kinesis.stream_name }
output "ecr_producer_url"         { value = module.ecr.producer_url }
output "glue_job_name"            { value = module.glue.job_name }
output "sagemaker_endpoint_name"  { value = module.sagemaker.endpoint_name }
output "lambda_function_name"     { value = module.lambda.function_name }
output "s3_raw_bucket"            { value = module.s3.raw_bucket_name }
output "s3_predictions_bucket"    { value = module.s3.predictions_bucket_name }
output "s3_model_artifacts"       { value = module.s3.model_artifacts_bucket_name }
output "s3_glue_assets"           { value = module.s3.glue_assets_bucket_name }
output "sns_fraud_alerts_arn"     { value = module.sns.fraud_alerts_arn }
output "sns_ops_alerts_arn"       { value = module.sns.ops_alerts_arn }
output "redshift_workgroup"       { value = module.redshift.workgroup_name }
output "redshift_endpoint"        { value = module.redshift.workgroup_endpoint }
output "github_actions_role_arn"  { value = module.iam.github_actions_role_arn }
