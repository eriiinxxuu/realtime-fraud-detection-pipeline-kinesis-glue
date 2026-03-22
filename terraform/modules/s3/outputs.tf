output "model_artifacts_bucket_name" { value = aws_s3_bucket.model_artifacts.bucket }
output "model_artifacts_bucket_arn"  { value = aws_s3_bucket.model_artifacts.arn }
output "raw_bucket_name"             { value = aws_s3_bucket.raw.bucket }
output "raw_bucket_arn"              { value = aws_s3_bucket.raw.arn }
output "predictions_bucket_name"     { value = aws_s3_bucket.predictions.bucket }
output "predictions_bucket_arn"      { value = aws_s3_bucket.predictions.arn }
output "glue_assets_bucket_name"     { value = aws_s3_bucket.glue_assets.bucket }
output "glue_assets_bucket_arn"      { value = aws_s3_bucket.glue_assets.arn }
