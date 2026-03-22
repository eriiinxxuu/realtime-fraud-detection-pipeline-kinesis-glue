# ============================================================
# lambda/main.tf
#
# Lambda Function: invoke-endpoint
# ├── triggered by S3 PutObject on raw-transactions/features/
# ├── reads parquet file from S3
# ├── batches rows (1000 per call) → SageMaker endpoint
# ├── fraud_score >= threshold → SNS fraud-alerts
# └── writes all predictions → S3 fraud-predictions/
#
# Flow:
#   S3 (features/*.parquet)
#       → Lambda
#       → SageMaker endpoint (batched)
#       → fraud detected → SNS fraud-alerts
#       → write predictions → S3
#
# CloudWatch Alarms for Lambda errors in cloudwatch module
# ============================================================

resource "aws_lambda_function" "invoke_endpoint" {
  function_name = "${var.project}-invoke-endpoint"
  role          = var.lambda_role_arn
  handler       = "invoke_endpoint.handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 512

  filename         = "${path.module}/placeholder/invoke_endpoint.zip"
  source_code_hash = filebase64sha256("${path.module}/placeholder/invoke_endpoint.zip")

  environment {
    variables = {
      SAGEMAKER_ENDPOINT_NAME = var.sagemaker_endpoint_name
      SNS_TOPIC_ARN           = var.fraud_alerts_arn
      S3_PREDICTIONS_BUCKET   = var.predictions_bucket
      FRAUD_THRESHOLD         = var.fraud_threshold
      BATCH_SIZE              = "1000"
    }
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  tags = { Name = "${var.project}-invoke-endpoint" }
}

resource "aws_lambda_permission" "s3_trigger" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.invoke_endpoint.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.raw_bucket_arn
}

resource "aws_s3_bucket_notification" "glue_output_trigger" {
  bucket = var.raw_bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.invoke_endpoint.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "features/"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.s3_trigger]
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project}-invoke-endpoint"
  retention_in_days = 7
  tags              = { Name = "${var.project}-lambda-logs" }
}
