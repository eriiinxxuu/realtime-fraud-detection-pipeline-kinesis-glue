# ============================================================
# glue/main.tf
#
# Glue Streaming Job
# └── fraud-streaming-features
#     ├── Glue 4.0 (Spark 3.3)
#     ├── G.1X worker (4 vCPU / 16 GB per worker)
#     ├── 2 workers
#     ├── reads from Kinesis Data Streams
#     ├── feature engineering (26 features, same as training)
#     └── writes enriched parquet to S3 raw-transactions/features/
#
# Glue Data Catalog
# └── fraud_detection_db
#
# CloudWatch Log Group
# └── /aws-glue/streaming/{project}
#
# Note: CloudWatch Alarms for Glue throughput in cloudwatch module
# ============================================================

resource "aws_glue_catalog_database" "main" {
  name = "${replace(var.project, "-", "_")}_db"
}

resource "aws_glue_job" "fraud_features" {
  name     = "${var.project}-streaming-features"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 2880
  max_retries       = 0

  command {
    name            = "gluestreaming"
    script_location = "s3://${var.glue_assets_bucket}/scripts/streaming_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/streaming/${var.project}"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.glue_assets_bucket}/spark-logs/"
    "--KINESIS_STREAM_NAME"              = var.kinesis_stream_name
    "--AWS_REGION"                       = var.aws_region
    "--S3_OUTPUT_PATH"                   = "s3://${var.raw_bucket}/features/"
    "--WINDOW_SIZE"                      = "30 seconds"
  }

  execution_property {
    max_concurrent_runs = 2
  }
}

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/streaming/${var.project}"
  retention_in_days = 7
  tags              = { Name = "${var.project}-glue-logs" }
}
