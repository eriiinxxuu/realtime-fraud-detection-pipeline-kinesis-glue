# ============================================================
# cloudwatch/main.tf
#
# CloudWatch Alarms → SNS ops-alerts
# ├── Kinesis: IteratorAgeMilliseconds > 60s (consumer lag)
# ├── Glue: glue.driver.aggregate.recordsRead = 0 for 5min (no throughput)
# ├── Lambda: Errors > 5 in 5min (invoke endpoint failures)
# └── SageMaker: ModelLatency > 2000ms (endpoint slow)
# ============================================================

# ── Kinesis: consumer lag ─────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "kinesis_consumer_lag" {
  alarm_name          = "${var.project}-kinesis-consumer-lag"
  alarm_description   = "Kinesis consumer lag > 1 minute — Glue may be falling behind"
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  dimensions          = { StreamName = var.kinesis_stream_name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 60000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [var.ops_alerts_arn]
  ok_actions    = [var.ops_alerts_arn]
}

# ── Glue: throughput divergence ───────────────────────────────

resource "aws_cloudwatch_metric_alarm" "glue_no_throughput" {
  alarm_name          = "${var.project}-glue-no-throughput"
  alarm_description   = "Glue streaming job reading 0 records — job may have stopped"
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.recordsRead"
  dimensions          = { JobName = var.glue_job_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 0
  comparison_operator = "LessThanOrEqualToThreshold"
  treat_missing_data  = "breaching"

  alarm_actions = [var.ops_alerts_arn]
  ok_actions    = [var.ops_alerts_arn]
}

# ── Lambda: error rate ────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project}-lambda-errors"
  alarm_description   = "Lambda invoke-endpoint errors > 5 in 5 minutes"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = var.lambda_function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 5
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [var.ops_alerts_arn]
  ok_actions    = [var.ops_alerts_arn]
}

# ── SageMaker: endpoint latency ───────────────────────────────

resource "aws_cloudwatch_metric_alarm" "sagemaker_latency" {
  alarm_name          = "${var.project}-sagemaker-latency"
  alarm_description   = "SageMaker endpoint p99 latency > 2000ms"
  namespace           = "AWS/SageMaker"
  metric_name         = "ModelLatency"
  dimensions          = {
    EndpointName = var.sagemaker_endpoint_name
    VariantName  = "primary"
  }
  extended_statistic  = "p99"
  period              = 60
  evaluation_periods  = 5
  threshold           = 2000000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [var.ops_alerts_arn]
  ok_actions    = [var.ops_alerts_arn]
}
