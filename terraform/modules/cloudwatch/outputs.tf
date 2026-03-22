output "kinesis_alarm_arn"    { value = aws_cloudwatch_metric_alarm.kinesis_consumer_lag.arn }
output "glue_alarm_arn"       { value = aws_cloudwatch_metric_alarm.glue_no_throughput.arn }
output "lambda_alarm_arn"     { value = aws_cloudwatch_metric_alarm.lambda_errors.arn }
output "sagemaker_alarm_arn"  { value = aws_cloudwatch_metric_alarm.sagemaker_latency.arn }
