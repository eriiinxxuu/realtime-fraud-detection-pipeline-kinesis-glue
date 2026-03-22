output "fraud_alerts_arn"  { value = aws_sns_topic.fraud_alerts.arn }
output "fraud_alerts_name" { value = aws_sns_topic.fraud_alerts.name }
output "ops_alerts_arn"    { value = aws_sns_topic.ops_alerts.arn }
output "ops_alerts_name"   { value = aws_sns_topic.ops_alerts.name }
