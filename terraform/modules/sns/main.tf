# ============================================================
# sns/main.tf
#
# SNS Topics
# ├── fraud-alerts    → Lambda publishes when fraud_score >= threshold
# │                     notifies fraud ops team in real-time
# │
# └── ops-alerts      → CloudWatch Alarms publish on pipeline issues
#                       ├── Kinesis consumer lag > 1 min
#                       ├── Glue throughput divergence
#                       ├── Lambda error rate high
#                       └── SageMaker endpoint latency high
# ============================================================

resource "aws_sns_topic" "fraud_alerts" {
  name = "${var.project}-fraud-alerts"
  tags = { Name = "${var.project}-fraud-alerts" }
}

resource "aws_sns_topic" "ops_alerts" {
  name = "${var.project}-ops-alerts"
  tags = { Name = "${var.project}-ops-alerts" }
}

# ── Email subscriptions (optional) ───────────────────────────

resource "aws_sns_topic_subscription" "fraud_email" {
  count     = var.fraud_alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.fraud_alerts.arn
  protocol  = "email"
  endpoint  = var.fraud_alert_email
}

resource "aws_sns_topic_subscription" "ops_email" {
  count     = var.ops_alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.ops_alerts.arn
  protocol  = "email"
  endpoint  = var.ops_alert_email
}
