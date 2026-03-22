# ============================================================
# Kinesis Data Streams
# └── transactions stream
#     ├── 2 shards (PROVISIONED)
#     ├── 24h retention
#     └── KMS encryption
#
# ============================================================

resource "aws_kinesis_stream" "transactions" {
  name             = "${var.project}-transactions"
  shard_count      = 2
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  tags = { Name = "${var.project}-transactions-stream" }
}