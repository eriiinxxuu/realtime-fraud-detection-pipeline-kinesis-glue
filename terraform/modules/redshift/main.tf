# ============================================================
# redshift/main.tf
#
# Redshift Serverless
# └── fraud-detection namespace + workgroup
#     ├── base capacity: 8 RPU
#     ├── database: fraud_detection
#     ├── private subnets only
#     └── IAM role for S3 COPY from predictions bucket
#
# Usage:
#   COPY fraud_predictions
#   FROM 's3://{predictions_bucket}/predictions/'
#   IAM_ROLE '{redshift_role_arn}'
#   FORMAT PARQUET;
# ============================================================

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project}-namespace"
  admin_username      = "admin"
  admin_user_password = var.admin_password
  db_name             = "fraud_detection"
  iam_roles           = [var.redshift_role_arn]

  tags = { Name = "${var.project}-namespace" }
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name      = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name      = "${var.project}-workgroup"
  base_capacity       = 8
  publicly_accessible = false
  subnet_ids          = var.subnet_ids
  security_group_ids  = var.security_group_ids

  tags = { Name = "${var.project}-workgroup" }
}
