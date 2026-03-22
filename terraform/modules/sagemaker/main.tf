# ============================================================
# sagemaker/main.tf
#
# SageMaker Real-time Endpoint
# └── fraud-detection-endpoint
#     ├── model: LightGBM (model.pkl → model.tar.gz in S3)
#     ├── instance: ml.t2.medium (1 instance)
#     ├── container: sklearn inference image (AWS managed)
#     └── invoked by Lambda after Glue writes features to S3
#
# Flow:
#   S3 (features) → Lambda → SageMaker endpoint → fraud_score
#
# Note: Before terraform apply, upload model to S3:
#   tar -czf model.tar.gz model.pkl
#   aws s3 cp model.tar.gz s3://{project}-model-artifacts/model/model.tar.gz
#
# CloudWatch Alarms for endpoint latency in cloudwatch module
# ============================================================

data "aws_region" "current" {}

locals {
  sklearn_image = "246618743249.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3"
}

resource "aws_sagemaker_model" "fraud" {
  name               = "${var.project}-fraud-model"
  execution_role_arn = var.sagemaker_role_arn

  primary_container {
    image          = local.sklearn_image
    model_data_url = "s3://${var.model_artifacts_bucket}/model/model.tar.gz"

    environment = {
      SAGEMAKER_PROGRAM             = "inference.py"
      SAGEMAKER_SUBMIT_DIRECTORY    = "/opt/ml/code"
    }
  }

  vpc_config {
    subnets            = var.subnet_ids
    security_group_ids = var.security_group_ids
  }
}

resource "aws_sagemaker_endpoint_configuration" "fraud" {
  name = "${var.project}-endpoint-config"

  production_variants {
    variant_name           = "primary"
    model_name             = aws_sagemaker_model.fraud.name
    initial_instance_count = 1
    instance_type          = "ml.t2.medium"
  }
}

resource "aws_sagemaker_endpoint" "fraud" {
  name                 = "${var.project}-endpoint"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.fraud.name

  tags = { Name = "${var.project}-fraud-endpoint" }
}

resource "aws_cloudwatch_log_group" "sagemaker" {
  name              = "/aws/sagemaker/Endpoints/${var.project}-endpoint"
  retention_in_days = 7
  tags              = { Name = "${var.project}-sagemaker-logs" }
}
