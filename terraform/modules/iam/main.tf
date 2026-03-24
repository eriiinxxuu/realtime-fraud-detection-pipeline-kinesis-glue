# ============================================================
# iam/main.tf
#
# IAM Roles
# ├── ecs-task-execution-role  → ECS platform: pull ECR, write CloudWatch logs
# ├── ecs-task-role            → producer runtime: write to Kinesis
# ├── glue-role                → Glue job: read Kinesis, read/write S3
# ├── sagemaker-role           → SageMaker: read S3 model artifacts
# ├── lambda-role              → Lambda: read S3, invoke SageMaker,
# │                              publish SNS, write S3 predictions
# ├── redshift-role            → Redshift: read S3 predictions (COPY)
# └── github-actions-role      → CI/CD: push ECR, update ECS (OIDC)
# ============================================================

# ── Trust policy helpers ──────────────────────────────────────

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "sagemaker_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = ["arn:aws:iam::${var.aws_account_id}:oidc-provider/token.actions.githubusercontent.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_org}/${var.github_repo}:*"]
    }
  }
}

# ── ECS Task Execution Role ───────────────────────────────────

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${var.project}-ecs-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ── ECS Task Role (producer runtime) ─────────────────────────

resource "aws_iam_role" "ecs_task" {
  name               = "${var.project}-ecs-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "ecs_task_kinesis" {
  name = "${var.project}-ecs-task-kinesis"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kinesis:PutRecord",
        "kinesis:PutRecords",
        "kinesis:DescribeStream",
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards"
      ]
      Resource = [var.kinesis_stream_arn]
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_cloudwatch" {
  name = "${var.project}-ecs-task-cloudwatch"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/ecs/${var.project}/*"
    }]
  })
}

# ── Glue Role ─────────────────────────────────────────────────

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_kinesis" {
  name = "${var.project}-glue-kinesis"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:DescribeStream",
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards",
        "kinesis:ListStreams"
      ]
      Resource = [var.kinesis_stream_arn]
    }]
  })
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${var.project}-glue-s3"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        var.raw_bucket_arn,
        "${var.raw_bucket_arn}/*",
        var.glue_assets_bucket_arn,
        "${var.glue_assets_bucket_arn}/*"
      ]
    }]
  })
}

# ── SageMaker Role ────────────────────────────────────────────

resource "aws_iam_role" "sagemaker" {
  name               = "${var.project}-sagemaker-role"
  assume_role_policy = data.aws_iam_policy_document.sagemaker_assume.json
}

resource "aws_iam_role_policy" "sagemaker_s3" {
  name = "${var.project}-sagemaker-s3"
  role = aws_iam_role.sagemaker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        var.model_artifacts_bucket_arn,
        "${var.model_artifacts_bucket_arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "sagemaker_cloudwatch" {
  name = "${var.project}-sagemaker-cloudwatch"
  role = aws_iam_role.sagemaker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/sagemaker/*"
    }]
  })
}

resource "aws_iam_role_policy" "sagemaker_ecr" {
  name = "${var.project}-sagemaker-ecr"
  role = aws_iam_role.sagemaker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = [
        "ecr:GetAuthorizationToken",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "sagemaker_vpc" {
  name = "${var.project}-sagemaker-vpc"
  role = aws_iam_role.sagemaker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeVpcs",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeDhcpOptions",
          "ec2:DescribeRouteTables",
          "ec2:DescribeVpcEndpoints"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:CreateNetworkInterface"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "aws:RequestedRegion" = var.aws_region
          }
        }
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:DeleteNetworkInterface"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:CreateNetworkInterfacePermission"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "ec2:AuthorizedService" = "sagemaker.amazonaws.com"
          }
        }
      }
    ]
  })
}

# ── Lambda Role ───────────────────────────────────────────────

resource "aws_iam_role" "lambda" {
  name               = "${var.project}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project}-lambda-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = [
          var.predictions_bucket_arn,
          "${var.predictions_bucket_arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["sagemaker:InvokeEndpoint"]
        Resource = "arn:aws:sagemaker:${var.aws_region}:${var.aws_account_id}:endpoint/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [var.fraud_alerts_arn]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface"
        ]
        Resource = "*"
      }
    ]
  })
}

# ── Redshift Role ─────────────────────────────────────────────

resource "aws_iam_role" "redshift" {
  name               = "${var.project}-redshift-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "${var.project}-redshift-s3"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        var.predictions_bucket_arn,
        "${var.predictions_bucket_arn}/*"
      ]
    }]
  })
}

# ── GitHub Actions Role (OIDC) ────────────────────────────────

# GitHub Actions OIDC Provider
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

resource "aws_iam_role" "github_actions" {
  name               = "${var.project}-github-actions"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

resource "aws_iam_role_policy_attachment" "github_actions_admin" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

# resource "aws_iam_role_policy" "github_actions_policy" {
#   name = "${var.project}-github-actions-policy"
#   role = aws_iam_role.github_actions.id

#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "ecr:GetAuthorizationToken",
#           "ecr:BatchCheckLayerAvailability",
#           "ecr:GetDownloadUrlForLayer",
#           "ecr:BatchGetImage",
#           "ecr:InitiateLayerUpload",
#           "ecr:UploadLayerPart",
#           "ecr:CompleteLayerUpload",
#           "ecr:PutImage"
#         ]
#         Resource = "*"
#       },
#           {
#       Effect = "Allow"
#       Action = [
#         "ecs:UpdateService",
#         "ecs:DescribeServices",
#         "ecs:RegisterTaskDefinition",
#         "ecs:DescribeTaskDefinition",
#          "ecs:ListTaskDefinitions"
#       ]
#       Resource = "*"
#     },
#      {
#         Effect = "Allow"
#         Action = [
#           "s3:GetObject",
#           "s3:PutObject",
#           "s3:ListBucket",
#           "s3:DeleteObject"
#         ]
#         Resource = [
#           "arn:aws:s3:::${var.tf_state_bucket}",
#           "arn:aws:s3:::${var.tf_state_bucket}/*"
#         ]
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "s3:GetObject",
#           "s3:PutObject",
#           "s3:ListBucket"
#         ]
#         Resource = [
#           var.glue_assets_bucket_arn,
#           "${var.glue_assets_bucket_arn}/*"
#         ]
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "glue:GetJob",
#           "glue:GetJobRun",
#           "glue:GetJobRuns",
#           "glue:StartJobRun",
#           "glue:BatchStopJobRun"
#         ]
#         Resource = "*"
#       }
#     ]
#   })
# }