# ============================================================
# networking/main.tf
#
# VPC
# └── Private Subnets (x2, one per AZ)
#     ├── Route Table (no internet route)
#     │
#     ├── Security Groups
#     │   ├── ecs-sg            → ECS producer (egress only)
#     │   ├── lambda-sg         → Lambda functions (egress only)
#     │   ├── sagemaker-sg      → SageMaker endpoint (port 443, from lambda-sg)
#     │   ├── redshift-sg       → Redshift Serverless (port 5439, VPC internal)
#     │   └── vpc-endpoints-sg  → Interface Endpoints (port 443, from ecs-sg + lambda-sg)
#     │
#     └── VPC Endpoints (all AWS traffic stays private)
#         ├── S3 Gateway              → Glue + Lambda → S3
#         ├── ECR API                 → ECS pull image metadata
#         ├── ECR Docker              → ECS pull image layers
#         ├── Kinesis Streams         → ECS producer → Kinesis
#         ├── SageMaker Runtime       → Lambda → SageMaker endpoint
#         ├── SNS                     → Lambda → fraud alerts
#         └── CloudWatch Logs         → ECS + Lambda + Glue → logs
#
# ============================================================

data "aws_availability_zones" "available" {}

resource "aws_vpc" "main" {
  cidr_block           = "10.1.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = "${var.project}-vpc" }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet("10.1.0.0/16", 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = { Name = "${var.project}-private-${count.index + 1}" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ── Security Groups ──────────────────────────────────────────

resource "aws_security_group" "ecs" {
  name        = "${var.project}-ecs-sg"
  description = "ECS producer - egress only"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-ecs-sg" }
}

resource "aws_security_group" "lambda" {
  name        = "${var.project}-lambda-sg"
  description = "Lambda - egress only"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-lambda-sg" }
}

resource "aws_security_group" "sagemaker" {
  name        = "${var.project}-sagemaker-sg"
  description = "SageMaker endpoint - accept from lambda-sg only"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-sagemaker-sg" }
}

resource "aws_security_group" "redshift" {
  name        = "${var.project}-redshift-sg"
  description = "Redshift Serverless - VPC internal only"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.1.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-redshift-sg" }
}

resource "aws_security_group" "vpc_endpoints" {
  name        = "${var.project}-vpc-endpoints-sg"
  description = "VPC Interface Endpoints - accept from ecs-sg and lambda-sg"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [
      aws_security_group.ecs.id,
      aws_security_group.lambda.id
    ]
  }

  tags = { Name = "${var.project}-vpc-endpoints-sg" }
}

# ── VPC Endpoints ─────────────────────────────────────────────

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.ap-southeast-2.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
  tags              = { Name = "${var.project}-s3-endpoint" }
}

resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.ap-southeast-2.ecr.api"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${var.project}-ecr-api-endpoint" }
}

resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.ap-southeast-2.ecr.dkr"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${var.project}-ecr-dkr-endpoint" }
}

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.ap-southeast-2.kinesis-streams"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${var.project}-kinesis-endpoint" }
}

resource "aws_vpc_endpoint" "sagemaker_runtime" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.ap-southeast-2.sagemaker.runtime"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${var.project}-sagemaker-runtime-endpoint" }
}

resource "aws_vpc_endpoint" "sns" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.ap-southeast-2.sns"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${var.project}-sns-endpoint" }
}

resource "aws_vpc_endpoint" "cloudwatch_logs" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.ap-southeast-2.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${var.project}-cloudwatch-logs-endpoint" }
}
