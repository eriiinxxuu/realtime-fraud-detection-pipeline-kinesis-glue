# ============================================================
#
# ECS Cluster
# └── producer service (x2)
#     ├── Fargate ARM64
#     ├── 0.5 vCPU / 1 GB
#     └── writes fake transactions to Kinesis Data Streams
#
# CloudWatch Log Group
# └── /ecs/{project}/producer
# ============================================================

resource "aws_ecs_cluster" "main" {
  name = var.project

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = { Name = "${var.project}-cluster" }
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name       = aws_ecs_cluster.main.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }
}

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/ecs/${var.project}/producer"
  retention_in_days = 7
  tags              = { Name = "${var.project}-producer-logs" }
}

# ============================================================
# SERVICE: producer (x2)
# ============================================================

resource "aws_ecs_task_definition" "producer" {
  family                   = "${var.project}-producer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = var.task_execution_role_arn
  task_role_arn            = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([{
    name      = "producer"
    image     = "${var.ecr_url}:${var.image_tag}"
    essential = true

    environment = [
      { name = "AWS_DEFAULT_REGION",  value = var.aws_region },
      { name = "KINESIS_STREAM_NAME", value = var.kinesis_stream_name },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${var.project}/producer"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])

  tags = { Name = "${var.project}-producer" }
}

resource "aws_ecs_service" "producer" {
  name                 = "producer"
  cluster              = aws_ecs_cluster.main.id
  task_definition      = aws_ecs_task_definition.producer.arn
  desired_count        = 2
  launch_type          = "FARGATE"
  force_new_deployment = true

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [var.ecs_sg_id]
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  lifecycle {
    ignore_changes = [desired_count]
  }

  tags = { Name = "${var.project}-producer" }
}
