# ============================================================
#
# ECR Repositories
# └── producer
#     ├── scan on push enabled
#     ├── AES256 encryption
#     └── lifecycle: keep last 3 tagged, remove untagged after 7 days
# ============================================================

resource "aws_ecr_repository" "producer" {
  name                 = "${var.project}-producer"
  force_delete = true
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = { Name = "${var.project}-producer" }
}

resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 3 tagged images (sha- or v prefix)"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["sha-", "v"]
          countType     = "imageCountMoreThan"
          countNumber   = 3
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Remove untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = { type = "expire" }
      }
    ]
  })
}
