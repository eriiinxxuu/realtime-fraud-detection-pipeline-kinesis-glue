# ============================================================
# s3/main.tf
#
# S3 Buckets
# ├── model-artifacts      → stores trained LightGBM model (model.pkl)
# │                          SageMaker loads model from here at deploy time
# ├── raw-transactions     → Glue streaming output (features + raw data)
# │   └── lifecycle: 90d → STANDARD_IA
# ├── fraud-predictions    → Lambda writes fraud_pred=1 results
# │   └── lifecycle: 90d → STANDARD_IA → 365d → GLACIER
# └── glue-assets          → Glue job scripts + spark event logs
# ============================================================

resource "aws_s3_bucket" "model_artifacts" {
  bucket = "${var.project}-model-artifacts"
  tags   = { Name = "${var.project}-model-artifacts" }
}

resource "aws_s3_bucket_versioning" "model_artifacts" {
  bucket = aws_s3_bucket.model_artifacts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "model_artifacts" {
  bucket = aws_s3_bucket.model_artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket" "raw" {
  bucket = "${var.project}-raw-transactions"
  tags   = { Name = "${var.project}-raw-transactions" }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"
    filter {}

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket" "predictions" {
  bucket = "${var.project}-fraud-predictions"
  tags   = { Name = "${var.project}-fraud-predictions" }
}

resource "aws_s3_bucket_versioning" "predictions" {
  bucket = aws_s3_bucket.predictions.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "predictions" {
  bucket = aws_s3_bucket.predictions.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "predictions" {
  bucket = aws_s3_bucket.predictions.id

  rule {
    id     = "transition-to-ia-then-glacier"
    status = "Enabled"
    filter {}

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket" "glue_assets" {
  bucket = "${var.project}-glue-assets"
  tags   = { Name = "${var.project}-glue-assets" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_assets" {
  bucket = aws_s3_bucket.glue_assets.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
