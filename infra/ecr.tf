# ECR repository for the WebSocket gateway service
resource "aws_ecr_repository" "gateway" {
  name                 = "${var.project_name}-gateway"
  image_tag_mutability = "MUTABLE" # allows overwriting the "latest" tag on each push

  image_scanning_configuration {
    scan_on_push = false # skip vulnerability scanning to keep deploys fast
  }

  tags = {
    Name    = "${var.project_name}-gateway"
    Project = var.project_name
  }
}

# ECR repository for the event ingestion API service
resource "aws_ecr_repository" "ingestion" {
  name                 = "${var.project_name}-ingestion"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = {
    Name    = "${var.project_name}-ingestion"
    Project = var.project_name
  }
}

# ECR repository for the fan-out worker service
resource "aws_ecr_repository" "fanout" {
  name                 = "${var.project_name}-fanout"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = {
    Name    = "${var.project_name}-fanout"
    Project = var.project_name
  }
}