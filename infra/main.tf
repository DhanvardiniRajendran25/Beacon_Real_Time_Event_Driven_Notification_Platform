terraform {
  required_version = ">= 1.3.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Local state — each teammate deploys independently in their own
  # Learner Lab account. Never commit terraform.tfstate to git.
  backend "local" {}
}

provider "aws" {
  region = var.aws_region

  # Learner Lab injects credentials via environment variables or
  # ~/.aws/credentials — no need to hardcode them here.
  # Just paste your Learner Lab credentials before running terraform apply.
}