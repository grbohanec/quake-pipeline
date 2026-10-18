terraform {
  required_version = ">= 1.10" # S3 backend with native state locking

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State lives in the project's own bucket, under a prefix the pipeline's
  # GitHub user cannot read or write (it is limited to data/*).
  backend "s3" {
    bucket       = "gabe-quake-pipeline"
    key          = "tfstate/quake-pipeline.tfstate"
    region       = "us-east-2"
    encrypt      = true
    use_lockfile = true # a lock file next to the state stops two applies at once
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "quake-pipeline"
      ManagedBy = "terraform"
    }
  }
}
