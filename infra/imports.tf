# The bucket, the GitHub user and the Glue database were first created by
# hand in the AWS console. These blocks adopt them into Terraform state on
# the first apply, with no downtime and nothing recreated. Once applied they
# are no-ops; they stay as a record of what existed before Terraform.
#
# New in Terraform: the lifecycle rules, the Athena workgroup, and the
# pipeline user's inline policy (replacing the one made by hand).

data "aws_caller_identity" "current" {}

import {
  to = aws_s3_bucket.lake
  id = var.bucket_name
}

# S3 gives every new bucket these three settings by default.
import {
  to = aws_s3_bucket_public_access_block.lake
  id = var.bucket_name
}

import {
  to = aws_s3_bucket_ownership_controls.lake
  id = var.bucket_name
}

import {
  to = aws_s3_bucket_server_side_encryption_configuration.lake
  id = var.bucket_name
}

import {
  to = aws_iam_user.github
  id = var.github_user_name
}

# Created by CREATE DATABASE in sql/athena/create_tables.sql.
import {
  to = aws_glue_catalog_database.quakes
  id = "${data.aws_caller_identity.current.account_id}:quakes"
}
