# The identity GitHub Actions uses. This AWS account can't create OIDC
# identity providers, so the workflows authenticate with this user's access
# key (stored as GitHub secrets). The key itself is created by hand, not here,
# so it never appears in Terraform state.

resource "aws_iam_user" "github" {
  name = var.github_user_name
}

# Least privilege: list the data/ prefix, and read, write and delete objects
# under it. Nothing else in the bucket (Athena results, Terraform state) and
# no other AWS service.
data "aws_iam_policy_document" "github_data_access" {
  statement {
    sid       = "ListDataPrefix"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["data/*", "data/"]
    }
  }

  statement {
    sid       = "ReadWriteData"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/data/*"]
  }
}

resource "aws_iam_user_policy" "github_data_access" {
  name   = "quake-pipeline-s3"
  user   = aws_iam_user.github.name
  policy = data.aws_iam_policy_document.github_data_access.json
}
