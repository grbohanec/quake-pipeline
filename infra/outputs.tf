output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "github_user_arn" {
  value = aws_iam_user.github.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.quakes.name
}
