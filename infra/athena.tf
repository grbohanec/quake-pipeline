# The Glue catalog database Athena's tables live in. The tables themselves are
# defined in sql/athena/create_tables.sql, next to the queries that use them.
resource "aws_glue_catalog_database" "quakes" {
  name        = "quakes"
  description = "Earthquake data lake: USGS and JMA events, quality check history."
}

# A workgroup with guardrails, so a bad query can't run up a bill:
# results always go to athena-results/ (expired by the bucket lifecycle rule),
# and any single query that would scan more than the limit is cancelled.
resource "aws_athena_workgroup" "quakes" {
  name        = "quakes"
  description = "Queries over the quake-pipeline data lake."

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_max_scan_gb * 1024 * 1024 * 1024

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
