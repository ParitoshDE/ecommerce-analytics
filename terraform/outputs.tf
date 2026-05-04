output "s3_data_lake_bucket" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "redshift_endpoint" {
  description = "Redshift Serverless workgroup endpoint (use as REDSHIFT_HOST in .env)"
  value       = aws_redshiftserverless_workgroup.olist.endpoint[0].address
}

output "redshift_port" {
  description = "Redshift Serverless port"
  value       = 5439
}

output "redshift_iam_role_arn" {
  description = "IAM role ARN for Redshift COPY from S3 (use as IAM_ROLE_ARN in .env)"
  value       = aws_iam_role.redshift_s3_role.arn
}
