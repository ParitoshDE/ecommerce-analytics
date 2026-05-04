terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "local" {}
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------
# Data sources — default VPC + subnets
# ---------------------------------------------------------------
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ---------------------------------------------------------------
# S3 Bucket — Data Lake
# ---------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket        = var.s3_bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    id     = "expire-old-objects"
    status = "Enabled"
    expiration {
      days = 90
    }
  }
}

# ---------------------------------------------------------------
# IAM Role — Redshift Serverless reads S3 via COPY command
# ---------------------------------------------------------------
resource "aws_iam_role" "redshift_s3_role" {
  name = "olist-redshift-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3" {
  role       = aws_iam_role.redshift_s3_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# ---------------------------------------------------------------
# Security Group — allow port 5439 for Redshift Serverless
# ---------------------------------------------------------------
resource "aws_security_group" "redshift_sg" {
  name        = "olist-redshift-sg"
  description = "Allow Redshift Serverless access on port 5439"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Redshift JDBC/psycopg2 access"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------------------------------------------
# Redshift Serverless — Namespace (database + credentials)
# ---------------------------------------------------------------
resource "aws_redshiftserverless_namespace" "olist" {
  namespace_name      = var.redshift_namespace_name
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3_role.arn]
}

# ---------------------------------------------------------------
# Redshift Serverless — Workgroup (compute + networking)
# ---------------------------------------------------------------
resource "aws_redshiftserverless_workgroup" "olist" {
  namespace_name      = aws_redshiftserverless_namespace.olist.namespace_name
  workgroup_name      = var.redshift_workgroup_name
  base_capacity       = 8
  publicly_accessible = true
  subnet_ids          = data.aws_subnets.default.ids
  security_group_ids  = [aws_security_group.redshift_sg.id]
}
