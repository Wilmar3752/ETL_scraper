terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

locals {
  parquet_serde         = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  parquet_input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

  athena_columns = [
    { name = "product",          type = "string" },
    { name = "price",            type = "bigint" },
    { name = "link",             type = "string" },
    { name = "years",            type = "bigint" },
    { name = "_created",         type = "string" },
    { name = "description",      type = "string" },
    { name = "color",            type = "string" },
    { name = "body_type",        type = "string" },
    { name = "fuel_type",        type = "string" },
    { name = "num_doors",        type = "double" },
    { name = "engine",           type = "double" },
    { name = "transmission",     type = "string" },
    { name = "image_url",        type = "string" },
    { name = "sku",              type = "string" },
    { name = "item_condition",   type = "string" },
    { name = "year",             type = "double" },
    { name = "version",          type = "string" },
    { name = "horsepower",       type = "string" },
    { name = "seating_capacity", type = "double" },
    { name = "traction_control", type = "string" },
    { name = "steering",         type = "string" },
    { name = "last_plate_digit", type = "double" },
    { name = "plate_parity",     type = "string" },
    { name = "single_owner",     type = "string" },
    { name = "negotiable_price", type = "string" },
    { name = "vehicle_brand",    type = "string" },
    { name = "vehicle_line",     type = "string" },
    { name = "id",               type = "bigint" },
    { name = "mileage",          type = "bigint" },
    { name = "location_city2",   type = "string" },
    { name = "location_city",    type = "string" },
    { name = "json_ld_extra",    type = "string" },
    { name = "specs_extra",      type = "string" },
  ]
}

provider "aws" {
  region = var.aws_region
}

# ECR Repository
resource "aws_ecr_repository" "this" {
  name                 = var.project_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "s3_put" {
  name = "${var.project_name}-s3-put"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "arn:aws:s3:::scraper-meli/*"
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "this" {
  function_name = var.project_name
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.this.repository_url}:latest"
  memory_size   = var.memory_size
  timeout       = var.timeout

  environment {
    variables = {
      SCRAPER_API_URL = var.scraper_api_url
      API_KEY         = var.api_key
    }
  }

  lifecycle {
    ignore_changes = [image_uri]
  }
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}"
  retention_in_days = 7
}

# EventBridge Schedule Rule
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.project_name}-daily"
  description         = "Trigger ${var.project_name} Lambda daily at 3 AM UTC"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule = aws_cloudwatch_event_rule.daily.name
  arn  = aws_lambda_function.this.arn
}

# ─── Athena ───────────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "this" {
  name = var.project_name

  configuration {
    result_configuration {
      output_location = "s3://scraper-meli/athena-results/"
    }
  }
}

resource "aws_glue_catalog_database" "this" {
  name = "scraper_meli"
}

resource "aws_glue_catalog_table" "carros_daily" {
  name          = "carros_daily"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://scraper-meli/carros/"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.athena_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "carros_initial" {
  name          = "carros_initial"
  database_name = aws_glue_catalog_database.this.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://scraper-meli/initial_load/"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.athena_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# Named query con el SQL de la vista carros (carros_daily + carros_initial)
resource "aws_athena_named_query" "create_carros_view" {
  name      = "create_carros_view"
  workgroup = aws_athena_workgroup.this.name
  database  = aws_glue_catalog_database.this.name
  query     = <<-SQL
    CREATE OR REPLACE VIEW carros AS
    SELECT *, 'daily'   AS source FROM carros_daily
    UNION ALL
    SELECT *, 'initial' AS source FROM carros_initial
  SQL
}

# ──────────────────────────────────────────────────────────────────────────────

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}
