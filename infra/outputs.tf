output "function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.this.function_name
}

output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.this.repository_url
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.this.name
}

output "athena_database" {
  description = "Glue/Athena database name"
  value       = aws_glue_catalog_database.this.name
}
