output "function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.this.function_name
}

output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.this.repository_url
}
