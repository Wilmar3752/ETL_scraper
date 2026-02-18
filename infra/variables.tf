variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "etl-scraper"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 512
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300
}

variable "schedule_expression" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "cron(0 3 * * ? *)"
}

variable "scraper_api_url" {
  description = "Base URL of the scraper API"
  type        = string
  sensitive   = true
}

variable "api_key" {
  description = "API key for the scraper API"
  type        = string
  sensitive   = true
}
