variable "aws_region" {
  description = "AWS region — Learner Lab is locked to us-east-1"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix for all resource names — change this to avoid conflicts if multiple teammates deploy simultaneously"
  type        = string
  default     = "notif-system"
}

variable "gateway_image" {
  description = "ECR image URI for the WebSocket gateway service"
  type        = string
  default     = ""
}

variable "ingestion_image" {
  description = "ECR image URI for the event ingestion API service"
  type        = string
  default     = ""
}

variable "fanout_image" {
  description = "ECR image URI for the fan-out worker service"
  type        = string
  default     = ""
}

variable "gateway_port" {
  description = "Port the WebSocket gateway listens on"
  type        = number
  default     = 8080
}

variable "ingestion_port" {
  description = "Port the ingestion API listens on"
  type        = number
  default     = 3000
}

variable "ecs_task_cpu" {
  description = "CPU units for each ECS task (1024 = 1 vCPU)"
  type        = number
  default     = 256
}

variable "ecs_task_memory" {
  description = "Memory in MB for each ECS task"
  type        = number
  default     = 512
}

variable "kafka_instance_type" {
  description = "MSK broker instance type — t3.small is the minimum for Learner Lab"
  type        = string
  default     = "kafka.t3.small"
}

variable "redis_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.t3.micro"
}

variable "db_password" {
  description = "PostgreSQL master password — override this when deploying"
  type        = string
  default     = "notif_password_123"
  sensitive   = true
}