# Printed after terraform apply — these are the endpoints you need for testing

output "alb_dns" {
  description = "ALB DNS name — use this to connect WebSocket clients to the gateway"
  value       = "ws://${aws_lb.gateway.dns_name}:8080/ws?user_id=YOUR_USER_ID"
}

output "ingestion_api_note" {
  description = "How to find the ingestion API endpoint"
  value       = "Find the ingestion ECS task public IP in the ECS console — http://<task-ip>:3000/event"
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint — used internally by gateway and fanout"
  value       = "${aws_elasticache_cluster.redis.cache_nodes[0].address}:6379"
}

output "kafka_bootstrap_brokers" {
  description = "MSK Kafka bootstrap brokers — used internally by ingestion and fanout"
  value       = aws_msk_cluster.main.bootstrap_brokers
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint — run init.sql against this after deploy"
  value       = aws_db_instance.postgres.address
}

output "ecr_gateway_url" {
  description = "ECR URL for gateway image — use this to push your Docker image"
  value       = aws_ecr_repository.gateway.repository_url
}

output "ecr_ingestion_url" {
  description = "ECR URL for ingestion image"
  value       = aws_ecr_repository.ingestion.repository_url
}

output "ecr_fanout_url" {
  description = "ECR URL for fanout image"
  value       = aws_ecr_repository.fanout.repository_url
}