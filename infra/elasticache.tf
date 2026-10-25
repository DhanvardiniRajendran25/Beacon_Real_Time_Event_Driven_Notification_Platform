# Subnet group — tells ElastiCache which subnets it can live in
# Requires at least 2 subnets in different AZs
resource "aws_elasticache_subnet_group" "main" {
  name       = "${var.project_name}-redis-subnet-group"
  subnet_ids = [aws_subnet.public_1.id, aws_subnet.public_2.id]

  tags = {
    Project = var.project_name
  }
}

# Custom parameter group — required to enable slow log delivery
resource "aws_elasticache_parameter_group" "redis" {
  name   = "${var.project_name}-redis-params"
  family = "redis7"

  # Log commands that take longer than 10 ms (Redis default)
  parameter {
    name  = "slowlog-log-slower-than"
    value = "10000"
  }
}

# CloudWatch log group for Redis slow logs — 3-day retention (nearest valid value to 2 days)
resource "aws_cloudwatch_log_group" "redis_slow_log" {
  name              = "/elasticache/${var.project_name}/redis/slow-log"
  retention_in_days = 3

  tags = {
    Project = var.project_name
  }
}

# Redis cluster — single node, no replication needed for Week 1 testing
resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "${var.project_name}-redis"
  engine               = "redis"
  node_type            = var.redis_node_type # cache.t3.micro by default
  num_cache_nodes      = 1
  parameter_group_name = aws_elasticache_parameter_group.redis.name
  engine_version       = "7.0"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.main.name
  security_group_ids   = [aws_security_group.redis.id]

  log_delivery_configuration {
    destination      = aws_cloudwatch_log_group.redis_slow_log.name
    destination_type = "cloudwatch-logs"
    log_format       = "text"
    log_type         = "slow-log"
  }

  tags = {
    Name    = "${var.project_name}-redis"
    Project = var.project_name
  }
}